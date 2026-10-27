"""Reproducible food profiling and ML pipeline. Run with spark-submit --master yarn."""
import json
import time
from pathlib import Path
from functools import reduce
from pyspark.sql import SparkSession, functions as F, Window
from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, ClusteringEvaluator

spark = SparkSession.builder.appName('BDA-Food-Nutrition-Full-Pipeline').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.sparkContext.master == 'yarn', 'This project must run on YARN'
started = time.time()
out = Path('/data/results')
out.mkdir(exist_ok=True)
summary = {'application_id': spark.sparkContext.applicationId, 'spark_version': spark.version,
           'input': 'hdfs://master:9000/datasets/food/products_500mb_plus.tsv', 'seed': 42}
def save(name, obj):
    (out / (name + '.json')).write_text(json.dumps(obj, indent=2, allow_nan=False))
def records(df):
    return [r.asDict(recursive=True) for r in df.collect()]
def checkpoint(stage):
    summary['stage'] = stage
    summary['elapsed_seconds'] = round(time.time()-started, 1)
    save('summary', summary)
    print('STAGE: '+stage, flush=True)

raw = spark.read.option('header', True).option('sep', '\t').option('mode','PERMISSIVE').csv(summary['input'])
summary['raw_rows'] = raw.count()
features = ['energy_kcal','fat','saturated_fat','sugars','fiber','proteins','salt']
source = ['energy-kcal_100g','fat_100g','saturated-fat_100g','sugars_100g','fiber_100g','proteins_100g','salt_100g']
df = raw.select('code','product_name','countries_tags','pnns_groups_2','main_category',
                F.lower(F.trim('nutriscore_grade')).alias('grade'),
                *[F.col(s).cast('double').alias(n) for s,n in zip(source,features)])
missing_before = df.agg(*[F.sum(F.when(F.col(c).isNull() | F.isnan(c),1).otherwise(0)).alias(c) for c in features]).first().asDict()
invalid_exprs = [(F.col(c)<0)|(F.col(c)>(900 if c=='energy_kcal' else 100))|F.isnan(c) for c in features]
summary['invalid_values_by_feature'] = df.agg(*[F.sum(F.when(e,1).otherwise(0)).alias(c) for c,e in zip(features,invalid_exprs)]).first().asDict()
valid_code = df.filter(F.col('code').isNotNull() & (F.length(F.trim('code'))>0))
summary['missing_code_rows'] = summary['raw_rows']-valid_code.count()
# Prefer the most complete duplicate deterministically, then break ties by row content.
completeness = reduce(lambda a,b:a+b,[F.col(c).isNotNull().cast('int') for c in features])
order = Window.partitionBy('code').orderBy(completeness.desc(),F.sha2(F.to_json(F.struct(*df.columns)),256))
dedup = valid_code.withColumn('_rank',F.row_number().over(order)).filter('_rank=1').drop('_rank').persist(StorageLevel.DISK_ONLY)
summary['deduplicated_rows'] = dedup.count()
summary['duplicate_rows_removed'] = summary['raw_rows']-summary['missing_code_rows']-summary['deduplicated_rows']
invalid = reduce(lambda a,b:a|b,[F.coalesce(e,F.lit(False)) for e in invalid_exprs])
clean = dedup.filter(~invalid).persist(StorageLevel.DISK_ONLY)
summary['clean_rows'] = clean.count()
summary['invalid_rows_removed'] = summary['deduplicated_rows']-summary['clean_rows']
summary['missing_before'] = missing_before
summary['missing_after'] = clean.agg(*[F.sum(F.col(c).isNull().cast('int')).alias(c) for c in features]).first().asDict()
clean.write.mode('overwrite').parquet('hdfs://master:9000/processed/food_clean')
dedup.unpersist()
checkpoint('cleaning complete')

clean.createOrReplaceTempView('food')
category = spark.sql("""SELECT pnns_groups_2 AS category, count(*) AS products,
 count(sugars) AS sugar_observations, avg(sugars) AS mean_sugars,
 count(salt) AS salt_observations, avg(salt) AS mean_salt,
 count(saturated_fat) AS saturated_fat_observations, avg(saturated_fat) AS mean_saturated_fat,
 avg(energy_kcal) AS mean_energy_kcal FROM food
 WHERE pnns_groups_2 IS NOT NULL AND lower(pnns_groups_2) NOT IN ('unknown','')
 GROUP BY pnns_groups_2 HAVING count(*) >= 100""")
save('category_summary', records(category.orderBy(F.desc('products'))))
for col,obs in [('mean_sugars','sugar_observations'),('mean_salt','salt_observations'),('mean_saturated_fat','saturated_fat_observations')]:
    save('top10_'+col,records(category.filter(F.col(obs)>=100).orderBy(F.desc(col)).limit(10)))
countries=clean.withColumn('country',F.explode(F.array_distinct(F.split('countries_tags',',')))).withColumn('country',F.trim('country')).filter("country IS NOT NULL AND country <> '' AND country <> 'unknown'")
save('country_summary',records(countries.groupBy('country').agg(F.count('*').alias('products'),F.avg('sugars').alias('mean_sugars'),F.avg('salt').alias('mean_salt'),F.avg('saturated_fat').alias('mean_saturated_fat')).filter('products>=100').orderBy(F.desc('products')).limit(100)))
save('grade_distribution',records(clean.groupBy('grade').count().orderBy(F.desc('count'))))
checkpoint('SQL analysis complete')

# No Nutri-Score score, category, or grade-derived input enters the model.
known = clean.filter(F.col('grade').isin('a','b','c','d','e')).filter(reduce(lambda a,b:a+b,[F.col(c).isNotNull().cast('int') for c in features])>=3)
label = F.create_map(*[x for i,g in enumerate('abcde') for x in (F.lit(g),F.lit(float(i)))])
known=known.withColumn('label',label[F.col('grade')]).withColumn('_split',F.pmod(F.xxhash64('code',F.lit(42)),F.lit(100)))
test=known.filter('_split>=80').persist(StorageLevel.DISK_ONLY)
# Disjoint hash buckets: about 10% train, 10% validation, 20% held-out test.
# Remaining 60% excluded from model fitting to fit this laptop's resource budget.
train=known.filter('_split<10').persist(StorageLevel.DISK_ONLY)
validation=known.filter('_split>=10 AND _split<20').persist(StorageLevel.DISK_ONLY)
summary['model_rows']={'eligible':known.count(),'train':train.count(),'validation':validation.count(),'test':test.count()}
summary['model_class_counts']={name:records(d.groupBy('grade').count().orderBy('grade')) for name,d in [('train',train),('validation',validation),('test',test)]}
summary['training_policy']='Product-code hash: 0-9 training, 10-19 validation, 80-99 held-out test; at least 3 of 7 nutrients observed. Median imputation fitted on training only.'
imputed=[c+'_imputed' for c in features]
pre=Pipeline(stages=[Imputer(inputCols=features,outputCols=imputed,strategy='median'),VectorAssembler(inputCols=imputed,outputCol='raw_features'),StandardScaler(inputCol='raw_features',outputCol='features',withMean=True,withStd=True)]).fit(train)
tr=pre.transform(train).select('label','features').persist(StorageLevel.MEMORY_AND_DISK)
va=pre.transform(validation).select('label','features').persist(StorageLevel.MEMORY_AND_DISK)
te=pre.transform(test).select('label','features').persist(StorageLevel.MEMORY_AND_DISK)
evaluator=MulticlassClassificationEvaluator(labelCol='label',predictionCol='prediction')
def metrics(pred):
    return {m:evaluator.setMetricName(m).evaluate(pred) for m in ['accuracy','f1','weightedPrecision','weightedRecall']}
models={}
validation_scores={}
for name,est in [('logistic_regression',LogisticRegression(maxIter=35,regParam=0.05)),('random_forest',RandomForestClassifier(numTrees=40,maxDepth=12,maxBins=32,seed=42,maxMemoryInMB=128))]:
    checkpoint('training '+name)
    models[name]=est.fit(tr)
    predictions=models[name].transform(va).persist(StorageLevel.MEMORY_AND_DISK)
    validation_scores[name]=metrics(predictions)
    predictions.unpersist()
    save('validation_metrics',validation_scores)
best=max(validation_scores,key=lambda n:validation_scores[n]['f1'])
pred=models[best].transform(te).persist(StorageLevel.MEMORY_AND_DISK)
summary['selected_model']=best
summary['validation_metrics']=validation_scores
summary['test_metrics']=metrics(pred)
summary['target_080_met']=summary['test_metrics']['accuracy']>0.8 and summary['test_metrics']['f1']>0.8
majority=train.groupBy('label').count().orderBy(F.desc('count')).first()['label']
summary['majority_baseline_test']=metrics(te.withColumn('prediction',F.lit(majority)))
save('confusion_matrix',records(pred.groupBy('label','prediction').count().orderBy('label','prediction')))
save('feature_importance',[{'feature':f,'importance':float(v)} for f,v in zip(features,models['random_forest'].featureImportances)])
pre.write().overwrite().save('hdfs://master:9000/models/nutrition_preprocessing')
for name,model in models.items(): model.write().overwrite().save('hdfs://master:9000/models/'+name)
pred.unpersist(); tr.unpersist(); va.unpersist(); te.unpersist(); train.unpersist();validation.unpersist();test.unpersist()
checkpoint('classification complete')

# Complete-case, deterministic sample; scaling is fitted independently for clustering.
cluster_data=clean.dropna(subset=features).filter(F.pmod(F.xxhash64('code',F.lit(123)),F.lit(100))<2)
summary['clustering_sample_rows']=cluster_data.count()
cp=Pipeline(stages=[VectorAssembler(inputCols=features,outputCol='raw_features'),StandardScaler(inputCol='raw_features',outputCol='features',withMean=True,withStd=True)]).fit(cluster_data)
cd=cp.transform(cluster_data).persist(StorageLevel.MEMORY_AND_DISK)
cluster_models={}; scores={}
for k in [3,4,5]:
    checkpoint('clustering k='+str(k))
    m=KMeans(k=k,seed=42,maxIter=30).fit(cd)
    scores[str(k)]=ClusteringEvaluator().evaluate(m.transform(cd))
    cluster_models[k]=m
bestk=int(max(scores,key=scores.get)); cm=cluster_models[bestk]
summary['clustering']={'selected_k':bestk,'silhouette_scores':scores,'distance':'squaredEuclidean','sample_policy':'2% hash sample of complete cases; standardized nutrient features'}
save('cluster_profiles',records(cm.transform(cd).groupBy('prediction').agg(F.count('*').alias('products'),*[F.avg(c).alias(c) for c in features]).orderBy('prediction')))
cp.write().overwrite().save('hdfs://master:9000/models/clustering_preprocessing')
cm.write().overwrite().save('hdfs://master:9000/models/kmeans')
cd.unpersist();clean.unpersist()
checkpoint('COMPLETE')
print(json.dumps(summary,indent=2),flush=True)
spark.stop()
