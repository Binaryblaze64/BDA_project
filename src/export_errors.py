"""Reload saved models on YARN and export illustrative held-out classification errors."""
import json
from pathlib import Path
from functools import reduce
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel

spark=SparkSession.builder.appName('BDA-Heldout-Error-Examples').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
pre=PipelineModel.load('hdfs://master:9000/models/nutrition_preprocessing')
model=RandomForestClassificationModel.load('hdfs://master:9000/models/random_forest')
features=['energy_kcal','fat','saturated_fat','sugars','fiber','proteins','salt']
df=spark.read.parquet('hdfs://master:9000/processed/food_clean')
df=df.filter(F.col('grade').isin(*list('abcde'))).filter(reduce(lambda a,b:a+b,[F.col(c).isNotNull().cast('int') for c in features])>=3)
df=df.filter(F.pmod(F.xxhash64('code',F.lit(42)),F.lit(100))>=80)
pred=model.transform(pre.transform(df)).withColumn('predicted_grade',F.element_at(F.array(*[F.lit(g) for g in 'abcde']),F.col('prediction').cast('int')+1))
errors=pred.filter(F.col('grade')!=F.col('predicted_grade'))
rows=[]
for grade in 'abcde':
    for r in errors.filter(F.col('grade')==grade).orderBy('code').select('code','product_name','grade','predicted_grade','probability',*features).limit(8).collect():
        item=r.asDict();item['model_score']=float(max(item.pop('probability')));rows.append(item)
Path('/data/results/error_examples.json').write_text(json.dumps({'application_id':spark.sparkContext.applicationId,'selection':'Up to eight errors per true grade, ordered by product code, from the original held-out partition; illustrative, not a random sample.','examples':rows},indent=2,allow_nan=False))
assert len(rows)>0
print('PASS: saved models reloaded; exported '+str(len(rows))+' held-out error examples',flush=True)
spark.stop()
