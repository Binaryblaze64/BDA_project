"""Demonstrate inference from saved preprocessing and random-forest artifacts."""
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel
spark=SparkSession.builder.appName('BDA-Saved-Model-Prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
pre=PipelineModel.load('hdfs://master:9000/models/nutrition_preprocessing')
model=RandomForestClassificationModel.load('hdfs://master:9000/models/random_forest')
df=spark.read.parquet('hdfs://master:9000/processed/food_clean')
features=['energy_kcal','fat','saturated_fat','sugars','fiber','proteins','salt']
observed=sum(F.col(c).isNotNull().cast('int') for c in features)
sample=df.filter(observed>=3).orderBy('code').limit(100)
pred=model.transform(pre.transform(sample)).withColumn('predicted_grade',F.element_at(F.array(*[F.lit(g) for g in 'abcde']),F.col('prediction').cast('int')+1))
pred.select('code','product_name','grade','predicted_grade').coalesce(1).write.mode('overwrite').option('header',True).csv('hdfs://master:9000/outputs/prediction_demo')
assert pred.count()==100
pred.select('code','grade','predicted_grade').show(10,truncate=False)
print('PASS: saved preprocessing and classifier reloaded; 100 demonstration predictions written')
spark.stop()
