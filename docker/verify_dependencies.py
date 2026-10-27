from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
spark = SparkSession.builder.appName('dependency-check').master('local[2]').getOrCreate()
rows = [(0.0, 0.1), (0.1, 0.0), (9.0, 9.1), (9.1, 9.0)]
df = spark.createDataFrame(rows, ['x', 'y'])
df.createOrReplaceTempView('points')
assert spark.sql('select count(*) as n from points').first().n == 4
features = VectorAssembler(inputCols=['x','y'], outputCol='features').transform(df)
model = KMeans(k=2, seed=42).fit(features)
assert len(model.clusterCenters()) == 2
print('PASS: Spark SQL, Python workers, and MLlib KMeans')
spark.stop()
