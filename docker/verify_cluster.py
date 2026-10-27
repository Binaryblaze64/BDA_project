from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
spark = SparkSession.builder.appName('BDA-HDFS-YARN-verification').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.sparkContext.master == 'yarn'
path='hdfs://master:9000/datasets/food/products_500mb_plus.tsv'
df=spark.read.option('header',True).option('sep','\t').csv(path)
assert 'nutriscore_grade' in df.columns
count=df.count()
assert count > 10000
print('DATASET_ROWS='+str(count),flush=True)
print('DATASET_COLUMNS='+str(len(df.columns)),flush=True)
df.createOrReplaceTempView('products')
spark.sql('SELECT nutriscore_grade, COUNT(*) AS products FROM products GROUP BY nutriscore_grade ORDER BY products DESC').show()
points=spark.createDataFrame([(0.0,0.1),(0.1,0.0),(9.0,9.1),(9.1,9.0)],['x','y'])
features=VectorAssembler(inputCols=['x','y'],outputCol='features').transform(points)
model=KMeans(k=2,seed=42).fit(features)
assert len(model.clusterCenters())==2
print('APPLICATION_ID='+spark.sparkContext.applicationId,flush=True)
print('PASS: HDFS dataset read, Spark SQL and MLlib on YARN',flush=True)
spark.stop()
