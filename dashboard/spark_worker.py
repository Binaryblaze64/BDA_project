"""Persistent local Spark inference process; models were trained on YARN."""
import json, sys
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.clustering import KMeansModel
from pyspark.sql.types import StructType,StructField,DoubleType

FEATURES=['energy_kcal','fat','saturated_fat','sugars','fiber','proteins','salt']
spark=SparkSession.builder.appName('BDA-Interactive-Saved-Model-Inference').config('spark.ui.enabled','false').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
pre=PipelineModel.load('hdfs://master:9000/models/nutrition_preprocessing')
rf=RandomForestClassificationModel.load('hdfs://master:9000/models/random_forest')
cluster_pre=PipelineModel.load('hdfs://master:9000/models/clustering_preprocessing')
km=KMeansModel.load('hdfs://master:9000/models/kmeans')
schema=StructType([StructField(c,DoubleType(),False) for c in FEATURES])
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == '/shutdown':
            self.send_response(200);self.end_headers();self.wfile.write(b'{}')
            threading.Thread(target=httpd.shutdown,daemon=True).start();return
        try:
            req=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            df=spark.createDataFrame([tuple(float(req[c]) for c in FEATURES)],schema)
            row=rf.transform(pre.transform(df)).select('prediction','probability').first()
            cluster=km.transform(cluster_pre.transform(df)).select('prediction').first()[0]
            result={'grade':'ABCDE'[int(row.prediction)],'probabilities':dict(zip('ABCDE',[float(v) for v in row.probability])),'cluster':int(cluster)}
            self.send_response(200)
        except Exception as exc:
            result={'error':str(exc)};self.send_response(500)
        self.send_header('Content-Type','application/json');self.end_headers()
        self.wfile.write(json.dumps(result,allow_nan=False).encode())
httpd=HTTPServer(('127.0.0.1',8790),Handler)
print('BDA_READY',flush=True)
try:httpd.serve_forever()
finally:httpd.server_close();spark.stop()
