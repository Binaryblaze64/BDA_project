"""Local-only dashboard, standard library HTTP server and persistent Spark inference."""
import atexit, json, math, queue, subprocess, threading, time
from pathlib import Path
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

ROOT=Path(__file__).resolve().parents[1]
FEATURES=['energy_kcal','fat','saturated_fat','sugars','fiber','proteins','salt']
DATA=['summary','category_summary','country_summary','grade_distribution','feature_importance','confusion_matrix','per_class_metrics','cluster_profiles','error_examples','yarn_evidence','executor_evidence']
def validate_products(body):
    if not isinstance(body,dict) or not isinstance(body.get('products'),list) or not 1<=len(body['products'])<=2:
        raise ValueError('Provide one or two products.')
    products=[]
    for product in body['products']:
        if not isinstance(product,dict): raise ValueError('Each product must be an object.')
        row={}
        for key in FEATURES:
            value=product.get(key)
            if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value):
                raise ValueError(f'{key}: enter a finite number for every nutrient.')
            upper=900 if key=='energy_kcal' else 100
            if not 0<=value<=upper: raise ValueError(f'{key}: use a value between 0 and {upper}.')
            row[key]=float(value)
        if row['saturated_fat']>row['fat']:raise ValueError('Saturated fat cannot exceed total fat.')
        if sum(row[k] for k in ['fat','sugars','fiber','proteins','salt'])>100.5:
            raise ValueError('These gram-based nutrients exceed 100 g in total. Check the per-100-g values.')
        products.append(row)
    return products

class ModelWorker:
    def __init__(self):
        self.process=None;self.messages=queue.Queue();self.lock=threading.Lock();self.ready=False;self.log=None
    def stop(self):
        self.ready=False
        if self.process:
            try:
                self.rpc('/shutdown',{},timeout=5)
                self.process.stdin.close()
            except (OSError,ValueError,RuntimeError,subprocess.TimeoutExpired):pass
            try:self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:self.process.terminate()
            self.process=None
        if self.log:self.log.close();self.log=None
    def _read(self,stream,q):
        for line in stream:
            if line.startswith('BDA_'):q.put(line.strip())
        q.put('BDA_EXIT')
    def _receive(self,timeout):
        try:msg=self.messages.get(timeout=timeout)
        except queue.Empty:
            self.stop();raise RuntimeError('Spark timed out. Check Docker and logs/dashboard-spark.log, then retry.')
        if msg=='BDA_EXIT':
            self.stop();raise RuntimeError('Spark could not load or run the saved models. Check Docker, HDFS and logs/dashboard-spark.log.')
        return msg
    def start(self):
        if self.ready and self.process and self.process.poll() is None:return
        self.stop();self.messages=queue.Queue()
        (ROOT/'logs').mkdir(exist_ok=True)
        self.log=(ROOT/'logs/dashboard-spark.log').open('a',encoding='utf-8')
        cmd=['docker','compose','exec','-T','master','spark-submit','--master','local[1]','--driver-memory','512m','--conf','spark.ui.enabled=false','--conf','spark.sql.shuffle.partitions=2','/workspace/dashboard/spark_worker.py']
        try:self.process=subprocess.Popen(cmd,cwd=ROOT,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.log,text=True,encoding='utf-8',bufsize=1)
        except OSError as exc:self.stop();raise RuntimeError('Docker CLI could not start. Start Docker Desktop first.') from exc
        threading.Thread(target=self._read,args=(self.process.stdout,self.messages),daemon=True).start()
        if self._receive(120)!='BDA_READY':self.stop();raise RuntimeError('Unexpected Spark startup response.')
        self.ready=True
    def rpc(self,path,body,timeout=90):
        code="import urllib.request,sys; r=urllib.request.Request('http://127.0.0.1:8790'+sys.argv[1],data=sys.argv[2].encode(),headers={'Content-Type':'application/json'}); print(urllib.request.urlopen(r,timeout=80).read().decode())"
        cmd=['docker','compose','exec','-T','master','python3','-c',code,path,json.dumps(body)]
        try:response=subprocess.run(cmd,cwd=ROOT,capture_output=True,text=True,timeout=timeout)
        except subprocess.TimeoutExpired as exc:raise RuntimeError('Prediction timed out. Check the Spark log and retry.') from exc
        if response.returncode:raise RuntimeError('Saved-model service is unavailable. Check logs/dashboard-spark.log.')
        return json.loads(response.stdout)
    def predict(self,products):
        if not self.lock.acquire(timeout=1):raise RuntimeError('Another prediction is running. Please retry shortly.')
        try:
            self.start();results=[]
            for row in products:
                result=self.rpc('/predict',row)
                if 'error' in result:raise RuntimeError('Model inference failed; check the Spark log.')
                results.append(result)
            return results
        finally:self.lock.release()

worker=ModelWorker();atexit.register(worker.stop)
class Handler(BaseHTTPRequestHandler):
    def send(self,status,data,kind='application/json'):
        if kind=='application/json':data=json.dumps(data,allow_nan=False).encode()
        self.send_response(status);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(data)));self.send_header('Cache-Control','no-store');self.send_header('X-Content-Type-Options','nosniff');self.end_headers();self.wfile.write(data)
    def do_GET(self):
        path=urlparse(self.path).path
        if path=='/api/data':
            result={}
            for n in DATA:
                p=ROOT/'results'/(n+'.json')
                if p.exists():result[n]=json.loads(p.read_text(encoding='utf-8-sig'))
            return self.send(200,result)
        if path=='/api/status':return self.send(200,{'model_ready':worker.ready,'mode':'Saved-model inference in local Spark; original training on YARN'})
        allowed={'/':'index.html','/index.html':'index.html','/app.js':'app.js','/style.css':'style.css'}
        if path in allowed:
            p=ROOT/'dashboard'/allowed[path];kind={'html':'text/html; charset=utf-8','js':'text/javascript; charset=utf-8','css':'text/css; charset=utf-8'}[p.suffix[1:]]
            return self.send(200,p.read_bytes(),kind)
        return self.send(404,{'error':'Not found'})
    def do_POST(self):
        if urlparse(self.path).path!='/api/predict':return self.send(404,{'error':'Not found'})
        origin=self.headers.get('Origin')
        if origin and origin not in ['http://localhost:8765','http://127.0.0.1:8765']:return self.send(403,{'error':'Origin not allowed'})
        if self.headers.get('Content-Type','').split(';')[0]!='application/json':return self.send(415,{'error':'Use application/json'})
        try:
            length=int(self.headers.get('Content-Length','0'))
            if not 0<length<=8192:raise ValueError('Invalid request size.')
            products=validate_products(json.loads(self.rfile.read(length)))
        except (ValueError,UnicodeDecodeError) as exc:return self.send(400,{'error':str(exc)})
        try:return self.send(200,{'predictions':worker.predict(products),'notice':'Model estimates, not official grades. Scores are uncalibrated model outputs.'})
        except (RuntimeError,OSError) as exc:return self.send(503,{'error':str(exc)})

if __name__=='__main__':
    print('Dashboard: http://localhost:8765 | Ctrl+C to stop',flush=True)
    server=ThreadingHTTPServer(('127.0.0.1',8765),Handler)
    try:server.serve_forever()
    except KeyboardInterrupt:pass
    finally:server.server_close();worker.stop()
