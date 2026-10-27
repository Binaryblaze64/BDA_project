import urllib.request, hashlib, concurrent.futures
from pathlib import Path
root=Path('downloads')
def fetch(item):
 name,url=item
 target=root/name
 print('Downloading '+name,flush=True)
 urllib.request.urlretrieve(url,str(target)+'.partial')
 expected=urllib.request.urlopen(url+'.sha512',timeout=60).read().decode()
 digest=hashlib.file_digest(open(str(target)+'.partial','rb'),'sha512').hexdigest()
 if digest.lower() not in expected.lower().replace(' ','').replace('\n','').replace('\r',''): raise RuntimeError('Checksum mismatch '+name)
 Path(str(target)+'.partial').replace(target)
 (root/(name+'.sha512')).write_text(expected)
 print('Verified '+name,flush=True)
items=[('hadoop-3.4.3.tar.gz','https://downloads.apache.org/hadoop/common/hadoop-3.4.3/hadoop-3.4.3.tar.gz'),('spark-3.5.9-bin-hadoop3.tgz','https://downloads.apache.org/spark/spark-3.5.9/spark-3.5.9-bin-hadoop3.tgz')]
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
 for result in pool.map(fetch,items): pass
