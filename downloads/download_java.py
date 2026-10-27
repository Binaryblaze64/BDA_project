import urllib.request,json,hashlib
from pathlib import Path
root=Path('downloads')
url='https://api.adoptium.net/v3/assets/latest/11/hotspot?architecture=x64&image_type=jdk&os=linux'
asset=json.load(urllib.request.urlopen(url,timeout=60))[0]['binary']['package']
name=asset['name']; target=root/name
print('Downloading '+name,flush=True)
(root/'java-download.json').write_text(json.dumps(asset,indent=2))
urllib.request.urlretrieve(asset['link'],str(target)+'.partial')
with open(str(target)+'.partial','rb') as f: digest=hashlib.file_digest(f,'sha256').hexdigest()
if digest!=asset['checksum']: raise RuntimeError('Java checksum mismatch')
Path(str(target)+'.partial').replace(target)
print('Verified '+name,flush=True)
