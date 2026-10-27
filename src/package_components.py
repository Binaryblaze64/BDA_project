"""Create the teammate component handoff without dataset, report or model binaries."""
import csv,hashlib,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
files=[ROOT/p for p in ['Live-Demo.ps1','Start-Dashboard.ps1','compose.yaml','SETUP.txt','requirements-report.txt','.gitattributes','src/pipeline.py','src/predict.py','src/export_errors.py','src/make_charts.py','src/package_components.py','logs/pipeline.log','logs/cluster-verification.log','logs/error-examples.log']]
for folder in ['docker','dashboard','tests','results','handoff']:
    files.extend(p for p in (ROOT/folder).rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.name not in ['COMPONENT_INDEX.csv','SHA256SUMS.txt'])
files=sorted(set(files))
with (ROOT/'handoff/COMPONENT_INDEX.csv').open('w',newline='',encoding='utf-8') as f:
    writer=csv.writer(f);writer.writerow(['path','bytes','type'])
    for p in files:writer.writerow([p.relative_to(ROOT).as_posix(),p.stat().st_size,p.suffix.lstrip('.')])
files.append(ROOT/'handoff/COMPONENT_INDEX.csv')
checks=[hashlib.sha256(p.read_bytes()).hexdigest()+'  '+p.relative_to(ROOT).as_posix() for p in files]
(ROOT/'handoff/SHA256SUMS.txt').write_text('\n'.join(checks)+'\n',encoding='utf-8')
files.append(ROOT/'handoff/SHA256SUMS.txt')
archive=ROOT/'BDA_Components_And_Live_Demo.zip'
with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
    for p in files:z.write(p,p.relative_to(ROOT).as_posix())
with zipfile.ZipFile(archive) as z:
    assert z.testzip() is None
    assert not any(n.endswith(('.pdf','.tsv')) for n in z.namelist())
    assert 'dashboard/server.py' in z.namelist()
print(f'Packaged and checked {len(files)} files: {archive}')
