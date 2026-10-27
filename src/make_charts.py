"""Generate shareable figures from the exported Spark results; no model fitting here."""
import json, csv
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
root=Path('results'); charts=root/'charts'; charts.mkdir(exist_ok=True)
def read(n): return json.loads((root/(n+'.json')).read_text())
plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,'figure.dpi':160})
def finish(name):
    plt.tight_layout();plt.savefig(charts/(name+'.png'),bbox_inches='tight');plt.close()
for p in root.glob('*.json'):
    if p.stem.endswith('_evidence'): continue
    data=json.loads(p.read_text())
    if isinstance(data,list) and data and isinstance(data[0],dict):
        with p.with_suffix('.csv').open('w',newline='',encoding='utf-8') as f:
            w=csv.DictWriter(f,fieldnames=list(data[0]));w.writeheader();w.writerows(data)
s=read('summary')
fig,ax=plt.subplots(figsize=(8,4))
names=list(s['missing_after']); values=[100*s['missing_after'][k]/s['clean_rows'] for k in names]
ax.barh(names,values,color='#267d91');ax.set_xlabel('Missing values after cleaning (%)');ax.set_title('Nutrient completeness');finish('missingness')
for metric,label in [('sugars','Sugars (g/100 g)'),('salt','Salt (g/100 g)'),('saturated_fat','Saturated fat (g/100 g)')]:
    data=read('top10_mean_'+metric)[::-1]
    fig,ax=plt.subplots(figsize=(9,4.8));ax.barh([d['category'] for d in data],[d['mean_'+metric] for d in data],color='#267d91');ax.set_xlabel(label);ax.set_title('Top categories: at least 100 observed values');finish('top_'+metric)
data=read('grade_distribution');fig,ax=plt.subplots(figsize=(8,4));ax.bar([str(d['grade']) for d in data],[d['count'] for d in data],color='#267d91');ax.tick_params(axis='x',rotation=25);ax.set_ylabel('Products');ax.set_title('Grade availability after cleaning');finish('grades')
scores=s['validation_metrics'];fig,ax=plt.subplots(figsize=(8,4));names=list(scores);x=np.arange(len(names));ax.bar(x-.18,[scores[n]['accuracy'] for n in names],.36,label='Accuracy');ax.bar(x+.18,[scores[n]['f1'] for n in names],.36,label='Weighted F1');ax.set_xticks(x,names);ax.set_ylim(0,1);ax.axhline(.8,color='#bb5522',linestyle='--',label='Project target');ax.legend();ax.set_title('Validation comparison (not held-out test)');finish('validation')
cm=np.zeros((5,5),dtype=int)
for r in read('confusion_matrix'): cm[int(r['label']),int(r['prediction'])]=r['count']
fig,ax=plt.subplots(figsize=(6,5));im=ax.imshow(cm,cmap='Blues');ax.set_xticks(range(5),list('ABCDE'));ax.set_yticks(range(5),list('ABCDE'));ax.set_xlabel('Predicted grade');ax.set_ylabel('True grade');ax.set_title('Held-out test confusion matrix')
for i in range(5):
    for j in range(5): ax.text(j,i,f'{cm[i,j]:,}',ha='center',va='center',color='white' if cm[i,j]>cm.max()/2 else 'black',fontsize=8)
fig.colorbar(im,ax=ax);finish('confusion')
data=sorted(read('feature_importance'),key=lambda r:r['importance']);fig,ax=plt.subplots(figsize=(8,4));ax.barh([r['feature'] for r in data],[r['importance'] for r in data],color='#267d91');ax.set_title('Random forest feature importance');ax.set_xlabel('Impurity-based importance (not causal effect)');finish('importance')
sc=s['clustering']['silhouette_scores'];fig,ax=plt.subplots(figsize=(7,3.5));ax.plot(list(sc),list(sc.values()),'o-',color='#267d91');ax.set_xlabel('Number of clusters');ax.set_ylabel('Silhouette (squared Euclidean)');ax.set_title('K-means sample comparison');finish('silhouette')
data=read('cluster_profiles');names=['fat','saturated_fat','sugars','fiber','proteins','salt'];arr=np.array([[r[n] for n in names] for r in data]);fig,ax=plt.subplots(figsize=(8,4));im=ax.imshow(arr,cmap='YlGnBu',aspect='auto');ax.set_xticks(range(len(names)),names,rotation=25,ha='right');ax.set_yticks(range(len(data)),['Cluster '+str(r['prediction']) for r in data]);ax.set_title('Cluster means in original units (g/100 g)')
for i in range(len(data)):
    for j in range(len(names)): ax.text(j,i,f'{arr[i,j]:.1f}',ha='center',va='center',fontsize=8,color='white' if arr[i,j]>arr.max()/2 else 'black')
fig.colorbar(im,ax=ax);finish('clusters')
perclass=[]
for i,g in enumerate('ABCDE'):
    tp=int(cm[i,i]); support=int(cm[i].sum()); pp=int(cm[:,i].sum());precision=tp/pp if pp else 0;recall=tp/support if support else 0
    perclass.append({'grade':g,'support':support,'precision':precision,'recall':recall,'f1':2*precision*recall/(precision+recall) if precision+recall else 0})
(root/'per_class_metrics.json').write_text(json.dumps(perclass,indent=2))
print('Charts and CSV tables generated.')

from matplotlib.patches import FancyBboxPatch
fig,ax=plt.subplots(figsize=(9,3));ax.set_xlim(0,10);ax.set_ylim(0,4);ax.axis('off')
for x,y,w,h,text in [(0.1,1.2,2.1,1.4,'Local TSV\n571 MB'),(3.2,1.2,2.8,1.4,'Master\nNameNode / ResourceManager\nSpark driver'),(7.1,2.2,2.8,1.4,'Worker 1\nDataNode / NodeManager\nSpark executor'),(7.1,0.2,2.8,1.4,'Worker 2\nDataNode / NodeManager\nSpark executor')]:
 ax.add_patch(FancyBboxPatch((x,y),w,h,boxstyle='round,pad=0.07',facecolor='#e6f0f3',edgecolor='#267d91'))
 ax.text(x+w/2,y+h/2,text,ha='center',va='center',fontsize=10)
for a,b in [((2.25,1.9),(3.1,1.9)),((6.1,2.0),(7,2.9)),((6.1,1.7),(7,0.9))]:ax.annotate('',xy=b,xytext=a,arrowprops=dict(arrowstyle='->',color='#267d91',lw=2))
ax.set_title('One physical host / three Docker nodes / HDFS replication = 2')
finish('architecture')
