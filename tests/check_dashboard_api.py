"""Integration checks against a running dashboard and the real saved Spark models."""
import json, math, urllib.request, urllib.error
from pathlib import Path
BASE='http://localhost:8765'
def request(path,body=None,origin=None):
    headers={}
    if body is not None:headers['Content-Type']='application/json'
    if origin:headers['Origin']=origin
    req=urllib.request.Request(BASE+path,data=None if body is None else json.dumps(body).encode(),headers=headers)
    with urllib.request.urlopen(req,timeout=180) as r:return r.status,json.load(r)
status,data=request('/api/data')
assert status==200 and len(data['error_examples']['examples'])==40
assert data['summary']['test_metrics']['accuracy']>.8
a=dict(energy_kcal=250,fat=8,saturated_fat=2,sugars=5,fiber=6,proteins=10,salt=.6)
b=dict(energy_kcal=430,fat=18,saturated_fat=7,sugars=32,fiber=3,proteins=6,salt=.8)
_,single=request('/api/predict',{'products':[a]})
_,pair=request('/api/predict',{'products':[a,b]})
assert single['predictions'][0]==pair['predictions'][0]
assert len(pair['predictions'])==2
for p in pair['predictions']:
    assert p['grade'] in 'ABCDE' and p['cluster'] in range(4)
    assert math.isclose(sum(p['probabilities'].values()),1,abs_tol=1e-9)
    assert all(0<=v<=1 for v in p['probabilities'].values())
for payload,origin,expected in [({'products':[dict(a,fat=-1)]},None,400),({'products':[dict(a,saturated_fat=20)]},None,400),({'products':[a]},'https://untrusted.example',403)]:
    try:request('/api/predict',payload,origin);raise AssertionError('Expected rejection')
    except urllib.error.HTTPError as exc:assert exc.code==expected
_,health=request('/api/status');assert health['model_ready']
checks={'data_endpoint':'PASS','saved_models':'loaded and predictions returned','single_and_pair_consistency':'PASS','class_score_sums':'PASS','invalid_inputs':'rejected with HTTP 400','cross_origin_requests':'rejected with HTTP 403','error_examples':40,'ui_visual_check':'Blocked: local browser use disabled in Work settings','example_predictions':pair['predictions']}
Path('results/dashboard_checks.json').write_text(json.dumps(checks,indent=2))
print(json.dumps(checks,indent=2))
