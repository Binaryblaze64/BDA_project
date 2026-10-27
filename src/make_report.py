"""Build the lab report from measured Spark outputs, using bundled ReportLab."""
import json
from pathlib import Path
from xml.sax.saxutils import escape
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle, Image
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.utils import ImageReader
root=Path('results'); dest=Path('output/pdf');dest.mkdir(parents=True,exist_ok=True)
def read(n):return json.loads((root/(n+'.json')).read_text())
s=read('summary');styles=getSampleStyleSheet()
styles.add(ParagraphStyle(name='Cover',fontName='Helvetica-Bold',fontSize=25,leading=31,textColor=colors.HexColor('#154c65'),spaceAfter=22))
styles['Normal'].fontSize=10;styles['Normal'].leading=15;styles['Normal'].spaceAfter=10
styles['Heading1'].fontSize=19;styles['Heading1'].leading=24;styles['Heading1'].textColor=colors.HexColor('#154c65')
story=[]
def p(t):story.append(Paragraph(t,styles['Normal']))
def title(n,t):
    if story:story.append(PageBreak())
    story.append(Paragraph(f'{n:02d} / {t}',styles['Heading1']));story.append(Spacer(1,12))
def table(headers,rows,widths=None):
    data=[[Paragraph(escape(str(v)),styles['Normal']) for v in row] for row in [headers]+rows]
    t=Table(data,colWidths=widths,repeatRows=1,hAlign='LEFT');t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#e6f0f3')),('VALIGN',(0,0),(-1,-1),'TOP'),('LINEBELOW',(0,0),(-1,0),.8,colors.HexColor('#267d91')),('BOTTOMPADDING',(0,0),(-1,-1),5),('TOPPADDING',(0,0),(-1,-1),5)]));story.append(t);story.append(Spacer(1,12))
def pic(path,width=475,maxheight=300):
    path=Path(path)
    if not path.exists():return
    w,h=ImageReader(str(path)).getSize();scale=min(width/w,maxheight/h)
    story.append(Image(str(path),width=w*scale,height=h*scale));story.append(Spacer(1,10))
def chart(name,**kw):pic(root/'charts'/(name+'.png'),**kw)
def pct(v):return f'{100*v:.2f}%'

story.append(Spacer(1,38));story.append(Paragraph('Nutritional Profiling and Nutri-Score Prediction',styles['Cover']))
p('Using Spark SQL and MLlib on a Multi-Node Hadoop Cluster')
p('<b>Big Data Analytics (DSE 3125) | Semester V | DSE C</b>')
p('Department of Data Science and Engineering')
table(['Team member','Roll number','Register number'],[['Soham Sahu',38,'240968416'],['Aakarsh Agrawal',39,'240968420'],['Atulya Ishan',56,'240968520']],[220,110,145])
p('Execution date: 27 October 2026. Report generated from saved pipeline outputs; metrics are measured rather than target values.')
p('<b>Scope:</b> distributed ingestion, quality checks, category and country analysis, two classifiers, and K-means clustering. The source dataset was supplied locally and was not downloaded by this pipeline.')
p('Application: '+s['application_id'])

title(2,'Abstract and objectives')
p(f'The pipeline processed {s["raw_rows"]:,} food-product records from a 570,666,925-byte TSV file using Spark on YARN. The cleaned dataset contains {s["clean_rows"]:,} products. HDFS stores two replicas across two DataNodes, and two NodeManagers supply distributed executor resources.')
p(f'Classification compares multinomial logistic regression and a random forest using seven nutritional variables. The selected model, {s["selected_model"].replace("_"," ")}, achieved held-out accuracy {pct(s["test_metrics"]["accuracy"])} and weighted F1 {s["test_metrics"]["f1"]:.4f}. The stated objective of exceeding 0.80 on both metrics was '+('met.' if s['target_080_met'] else 'not met; the measured result is retained without changing the test set.'))
p('The work answers three questions: which food categories have the highest average sugar, salt and saturated fat; how accurately observed grades can be predicted from nutrient profiles; and whether standardized profiles form separable clusters.')
p('Missing nutrients and incomplete labels limit coverage. The model approximates labels in this extract; it is not an official Nutri-Score calculator or dietary recommendation system.')
table(['Objective','Evidence'],[['Multi-node Hadoop and YARN','One master, two workers; dashboard screenshots'],['Spark SQL and MLlib','Category aggregations, classifiers, K-means'],['Reproducible evaluation','Product-code hash partitions and saved models'],['Interpretation','Feature importance, confusion matrix, silhouette and cluster means']],[190,285])

title(3,'Dataset and provenance')
table(['Property','Observed or documented value'],[['Local file','products_500mb_plus.tsv'],['File size','570,666,925 bytes (544.2 MiB)'],['Rows / source columns',f'{s["raw_rows"]:,} / 22'],['Source described in synopsis','Open Food Facts products database'],['Format','Tab-separated text; header row; fields initially read as strings'],['HDFS path','/datasets/food/products_500mb_plus.tsv'],['Database license in synopsis','Open Database License (ODbL); Database Contents License (DbCL) for individual contents']],[160,315])
p('The original synopsis described a wider database. This supplied extract has 22 columns, not the 211 attributes mentioned there. Export date and extraction/filtering procedure were not provided, so representativeness of the global database cannot be independently established.')
p('Selected columns include product code and name, country tags, PNNS food group, grade, and nutrient values per 100 g. Product codes are retained as strings to preserve leading zeros.')
p('Country tags indicate product availability/association and may contain multiple countries. They do not measure consumer intake, production origin, or country-level population health. A product can contribute to several country groups.')

title(4,'Architecture and configuration')
table(['Node','Services','Resources'],[['master','NameNode, ResourceManager, History Server and client driver','Container limit 1800 MiB'],['worker1','DataNode and NodeManager','2800 MiB container; YARN 2304 MiB / 2 vcores'],['worker2','DataNode and NodeManager','2800 MiB container; YARN 2304 MiB / 2 vcores']],[85,250,140])
p('Data flow: local TSV -> HDFS blocks on worker1 and worker2 -> YARN executor tasks -> cleaned Parquet, saved models and results. The client driver runs in the master container and coordinates two executors.')
p('Software: Hadoop 3.4.3; Spark 3.5.9; Java 11; Python 3.10; NumPy 1.26.4. The Docker engine has approximately 7.4 GiB RAM available on the Windows host.')
p('This is a multi-container cluster on one physical computer. It demonstrates distributed service execution but does not establish speedup over a separate multi-machine deployment or resilience to loss of the host.')
p('HDFS uses replication factor 2 and persistent Docker volumes. The master formats its NameNode only when no existing VERSION file is present. Web interfaces bind to localhost.')
chart('architecture',maxheight=140)
pic(root/'screenshots'/'hdfs.png',maxheight=160)

title(5,'Data quality and cleaning')
table(['Step','Rows'],[['Input',f'{s["raw_rows"]:,}'],['Missing/blank product codes removed',s['missing_code_rows']],['Duplicate product codes removed',s['duplicate_rows_removed']],['Invalid nutrient rows removed',f'{s["invalid_rows_removed"]:,}'],['Retained',f'{s["clean_rows"]:,}']],[320,155])
p('Numeric casts convert unparseable text to null. NaN and out-of-range observed values invalidate a row. Nutrients measured in grams must be within 0 to 100 g per 100 g; energy must be within 0 to 900 kcal per 100 g. The energy bound is an operational plausibility filter and is separate from the gram-based rule.')
p('Duplicates are resolved by preferring the record with the most non-null model features, followed by a deterministic content hash. This avoids arbitrary selection but does not guarantee that the selected row is the most recent or authoritative record.')
p('Rows with missing nutrients remain in general analysis. SQL averages use available values; supervised models require at least three observed inputs and use training-fitted medians. K-means uses complete cases only.')
p('Output: hdfs://master:9000/processed/food_clean. Cleaning counts reconcile exactly: input minus missing codes, duplicates and invalid rows equals the retained total.')

title(6,'Missingness and label coverage')
chart('missingness',maxheight=265);chart('grades',maxheight=240)
p(f'Fiber is missing in {pct(s["missing_after"]["fiber"]/s["clean_rows"])} of cleaned products. Missingness can reflect reporting practices rather than low nutrient content; missing values are not treated as zero.')
p('Unknown, not-applicable and null grades are excluded from supervised learning, while remaining available for descriptive analysis. Labelled, sufficiently complete products may differ systematically from excluded products.')

title(7,'Sugar concentrations by category')
chart('top_sugars',maxheight=330)
top=read('top10_mean_sugars')[0]
p(f'The highest observed category mean is {escape(top["category"])}: {top["mean_sugars"]:.2f} g sugar per 100 g, based on {top["sugar_observations"]:,} non-null sugar observations.')
p('The SQL groups by PNNS group 2, excludes empty and unknown group labels, and requires at least 100 products per group. Each ranking additionally requires at least 100 observations of the ranked nutrient.')
p('These are unweighted product averages, not sales-weighted exposure estimates. Different product mix and reporting completeness can alter rankings. A category average does not describe every product within it.')

title(8,'Salt, saturated fat and geography')
chart('top_salt',maxheight=220);chart('top_saturated_fat',maxheight=220)
countries=read('country_summary')[:3]
p('The largest country groups by represented product count are '+', '.join(f'{escape(r["country"])} ({r["products"]:,})' for r in countries)+'. Country means and counts are supplied in country_summary.csv.')
p('Multi-country tags are deduplicated within a product before exploding. Country counts are therefore not additive to the dataset total. Results describe this extract and should not be interpreted as national dietary comparisons.')

title(9,'Classification protocol')
table(['Partition','Rows','Hash buckets'],[['Training',f'{s["model_rows"]["train"]:,}','0-9'],['Validation',f'{s["model_rows"]["validation"]:,}','10-19'],['Held-out test',f'{s["model_rows"]["test"]:,}','80-99']],[145,130,200])
p(f'There are {s["model_rows"]["eligible"]:,} eligible products with grade A-E and at least three observed nutrients. A seeded xxhash64 of product code creates stable partitions. Buckets 20-79 are unused for modelling to bound laptop compute; all clean rows still contribute to descriptive analysis.')
p('Inputs: energy in kcal, fat, saturated fat, sugars, fiber, proteins and salt. The provided numeric Nutri-Score score and the target grade are excluded from features. Median imputation and standardization are fitted only on training rows, then applied unchanged to validation and test.')
p('Two candidates are compared using validation weighted F1. Only the selected candidate is evaluated on the held-out test set. Product-code separation prevents exact-code overlap but similar products from the same brand may still span partitions; this is not a brand-disjoint or time-based evaluation.')
table(['Model','Settings'],[['Logistic regression','35 iterations; regularization 0.05; multiclass'],['Random forest','40 trees; maximum depth 12; 32 bins; seed 42']],[160,315])

title(10,'Model comparison and held-out results')
chart('validation',maxheight=260)
table(['Held-out metric','Selected model','Majority baseline'],[[k,f'{s["test_metrics"][k]:.4f}',f'{s["majority_baseline_test"][k]:.4f}'] for k in ['accuracy','f1','weightedPrecision','weightedRecall']],[185,145,145])
p('Selection: '+s['selected_model'].replace('_',' ')+'. The baseline always predicts the most frequent training grade. Accuracy is the fraction of correct predictions; weighted F1 averages class F1 values using class support.')
p('Project target (>0.80 for both accuracy and weighted F1): '+('achieved on this test partition.' if s['target_080_met'] else 'not achieved. This is a valid experimental outcome, not evidence to alter labels or evaluate on training data.'))

title(11,'Errors and feature importance')
chart('confusion',maxheight=260);chart('importance',maxheight=230)
p('Rows of the confusion matrix are true grades and columns are predicted grades. Feature importance is from the random forest, even if another candidate is selected. Impurity-based importance is model-specific, can favor some predictors, and is not a causal or official Nutri-Score weight.')

title(12,'K-means methodology and findings')
p(f'Clustering uses {s["clustering_sample_rows"]:,} complete-case products from a deterministic 2% hash sample. Nutrient features are standardized so that energy does not dominate solely because its numerical units are larger.')
chart('silhouette',maxheight=220);chart('clusters',maxheight=245)
p(f'The selected k is {s["clustering"]["selected_k"]}, chosen by maximum sample silhouette among k=3,4,5. Evaluation uses squared Euclidean distance. These sample clusters are exploratory nutrient profiles, not validated health classes or an independent generalization test.')

profiles=read('cluster_profiles')
p('Cluster sizes: '+', '.join(f"{r['prediction']}: {r['products']:,}" for r in profiles)+'. Profiles describe sample means; cluster IDs have no ordinal meaning.')

title(13,'Execution evidence and reproducibility')
p('YARN application: '+s['application_id']+'. The saved evidence includes the final application status and executor hosts. Logs and Spark event history are retained separately from the report.')
pic(root/'screenshots'/'yarn.png',maxheight=215);pic(root/'screenshots'/'spark.png',maxheight=215)
p('Run from the project folder: <font name="Courier">docker compose up -d</font>, then <font name="Courier">docker compose exec -T master bash /cluster/run-pipeline.sh</font>. The input is read from HDFS; successful reruns overwrite the project output/model paths.')
p('Model artifacts: /models/nutrition_preprocessing, /models/logistic_regression, /models/random_forest, /models/clustering_preprocessing and /models/kmeans in HDFS. JSON/CSV outputs, figures and execution evidence are in the local results folder.')

title(14,'Limitations and conclusions')
p(f'The pipeline completes distributed data preparation and analysis over {s["raw_rows"]:,} records and retains {s["clean_rows"]:,}. The selected classifier records test accuracy {pct(s["test_metrics"]["accuracy"])} and weighted F1 {s["test_metrics"]["f1"]:.4f}; the selected cluster count is {s["clustering"]["selected_k"]}.')
p('Limitations: the extract lacks a documented export date; nutrients and grades have substantial missingness; model fitting uses a subset; the split is not temporal or brand-disjoint; no confidence intervals or repeated-seed assessment are supplied. Seven nutrient inputs do not encode every category-specific rule or ingredient quantity relevant to official grading.')
p('Category and country averages are descriptive and observational. K-means is sensitive to scaling, outliers and missing-data selection, and its cluster count is chosen on the same exploratory sample. This work does not claim causal effects, consumer-level dietary outcomes, or official grade certification.')
p('Future work should first verify provenance and Nutri-Score version, add relevant ingredient/category inputs without label leakage, compare brand-disjoint evaluation, examine per-class failures and missingness patterns, and repeat validation under several seeds. Additional tuning should use validation data, with a new untouched holdout if the existing test informs design.')
p('The infrastructure remains running for demonstration. Use docker compose stop to release resources while preserving volumes; do not remove volumes unless intentionally discarding HDFS data.')

title(15,'References and demonstration guide')
p('Project sources: BDA_Mini_Project_Synopsis.pdf and Mini_Project_Lab_Guidelines.pdf in the supplied folder. Dataset attribution: Open Food Facts, https://world.openfoodfacts.org/data. License details should accompany any redistribution of the source extract.')
p('Software references: Apache Spark 3.5.9 overview, https://spark.apache.org/docs/3.5.9/ ; Apache Hadoop releases, https://hadoop.apache.org/releases.html . The exact implementation and hyperparameters are recorded in src/pipeline.py and docker/run-pipeline.sh.')
table(['Demo step','What to show'],[['1. HDFS dashboard','http://localhost:9870; two live DataNodes and stored dataset'],['2. YARN dashboard','http://localhost:8088; completed application status'],['3. Spark History','http://localhost:18080; tasks on both worker hosts'],['4. Quality results','summary.json counts and missing-value tables'],['5. Models','Validation comparison, test confusion matrix, feature importance'],['6. Clustering','Silhouette selection and original-unit profiles']],[155,320])
p('Viva preparation: explain why replicas are not extra independent data, why train-fitted imputation avoids leakage, why weighted F1 differs from accuracy, why country counts overlap, why clustering requires scaling, and why the reported accuracy may differ from the synopsis target.')
p('All reported numeric results are loaded from saved pipeline outputs. Review team details and institutional formatting before submission. This report documents the run; a live viva still requires each team member to understand and demonstrate the code.')

def footer(canvas,doc):
    canvas.setStrokeColor(colors.HexColor('#267d91'));canvas.line(48,43,547,43)
    canvas.setFont('Helvetica',8);canvas.setFillColor(colors.HexColor('#52636a'));canvas.drawString(48,30,'BDA | Nutritional profiling | 27 October 2026');canvas.drawRightString(547,30,str(doc.page))
SimpleDocTemplate(str(dest/'BDA_Project_Report.pdf'),pagesize=(595.28,841.89),rightMargin=48,leftMargin=48,topMargin=48,bottomMargin=58,title='Nutritional Profiling and Nutri-Score Prediction').build(story,onFirstPage=footer,onLaterPages=footer)
print('Report generated.')
