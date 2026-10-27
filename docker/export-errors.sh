#!/bin/bash
set -euo pipefail
hdfs dfsadmin -safemode wait
spark-submit --master yarn --deploy-mode client --driver-memory 512m --num-executors 2 --executor-cores 1 --executor-memory 768m --conf spark.executor.memoryOverhead=384 --conf spark.yarn.am.memory=512m --conf spark.yarn.am.memoryOverhead=256 --conf spark.driver.host=master --conf spark.driver.bindAddress=0.0.0.0 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://master:9000/spark-events --conf spark.sql.shuffle.partitions=12 /workspace/src/export_errors.py
