#!/bin/bash
set -euo pipefail
cp /cluster-conf/*.xml /opt/hadoop/etc/hadoop/
export HADOOP_HEAPSIZE=256
mkdir -p /data/tmp /data/yarn/local /data/yarn/logs
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true; wait || true' TERM INT EXIT
if [ "$1" = master ]; then
  if [ ! -f /data/namenode/current/VERSION ]; then
    hdfs namenode -format -nonInteractive
  fi
  hdfs namenode & pids+=($!)
  yarn resourcemanager & pids+=($!)
  (
    until hdfs dfs -mkdir -p /spark-events 2>/dev/null; do sleep 3; done
    export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=hdfs://master:9000/spark-events -Dspark.history.ui.port=18080"
    export SPARK_DAEMON_MEMORY=256m
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer
  ) & pids+=($!)
else
  hdfs datanode & pids+=($!)
  yarn nodemanager & pids+=($!)
fi
wait -n "${pids[@]}"
exit 1
