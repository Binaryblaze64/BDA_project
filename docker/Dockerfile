FROM spark:3.5.9-python3 AS spark
FROM apache/hadoop:3.4.3
USER root
COPY --from=spark /opt/spark /opt/spark
ENV HADOOP_HOME=/opt/hadoop SPARK_HOME=/opt/spark HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop PYSPARK_PYTHON=/usr/bin/python3
ENV PATH=/opt/spark/bin:/opt/hadoop/bin:/opt/hadoop/sbin:${PATH}
RUN python3 -m pip install --no-cache-dir numpy==1.26.4
WORKDIR /workspace
ENTRYPOINT ["/bin/bash"]
