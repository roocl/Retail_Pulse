FROM apache/hive:4.1.0@sha256:b957bb0e698e560d24e162b7549d79f191d58703e92899a886618c317f6e8f30 AS hive
USER root
ADD --checksum=sha256:7e9941bbdcca244d878ea95bfff788fd9ba6a65af757f24be6c632930d61c7ed https://repo.maven.apache.org/maven2/com/mysql/mysql-connector-j/9.2.0/mysql-connector-j-9.2.0.jar /opt/hive/lib/mysql-connector.jar
RUN chmod 644 /opt/hive/lib/mysql-connector.jar && mkdir -p /warehouse && chown 1000:1000 /warehouse
COPY conf/hive-site.xml /opt/hive/conf/hive-site.xml
USER 1000:1000
ENV HADOOP_HEAPSIZE=512
ENTRYPOINT ["/opt/hive/bin/hive", "--service", "metastore"]

FROM apache/spark:4.1.0-scala2.13-java17-ubuntu@sha256:ce12b4f3df67a023107e1fea22346a996f34fd5350027ad35f6a17d7593cff62 AS spark
USER root
COPY --from=hive /opt/hive/lib /opt/hive-client/lib
COPY --from=hive /opt/hadoop/share/hadoop/common /opt/hive-client/hadoop/common
COPY --from=hive /opt/hadoop/share/hadoop/mapreduce /opt/hive-client/hadoop/mapreduce
COPY --from=hive /opt/hadoop/share/hadoop/hdfs /opt/hive-client/hadoop/hdfs
RUN useradd --uid 1000 --create-home offline && mkdir -p /warehouse /data && chown 1000:1000 /warehouse /data
COPY target/customer-analytics-0.1.0-SNAPSHOT.jar /app/application.jar
COPY target/customer-analytics-0.1.0-SNAPSHOT-tests.jar /app/tests.jar
COPY target/lib /app/lib
COPY target/test-lib/junit-platform-console-standalone-1.10.5.jar /app/test-console.jar
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY conf/run.sh /app/run.sh
WORKDIR /app
USER offline
ENTRYPOINT ["sh", "/app/run.sh"]
