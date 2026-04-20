FROM apache/spark:3.5.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL -o /opt/spark/jars/postgresql.jar \
      https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
    && chmod 0644 /opt/spark/jars/postgresql.jar

COPY docker/spark-java-job/run-job.sh /opt/spark/scripts/run-job.sh
RUN chmod +x /opt/spark/scripts/run-job.sh

USER spark

ENTRYPOINT ["/opt/spark/scripts/run-job.sh"]
