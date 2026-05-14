FROM apache/airflow:2.9.1

RUN pip install --no-cache-dir \
    confluent-kafka==2.4.0 \
    "confluent-kafka[avro,schemaregistry]==2.4.0" \
    requests==2.31.0 \
    apache-airflow-providers-docker==3.10.0

COPY --chmod=755 entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
