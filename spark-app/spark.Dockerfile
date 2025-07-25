FROM bitnami/spark:3.3

USER root
RUN install_packages curl

ENV SPARK_EXTRA_CLASSPATH="/opt/bitnami/spark/jars"

RUN curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.3.4.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.4/spark-sql-kafka-0-10_2.12-3.3.4.jar && \
    curl -L -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.3.4.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.4/spark-token-provider-kafka-0-10_2.12-3.3.4.jar && \
    curl -L -o /opt/bitnami/spark/jars/kafka-clients-3.5.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar