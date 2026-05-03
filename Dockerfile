FROM apache/airflow:2.9.1-python3.10

USER root

RUN apt-get update && \
    apt-get install -y default-jre curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/default-java

RUN mkdir -p /opt/airflow/jars && \
    curl -L https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -o /opt/airflow/jars/postgresql-42.7.3.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar -o /opt/airflow/jars/delta-spark_2.12-3.1.0.jar && \
    chmod -R a+r /opt/airflow/jars

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
