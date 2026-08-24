FROM apache/airflow:2.10.2-python3.10

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV SPARK_HOME=/home/airflow/.local/lib/python3.10/site-packages/pyspark
ENV PATH=${SPARK_HOME}/bin:${PATH}
