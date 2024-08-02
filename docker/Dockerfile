# Dockerfile for Airflow with Spark
FROM apache/airflow:2.9.3-python3.8


ARG AIRFLOW_HOME=/opt/airflow
ARG SPARK_VERSION="3.5.1"
ARG HADOOP_VERSION="3"

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

USER root

# Cài đặt Spark
ENV SPARK_HOME=/opt/spark

RUN apt-get update && \
    apt-get install -y wget unzip less && \
    wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN export SPARK_HOME
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Sao chép script entrypoint vào container
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

USER airflow
COPY ./requirements.txt /requirements.txt
RUN cat requirements.txt

WORKDIR ${AIRFLOW_HOME}
# Sử dụng entrypoint script
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
