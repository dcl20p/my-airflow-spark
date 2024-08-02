# Dockerfile for Airflow with Spark
FROM apache/airflow:2.9.3-python3.8


ARG AIRFLOW_HOME=/opt/airflow
ARG SPARK_VERSION="3.5.1"
ARG HADOOP_VERSION="3"

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

USER root

# Cài đặt Spark
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/home/jdk-11.0.2

RUN apt-get update && \
    apt-get install -y wget unzip less procps && \
    wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN DOWNLOAD_URL="https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/openjdk-11.0.2_linux-x64_bin.tar.gz" \
    && mkdir -p "${JAVA_HOME}" \
    && tar xzf "${TMP_DIR}/openjdk-11.0.2_linux-x64_bin.tar.gz" -C "${JAVA_HOME}" --strip-components=1 \
    && rm -rf "${TMP_DIR}" \
    && java --version

RUN export SPARK_HOME
RUN export JAVA_HOME
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

# Sao chép script entrypoint vào container
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793

USER airflow
COPY ./requirements.txt /
RUN pip install -r /requirements.txt

WORKDIR ${AIRFLOW_HOME}
# Sử dụng entrypoint script
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
