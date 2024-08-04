# Dockerfile for Airflow with Spark
FROM python:3.12

ARG AIRFLOW_VERSION=2.9.3
ARG AIRFLOW_HOME=/opt/airflow
ARG SPARK_VERSION="3.5.1"
ARG HADOOP_VERSION="3"
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

USER root

RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        locales \
        iputils-ping \
        telnet \
        netcat-openbsd \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && pip install --upgrade pip \
    && pip install apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \    
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base \
        && python --version \
        && pip freeze

# Cài đặt Spark
ENV SPARK_HOME=/opt/spark

RUN apt-get update && \
    apt-get install -y wget unzip less procps && \
    wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Cài đặt OpenJDK-11
RUN apt-get update && \
    apt-get install -y wget apt-transport-https ca-certificates gnupg2 && \
    wget -O- https://apt.corretto.aws/corretto.key | apt-key add - && \
    echo "deb https://apt.corretto.aws stable main" | tee /etc/apt/sources.list.d/corretto.list && \
    apt-get update && \
    apt-get install -y java-1.8.0-amazon-corretto-jdk && \
    java -version && \
    javac -version

ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-amazon-corretto
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
