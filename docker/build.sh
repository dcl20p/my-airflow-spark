#!/usr/bin/bash

# Load environment variables from .env file
export $(grep -v '^#' .env | xargs)

# Build Docker image with versions from environment variables
docker build -t docker-airflow-spark:${AIRFLOW_VERSION}_${SPARK_VERSION} .

exit 0