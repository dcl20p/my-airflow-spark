import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
postgres_driver_jar = "/opt/spark/resources/jars/postgresql-42.7.3.jar"
conn_id = "my_spark_connection"

movies_file = "/opt/spark/resources/data/movies.csv"
ratings_file = "/opt/spark/resources/data/ratings.csv"
postgres_db = "jdbc:postgresql://postgres:5432/airflow"
postgres_user = "airflow"
postgres_pwd = "airflow"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="spark-postgres",
    description="This DAG is a sample of integration between Spark and DB. It reads CSV files, loads them into a Postgres DB, and then reads them from the same Postgres DB.",
    default_args=default_args,
    schedule_interval=timedelta(1)
)

start = DummyOperator(task_id="start", dag=dag)

spark_job_load_postgres = SparkSubmitOperator(
    task_id="spark_job_load_postgres",
    application="/opt/spark/app/load-postgres.py",
    name="load-postgres",
    conn_id=conn_id,
    verbose=1,
    conf={
        "spark.master": spark_master,
        "spark.jars": postgres_driver_jar,
        "spark.driver.extraClassPath": postgres_driver_jar,
        "spark.executor.extraClassPath": postgres_driver_jar
    },
    application_args=[movies_file, ratings_file, postgres_db, postgres_user, postgres_pwd],
    dag=dag
)

spark_job_read_postgres = SparkSubmitOperator(
    task_id="spark_job_read_postgres",
    application="/opt/spark/app/read-postgres.py",
    name="read-postgres",
    conn_id=conn_id,
    verbose=1,
    conf={
        "spark.master": spark_master,
        "spark.jars": postgres_driver_jar,
        "spark.driver.extraClassPath": postgres_driver_jar,
        "spark.executor.extraClassPath": postgres_driver_jar
    },
    application_args=[postgres_db, postgres_user, postgres_pwd],
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job_load_postgres >> spark_job_read_postgres >> end
