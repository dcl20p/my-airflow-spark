import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LoadPostgres") \
    .getOrCreate()

movies_file = sys.argv[1]
ratings_file = sys.argv[2]
postgres_db = sys.argv[3]
postgres_user = sys.argv[4]
postgres_pwd = sys.argv[5]

# Đọc dữ liệu từ CSV
movies_df = spark.read.csv(movies_file, header=True, inferSchema=True)
ratings_df = spark.read.csv(ratings_file, header=True, inferSchema=True)

# Cấu hình JDBC
jdbc_url = postgres_db
properties = {
    "user": postgres_user,
    "password": postgres_pwd,
    "driver": "org.postgresql.Driver"
}

# Ghi dữ liệu vào PostgreSQL
movies_df.write.jdbc(url=jdbc_url, table="movies", mode="overwrite", properties=properties)
ratings_df.write.jdbc(url=jdbc_url, table="ratings", mode="overwrite", properties=properties)
