import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadPostgres") \
    .getOrCreate()

postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]

# Cấu hình JDBC
jdbc_url = postgres_db
properties = {
    "user": postgres_user,
    "password": postgres_pwd,
    "driver": "org.postgresql.Driver"
}

# Đọc dữ liệu từ PostgreSQL
movies_df = spark.read.jdbc(url=jdbc_url, table="movies", properties=properties)
ratings_df = spark.read.jdbc(url=jdbc_url, table="ratings", properties=properties)

# Xử lý dữ liệu
movies_df.show()
ratings_df.show()
