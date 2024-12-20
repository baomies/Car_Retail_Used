from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Membuat SparkSession
spark = SparkSession.builder \
    .appName("Car Retail Validation") \
    .config("spark.jars", "/home/hadoop/postgresql-42.2.26.jar") \
    .config("spark.sql.debug.maxToStringFields", 1000) \
    .getOrCreate()

# Membaca tabel main_dataset
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://172.19.162.219:5432/car_retail") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "main_dataset") \
    .option("user", "mloonmare") \
    .option("password", "doman1") \
    .load()

null_counts = df.select([df[col].isNull().cast("int").alias(col) for col in df.columns])
null_sum = null_counts.agg(*[F.sum(null_counts[col]).alias(col) for col in null_counts.columns])
null_sum.show()

spark.stop()
