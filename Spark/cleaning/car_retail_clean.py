from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, when, regexp_replace, trim

# Membuat SparkSession
spark = SparkSession.builder \
    .appName("Currency_Conversion") \
    .config("spark.jars", "/home/hadoop/postgresql-42.2.26.jar") \
    .config("spark.sql.debug.maxToStringFields", 1000) \
    .getOrCreate()

# Membaca tabel data_cleaned
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://172.19.162.219:5432/car_retail") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "data_cleaned") \
    .option("user", "mloonmare") \
    .option("password", "doman1") \
    .load()

# Bersihkan data AskPrice (hapus karakter non-numerik)
cleaning_currency = df.withColumn(
    "AskPrice",
    trim(regexp_replace(col("AskPrice"), "[^0-9.]", ""))
)

# Mengubah nama column
cleaning_currency = cleaning_currency.withColumnRenamed("AskPrice", "IDR_Price")

# Kurs Rupee ke Rupiah (contoh kurs: 1 INR = 191 IDR)
conversion_rate = 191

# Membersihkan data kolom IDR_Price
# Mengganti nilai non-numerik atau NULL dengan 0 sebelum konversi
cleaning_currency = cleaning_currency.withColumn(
    "IDR_Price",
    when(
        col("IDR_Price").cast("float").isNotNull(), 
        col("IDR_Price").cast("float")
    ).otherwise(0)
)

# Konversi nilai IDR_Price dari INR ke IDR dan ubah menjadi number
cleaning_currency = cleaning_currency.withColumn(
    "IDR_Price", 
    round(col("IDR_Price") * conversion_rate, 2)
)

# Verifikasi hasil cleaning
# cleaning_currency.select("IDR_Price").show(20, truncate=False)

#Menghapus Hapus kolom 'AdditionalInfo'
cleaning_currency = cleaning_currency.drop("AdditionInfo")

# Menyimpan hasil ke PostgreSQL
cleaning_currency.write.format("jdbc") \
    .option("url", "jdbc:postgresql://172.19.162.219:5432/car_retail") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "main_dataset") \
    .option("user", "mloonmare") \
    .option("password", "doman1") \
    .mode("overwrite") \
    .save()

# Menutup Spark session
spark.stop()
