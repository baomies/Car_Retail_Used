from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, when, avg, round, sum, regexp_replace
from pyspark.sql.functions import year, current_date, datediff

# -------------------------------------------------------------------------------------
# Membuat SparkSession
spark = SparkSession.builder \
    .appName("Car Retail Case") \
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


# -------------------------------------------------------------------------------------
# 1. Menghitung Jumlah dari setiap bahan bakar mobil
# Memastikan kolom FuelType tidak null atau kosong
df_fueltype = df.filter(col("FuelType").isNotNull())
df_fueltype = df.filter(col("FuelType") != "")

# Menghitung jumlah masing-masing FuelType
fueltype_count = df_fueltype.groupBy("FuelType").agg(count("FuelType").alias("Count"))

# Mengurutkan berdasarkan jumlah terbanyak
fueltype_count = fueltype_count.orderBy(col("Count").desc())

# Menyimpan final_df ke tabel PostgreSQL
fueltype_count.write.format("jdbc") \
    .option("url", "jdbc:postgresql://172.19.162.219:5432/car_retail") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "fueltype_value") \
    .option("user", "mloonmare") \
    .option("password", "doman1") \
    .mode("overwrite") \
    .save()

# -------------------------------------------------------------------------------------
# 2. Klasifikasi Brand berdasarakan frekuensi kemunculan di data set
# Total jumlah data dalam dataset
total_rows = df.count()

# Menghitung frekuensi kemunculan setiap brand
brand_frequency = df.groupBy("Brand").agg(count("Brand").alias("Frequency"))

# Menambahkan kolom persentase kemunculan
brand_frequency = brand_frequency.withColumn(
    "Percentage",
    round((col("Frequency") / total_rows) * 100, 2)
)

# Menambahkan klasifikasi berdasarkan persentase kemunculan
classified_brands = brand_frequency.withColumn(
    "Classification",
    when(col("Percentage") > 10, "High Frequency")
    .when((col("Percentage") <= 10) & (col("Percentage") >= 5), "Medium Frequency")
    .otherwise("Low Frequency")
)

# Menambahkan kolom untuk pengurutan berdasarkan level klasifikasi
classified_brands = classified_brands.withColumn(
    "SortOrder",
    when(col("Classification") == "High Frequency", 1)
    .when(col("Classification") == "Medium Frequency", 2)
    .otherwise(3)
)

# Mengurutkan berdasarkan SortOrder dan Frequency secara descending
sorted_brands = classified_brands.orderBy(col("SortOrder"), col("Frequency").desc())

# Menampilkan hasil klasifikasi yang sudah diurutkan
sorted_brands.select("Brand", "Frequency", "Percentage", "Classification").show()

# Menyimpan hasil ke tabel PostgreSQL
sorted_brands.write.format("jdbc") \
    .option("url", "jdbc:postgresql://172.19.162.219:5432/car_retail") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "classified_brands") \
    .option("user", "mloonmare") \
    .option("password", "doman1") \
    .mode("overwrite") \
    .save()

# -------------------------------------------------------------------------------------
# 3. Distribusi Tahun Produksi Mobil
# Membuat klasifikasi range tahun produksi
df_year_distribution = df.withColumn(
    "YearRange",
    when(col("Year") >= 2020, "2020+")
    .when((col("Year") >= 2015) & (col("Year") < 2020), "2015-2019")
    .when((col("Year") >= 2010) & (col("Year") < 2015), "2010-2014")
    .otherwise("Before 2010")
)

# Menghitung jumlah mobil dalam setiap range tahun
year_distribution = df_year_distribution.groupBy("YearRange") \
    .agg(count("Year").alias("Count"))

# Menyimpan hasil ke PostgreSQL
year_distribution.write.format("jdbc") \
    .option("url", "jdbc:postgresql://172.19.162.219:5432/car_retail") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "year_distribution") \
    .option("user", "mloonmare") \
    .option("password", "doman1") \
    .mode("overwrite") \
    .save()

# -------------------------------------------------------------------------------------
# 4. Kilometer Rata-Rata Berdasarkan Bahan Bakar yang dipakai
# Membersihkan kolom kmDriven (menghapus 'km' dan koma)
df_clean_km = df.withColumn("kmDriven", regexp_replace(col("kmDriven"), "[^0-9]", "").cast("int"))

# Menghitung kilometer rata-rata berdasarkan FuelType
avg_km_per_fueltype = df_clean_km.filter(col("kmDriven").isNotNull()) \
    .groupBy("FuelType") \
    .agg(round(avg("kmDriven"), 2).alias("AvgKmDriven"))

# Menyimpan hasil ke PostgreSQL
avg_km_per_fueltype.write.format("jdbc") \
    .option("url", "jdbc:postgresql://172.19.162.219:5432/car_retail") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "avg_km_per_fueltype") \
    .option("user", "mloonmare") \
    .option("password", "doman1") \
    .mode("overwrite") \
    .save()

# -------------------------------------------------------------------------------------

# 5. Analisis Pemilik Mobil Berdasarkan Usia Kendaraan

# Menghitung usia kendaraan
current_year = lit(year(current_date()))
df_with_age = df.withColumn("VehicleAge", current_year - col("Year"))

# Mengelompokkan data berdasarkan kategori pemilik dan usia kendaraan
owner_age_distribution = df_with_age.groupBy("Owner", "VehicleAge") \
    .agg(count("VehicleAge").alias("Count")) \
    .orderBy("Owner", "VehicleAge")

# Menyimpan hasil ke PostgreSQL
owner_age_distribution.write.format("jdbc") \
    .option("url", "jdbc:postgresql://172.19.162.219:5432/car_retail") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "owner_age_distribution") \
    .option("user", "mloonmare") \
    .option("password", "doman1") \
    .mode("overwrite") \
    .save()

# -------------------------------------------------------------------------------------
# 6. Distribusi Harga Berdasarkan Jenis Transmisi

# Membersihkan kolom harga
cleaned_price_df = df.withColumn("IDR_Price", regexp_replace(col("IDR_Price"), "[^0-9]", "").cast("int"))

# Menghitung rata-rata harga berdasarkan jenis transmisi
price_distribution_by_transmission = cleaned_price_df.filter(col("IDR_Price").isNotNull()) \
    .groupBy("Transmission") \
    .agg(round(avg("IDR_Price"), 2).alias("AvgPrice")) \
    .orderBy("AvgPrice", ascending=False)

# Menyimpan hasil ke PostgreSQL
price_distribution_by_transmission.write.format("jdbc") \
    .option("url", "jdbc:postgresql://172.19.162.219:5432/car_retail") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "price_distribution_by_transmission") \
    .option("user", "mloonmare") \
    .option("password", "doman1") \
    .mode("overwrite") \
    .save()



# Menutup Spark session
spark.stop()