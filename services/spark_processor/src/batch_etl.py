from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# ====================================================================
# 1. Konfigurasi dan Inisialisasi Spark Session (Sama seperti sebelumnya)
# ====================================================================
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password123"

spark = SparkSession.builder \
    .appName("BatchETLForProducts") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Spark Session untuk Batch ETL berhasil dibuat.")

# ====================================================================
# 2. Baca Data dari CSV
# ====================================================================

# Path ke file CSV di dalam kontainer Spark (akan kita map nanti)
products_csv_path = "/opt/bitnami/spark/data/amazon_products.csv"
categories_csv_path = "/opt/bitnami/spark/data/amazon_categories.csv"

# Membaca data dengan beberapa opsi penting
# - header=True: Baris pertama adalah nama kolom
# - inferSchema=True: Spark akan mencoba menebak tipe data setiap kolom
products_df = spark.read.csv(products_csv_path, header=True, inferSchema=True)
categories_df = spark.read.csv(categories_csv_path, header=True, inferSchema=True)

print("Berhasil membaca file products dan categories CSV.")
products_df.printSchema()
categories_df.printSchema()

# ====================================================================
# 3. Transformasi dan Pembersihan Data
# ====================================================================

# Memilih hanya kolom yang kita butuhkan
products_cleaned_df = products_df.select(
    col("asin").alias("product_id"),
    "title",
    "price",
    "stars",
    "reviews",
    "category_id",
    "isBestSeller",
    "boughtInLastMonth"
)

# Mengganti nama kolom di tabel kategori agar lebih jelas saat di-join
categories_renamed_df = categories_df.withColumnRenamed("id", "cat_id") \
                                     .withColumnRenamed("category_name", "category")

# ====================================================================
# 4. Gabungkan (Join) Data Produk dan Kategori
# ====================================================================

# Join kedua DataFrame berdasarkan category_id
# Ini akan menambahkan kolom 'category' ke setiap produk
final_products_df = products_cleaned_df.join(
    categories_renamed_df,
    products_cleaned_df.category_id == categories_renamed_df.cat_id,
    "inner"  # 'inner' join berarti hanya produk yang punya kategori yang akan masuk
).drop("cat_id", "category_id") # Hapus kolom ID duplikat

print("Berhasil menggabungkan data produk dan kategori.")
final_products_df.show(5)

# ====================================================================
# 5. Tulis Data ke Gold Layer di MinIO
# ====================================================================

gold_path = "s3a://datalake/gold/products_dimension"

# .mode("overwrite") akan menimpa data lama jika skrip ini dijalankan lagi.
# Ini cocok untuk tabel dimensi yang ingin kita refresh sepenuhnya.
final_products_df.write.format("delta").mode("overwrite").save(gold_path)

print(f"Berhasil menulis tabel dimensi produk ke Gold Layer di: {gold_path}")

spark.stop()