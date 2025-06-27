from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --- Konfigurasi S3 (MinIO) ---
S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password123"

print("Memulai konfigurasi Spark Session...")

# Membangun Spark Session
# Perhatikan bahwa baris .config("spark.jars.packages", ...) sudah dihapus
spark = SparkSession.builder \
    .appName("BatchETLForProducts") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session untuk Batch ETL berhasil dibuat.")

# --- Baca, Transformasi, dan Simpan Data (Logika sama seperti sebelumnya) ---
try:
    products_csv_path = "/opt/bitnami/spark/data/amazon_products.csv"
    categories_csv_path = "/opt/bitnami/spark/data/amazon_categories.csv"

    products_df = spark.read.csv(products_csv_path, header=True, inferSchema=True)
    categories_df = spark.read.csv(categories_csv_path, header=True, inferSchema=True)
    print("✅ Berhasil membaca file products dan categories CSV.")

    products_cleaned_df = products_df.select(
        col("asin").alias("product_id"), "title", col("price").cast("double"),
        col("stars").cast("double"), col("reviews").cast("integer"), "category_id",
        col("isBestSeller").cast("boolean"), col("boughtInLastMonth").cast("integer")
    )
    categories_renamed_df = categories_df.withColumnRenamed("id", "cat_id").withColumnRenamed("category_name", "category")
    final_products_df = products_cleaned_df.join(
        categories_renamed_df, products_cleaned_df.category_id == categories_renamed_df.cat_id, "inner"
    ).drop("cat_id", "category_id")
    print("✅ Berhasil menggabungkan data produk dan kategori.")

    gold_path = "s3a://datalake/gold/products_dimension"
    final_products_df.write.format("delta").mode("overwrite").save(gold_path)
    print(f"✅ Berhasil menulis tabel dimensi produk ke Gold Layer di: {gold_path}")

except Exception as e:
    print(f"🔥 Terjadi error selama proses ETL: {e}")

finally:
    print("Menghentikan Spark Session.")
    spark.stop()

