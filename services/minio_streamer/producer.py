import os
import time
import json
import csv
import io
from minio import Minio
from kafka import KafkaProducer

# --- Konfigurasi ---
# Mengambil detail koneksi dari environment variables yang di-set di docker-compose
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password123")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

MINIO_BUCKET_NAME = "landing-zone"
KAFKA_TOPIC = "amazon-products"
POLL_INTERVAL_SECONDS = 10

# --- Inisialisasi Klien ---
# Inisialisasi klien MinIO
try:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # Set ke True jika MinIO Anda menggunakan HTTPS
    )
    print("✅ Klien MinIO berhasil diinisialisasi.")
except Exception as e:
    print(f"🔥 Gagal menginisialisasi klien MinIO: {e}")
    exit(1)

# Inisialisasi Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Mengubah pesan menjadi format JSON
    )
    print("✅ Kafka producer berhasil diinisialisasi.")
except Exception as e:
    print(f"🔥 Gagal menginisialisasi Kafka producer: {e}")
    exit(1)


def process_file_stream(bucket_name: str, object_name: str):
    """
    Membaca file dari MinIO sebagai stream (baris per baris) dan mengirim setiap baris ke Kafka.
    Metode ini sangat efisien dalam penggunaan memori.
    """
    response = None
    try:
        # Mengambil objek dari MinIO, yang mengembalikan sebuah stream
        response = minio_client.get_object(bucket_name, object_name)
        print(f"    - Memulai streaming file '{object_name}'...")
        
        # Stream dari MinIO adalah bytes, sedangkan modul CSV butuh teks.
        # Kita bungkus stream byte dengan TextIOWrapper untuk mengubahnya menjadi teks saat dibaca.
        text_stream = io.TextIOWrapper(response, encoding='utf-8')
        reader = csv.DictReader(text_stream)
        
        record_count = 0
        for row in reader:
            # Mengirim setiap baris sebagai pesan terpisah ke Kafka
            producer.send(KAFKA_TOPIC, row)
            record_count += 1
            if record_count % 1000 == 0:  # Memberi log progres setiap 1000 baris
                print(f"      ... sudah {record_count} baris terkirim...")
        
        producer.flush() # Memastikan semua pesan terkirim
        print(f"    ✅ Selesai streaming. Total {record_count} baris dari '{object_name}' telah dikirim ke topik '{KAFKA_TOPIC}'.")
    finally:
        # PENTING: Selalu tutup koneksi untuk melepaskan sumber daya jaringan
        if response:
            response.close()
            response.release_conn()


def poll_for_new_files(processed_files: set):
    """
    Mengecek apakah ada file baru di bucket MinIO.
    """
    print(f"\nMemeriksa file baru di bucket '{MINIO_BUCKET_NAME}'...")
    try:
        found = minio_client.bucket_exists(MINIO_BUCKET_NAME)
        if not found:
            print(f"Bucket '{MINIO_BUCKET_NAME}' tidak ditemukan. Mohon buat bucket tersebut di console MinIO.")
            return

        objects = minio_client.list_objects(MINIO_BUCKET_NAME)
        new_files_found = False
        
        for obj in objects:
            if obj.object_name.endswith('.csv') and obj.object_name not in processed_files:
                new_files_found = True
                print(f"  -> Ditemukan file baru: {obj.object_name}")
                process_file_stream(MINIO_BUCKET_NAME, obj.object_name)
                processed_files.add(obj.object_name)

        if not new_files_found:
            print("  -> Tidak ada file baru ditemukan.")

    except Exception as e:
        print(f"🔥 Terjadi error saat memeriksa file: {e}")

if __name__ == "__main__":
    print("--- MinIO to Kafka Streaming Producer (Mode File Besar) ---")
    processed_files_set = set()
    while True:
        poll_for_new_files(processed_files_set)
        time.sleep(POLL_INTERVAL_SECONDS)