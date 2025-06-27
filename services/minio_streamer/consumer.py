import os
import json
from kafka import KafkaConsumer

# --- Konfigurasi ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "amazon-products"

print("--- Kafka Stream Consumer ---")

try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        auto_offset_reset='earliest',  # Mulai membaca dari pesan paling awal di topik
        group_id='product-processor-group',  # ID grup konsumen
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Mengubah pesan JSON kembali menjadi objek Python
    )
    print(f"✅ Berhasil terhubung ke topik '{KAFKA_TOPIC}'. Menunggu pesan...")

    for message in consumer:
        # message.value adalah data yang sudah di-decode
        data = message.value
        print("\n=================================")
        print(f"📨 Pesan Diterima:")
        print(f"  - Topik: {message.topic}")
        print(f"  - Partisi: {message.partition}")
        print(f"  - Offset: {message.offset}")
        # Mencetak data produk dengan format yang lebih rapi
        print(f"  - Data Produk: {json.dumps(data, indent=2)}")
        print("=================================")

except Exception as e:
    print(f"🔥 Gagal terhubung ke Kafka atau memproses pesan: {e}")