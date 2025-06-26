from confluent_kafka import Producer
from app.config import settings

producer = Producer({'bootstrap.servers': settings.kafka_bootstrap_servers})

def send_kafka_message(topic: str, message: str):
    producer.produce(topic, message.encode('utf-8'))
    producer.flush()