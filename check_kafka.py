import json
from kafka import KafkaConsumer

KAFKA_TOPIC = "dbserver1.public.orders"
BOOTSTRAP_SERVERS = "127.0.0.1:9092"

def check_messages():
    print(f"🔍 Connecting to Kafka at {BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("📥 Reading messages from Kafka topic (press Ctrl+C to exit):")
    count = 0
    for message in consumer:
        count += 1
        print(f"\n--- Message #{count} ---")
        print(json.dumps(message.value, indent=2))
        if count >= 10:  
            break

if __name__ == "__main__":
    check_messages()