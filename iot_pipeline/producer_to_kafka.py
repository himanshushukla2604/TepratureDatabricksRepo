import os
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer


def generate_sensor_data():
    """Generate a random IoT sensor reading."""
    return {
        "device_id": f"sensor_{random.randint(100, 110)}",
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(-22, 80), 2),
        "humidity": round(random.uniform(-1, 101), 2),
        "battery": random.randint(-1, 102),
    }


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "iot-sensor")
    interval_seconds = float(os.getenv("INTERVAL_SECONDS", "5"))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        linger_ms=10,
        retries=5,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print(f"Producing to Kafka topic='{topic}' on brokers '{bootstrap_servers}' every {interval_seconds}s ...")

    try:
        while True:
            data = generate_sensor_data()
            key = data["device_id"]
            producer.send(topic, key=key, value=data)
            # Optionally flush occasionally; KafkaProducer batches by default
            producer.flush(timeout=5)
            print(f"Produced -> topic={topic} key={key} value={json.dumps(data)}")
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush(timeout=10)
        producer.close()


if __name__ == "__main__":
    main()