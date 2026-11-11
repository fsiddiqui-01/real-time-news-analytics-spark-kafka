
import json
import os

from kafka import KafkaProducer

# Address of the Kafka broker
KAFKA_SERVER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

# Connect Python to Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda message: json.dumps(
        message
    ).encode("utf-8")
)

# Example news message
message = {
    "id": "test-001",
    "text": "Microsoft announced a new AI research center in Seattle.",
    "source": "manual_test"
}

# Send the message to topic1
future = producer.send("topic1", value=message)

# Wait for Kafka to acknowledge it
metadata = future.get(timeout=15)

producer.flush()
producer.close()

print("Message sent successfully!")
print("Topic:", metadata.topic)
print("Partition:", metadata.partition)
print("Offset:", metadata.offset)
