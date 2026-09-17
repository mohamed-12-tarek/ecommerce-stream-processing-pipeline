from kafka import KafkaConsumer
import json


consumer = KafkaConsumer(
    "ecommerce-events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=False,
    group_id="test-python-consumer",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print("Listening for events...")

for message in consumer:
    print(
        f"event_type={message.value.get('event_type')} "
        f"user_id={message.value.get('user_id')} "
        f"timestamp={message.value.get('timestamp')}"
    )
