import json
from kafka import KafkaProducer
from .models import Event


class EcommerceKafkaProducer:

    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=self._serialize
        )

    @staticmethod
    def _serialize(event: Event) -> bytes:
        event_dict = {
            "event_id": event.event_id,
            "event_type": event.event_type,

            "user_id": event.user_id,
            "session_id": event.session_id,

            "product_id": event.product_id,
            "category": event.category,

            "quantity": event.quantity,
            "price": event.price,

            "search_query": event.search_query,

            "timestamp": event.timestamp,
        }

        return json.dumps(event_dict).encode("utf-8")


    def send(self, event: Event) -> None:
        self.producer.send(self.topic, key=str(event.user_id).encode("utf-8"), value=event)


    def flush(self) -> None:
        self.producer.flush()


    def close(self) -> None:
        self.producer.flush()
        self.producer.close()