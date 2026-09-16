import random
import time

from .catalog import ProductCatalog
from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MAX_EVENT_DELAY,
    MIN_EVENT_DELAY,
    NUM_PRODUCTS,
    NUM_USERS,
)
from .event_generator import EcommerceEventGenerator
from .kafka_producer import EcommerceKafkaProducer


def main() -> None:

    catalog = ProductCatalog(num_products=NUM_PRODUCTS)
    generator = EcommerceEventGenerator(num_users=NUM_USERS, catalog=catalog)
    producer = EcommerceKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC)

    print("E-Commerce Kafka Producer")
    print("==========================")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print("Producing events...\n")

    try:
        while True:
            user_id = random.randint(1, NUM_USERS)
            events = generator.generate_session(user_id=user_id)

            for event in events:
                producer.send(event)
                print(f"[SENT] "f"{event.event_type:<16} "f"user={event.user_id:<5} "f"product={event.product_id}")

                time.sleep(
                    random.uniform(
                        MIN_EVENT_DELAY,
                        MAX_EVENT_DELAY,
                    )
                )

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.close()
        print("Producer closed.")


if __name__ == "__main__":
    main()