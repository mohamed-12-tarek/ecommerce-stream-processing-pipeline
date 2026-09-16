import json
import random
import time
from dataclasses import asdict

from producer.config import (
    MAX_EVENT_DELAY,
    MIN_EVENT_DELAY,
    NUM_PRODUCTS,
    NUM_USERS,
)
from producer.catalog import ProductCatalog
from producer.event_generator import EcommerceEventGenerator


def main() -> None:

    catalog = ProductCatalog(
        num_products=NUM_PRODUCTS
    )

    generator = EcommerceEventGenerator(
        num_users=NUM_USERS,
        catalog=catalog,
    )

    print("E-Commerce Event Generator")
    print("===========================")
    print(f"Users: {NUM_USERS:,}")
    print(f"Products: {NUM_PRODUCTS:,}")
    print("Generating events...\n")

    while True:

        user_id = random.randint(
            1,
            NUM_USERS,
        )

        events = generator.generate_session(
            user_id=user_id
        )

        for event in events:

            event_json = json.dumps(
                asdict(event),
                ensure_ascii=False,
            )

            print(event_json)

            time.sleep(
                random.uniform(
                    MIN_EVENT_DELAY,
                    MAX_EVENT_DELAY,
                )
            )


if __name__ == "__main__":
    main()