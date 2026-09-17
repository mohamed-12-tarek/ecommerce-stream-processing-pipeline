from datetime import datetime

from producer.catalog import ProductCatalog
from producer.config import NUM_PRODUCTS, NUM_USERS
from producer.event_generator import EcommerceEventGenerator


def main() -> None:
    catalog = ProductCatalog(num_products=NUM_PRODUCTS)
    generator = EcommerceEventGenerator(
        num_users=NUM_USERS,
        catalog=catalog,
    )

    all_events = []

    for _ in range(300):
        all_events.extend(generator.generate_session())

    timestamps = [
        datetime.fromisoformat(event.timestamp)
        for event in all_events
    ]

    event_types = {event.event_type for event in all_events}
    span_days = (max(timestamps) - min(timestamps)).total_seconds() / 86_400

    assert len(all_events) > 1_000
    assert span_days > 3, f"Timeline span is too short: {span_days:.2f} days"
    assert "session_start" in event_types
    assert "session_end" in event_types
    assert "product_view" in event_types
    assert "add_to_cart" in event_types
    assert "search" in event_types

    print("Generator smoke test passed.")
    print(f"Events generated: {len(all_events):,}")
    print(f"Timeline span: {span_days:.2f} days")
    print(f"Event types: {sorted(event_types)}")
    print(f"First timestamp: {min(timestamps).isoformat()}")
    print(f"Last timestamp:  {max(timestamps).isoformat()}")


if __name__ == "__main__":
    main()
