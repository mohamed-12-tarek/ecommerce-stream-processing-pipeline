import random
import uuid
from datetime import datetime, timedelta, timezone

from .catalog import ProductCatalog
from .config import (
    CATEGORIES,
    MAX_SESSION_GAP_MINUTES,
    MAX_SESSION_ACTIONS,
    MIN_SESSION_ACTIONS,
    MIN_SESSION_GAP_MINUTES,
    SIMULATION_START_DATE
)
from .models import Event, EventType, UserProfile


class EcommerceEventGenerator:

    SEARCH_TERMS = {
        "electronics": [
            "headphones",
            "smart watch",
            "camera",
            "tv",
            "speaker",
        ],
        "computers": [
            "laptop",
            "keyboard",
            "mouse",
            "monitor",
            "ssd",
        ],
        "smartphones": [
            "iphone",
            "android phone",
            "charger",
            "phone case",
            "power bank",
        ],
        "home": [
            "chair",
            "table",
            "lamp",
            "sofa",
            "kitchen",
        ],
        "fashion": [
            "shoes",
            "shirt",
            "jacket",
            "jeans",
            "dress",
        ],
        "beauty": [
            "perfume",
            "skincare",
            "makeup",
            "shampoo",
        ],
        "sports": [
            "football",
            "running shoes",
            "gym equipment",
            "basketball",
        ],
        "books": [
            "python book",
            "data engineering book",
            "machine learning book",
            "novel",
        ],
        "toys": [
            "lego",
            "car toy",
            "board game",
            "puzzle",
        ],
        "grocery": [
            "coffee",
            "tea",
            "snacks",
            "cereal",
        ],
    }

    def __init__(self, num_users: int, catalog: ProductCatalog, start_time: datetime | None = None) -> None:
        self.catalog = catalog
        self.current_time = (
            start_time
            if start_time is not None
            else datetime.fromisoformat(SIMULATION_START_DATE)
        )
        self.users = self._generate_users(num_users)


    def _generate_users(self, num_users: int) -> dict[int, UserProfile]:
        users = {}
        rng = random.Random(42)

        for user_id in range(1, num_users + 1):
            activity_level = rng.choices(
                [0.4, 1.0, 1.8],
                weights=[0.30, 0.55, 0.15],
                k=1
            )[0]

            base_purchase_probability = rng.uniform(0.05, 0.25)
            purchase_probability = min(
                0.75,
                base_purchase_probability * activity_level,
            )

            users[user_id] = UserProfile(
                user_id=user_id,
                preferred_category=rng.choice(CATEGORIES),
                purchase_probability=purchase_probability,
                activity_level=activity_level,
            )

        return users

    @staticmethod
    def _event_id() -> str:
        return str(uuid.uuid4())


    @staticmethod
    def _session_id() -> str:
        return str(uuid.uuid4())


    def _advance_time(self, minutes: int) -> None:
        self.current_time += timedelta(minutes=minutes)


    def _timestamp(self) -> str:
        return self.current_time.astimezone(timezone.utc).isoformat()


    def _create_event(
        self,
        event_type: EventType,
        user_id: int,
        session_id: str,
        product_id: int | None = None,
        category: str | None = None,
        quantity: int | None = None,
        price: float | None = None,
        search_query: str | None = None,
    ) -> Event:
        
        event = Event(
            event_id=self._event_id(),
            event_type=event_type.value,
            user_id=user_id,
            session_id=session_id,
            product_id=product_id,
            category=category,
            quantity=quantity,
            price=price,
            search_query=search_query,
            timestamp=self._timestamp(),
        )
        self._advance_time(random.randint(1, 5))
        return event


    def _search(self, user_id: int, session_id: str) -> Event:
        user = self.users[user_id]
        query = random.choice(self.SEARCH_TERMS[user.preferred_category])
        return self._create_event(
            event_type=EventType.SEARCH,
            user_id=user_id,
            session_id=session_id,
            category=user.preferred_category,
            search_query=query,
        )


    def _product_view(self, user_id: int, session_id: str) -> Event:
        user = self.users[user_id]
        category_products = [
            product
            for product in self.catalog.products.values()
            if product.category == user.preferred_category
        ]

        product = (
            random.choice(category_products)
            if category_products
            else self.catalog.random_product()
        )

        return self._create_event(
            event_type=EventType.PRODUCT_VIEW,
            user_id=user_id,
            session_id=session_id,
            product_id=product.product_id,
            category=product.category,
        )


    def _add_to_cart(self, user_id: int, session_id: str, product_id: int) -> Event:
        product = self.catalog.get_product(product_id)
        return self._create_event(
            event_type=EventType.ADD_TO_CART,
            user_id=user_id,
            session_id=session_id,
            product_id=product.product_id,
            category=product.category,
            quantity=random.randint(1, 3),
        )


    def _remove_from_cart(self, user_id: int, session_id: str, product_id: int) -> Event:
        product = self.catalog.get_product(product_id)
        return self._create_event(
            event_type=EventType.REMOVE_FROM_CART,
            user_id=user_id,
            session_id=session_id,
            product_id=product.product_id,
            category=product.category,
        )


    def _purchase(self, user_id: int, session_id: str, product_id: int) -> Event:
        product = self.catalog.get_product(product_id)
        return self._create_event(
            event_type=EventType.PURCHASE,
            user_id=user_id,
            session_id=session_id,
            product_id=product.product_id,
            category=product.category,
            quantity=random.randint(1, 3),
            price=product.price,
        )


    def _session_start(self, user_id: int, session_id: str) -> Event:
        return self._create_event(
            event_type=EventType.SESSION_START,
            user_id=user_id,
            session_id=session_id,
        )


    def _session_end(self, user_id: int, session_id: str) -> Event:
        return self._create_event(
            event_type=EventType.SESSION_END,
            user_id=user_id,
            session_id=session_id,
        )


    def generate_session(self, user_id: int | None = None) -> list[Event]:
        if user_id is None:
            user_id = random.choice(list(self.users.keys()))

        user = self.users[user_id]
        session_id = self._session_id()
        events: list[Event] = []
        cart: list[int] = []
        viewed_products: list[int] = []

        events.append(self._session_start(user_id, session_id))

        base_actions = random.randint(MIN_SESSION_ACTIONS, MAX_SESSION_ACTIONS)
        number_of_actions = max(
            MIN_SESSION_ACTIONS,
            min(
                MAX_SESSION_ACTIONS,
                round(base_actions * user.activity_level)
            )
        )

        for _ in range(number_of_actions):
            action = random.random()

            if action < 0.20:
                events.append(self._search(user_id, session_id))

            elif action < 0.65:
                event = self._product_view(user_id, session_id)
                events.append(event)
                if event.product_id is not None:
                    viewed_products.append(event.product_id)

            elif action < 0.85:
                if viewed_products:
                    product_id = random.choice(viewed_products)
                    events.append(
                        self._add_to_cart(user_id, session_id, product_id)
                    )
                    if product_id not in cart:
                        cart.append(product_id)

            elif action < 0.93:
                if cart:
                    product_id = random.choice(cart)
                    events.append(
                        self._remove_from_cart(user_id, session_id, product_id)
                    )
                    cart.remove(product_id)

            else:
                if cart and random.random() < user.purchase_probability:
                    product_id = random.choice(cart)
                    events.append(
                        self._purchase(user_id, session_id, product_id)
                    )
                    cart.remove(product_id)

        events.append(self._session_end(user_id, session_id))

        self._advance_time(
            random.randint(MIN_SESSION_GAP_MINUTES, MAX_SESSION_GAP_MINUTES)
        )

        return events
