from dataclasses import dataclass
from enum import Enum


class EventType(str, Enum):
    SESSION_START = "session_start"
    SEARCH = "search"
    PRODUCT_VIEW = "product_view"
    ADD_TO_CART = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    PURCHASE = "purchase"
    SESSION_END = "session_end"


@dataclass(frozen=True)
class UserProfile:
    user_id: int
    preferred_category: str
    purchase_probability: float
    activity_level: float


@dataclass(frozen=True)
class Product:
    product_id: int
    category: str
    price: float


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str

    user_id: int
    session_id: str

    product_id: int | None
    category: str | None

    quantity: int | None
    price: float | None

    search_query: str | None

    timestamp: str
