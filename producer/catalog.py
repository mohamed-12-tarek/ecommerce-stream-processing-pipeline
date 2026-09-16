import random

from .config import CATEGORIES
from .models import Product


class ProductCatalog:

    def __init__(
        self,
        num_products: int,
    ) -> None:

        self.products: dict[int, Product] = {}

        self._generate_products(num_products)

    def _generate_products(
        self,
        num_products: int,
    ) -> None:

        for product_id in range(1, num_products + 1):

            product = Product(
                product_id=product_id,
                category=random.choice(CATEGORIES),
                price=round(
                    random.uniform(10, 2000),
                    2,
                ),
            )

            self.products[product_id] = product

    def get_product(
        self,
        product_id: int,
    ) -> Product:

        return self.products[product_id]

    def random_product(
        self,
    ) -> Product:

        return random.choice(
            list(self.products.values())
        )