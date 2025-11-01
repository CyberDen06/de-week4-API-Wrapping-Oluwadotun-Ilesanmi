
import requests
import pandas as pd
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class APIClient:
    def __init__(self, base_url: str, limit: int = 5):
        self.base_url = base_url.rstrip("/")
        self.limit = limit

    def get_all_products(self) -> List[Dict[str, Any]]:
        """Fetch all products with pagination."""
        all_products = []
        page = 1
        while True:
            try:
                url = f"{self.base_url}/products?limit={self.limit}&page={page}"
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                products = response.json()
                if not products:
                    break
                all_products.extend(products)
                page += 1
            except Exception as e:
                logger.error(f"Error fetching products page {page}: {e}")
                break
        return all_products

    def get_all_users(self) -> List[Dict[str, Any]]:
        """Fetch all users."""
        try:
            url = f"{self.base_url}/users"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching users: {e}")
            return []

    @staticmethod
    def normalize_products(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for item in products:
            rating = item.get("rating") or {}
            out.append({
                "id": item.get("id"),
                "price": item.get("price"),
                "category": item.get("category"),
                "userId": item.get("userId"),
                "quantity": rating.get("count", 0),
                "rating": rating.get("rate", None),
            })
        return out

    @staticmethod
    def normalize_users(users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out = []
        for item in users:
            name = item.get("name") or {}
            out.append({
                "email": item.get("email"),
                "username": item.get("username"),
                "first_name": name.get("firstname"),
                "last_name": name.get("lastname"),
                "id": item.get("id"),
            })
        return out

    def get_dataframes(self) -> Dict[str, pd.DataFrame]:
        products_raw = self.get_all_products()
        users_raw = self.get_all_users()
        products = self.normalize_products(products_raw)
        users = self.normalize_users(users_raw)
        products_df = pd.DataFrame(products)
        users_df = pd.DataFrame(users)
        return {"products": products_df, "users": users_df}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Example usage with default config
    client = APIClient(base_url="https://fakestoreapi.com", limit=5)
    dfs = client.get_dataframes()
    products_df = dfs["products"]
    users_df = dfs["users"]
    print(products_df.head())
    products_df.info()
    print(users_df.head())

