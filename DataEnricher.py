"""DataEnricher

Safely converts lists/dicts to DataFrames, merges products with users,
and computes revenue = price * quantity (where quantity := rating.count).
"""

from typing import List, Dict, Any
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class DataEnricher:
    def __init__(self) -> None:
        pass

    def to_dataframe(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Convert a list of dicts (or DataFrame) to a pandas DataFrame.

        Returns an empty DataFrame for falsy input.
        """
        if isinstance(data, pd.DataFrame):
            return data.copy()
        if not data:
            return pd.DataFrame()
        try:
            return pd.DataFrame(data)
        except Exception as e:
            logger.error("Failed to convert data to DataFrame: %s", e)
            return pd.DataFrame()

    def enrich_product_data(self, products_df: Any, users_df: Any) -> pd.DataFrame:
        """Enrich product records with seller info and compute revenue.

        Args:
            products_df: DataFrame or list of product dicts (needs 'price' and 'rating')
            users_df: DataFrame or list of user dicts (needs nested 'name' and 'email')

        Returns:
            pd.DataFrame: enriched products with columns 'seller_name','email','quantity','revenue'
        """
        # Normalize inputs to DataFrames
        products_df = self.to_dataframe(products_df)
        users_df = self.to_dataframe(users_df)

        if products_df.empty:
            logger.warning("products_df is empty")
            return pd.DataFrame()

        # Ensure price column exists and is numeric
        if 'price' not in products_df.columns:
            products_df['price'] = 0.0
        products_df['price'] = pd.to_numeric(products_df['price'], errors='coerce').fillna(0.0)

        # Extract rating count as quantity
        if 'rating' in products_df.columns:
            products_df['quantity'] = products_df['rating'].apply(
                lambda x: x.get('count', 0) if isinstance(x, dict) else 0
            )
        else:
            products_df['quantity'] = 0

        # Process user data
        if not users_df.empty:
            # Extract nested name fields
            users_df['seller_name'] = users_df.apply(
                lambda row: f"{row['name']['firstname']} {row['name']['lastname']}" 
                if isinstance(row.get('name'), dict) else "Unknown",
                axis=1
            )
            
            # For demo purposes, randomly assign products to sellers
            import numpy as np
            products_df['seller_id'] = np.random.choice(users_df['id'].values, size=len(products_df))

        # Merge products with users
        enriched_df = products_df.copy()
        if not users_df.empty:
            enriched_df = pd.merge(
                enriched_df,
                users_df[['id', 'seller_name', 'email']],
                how='left',
                left_on='seller_id',
                right_on='id',
                suffixes=('', '_seller')
            )
        else:
            enriched_df['seller_name'] = 'Unknown Seller'
            enriched_df['email'] = None

        # Compute revenue
        enriched_df['quantity'] = pd.to_numeric(enriched_df['quantity'], errors='coerce').fillna(0).astype(int)
        enriched_df['revenue'] = (enriched_df['price'] * enriched_df['quantity']).round(2)

        return enriched_df