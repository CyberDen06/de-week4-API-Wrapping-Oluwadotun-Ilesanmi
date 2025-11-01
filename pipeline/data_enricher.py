
import pandas as pd

class DataEnricher:
    def to_dataframe(self, data):
        """Converts a list of dictionaries to a pandas DataFrame."""
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)

    def enrich_product_data(self, products_df, users_df):
        """
        Enriches product data with user information and calculates revenue.
        Returns DataFrame with seller info and revenue column.
        """
        # Prepare user columns for join
        user_cols = ['id', 'username', 'email', 'first_name', 'last_name']
        users_df = users_df.copy()
        for col in user_cols:
            if col not in users_df.columns:
                users_df[col] = None
        # Merge on userId (product) to id (user)
        enriched_df = pd.merge(
            products_df,
            users_df[user_cols],
            how='left',
            left_on='userId',
            right_on='id',
            suffixes=('', '_seller')
        )
        # Always add seller info columns, even if NaN
        for col in ['username', 'email', 'first_name', 'last_name']:
            if col not in enriched_df.columns:
                enriched_df[col] = None
        # Calculate quantity from rating.count if rating is dict
        if 'rating' in enriched_df.columns:
            enriched_df['quantity'] = enriched_df['rating'].apply(
                lambda x: x.get('count') if isinstance(x, dict) and 'count' in x else 0
            )
        elif 'quantity' not in enriched_df.columns:
            enriched_df['quantity'] = 0
        # Calculate revenue
        enriched_df['revenue'] = enriched_df['price'] * enriched_df['quantity']
        return enriched_df

if __name__ == "__main__":
    # Example usage
    from pipeline.api_client import APIClient
    from pipeline.config import ConfigManager
    cfg = ConfigManager()
    client = APIClient(cfg.base_url, cfg.limit)
    dfs = client.get_dataframes()
    enricher = DataEnricher()
    enriched = enricher.enrich_product_data(dfs['products'], dfs['users'])
    print(enriched.head())
            # Drop the intermediate 'quantity' column if not needed in final output
