"""
DataAnalyzer for OmniCart Pipeline
Generates seller performance insights from enriched product data
"""

import pandas as pd
from typing import Dict, Any

class DataAnalyzer:
    """Analyzes enriched product data to generate seller performance metrics."""
    
    def analyze_seller_performance(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Generate performance metrics for each seller.
        
        Args:
            df: Enriched DataFrame with products and seller info
            
        Returns:
            Dict with seller metrics:
            {
                "seller_username": {
                    "total_revenue": float,
                    "product_count": int,
                    "avg_price": float,
                    "categories": List[str],
                    "top_product": str,
                    "performance_metrics": {
                        "avg_rating": float,
                        "total_quantity_sold": int
                    }
                }
            }
        """
        if df.empty:
            return {}

        # Ensure required columns exist
        required_cols = ['username', 'price', 'revenue', 'category', 'title', 'rating', 'quantity']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"Warning: Missing required columns: {missing_cols}")
            # Add missing columns with default values
            for col in missing_cols:
                if col in ['price', 'revenue', 'rating']:
                    df[col] = 0.0
                elif col in ['quantity']:
                    df[col] = 0
                else:
                    df[col] = 'Unknown'

        # Group by seller
        seller_groups = df.groupby('username', dropna=False)
        
        performance_data = {}
        
        for username, seller_data in seller_groups:
            if pd.isna(username):
                username = "Unknown Seller"
                
            # Basic metrics
            total_revenue = seller_data['revenue'].sum()
            product_count = len(seller_data)
            avg_price = seller_data['price'].mean()
            
            # Categories and top product
            categories = seller_data['category'].unique().tolist()
            top_product_idx = seller_data['revenue'].idxmax()
            top_product = seller_data.loc[top_product_idx, 'title']
            
            # Performance metrics
            performance_metrics = {
                "avg_rating": seller_data['rating'].mean(),
                "total_quantity_sold": seller_data['quantity'].sum()
            }
            
            # Compile seller metrics
            performance_data[username] = {
                "total_revenue": round(float(total_revenue), 2),
                "product_count": int(product_count),
                "avg_price": round(float(avg_price), 2),
                "categories": categories,
                "top_product": top_product,
                "performance_metrics": {
                    "avg_rating": round(float(performance_metrics["avg_rating"]), 2),
                    "total_quantity_sold": int(performance_metrics["total_quantity_sold"])
                }
            }
        
        return performance_data

    def get_overall_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate overall marketplace performance summary.
        
        Args:
            df: Enriched DataFrame with products and seller info
            
        Returns:
            Dict with overall metrics
        """
        if df.empty:
            return {}
            
        total_revenue = df['revenue'].sum()
        total_products = len(df)
        active_sellers = df['username'].nunique()
        
        top_categories = (
            df.groupby('category')['revenue']
            .sum()
            .sort_values(ascending=False)
            .head(3)
            .to_dict()
        )
        
        return {
            "total_revenue": round(float(total_revenue), 2),
            "total_products": int(total_products),
            "active_sellers": int(active_sellers),
            "top_categories": {
                k: round(float(v), 2) for k, v in top_categories.items()
            }
        }

if __name__ == "__main__":
    # Example usage
    from pipeline.api_client import APIClient
    from pipeline.data_enricher import DataEnricher
    from pipeline.config import ConfigManager
    
    # Get data
    cfg = ConfigManager()
    client = APIClient(cfg.base_url, cfg.limit)
    dfs = client.get_dataframes()
    
    # Enrich data
    enricher = DataEnricher()
    enriched_df = enricher.enrich_product_data(dfs['products'], dfs['users'])
    
    # Analyze data
    analyzer = DataAnalyzer()
    seller_performance = analyzer.analyze_seller_performance(enriched_df)
    overall_summary = analyzer.get_overall_summary(enriched_df)
    
    # Print sample results
    print("\nOverall Summary:")
    print(overall_summary)
    print("\nSample Seller Performance:")
    for seller, metrics in list(seller_performance.items())[:2]:
        print(f"\n{seller}:")
        print(metrics)
