"""
Main pipeline orchestrator for OmniCart
Coordinates the data fetching, enrichment, and analysis process
"""

import json
import os
import logging
from typing import Dict, Any

from .config import ConfigManager
from .api_client import APIClient
from .data_enricher import DataEnricher
from .data_analyzer import DataAnalyzer

logger = logging.getLogger(__name__)

class Pipeline:
    """Orchestrates the OmniCart data pipeline workflow."""
    
    def __init__(self, config_path: str = None):
        """Initialize pipeline with configuration."""
        self.config = ConfigManager(config_path)
        self.api_client = APIClient(self.config.base_url, self.config.limit)
        self.enricher = DataEnricher()
        self.analyzer = DataAnalyzer()
        
    def fetch_data(self) -> Dict[str, Any]:
        """Fetch products and users data from API."""
        logger.info("Fetching data from API...")
        
        # Fetch products with pagination
        products_data = self.api_client.get_all_products()
        if not products_data:
            logger.error("Failed to fetch products data")
            return {}
        
        # Fetch users
        users_data = self.api_client.get_all_users()
        if not users_data:
            logger.warning("No user data available - some enrichment features will be limited")
            users_data = []
            
        logger.info(f"Fetched {len(products_data)} products and {len(users_data)} users")
        return {
            "products": products_data,
            "users": users_data
        }
        
    def process_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process and analyze the fetched data."""
        if not raw_data.get("products"):
            logger.error("No product data to process")
            return {}
            
        logger.info("Converting data to DataFrames...")
        products_df = self.enricher.to_dataframe(raw_data["products"])
        users_df = self.enricher.to_dataframe(raw_data["users"])
        
        logger.info("Enriching product data with user information...")
        enriched_df = self.enricher.enrich_product_data(products_df, users_df)
        if enriched_df.empty:
            logger.error("Failed to enrich data")
            return {}
            
        logger.info("Analyzing seller performance...")
        seller_performance = self.analyzer.analyze_seller_performance(enriched_df)
        
        logger.info("Generating overall marketplace summary...")
        overall_summary = self.analyzer.get_overall_summary(enriched_df)
        
        return {
            "seller_performance": seller_performance,
            "overall_summary": overall_summary
        }
        
    def save_report(self, data: Dict[str, Any], output_file: str) -> bool:
        """Save analysis results to a JSON file."""
        if not data:
            logger.error("No data to save")
            return False
            
        try:
            # Ensure output directory exists
            output_dir = os.path.dirname(output_file)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                
            # Save report
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Report saved to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save report: {str(e)}")
            return False
            
    def run(self, output_file: str = "seller_performance_report.json") -> bool:
        """
        Execute the complete pipeline workflow.
        
        Args:
            output_file: Path where to save the final report
            
        Returns:
            bool: True if pipeline completed successfully, False otherwise
        """
        try:
            # Configure logging
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            
            logger.info("Starting OmniCart data pipeline...")
            
            # Fetch raw data
            raw_data = self.fetch_data()
            if not raw_data:
                return False
                
            # Process and analyze data
            results = self.process_data(raw_data)
            if not results:
                return False
                
            # Save report
            if not self.save_report(results, output_file):
                return False
                
            logger.info("Pipeline completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return False

if __name__ == "__main__":
    # Example usage
    pipeline = Pipeline()
    success = pipeline.run()
    
    if success:
        print("\nPipeline completed successfully!")
        print("Check seller_performance_report.json for results")
    else:
        print("\nPipeline failed. Check the logs for details.")
