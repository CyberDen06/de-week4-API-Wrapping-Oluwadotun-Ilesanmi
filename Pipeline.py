import json
import logging
import os
from typing import Any, Dict

from APIClient import APIClient
from DataEnricher import DataEnricher
from DataAnalyzer import DataAnalyzer

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Pipeline:
	"""Orchestrates fetching, enriching, analyzing and saving seller reports."""

	def __init__(self, base_url: str = None) -> None:
		self.base_url = base_url or "https://fakestoreapi.com"
		self.client = APIClient(self.base_url)
		self.enricher = DataEnricher()
		self.analyzer = DataAnalyzer()

	def run(self, output_file: str = "seller_performance_report.json") -> bool:
		"""Execute the pipeline end-to-end and save the analysis to JSON.

		Returns True on success, False on failure.
		"""
		try:
			logger.info("Starting pipeline")

			products = self.client.get_all_products()
			if not products:
				logger.error("No products fetched from API")
				return False

			users = self.client.get_all_users() or []

			logger.info(f"Fetched {len(products)} products and {len(users)} users")

			# Enrich
			enriched = self.enricher.enrich_product_data(products, users)
			if enriched.empty:
				logger.error("Enrichment produced empty DataFrame")
				return False

			# Analyze
			report = self.analyzer.analyze_seller_performance(enriched)

			# Save
			out_dir = os.path.dirname(output_file)
			if out_dir and not os.path.exists(out_dir):
				os.makedirs(out_dir, exist_ok=True)

			with open(output_file, 'w', encoding='utf-8') as f:
				json.dump(report, f, indent=2)

			logger.info(f"Saved seller report to {output_file}")
			return True

		except Exception as e:
			logger.exception("Pipeline failed: %s", e)
			return False


if __name__ == '__main__':
	p = Pipeline()
	success = p.run()
	if success:
		print("Pipeline completed successfully")
	else:
		print("Pipeline failed")

