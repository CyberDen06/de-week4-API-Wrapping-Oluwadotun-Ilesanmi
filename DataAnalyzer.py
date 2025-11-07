"""DataAnalyzer

Computes seller-level metrics from enriched product data.
"""

from typing import Dict, Any
import pandas as pd


class DataAnalyzer:
	def __init__(self) -> None:
		pass

	def analyze_seller_performance(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
		"""Generate per-seller metrics from an enriched products DataFrame.

		Metrics computed per username:
		  - total_revenue: sum of the 'revenue' column
		  - products_sold: if 'quantity' exists -> sum(quantity); otherwise -> number of products (rows)
		  - avg_price: average of the 'price' column

		Args:
			df (pd.DataFrame): Enriched DataFrame containing at least 'username', 'price', and 'revenue'.

		Returns:
			dict: mapping username -> metrics dict
		"""
		if df is None or df.empty:
			return {}

		work = df.copy()

		# Ensure seller_name column exists
		if 'seller_name' not in work.columns:
			work['seller_name'] = None

		# Replace missing seller names with a sentinel
		work['seller_name'] = work['seller_name'].fillna('Unknown Seller')

		# Ensure numeric columns
		if 'revenue' not in work.columns:
			work['revenue'] = 0.0
		work['revenue'] = pd.to_numeric(work['revenue'], errors='coerce').fillna(0.0)

		if 'price' not in work.columns:
			work['price'] = 0.0
		work['price'] = pd.to_numeric(work['price'], errors='coerce').fillna(0.0)

		# Determine products_sold definition: prefer quantity column if present
		if 'quantity' in work.columns:
			work['quantity'] = pd.to_numeric(work['quantity'], errors='coerce').fillna(0).astype(int)
			sold_series = work.groupby('seller_name')['quantity'].sum()
		else:
			sold_series = work.groupby('seller_name').size()

		revenue_series = work.groupby('seller_name')['revenue'].sum()
		avg_price_series = work.groupby('seller_name')['price'].mean()

		result: Dict[str, Dict[str, Any]] = {}
		users = sorted(revenue_series.index.tolist())
		for u in users:
			total_revenue = float(revenue_series.get(u, 0.0))
			products_sold = int(sold_series.get(u, 0))
			avg_price = float(avg_price_series.get(u, 0.0))

			result[u] = {
				'total_revenue': round(total_revenue, 2),
				'products_sold': products_sold,
				'avg_price': round(avg_price, 2)
			}

		return result


if __name__ == '__main__':
	# tiny smoke test
	df = pd.DataFrame([
		{'username': 'alice', 'price': 10.0, 'revenue': 100.0, 'quantity': 10},
		{'username': 'bob', 'price': 20.0, 'revenue': 40.0, 'quantity': 2},
		{'username': None, 'price': 5.0, 'revenue': 5.0, 'quantity': 1},
	])
	analyzer = DataAnalyzer()
	print(analyzer.analyze_seller_performance(df))

