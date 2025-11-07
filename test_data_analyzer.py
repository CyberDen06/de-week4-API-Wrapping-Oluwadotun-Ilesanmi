"""Tests for DataAnalyzer in API Wrapping package."""

import pandas as pd
import pytest
from DataAnalyzer import DataAnalyzer


@pytest.fixture
def sample_enriched_df():
    return pd.DataFrame([
        {'username': 'alice', 'price': 10.0, 'revenue': 100.0, 'quantity': 10},
        {'username': 'alice', 'price': 20.0, 'revenue': 200.0, 'quantity': 10},
        {'username': 'bob', 'price': 15.0, 'revenue': 45.0, 'quantity': 3},
    ])


def test_groupby_aggregations(sample_enriched_df):
    analyzer = DataAnalyzer()
    report = analyzer.analyze_seller_performance(sample_enriched_df)

    # Check alice
    assert 'alice' in report
    assert report['alice']['total_revenue'] == pytest.approx(300.0)
    # products_sold should sum quantity
    assert report['alice']['products_sold'] == 20
    assert report['alice']['avg_price'] == pytest.approx((10.0 + 20.0) / 2)

    # Check bob
    assert 'bob' in report
    assert report['bob']['total_revenue'] == pytest.approx(45.0)
    assert report['bob']['products_sold'] == 3
    assert report['bob']['avg_price'] == pytest.approx(15.0)
