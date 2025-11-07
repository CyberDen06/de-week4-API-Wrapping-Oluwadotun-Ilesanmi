"""pytest tests for DataEnricher in API Wrapping package."""

import pandas as pd
import pytest
from DataEnricher import DataEnricher


@pytest.fixture
def sample_products_df():
    return pd.DataFrame([
        {
            "id": 1,
            "title": "Prod A",
            "price": 10.0,
            "category": "cat1",
            "userId": 1,
            "rating": {"rate": 4.5, "count": 2},
        },
        {
            "id": 2,
            "title": "Prod B",
            "price": 20.0,
            "category": "cat2",
            "userId": 2,
            "rating": {"rate": 3.0, "count": 1},
        },
    ])


@pytest.fixture
def sample_users_df():
    return pd.DataFrame([
        {"id": 1, "username": "alice", "email": "alice@example.com", "first_name": "Alice", "last_name": "A"},
        {"id": 2, "username": "bob", "email": "bob@example.com", "first_name": "Bob", "last_name": "B"},
    ])


def test_successful_join(sample_products_df, sample_users_df):
    enricher = DataEnricher()
    enriched = enricher.enrich_product_data(sample_products_df, sample_users_df)

    # Both products should have username filled
    assert 'username' in enriched.columns
    assert enriched.loc[enriched['id'] == 1, 'username'].iat[0] == 'alice'
    assert enriched.loc[enriched['id'] == 2, 'username'].iat[0] == 'bob'


def test_missing_user_results_in_nan(sample_products_df):
    # Create users df missing userId 2
    users_df = pd.DataFrame([
        {"id": 1, "username": "alice", "email": "alice@example.com"}
    ])

    enricher = DataEnricher()
    enriched = enricher.enrich_product_data(sample_products_df, users_df)

    # Product with id 2 should have NaN username
    val = enriched.loc[enriched['id'] == 2, 'username'].iat[0]
    assert pd.isna(val)


def test_revenue_calculation(sample_products_df, sample_users_df):
    enricher = DataEnricher()
    enriched = enricher.enrich_product_data(sample_products_df, sample_users_df)

    # revenue = price * rating.count
    r1 = enriched.loc[enriched['id'] == 1, 'revenue'].iat[0]
    r2 = enriched.loc[enriched['id'] == 2, 'revenue'].iat[0]

    assert r1 == pytest.approx(10.0 * 2, rel=1e-9)
    assert r2 == pytest.approx(20.0 * 1, rel=1e-9)
