"""
Test suite for OmniCart Pipeline components
"""

import pytest
import pandas as pd
import json
import os
from unittest.mock import MagicMock, patch
from typing import Dict, Any

from pipeline.config import ConfigManager
from pipeline.api_client import APIClient
from pipeline.data_enricher import DataEnricher
from pipeline.data_analyzer import DataAnalyzer
from pipeline.pipeline import Pipeline

# Test Data Fixtures
@pytest.fixture
def sample_products():
    """Sample product data for testing."""
    return [
        {
            "id": 1,
            "title": "Test Product 1",
            "price": 29.99,
            "category": "electronics",
            "userId": 1,
            "rating": {"rate": 4.5, "count": 100},
        },
        {
            "id": 2,
            "title": "Test Product 2",
            "price": 49.99,
            "category": "clothing",
            "userId": 2,
            "rating": {"rate": 3.8, "count": 50},
        }
    ]

@pytest.fixture
def sample_users():
    """Sample user data for testing."""
    return [
        {
            "id": 1,
            "email": "user1@test.com",
            "username": "testuser1",
            "name": {"firstname": "Test", "lastname": "User1"},
        },
        {
            "id": 2,
            "email": "user2@test.com",
            "username": "testuser2",
            "name": {"firstname": "Test", "lastname": "User2"},
        }
    ]

@pytest.fixture
def config_file(tmp_path):
    """Create a temporary config file for testing."""
    config_content = """[API]
base_url = https://fakestoreapi.com
limit = 5
"""
    config_file = tmp_path / "test_pipeline.cfg"
    config_file.write_text(config_content)
    return str(config_file)

@pytest.fixture
def sample_enriched_df():
    """Create a sample enriched DataFrame for testing analysis."""
    return pd.DataFrame({
        'id': [1, 2],
        'title': ['Test Product 1', 'Test Product 2'],
        'price': [29.99, 49.99],
        'category': ['electronics', 'clothing'],
        'userId': [1, 2],
        'username': ['testuser1', 'testuser2'],
        'quantity': [100, 50],
        'rating': [4.5, 3.8],
        'revenue': [2999.0, 2499.5]
    })

# Config Tests
def test_config_manager(config_file):
    """Test ConfigManager reads settings correctly."""
    config = ConfigManager(config_file)
    assert config.base_url == "https://fakestoreapi.com"
    assert config.limit == 5

def test_config_manager_missing_file():
    """Test ConfigManager handles missing file."""
    with pytest.raises(FileNotFoundError):
        ConfigManager("nonexistent.cfg")

# API Client Tests
def test_api_client_pagination():
    """Test API client handles pagination correctly."""
    with patch('requests.get') as mock_get:
        # First call returns products, second call empty list
        mock_get.side_effect = [
            MagicMock(json=lambda: [{"id": 1}, {"id": 2}]),
            MagicMock(json=lambda: [])
        ]
        
        client = APIClient("http://test.com", limit=2)
        products = client.get_all_products()
        
        assert len(products) == 2
        assert mock_get.call_count == 2

def test_api_client_error_handling():
    """Test API client handles errors gracefully."""
    with patch('requests.get') as mock_get:
        mock_get.side_effect = Exception("API Error")
        
        client = APIClient("http://test.com")
        products = client.get_all_products()
        users = client.get_all_users()
        
        assert products == []
        assert users == []

# Data Enricher Tests
def test_enricher_product_data(sample_products, sample_users):
    """Test DataEnricher combines product and user data correctly."""
    enricher = DataEnricher()
    products_df = pd.DataFrame(sample_products)
    users_df = pd.DataFrame(sample_users)
    
    enriched = enricher.enrich_product_data(products_df, users_df)
    
    assert 'username' in enriched.columns
    assert 'revenue' in enriched.columns
    assert len(enriched) == len(products_df)

def test_enricher_missing_user():
    """Test enricher handles missing user data gracefully."""
    enricher = DataEnricher()
    products_df = pd.DataFrame([{
        "id": 1,
        "price": 29.99,
        "userId": 999  # Non-existent user
    }])
    users_df = pd.DataFrame([{
        "id": 1,
        "username": "testuser"
    }])
    
    enriched = enricher.enrich_product_data(products_df, users_df)
    assert pd.isna(enriched.loc[0, 'username'])

# Data Analyzer Tests
def test_analyzer_seller_performance(sample_enriched_df):
    """Test analyzer calculates seller metrics correctly."""
    analyzer = DataAnalyzer()
    performance = analyzer.analyze_seller_performance(sample_enriched_df)
    
    assert 'testuser1' in performance
    assert 'total_revenue' in performance['testuser1']
    assert performance['testuser1']['product_count'] == 1
    assert 'avg_rating' in performance['testuser1']['performance_metrics']

def test_analyzer_overall_summary(sample_enriched_df):
    """Test analyzer calculates overall metrics correctly."""
    analyzer = DataAnalyzer()
    summary = analyzer.get_overall_summary(sample_enriched_df)
    
    assert 'total_revenue' in summary
    assert 'active_sellers' in summary
    assert summary['total_products'] == 2
    assert len(summary['top_categories']) <= 3

# Pipeline Integration Tests
def test_pipeline_full_run(tmp_path, config_file):
    """Test complete pipeline execution."""
    with patch('pipeline.api_client.APIClient') as mock_client:
        # Mock API responses
        mock_client.return_value.get_all_products.return_value = [
            {"id": 1, "price": 29.99, "userId": 1}
        ]
        mock_client.return_value.get_all_users.return_value = [
            {"id": 1, "username": "testuser"}
        ]
        
        # Run pipeline
        output_file = str(tmp_path / "test_report.json")
        pipeline = Pipeline(config_file)
        success = pipeline.run(output_file)
        
        assert success
        assert os.path.exists(output_file)
        
        # Verify report content
        with open(output_file) as f:
            report = json.load(f)
            assert "seller_performance" in report
            assert "overall_summary" in report

def test_pipeline_error_handling():
    """Test pipeline handles errors gracefully."""
    with patch('pipeline.api_client.APIClient') as mock_client:
        mock_client.return_value.get_all_products.return_value = []
        
        pipeline = Pipeline()
        success = pipeline.run()
        
        assert not success  # Pipeline should fail gracefully

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
