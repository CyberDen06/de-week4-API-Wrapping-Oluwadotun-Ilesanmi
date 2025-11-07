"""Tests for APIClient class functionality."""

import pytest
from unittest.mock import MagicMock, patch
import requests

from APIClient import APIClient


def test_api_client_initialization():
    """Test APIClient initialization with custom parameters."""
    client = APIClient(base_url="http://test.com", limit=20)
    assert client.base_url == "http://test.com"
    assert client.limit == 20

def test_get_all_products_pagination_multiple_pages():
    """Test pagination with multiple pages of products."""
    first_response = MagicMock()
    first_products = [{"id": 1, "title": "A"}, {"id": 2, "title": "B"}]
    first_response.json.return_value = first_products

    second_response = MagicMock()
    second_products = [{"id": 3, "title": "C"}]
    second_response.json.return_value = second_products

    third_response = MagicMock()
    third_response.json.return_value = []

    with patch('APIClient.requests.get') as mock_get:
        mock_get.side_effect = [first_response, second_response, third_response]

        client = APIClient(base_url="http://example.com", limit=2)
        products = client.get_all_products()

        assert mock_get.call_count == 3
        assert products == first_products + second_products
        
        # Verify correct URL parameters
        calls = mock_get.call_args_list
        assert "limit=2&offset=0" in calls[0][0][0]
        assert "limit=2&offset=2" in calls[1][0][0]
        assert "limit=2&offset=4" in calls[2][0][0]

def test_get_all_products_handles_error():
    """Test error handling in get_all_products method."""
    with patch('APIClient.requests.get') as mock_get:
        mock_get.side_effect = requests.exceptions.RequestException("API Error")
        
        client = APIClient()
        products = client.get_all_products()
        
        assert products == []
        assert mock_get.call_count == 1

def test_get_all_users_successful():
    """Test successful user data retrieval."""
    mock_response = MagicMock()
    users_data = [{"id": 1, "name": "User1"}, {"id": 2, "name": "User2"}]
    mock_response.json.return_value = users_data

    with patch('APIClient.requests.get') as mock_get:
        mock_get.return_value = mock_response
        
        client = APIClient()
        users = client.get_all_users()
        
        assert users == users_data
        mock_get.assert_called_once()
        assert "/users" in mock_get.call_args[0][0]

def test_get_all_users_handles_error():
    """Test error handling in get_all_users method."""
    with patch('APIClient.requests.get') as mock_get:
        mock_get.side_effect = requests.exceptions.RequestException("API Error")
        
        client = APIClient()
        users = client.get_all_users()
        
        assert users == []
        mock_get.assert_called_once()

def test_get_all_products_partial_page():
    """Test pagination when last page is partially filled."""
    first_response = MagicMock()
    first_products = [{"id": 1}, {"id": 2}, {"id": 3}]
    first_response.json.return_value = first_products

    second_response = MagicMock()
    second_products = [{"id": 4}]  # Less than limit
    second_response.json.return_value = second_products

    third_response = MagicMock()
    third_response.json.return_value = []  # Empty list to end pagination

    with patch('APIClient.requests.get') as mock_get:
        mock_get.side_effect = [first_response, second_response, third_response]

        client = APIClient(limit=3)
        products = client.get_all_products()

        assert mock_get.call_count == 3
        assert products == first_products + second_products

@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 500])
def test_api_client_handles_http_errors(status_code):
    """Test handling of various HTTP error status codes."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        response=MagicMock(status_code=status_code)
    )

    with patch('APIClient.requests.get') as mock_get:
        mock_get.return_value = mock_response
        
        client = APIClient()
        products = client.get_all_products()
        users = client.get_all_users()
        
        assert products == []
        assert users == []
