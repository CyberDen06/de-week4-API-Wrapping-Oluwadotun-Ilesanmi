#We're applying OOPprinciples and creating a class to manage our API interactions. 
import requests
import logging

# Configure basic logging 
logging.basicConfig(level=logging.INFO, format = '%(asctime)s - %(levelname)s - %(messages)s')

class APIClient: 
    """ A simple wrapper for the JSONPlaceholder API. """
    def __init__(self, base_url="https://jsonplaceholder.typicode.com"):
        self.base_url = base_url # What does this self.base mean, what's the essence of adding that step to defining a method? 
    
    def get_user(self, user_id: int) -> dict:
        """Fetches a single user by their ID."""
        endpoint = f"/users/{user_id}"
        url = self.base_url + endpoint  #i.e, The URl will be an addition of the base_url + endpoint

        logging.info(f"Fetching user from {url}")
        try:
            response = requests.get(url)
            response.raise_for_status() # Check for HTTP errors
            return response.json()
        except requests.exceptions.RequestException as e: 
            logging.error(f'API request failed: {e}')
            # Return an empty dict or raise a custom exception. 
            return{}
        

""" Testing Without Hitting the Network """
# We don't want our tests to fail if the API is down. We use mocking to simulate API responses. This makes our tests fast and reliable. 
from unittest.mock import patch, MagicMock # What are these? 
# Assume APIClient class from previous slide is in a file named `api_client`
from api_client import APIClient

@patch('requests.get')
def test_get_user_success(mock_get): 
    """ Tests a successful API call for a user """
# Arrange: Configure the mock to simulate a successful response. 
mock_response = MagicMock()
mock_response.status_code = 200
mock_response.json.return_value = {"id":1, "name": "Test User"}
mock_get.return_value = mock_response

# Act: Call the method we're testing. 
client = APIClient 
user_data = client.get_user(1)

# Assert: Check that the method returned the expected data
assert user_data("name") == "Test User"
# Assert that our mock was called correctly
mock_get.assert_called_once_with("https://jsonplaceholder.typicode.com/users/1")




