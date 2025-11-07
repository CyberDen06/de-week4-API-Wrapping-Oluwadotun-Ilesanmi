import requests   # To make HTTP requests
import logging    # To log information and errors

# Set the basic logging configuration. 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class APIClient:
    def __init__(self, base_url="https://fakestoreapi.com", limit=10):
        self.base_url = base_url
        self.limit = limit  # Number of items per page
        # Method to get all API product data
    def get_all_products(self) -> list:
        """
        Fetches all products. For this demo API, we'll fetch all products at once
        since pagination is simulated and the dataset is small.
        Returns a list of all products.
        """
        endpoint = "/products"
        url = f"{self.base_url}{endpoint}"
        logging.info(f"Fetching all product data from {url}")
        
        try:
            response = requests.get(url, verify=False)  # Note: In production, proper SSL verification should be used
            response.raise_for_status()
            products = response.json()
            return products
        except requests.exceptions.RequestException as e:
            logging.error(f"Couldn't fetch API data: {e}")
            return []
        
        # Method to get all users data from the API
    def get_all_users(self) -> list: 
        """Fetches all users from the API."""
        endpoint = "/users"
        url = self.base_url + endpoint
        logging.info(f"Fetching user data from {url}")
        
        try:
            response = requests.get(url, verify=False)  # Note: In production, proper SSL verification should be used
            response.raise_for_status()  # Fixed: call raise_for_status on response object
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed: {e}")
            return []  # Return empty list instead of dict for consistency

