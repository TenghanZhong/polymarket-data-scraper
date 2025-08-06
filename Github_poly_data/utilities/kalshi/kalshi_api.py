import time
import json
import requests
import base64
from datetime import datetime

# Import crypto libraries for RSA signing
try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
except ImportError:
    print("Cryptography package is required for API key authentication.")
    print("Install it using: pip install cryptography")
    print("Continuing with limited functionality...")

# Kalshi API base URLs
KALSHI_ELECTIONS_API_URL = "https://api.elections.kalshi.com/trade-api/v2"  # For election markets (KX tickers)
KALSHI_LEGACY_API_URL = "https://trading-api.kalshi.com/trade-api/v2"  # For legacy markets
KALSHI_DEMO_API_URL = "https://demo-api.kalshi.co/trade-api/v2"  # For demo testing

# Default API URL - change this based on which markets you're targeting
KALSHI_API_BASE_URL = KALSHI_ELECTIONS_API_URL

class KalshiAPI:
    def __init__(self, email=None, password=None, api_key=None, key_id=None):
        """
        Initialize Kalshi API client.
        You can authenticate either with email/password or with API key/key ID.
        """
        self.auth_token = None
        self.headers = {"accept": "application/json"}
        
        if email and password:
            self.login(email, password)
        elif api_key and key_id:
            self.set_api_key_auth(api_key, key_id)
        else:
            print("WARNING: No credentials provided. Only public endpoints will be accessible.")
    
    def login(self, email, password):
        """
        Login to Kalshi API using email and password
        """
        url = f"{KALSHI_API_BASE_URL}/login"
        payload = {
            "email": email,
            "password": password
        }
        
        try:
            response = requests.post(url, json=payload, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                self.auth_token = data.get('token')
                self.headers['Authorization'] = f"Bearer {self.auth_token}"
                print("Successfully logged in")
                return True
            else:
                print(f"Login failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"Exception during login: {str(e)}")
            return False
    
    def set_api_key_auth(self, private_key_str, key_id):
        """
        Set up API key authentication using RSA private key and key ID.
        This method stores the credentials for use in generating request signatures.
        
        Returns:
            bool: True if the key is valid and authentication is set up successfully
        """
        # Check if the private key is properly formatted
        if not private_key_str.startswith("-----BEGIN RSA PRIVATE KEY-----"):
            print("Error: Invalid RSA private key format. The key should start with '-----BEGIN RSA PRIVATE KEY-----'")
            return False
            
        # Verify that we can load the key
        try:
            # Check if we have the necessary crypto libraries
            if 'load_pem_private_key' not in globals():
                print("Warning: Cryptography libraries not imported. Cannot verify key.")
            else:
                # Try to load the key to verify it
                load_pem_private_key(
                    private_key_str.encode(),
                    password=None,
                    backend=default_backend()
                )
                
            # Store the credentials
            self.private_key_str = private_key_str
            self.key_id = key_id
            
            # No Authorization header for API key auth - we'll create custom headers per request
            if 'Authorization' in self.headers:
                del self.headers['Authorization']
                
            print("API key authentication successfully configured")
            return True
        except Exception as e:
            print(f"Error setting up API key authentication: {str(e)}")
            return False
    
    def _sign_request(self, method, path, body=None):
        """
        Sign a request using the RSA private key according to Kalshi API specs.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            path: API endpoint path
            body: Request body for POST/PUT requests
            
        Returns:
            Dictionary of headers to add to the request
        """
        # Current timestamp in milliseconds
        timestamp = str(int(time.time() * 1000))
        
        # Create the message to sign
        message = timestamp + method + path
        
        # If there's a body, include it in the message
        if body:
            if isinstance(body, dict):
                body_str = json.dumps(body)
            else:
                body_str = body
            message += body_str
        
        try:
            # Load the private key
            private_key = load_pem_private_key(
                self.private_key_str.encode(),
                password=None,
                backend=default_backend()
            )
            
            # Sign the message
            signature = private_key.sign(
                message.encode(),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            
            # Base64 encode the signature
            signature_b64 = base64.b64encode(signature).decode()
            
            # Return the headers
            return {
                'KALSHI-ACCESS-KEY': self.key_id,
                'KALSHI-ACCESS-SIGNATURE': signature_b64,
                'KALSHI-ACCESS-TIMESTAMP': timestamp
            }
        except NameError:
            print("Error: Cryptography libraries not available. Cannot sign request.")
            return {}
        except Exception as e:
            print(f"Error signing request: {str(e)}")
            return {}
    
    def get_exchange_status(self):
        """
        Check if the exchange is available
        """
        path = "/exchange/status"
        url = f"{KALSHI_API_BASE_URL}{path}"
        
        try:
            # Add signature headers if using API key auth
            headers = self.headers.copy()
            if hasattr(self, 'key_id') and hasattr(self, 'private_key_str'):
                headers.update(self._sign_request("GET", path))
            
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error getting exchange status: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"Exception while getting exchange status: {str(e)}")
            return None
    
    def get_event_by_ticker(self, event_ticker):
        """
        Fetch event data by its ticker from the Kalshi API
        """
        path = f"/events/{event_ticker}"
        url = f"{KALSHI_API_BASE_URL}{path}"
        
        try:
            # Add signature headers if using API key auth
            headers = self.headers.copy()
            if hasattr(self, 'key_id') and hasattr(self, 'private_key_str'):
                headers.update(self._sign_request("GET", path))
                
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching event data: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"Exception while fetching event data: {str(e)}")
            return None
    
    def get_market(self, market_ticker):
        """
        Fetch data for a specific market by its ticker
        """
        path = f"/markets/{market_ticker}"
        url = f"{KALSHI_API_BASE_URL}{path}"
        
        try:
            # Add signature headers if using API key auth
            headers = self.headers.copy()
            if hasattr(self, 'key_id') and hasattr(self, 'private_key_str'):
                headers.update(self._sign_request("GET", path))
                
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching market data: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"Exception while fetching market data: {str(e)}")
            return None
    
    def get_market_orderbook(self, market_ticker):
        """
        Fetch orderbook data for a specific market
        """
        path = f"/markets/{market_ticker}/orderbook"
        url = f"{KALSHI_API_BASE_URL}{path}"
        
        try:
            # Add signature headers if using API key auth
            headers = self.headers.copy()
            if hasattr(self, 'key_id') and hasattr(self, 'private_key_str'):
                headers.update(self._sign_request("GET", path))
                
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching orderbook: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"Exception while fetching orderbook: {str(e)}")
            return None
    
    def fetch_all_events(self, status="open", series_ticker=None, limit=100, cursor=None):
        """
        Fetch multiple events with optional filtering
        """
        path = "/events"
        url = f"{KALSHI_API_BASE_URL}{path}"
        params = {
            "status": status,
            "limit": limit
        }
        
        if series_ticker:
            params["series_ticker"] = series_ticker
            
        if cursor:
            params["cursor"] = cursor
        
        try:
            # Add signature headers if using API key auth
            headers = self.headers.copy()
            if hasattr(self, 'key_id') and hasattr(self, 'private_key_str'):
                # Add query params to the path for signing
                query_string = "&".join([f"{k}={v}" for k, v in params.items()])
                sign_path = f"{path}?{query_string}" if query_string else path
                headers.update(self._sign_request("GET", sign_path))
            
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching events: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"Exception while fetching events: {str(e)}")
            return None
    
    def fetch_all_markets(self, status="open", event_ticker=None, limit=100, cursor=None):
        """
        Fetch multiple markets with optional filtering
        """
        path = "/markets"
        url = f"{KALSHI_API_BASE_URL}{path}"
        params = {
            "status": status,
            "limit": limit
        }
        
        if event_ticker:
            params["event_ticker"] = event_ticker
            
        if cursor:
            params["cursor"] = cursor
        
        try:
            # Add signature headers if using API key auth
            headers = self.headers.copy()
            if hasattr(self, 'key_id') and hasattr(self, 'private_key_str'):
                # Add query params to the path for signing
                query_string = "&".join([f"{k}={v}" for k, v in params.items()])
                sign_path = f"{path}?{query_string}" if query_string else path
                headers.update(self._sign_request("GET", sign_path))
            
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching markets: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"Exception while fetching markets: {str(e)}")
            return None