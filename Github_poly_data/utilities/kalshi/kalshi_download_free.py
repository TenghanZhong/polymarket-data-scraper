# general script
import time
import json
import requests
import uuid
import base64
from datetime import datetime
import os
import csv
import psycopg2

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

from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
import sys
sys.path.append(str(PROJECT_ROOT))
from utilities.db_utils import get_connection, release_connection, ensure_table_exists, insert_kalshi_market_data

# Kalshi API base URLs
KALSHI_ELECTIONS_API_URL = "https://api.elections.kalshi.com/trade-api/v2"  # For election markets (KX tickers)
KALSHI_LEGACY_API_URL = "https://trading-api.kalshi.com/trade-api/v2"  # For legacy markets
KALSHI_DEMO_API_URL = "https://demo-api.kalshi.co/trade-api/v2"  # For demo testing

# Default API URL - change this based on which markets you're targeting
KALSHI_API_BASE_URL = KALSHI_ELECTIONS_API_URL



LOCAL_BACKUP_DIR = "kalshi_local_backup"

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

def print_available_events(kalshi_api, limit=5, status="open"):
    """
    Print a list of available events to help find the right ticker
    """
    events_response = kalshi_api.fetch_all_events(status=status, limit=limit)
    
    if not events_response:
        print("Could not fetch events")
        return
    
    events = events_response.get('events', [])
    
    print("\n=== Available Events ===")
    for i, event in enumerate(events):
        
        print(f"{i+1}. {event.get('title')} {event.get('sub_title')} (Ticker: {event.get('event_ticker')})")

        # Fetch markets for this event
        markets_response = kalshi_api.fetch_all_markets(event_ticker=event.get('event_ticker'), limit=5)
        markets = markets_response.get('markets', []) if markets_response else []
        
        # Print markets within this event
        for j, market in enumerate(markets):
            print(f"   {i+1}.{j+1}. Market: {market.get('title')} (Ticker: {market.get('ticker')})")
    print("========================\n")

def save_market_data_locally(event_ticker, timestamp, markets_data, local_dir=LOCAL_BACKUP_DIR):
    """Save the market data to a local CSV if DB insert fails."""
    os.makedirs(local_dir, exist_ok=True)
    filename = os.path.join(local_dir, f"{event_ticker.lower()}.csv")
    file_exists = os.path.isfile(filename)

    with open(filename, mode="a", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "ticker", "title", "best_bid", "best_ask"])
        for market in markets_data:
            writer.writerow([
                timestamp,
                market["ticker"],
                market["title"],
                market["best_bid"],
                market["best_ask"]
            ])

def is_event_active(event_data):
    """Check if an event is still active."""
    return event_data and not event_data.get('event', {}).get('closed', False)

def extract_market_data(event_data, kalshi_api=None):
    """
    Extract market data from event. Cleaned version.
    """
    markets = event_data.get('markets', [])
    markets_data = []
    
    for market in markets:
        ticker = market.get('ticker')
        title = market.get('title', '')

        if not ticker:
            continue  # Skip markets with no ticker
        
        yes_bid = market.get('yes_bid')
        yes_ask = market.get('yes_ask')

        # Convert prices if they exist
        best_bid = yes_bid / 100.0 if yes_bid is not None else None
        best_ask = yes_ask / 100.0 if yes_ask is not None else None

        # If bid or ask missing, try to fallback to last price
        if (best_bid is None or best_ask is None) and 'last_price' in market:
            last_price = market.get('last_price') / 100.0
            if best_bid is None:
                best_bid = last_price
            if best_ask is None:
                best_ask = last_price

        markets_data.append({
            'ticker': ticker,
            'title': title,
            'best_bid': best_bid,
            'best_ask': best_ask
        })
    
    return markets_data

def monitor_event(kalshi_api, event_ticker, interval_seconds=60, max_retries=3, retry_delay=10):
    """Continuously monitor an event and insert market data."""
    print(f"Starting to monitor event: {event_ticker}")

    attempt = 0

    while attempt < max_retries:
        conn = get_connection()
        ensure_table_exists(conn, event_ticker)

        try:
            while True:
                timestamp = datetime.utcnow().isoformat()
                event_data = kalshi_api.get_event_by_ticker(event_ticker)

                if not is_event_active(event_data):
                    print(f"{timestamp} | Event {event_ticker} closed. Stopping monitor.")
                    return  # Exit the function if the market is closed

                markets_data = extract_market_data(event_data, kalshi_api)

                if markets_data:
                    try:
                        if conn.closed:
                            raise psycopg2.OperationalError("Connection closed unexpectedly")

                        insert_kalshi_market_data(conn, event_ticker, datetime.utcnow(), markets_data)
                        print(f"{timestamp} | Inserted {len(markets_data)} markets.")

                    except psycopg2.OperationalError as e:
                        print(f"{timestamp} | DB Error: {e}")
                        print(f"{timestamp} | Saving locally...")
                        save_market_data_locally(event_ticker, timestamp, markets_data)

                        # Retry logic
                        print(f"{timestamp} | Retrying connection in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        attempt += 1
                        break  # Exit the inner loop to reconnect

                    except Exception as e:
                        print(f"{timestamp} | Unexpected Error: {e}")
                        print(f"{timestamp} | Saving locally...")
                        save_market_data_locally(event_ticker, timestamp, markets_data)

                else:
                    print(f"{timestamp} | No active markets found for {event_ticker}")

                time.sleep(interval_seconds)

        except Exception as e:
            print(f"{timestamp} | Critical error in monitor loop: {e}")
            attempt += 1
            print(f"{timestamp} | Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

        finally:
            release_connection(conn)
            print(f"Released database connection for event: {event_ticker}")

    print(f"{event_ticker} | Exceeded maximum retries. Giving up.")

def main():
    # 读取 API 密钥
    try:
        with open('kalshi_private_key.txt', 'r') as f:
            api_private_key = f.read().strip()
    except FileNotFoundError:
        print("Error: Could not find kalshi_private_key.txt")
        return
    except Exception as e:
        print(f"Error reading private key file: {str(e)}")
        return

    # 设置 key ID
    api_key_id = "94864691-fc70-4207-96ba-e369878a8e6a"

    # 初始化 Kalshi 客户端
    kalshi_api = KalshiAPI()
    if not kalshi_api.set_api_key_auth(api_private_key, api_key_id):
        print("Failed to set up API key authentication. Exiting.")
        return

    # 检查交易所状态
    exchange_status = kalshi_api.get_exchange_status()
    if not exchange_status:
        print("Failed to get exchange status. Check your API key configuration.")
        return
    if not exchange_status.get('trading_active', False):
        print("Warning: Exchange trading is not active")

    # 显示可选事件
    print_available_events(kalshi_api)

    # ✅ 支持命令行传入 ticker
    if len(sys.argv) > 1:
        ticker = sys.argv[1]
        print(f"⚙️ Using command-line ticker: {ticker}")
    else:
        ticker = input("Enter an event ticker to monitor (from the list above): ")

    monitor_event(kalshi_api, ticker.upper())
if __name__ == "__main__":
    main()