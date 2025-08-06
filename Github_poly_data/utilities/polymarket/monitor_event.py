import time
import json
import requests
import psycopg2
from psycopg2 import sql
from datetime import datetime
import sys
import os
import csv
from utilities.db_utils import get_connection, release_connection, ensure_table_exists, insert_market_data

# Constants
GAMMA_API_BASE_URL = "https://gamma-api.polymarket.com"

# def get_schema_from_slug(slug: str) -> str:
#     slug = slug.lower()
#     if slug.startswith("kxhigh"):
#         return "kalshi_temperature"
#     elif slug.startswith("elon") or slug.startswith("elonmusk"):
#         return "polymarket_tweets"
#     elif slug.startswith("highest_temperature"):
#         return "polymarket_temperature"
#     elif slug.startswith("nba"):
#         return "sports"
#     elif slug.startswith("what_price_will_"):
#         return "crypto"
#     elif slug.startswith("bitcoin") or slug.startswith("ethereum"):
#         return "crypto"
#     return "public"


# API Utilities
def get_event_by_slug(slug):
    url = f"{GAMMA_API_BASE_URL}/events"
    params = {
        "slug": slug,
        "archived": False
    }

    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            events = response.json()
            if events and len(events) > 0:
                return events[0]
            else:
                print(f"No events found with slug: {slug}")
                return None
        else:
            print(f"Error fetching event data: {response.status_code}")
            return None
    except Exception as e:
        print(f"Exception while fetching event data: {str(e)}")
        return None

def extract_markets_data(event_data):
    markets = event_data.get('markets', [])
    markets_data = []

    for market in markets:
        question = market.get('question')
        best_bid = market.get('bestBid')
        best_ask = market.get('bestAsk')

        if (best_bid is None or best_ask is None) and 'lastTradePrice' in market:
            last_price = market.get('lastTradePrice')
            if best_bid is None:
                best_bid = last_price
            if best_ask is None:
                best_ask = last_price

        markets_data.append({
            'question': question,
            'best_bid': best_bid,
            'best_ask': best_ask
        })

    return markets_data

# Helper Utilities
def is_market_active(event_data):
    return not (event_data.get("closed", False) or event_data.get("archived", False))

def save_market_data_locally(slug, timestamp, markets_data):
    """Save the market data to a local CSV file when DB fails."""
    local_dir="local_backup"
    os.makedirs(local_dir, exist_ok=True)
    filename = os.path.join(local_dir, f"{slug.replace('-', '_')}.csv")

    file_exists = os.path.isfile(filename)

    with open(filename, mode="a", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow(["timestamp", "question", "best_bid", "best_ask"])

        for market in markets_data:
            writer.writerow([
                timestamp.isoformat(),
                market["question"],
                market["best_bid"],
                market["best_ask"]
            ])

# Monitoring Utilities
def monitor_market(slug, event_data, interval_seconds=60):
    """Monitor a known event until it closes."""
    while True:
        conn = None
        try:
            # Get a fresh connection for each monitoring cycle
            conn = get_connection()
            ensure_table_exists(conn, slug)
            print(f"Monitoring: {slug} | Title: {event_data.get('title')}")

            while True:
                timestamp = datetime.now()
                event_data = get_event_by_slug(slug)

                if not event_data or not is_market_active(event_data):
                    print(f"{timestamp.isoformat()} | Market {slug} closed or not found. Stopping monitoring.")
                    break

                markets_data = extract_markets_data(event_data)

                if markets_data:
                    try:
                        insert_market_data(conn, slug, timestamp, markets_data)
                        print(f"{timestamp.isoformat()} | Inserted {len(markets_data)} markets.")

                    except psycopg2.OperationalError as e:
                        print(f"{timestamp.isoformat()} | Database connection lost: {e}")
                        print(f"{timestamp.isoformat()} | Saving data locally instead...")
                        save_market_data_locally(slug, timestamp, markets_data)
                        print(f"{timestamp.isoformat()} | Data saved to local backup.")
                        print(f"Retrying connection in 10 seconds...")
                        time.sleep(10)
                        break  # Exit the inner loop to reconnect

                    except Exception as e:
                        print(f"{timestamp.isoformat()} | Unexpected error: {e}")
                        print(f"{timestamp.isoformat()} | Saving data locally just in case...")
                        save_market_data_locally(slug, timestamp, markets_data)

                time.sleep(interval_seconds)

        except Exception as e:
            print(f"[ERROR] Unexpected error in monitor_market: {e}")

        finally:
            if conn:
                release_connection(conn)
                print(f"Released database connection for slug: {slug}")
            # Wait before trying to reconnect
            time.sleep(10)

def monitor_event(slug, interval_seconds=60, max_attempts=20, retry_seconds=30*60):
    """
    A wrapper for monitor_market(), use with threading
    """
    print(f"[{slug}] Starting monitor thread.")
    attempt = 0

    try:
        while attempt < max_attempts:
            event_data = get_event_by_slug(slug)

            if event_data:
                if not is_market_active(event_data):
                    print(f"[{slug}] Market found but not active. Exiting thread.")
                    return

                print(f"[{slug}] Market found and active. Starting monitor.")
                monitor_market(slug, event_data, interval_seconds)
                print(f"[{slug}] Monitor completed. Exiting thread.")
                return  # Exit the thread after monitoring completes

            else:
                print(f"[{slug}] Market not found. Attempt {attempt+1}/{max_attempts}. Retrying in {retry_seconds} seconds...")
                time.sleep(retry_seconds)
                attempt += 1

        print(f"[{slug}] Market not found after {max_attempts} attempts. Giving up.")

    except Exception as e:
        print(f"[{slug}] Error in monitor thread: {e}")

    finally:
        print(f"[{slug}] Thread exiting.")

# CLI usage
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python monitor_event.py <event-slug>")
        sys.exit(1)

    slug = sys.argv[1]
    monitor_event(slug)