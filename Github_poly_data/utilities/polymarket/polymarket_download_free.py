import time
import json
import requests
from datetime import datetime

# Gamma API base URL
GAMMA_API_BASE_URL = "https://gamma-api.polymarket.com"

# Configure which events to track by slug
EVENT_SLUG = "what-price-will-bitcoin-hit-in-april"

def get_event_by_slug(slug):
    """
    Fetch event data by its slug from the Gamma API
    """
    url = f"{GAMMA_API_BASE_URL}/events"
    params = {
        "slug": slug,
        "active": True,
        "archived": False,
        "closed": False
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

def fetch_all_events(active=True, limit=10, offset=0):
    """
    Fetch all events with optional filtering
    """
    url = f"{GAMMA_API_BASE_URL}/events"
    params = {
        "active": active,
        "limit": limit,
        "offset": offset
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching all events: {response.status_code}")
            return []
    except Exception as e:
        print(f"Exception while fetching all events: {str(e)}")
        return []

def print_available_events(limit=5):
    """
    Print a list of available events to help find the right slug
    """
    events = fetch_all_events(limit=limit)
    
    print("\n=== Available Events ===")
    for i, event in enumerate(events):
        print(f"{i+1}. {event.get('title')} (Slug: {event.get('slug')})")
        # Print markets within this event
        markets = event.get('markets', [])
        for j, market in enumerate(markets):
            print(f"   {i+1}.{j+1}. Market: {market.get('question')}")
    print("========================\n")

def extract_markets_data(event_data):
    """
    Extract all markets data from an event, including bids and asks
    """
    markets = event_data.get('markets', [])
    markets_data = []
    
    for market in markets:
        question = market.get('question')
        best_bid = market.get('bestBid')
        best_ask = market.get('bestAsk')
        
        # Fallback to lastTradePrice if bid/ask not available
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

def monitor_event(slug, interval_seconds=60):
    """
    Continuously monitor an event and all its markets by slug
    """
    print(f"Starting to monitor event with slug: {slug}")
    print(f"Checking every {interval_seconds} seconds")
    print("Press Ctrl+C to stop monitoring\n")
    
    try:
        while True:
            timestamp = datetime.utcnow().isoformat()
            event_data = get_event_by_slug(slug)
            
            if event_data:
                event_title = event_data.get('title')
                print(f"\n{timestamp} | Event: {event_title}")
                
                markets_data = extract_markets_data(event_data)
                if markets_data:
                    for market in markets_data:
                        print(f"Market: {market['question']}")
                        print(f"Best Bid: {market['best_bid']} | Best Ask: {market['best_ask']}")
                        print("-" * 50)
                else:
                    print("No markets found in this event")
            else:
                print(f"{timestamp} | Could not fetch event data for slug: {slug}")
            
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("\nStopping event monitoring...")

def main():
    """
    Main function to execute the script
    """
    # Show some available events to help user find the right slug
    print_available_events()
    
    # If EVENT_SLUG is provided, start monitoring it
    if EVENT_SLUG:
        monitor_event(EVENT_SLUG)
    else:
        # Otherwise ask user to input a slug
        slug = input("Enter an event slug to monitor (from the list above): ")
        monitor_event(slug)

if __name__ == "__main__":
    main()