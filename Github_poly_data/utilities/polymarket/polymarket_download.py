import time
import json
from datetime import datetime
from apify_client import ApifyClient

# Initialize the ApifyClient with your API token
client = ApifyClient("apify_api_gCjYow167TvvxpePpgvRRUe3hjsK7T2uFtjX")  # Replace with your actual API token

# Prepare the Actor input
run_input = {
    "slug": ["what-price-will-bitcoin-hit-in-april"],
    "active": True,
    "archived": False,
    "closed": False
}

def fetch_market_data():
    """Fetch market data and print detailed information for each outcome."""
    # Run the Actor and wait for it to finish
    run = client.actor("louisdeconinck/polymarket-events-scraper").call(run_input=run_input)

    # Fetch and process Actor results
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        market_id = item.get("id")
        market_title = item.get("title")
        timestamp = datetime.utcnow().isoformat()
        
        print(f"\n{timestamp} | Market: {market_title} (ID: {market_id})")
        
        # Check if outcomes data exists and process it
        outcomes = item.get("outcomes", [])
        if outcomes:
            print(f"{'Outcome':<30} | {'Probability':<12} | {'Price':<8} | {'Bid':<8} | {'Ask':<8}")
            print("-" * 80)
            
            for outcome in outcomes:
                outcome_name = outcome.get("name", "Unknown")
                probability = outcome.get("probability", "N/A")
                price = outcome.get("price", "N/A")
                bid = outcome.get("bid", "N/A")
                ask = outcome.get("ask", "N/A")
                
                print(f"{outcome_name:<30} | {probability:<12.4f} | {price:<8.4f} | {bid:<8.4f} | {ask:<8.4f}")
        else:
            # Debug: Print the entire item to see its structure
            print("No outcomes found. Raw item structure:")
            print(json.dumps(item, indent=2))

if __name__ == "__main__":
    try:
        while True:
            print(f"\n--- Fetching market data at {datetime.utcnow().isoformat()} ---")
            fetch_market_data()
            print(f"Waiting 60 seconds before next fetch...\n")
            time.sleep(60)  # Sleep for 60 seconds
    except KeyboardInterrupt:
        print("Script terminated by user")
    except Exception as e:
        print(f"Error: {str(e)}")