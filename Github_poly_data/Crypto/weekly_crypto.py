#%%
import threading
import time
from datetime import datetime, timedelta
import pytz
import sys
from pathlib import Path
import re

# Setup imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

from utilities.polymarket.monitor_event import monitor_event

from utilities.polymarket.polymarket_download_free import fetch_all_events

# Constants
eastern = pytz.timezone("US/Eastern")
base_start = eastern.localize(datetime(2025, 5, 2, 12, 0))

ASSETS = {
    "btc": "bitcoin-price-on-{}",
    "eth": "ethereum-price-on-{}",
}

# Time threshold to refresh ETH events (hours before period end/start)
ETH_REFRESH_THRESHOLD_HOURS = 12

# Cache for ETH events
eth_events_cache = {
    "last_update": None,
    "events": []
}

# Generate weekly periods
def get_week_periods():
    now = datetime.now(eastern)
    current = base_start
    periods = []

    for _ in range(52):
        next_week = current + timedelta(days=7)
        label = next_week.strftime("%B").lower() + "-" + str(next_week.day)
        periods.append({
            "start": current,
            "end": next_week,
            "label": label,
        })
        if current <= now < next_week:
            idx = len(periods) - 1
            break
        current = next_week

    return {
        "previous": periods[idx - 1] if idx > 0 else None,
        "current": periods[idx],
        "next": periods[idx + 1] if idx + 1 < len(periods) else None,
    }


def get_eth_events():
    """
    Fetch active weekly crypto events for ETH.
    """
    eth_events = []
    limit = 100
    offset = 0

    slug_pattern = re.compile(r'^ethereum-price-on-[a-z]+-\d+(-\d+)?$')

    print("Fetching all ETH events (this may take some time)...")
    start_time = time.time()
    
    while True:
        events = fetch_all_events(active=True, limit=limit, offset=offset)
        if not events:
            break
        for event in events:
            slug = event.get('slug', '')
            if slug_pattern.match(slug) and not event.get('closed'):
                eth_events.append(event)

        # Stop if fewer results than the limit indicate no more pages
        if len(events) < limit:
            break

        # Increment offset for the next page
        offset += limit

    elapsed = time.time() - start_time
    print(f"Found {len(eth_events)} active weekly ETH events. Took {elapsed:.2f} seconds.")
    return eth_events


ETH_REFRESH_INTERVAL_MINUTES = 60

# refresh every 60 minutes
def should_refresh_eth_events():
    if eth_events_cache["last_update"] is None:
        return True
    now = datetime.now(eastern)
    return (now - eth_events_cache["last_update"]) > timedelta(minutes=ETH_REFRESH_INTERVAL_MINUTES)


def continuous_crypto_monitor(interval_minutes=5):
    """
    Continuously monitor weekly crypto markets, but only refresh ETH events
    when necessary (near period transitions).
    """
    print("[INFO] Starting continuous crypto market monitor...")
    active_threads = {}

    try:
        while True:
            # Fetch current weekly periods
            periods = get_week_periods()
            
            # Only refresh ETH events when necessary
            if should_refresh_eth_events():
                print("[INFO] Refreshing ETH events...")
                eth_events_cache["events"] = get_eth_events()
                eth_events_cache["last_update"] = datetime.now(eastern)
                print(f"[INFO] ETH events refreshed at {eth_events_cache['last_update']}")
            else:
                print(f"[INFO] Using cached ETH events from {eth_events_cache['last_update']}")
            
            # Print time until next period transition
            now = datetime.now(eastern)
            time_to_period_end = periods["current"]["end"] - now
            print(f"[INFO] Current period ends in {time_to_period_end}")

            # Monitor previous, current, and next week
            for p in [periods["previous"], periods["current"], periods["next"]]:
                if not p:
                    continue
                week_label = p["label"]

                for asset, slug_template in ASSETS.items():
                    if asset == "eth":
                        # Use cached ETH events
                        for event in eth_events_cache["events"]:
                            slug = event["slug"]
                            if slug not in active_threads or not active_threads[slug].is_alive():
                                print(f"Starting ETH market monitor for slug: {slug}")
                                t = threading.Thread(target=monitor_event, args=(slug,))
                                t.start()
                                active_threads[slug] = t

                    else:
                        # Use exact match for other assets (BTC)
                        slug = slug_template.format(week_label)
                        if slug not in active_threads or not active_threads[slug].is_alive():
                            print(f"Starting market monitor for slug: {slug}")
                            t = threading.Thread(target=monitor_event, args=(slug,))
                            t.start()
                            active_threads[slug] = t

            # Clean up closed threads to free memory
            active_threads = {s: t for s, t in active_threads.items() if t.is_alive()}

            print(f"[INFO] Active threads: {len(active_threads)}")
            time.sleep(interval_minutes * 60)

    except KeyboardInterrupt:
        print("\n[INFO] KeyboardInterrupt detected. Exiting...")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")


if __name__ == "__main__":
    continuous_crypto_monitor()
# %%
