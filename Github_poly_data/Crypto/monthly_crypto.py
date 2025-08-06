# monthly crypto data downloading
import threading
import time
from datetime import datetime, timedelta
import pytz
import sys
from pathlib import Path

# Setup imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.append(str(PROJECT_ROOT))

from utilities.polymarket.monitor_event import monitor_event

# Constants
eastern = pytz.timezone("US/Eastern")
base_start = eastern.localize(datetime(2025, 1, 1, 0, 0))  # January 2025 start point

ASSETS = {
    "btc": "what-price-will-bitcoin-hit-in-{}",
    "xrp": "what-price-will-xrp-hit-in-{}",
    "eth": "what-price-will-ethereum-hit-in-{}"
}

# Helper to format month slugs
def format_month_label(date):
    return date.strftime("%B").lower()

# Generate monthly periods
def get_month_periods():
    now = datetime.now(eastern)
    current = base_start
    periods = []

    for _ in range(24):  # 2 years ahead
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        periods.append({
            "start": current,
            "end": next_month,
            "month_label": format_month_label(current)
        })
        if current <= now < next_month:
            idx = len(periods) - 1
            break
        current = next_month

    return {
        "previous": periods[idx - 1] if idx > 0 else None,
        "current": periods[idx],
        "next": periods[idx + 1] if idx + 1 < len(periods) else None,
    }

# Main runner
def find_and_monitor_crypto_markets():
    try:
        periods = get_month_periods()
        threads = []

        for p in [periods["current"], periods["previous"], periods["next"]]:
            if not p:
                continue
            month_label = p["month_label"]

            for asset, slug_template in ASSETS.items():
                slug = slug_template.format(month_label)
                print(f"Checking market slug: {slug}")

                # Always create a new thread for each slug
                t = threading.Thread(target=monitor_event, args=(slug,))
                t.start()
                threads.append(t)

        # Wait for all threads to complete
        for t in threads:
            t.join()

    except KeyboardInterrupt:
        print("\nKeyboardInterrupt detected. Exiting...")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    find_and_monitor_crypto_markets()