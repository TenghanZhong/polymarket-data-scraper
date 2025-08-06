import time
import threading
import requests
import pytz
import re
import sys
from datetime import datetime
from pathlib import Path
from psycopg2.extras import execute_values
from psycopg2 import sql
import os

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from utilities.db_utils import get_connection, release_connection
from utilities.polymarket.monitor_event import get_event_by_slug, is_market_active, extract_markets_data, save_market_data_locally

# Constants
ET_TIMEZONE = pytz.timezone('US/Eastern')
GAMMA_API_BASE_URL = "https://gamma-api.polymarket.com"
ESPN_API_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"

def save_market_and_score_data_locally(slug, timestamp, markets_data, score_data):
    """
    Save both market data and corresponding score data to a local CSV when DB insert fails.
    """
    local_dir = "local_backup"
    os.makedirs(local_dir, exist_ok=True)
    filename = os.path.join(local_dir, f"{slug.replace('-', '_')}.csv")

    file_exists = os.path.isfile(filename)

    with open(filename, mode="a", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)

        # Header on first write
        if not file_exists:
            writer.writerow([
                "timestamp",
                "question",
                "best_bid",
                "best_ask",
                "home_team",
                "away_team",
                "home_score",
                "away_score",
                "status",
                "clock",
                "quarter"
            ])

        for market in markets_data:
            writer.writerow([
                timestamp.isoformat(),
                market.get("question"),
                market.get("best_bid"),
                market.get("best_ask"),
                score_data.get("home_team"),
                score_data.get("away_team"),
                score_data.get("home_score"),
                score_data.get("away_score"),
                score_data.get("status"),
                score_data.get("clock"),
                score_data.get("quarter"),
            ])

def get_nba_events(today_only=True):
    """
    Fetch NBA events from Polymarket, optionally filtering for today's events.
    """
    url = f"{GAMMA_API_BASE_URL}/events"
    
    params = {
        "archived": False,
        "closed": False,
        "series_slug": "nba",  # Filter by NBA series
    }
    
    # If today_only is True, add date filter
    if today_only:
        today_et = datetime.now(ET_TIMEZONE).date()
        params["event_date"] = today_et.strftime("%Y-%m-%d")
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        events = response.json()
        
        # Double-check filtering with slug pattern for safety
        nba_slug_pattern = re.compile(r'^nba-[a-z]+-[a-z]+-\d{4}-\d{2}-\d{2}$')
        
        return [event for event in events if nba_slug_pattern.match(event.get("slug", ""))]

    except Exception as e:
        print(f"Exception fetching NBA events: {e}")
        return []
    
def fetch_nba_scores():
    """Fetch NBA scores from ESPN."""
    try:
        response = requests.get(ESPN_API_URL, timeout=5)
        response.raise_for_status()
        data = response.json()
        games = data.get("events", [])

        scores = {}
        for game in games:
            competition = game.get("competitions", [])[0]
            if not competition or len(competition["competitors"]) != 2:
                continue

            home_team = competition["competitors"][0]
            away_team = competition["competitors"][1]

            home_abbr = home_team["team"].get("abbreviation", "N/A")
            away_abbr = away_team["team"].get("abbreviation", "N/A")
            home_full = home_team["team"].get("displayName", "")
            away_full = away_team["team"].get("displayName", "")
            home_score = int(home_team.get("score", 0))
            away_score = int(away_team.get("score", 0))
            status = competition["status"]["type"].get("description", "N/A")
            clock = competition["status"].get("displayClock", "N/A")
            quarter = competition["status"].get("period", 0)

            # Generate both slug formats
            game_date = datetime.now().strftime('%Y-%m-%d')
            key1 = f"nba-{home_abbr.lower()}-{away_abbr.lower()}-{game_date}"
            key2 = f"nba-{away_abbr.lower()}-{home_abbr.lower()}-{game_date}"

            score_entry = {
                "home_team": home_abbr,
                "away_team": away_abbr,
                "home_full": home_full,
                "away_full": away_full,
                "home_score": home_score,
                "away_score": away_score,
                "status": status,
                "clock": clock,
                "quarter": quarter if isinstance(quarter, int) else None
            }

            scores[key1] = score_entry
            scores[key2] = score_entry  # same data under both keys

        return scores

    except Exception as e:
        print(f"Error fetching NBA scores: {e}")
        return {}

def normalize_name(full_team_name):
    """Extract mascot (last word) and lowercase it."""
    # e.g. full team name: "New York Knicks" -> normalized: "knicks"
    return full_team_name.strip().split()[-1].lower() if full_team_name else ""

def find_matching_score_data(slug, scores, markets_data):
    """Find score data by exact slug match or fuzzy mascot matching."""
    
    # Step 1: Try exact slug match first
    if slug in scores:
        return scores[slug]
    
    print(f"[{slug}] ⚠️ No exact slug match. Attempting mascot-based matching...")
    
    # print(f"available keys in scores {scores.keys()}")
    # Step 2: Extract all mascots from market questions
    question_mascots = set()
    for market in markets_data:
        question = market.get("question", "").lower()
        # question_mascots: all lowercase words that appear in any market question, e.g. "knicks", "pacers"
        words = question.replace(":", " ").replace("(", " ").replace(")", " ").split()
        question_mascots.update(words)
    
    # Step 3: Find score entry where both team mascots appear in questions
    for score_data in scores.values():
        home_mascot = normalize_name(score_data.get("home_full", ""))
        away_mascot = normalize_name(score_data.get("away_full", ""))
        
        # Check if either mascot appears in the question
        if home_mascot in question_mascots or away_mascot in question_mascots:
            print(f"[{slug}] ✅ Fuzzy match successful using mascots: {home_mascot}, {away_mascot}")
            return score_data
    
    print(f"[{slug}] ❌ No suitable match found. Question mascots: {question_mascots}")
    print(f"home mascot: {home_mascot}, away mascot: {away_mascot}")
    return None

def monitor_event_with_scores(slug, interval_seconds=60):
    """Monitor a specific market with score data"""
    conn = get_connection()
    schema_name = "sports"
    table_name = slug.replace("-", "_")

    try:
        # Ensure table exists
        with conn.cursor() as cur:
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.{} (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL,
                    event_slug TEXT NOT NULL,
                    question TEXT,
                    best_bid NUMERIC(10, 6),
                    best_ask NUMERIC(10, 6),
                    home_team TEXT,
                    away_team TEXT,
                    home_score INT,
                    away_score INT,
                    status TEXT,
                    clock TEXT,
                    quarter INT
                );
            """).format(sql.Identifier(schema_name), sql.Identifier(table_name)))
            conn.commit()
            print(f"[{slug}] Ensured table exists: {schema_name}.{table_name}")

        # Monitor the market until it closes
        while True:
            # Fetch fresh market snapshot
            event_data = get_event_by_slug(slug)
            if not event_data or not is_market_active(event_data):
                print(f"[{slug}] Market closed or not found. Exiting.")
                break

            current_time = datetime.now()
            scores = fetch_nba_scores()
            markets_data = extract_markets_data(event_data)
            
            # matching
            score_data = find_matching_score_data(slug, scores, markets_data)

            if not score_data:
                score_data = {
                    "home_team": None,
                    "away_team": None,
                    "home_score": None,
                    "away_score": None,
                    "status": None,
                    "clock": None,
                    "quarter": None
                }

            rows = []
            for market in markets_data:
                rows.append((
                    current_time, slug, market.get("question"), market.get("best_bid"), market.get("best_ask"),
                    score_data.get("home_team"), score_data.get("away_team"),
                    score_data.get("home_score"), score_data.get("away_score"),
                    score_data.get("status"), score_data.get("clock"), score_data.get("quarter")
                ))

            try:
                with conn.cursor() as cur:
                    execute_values(cur, sql.SQL("""
                        INSERT INTO {}.{} (
                            timestamp, event_slug, question, best_bid, best_ask,
                            home_team, away_team, home_score, away_score, status, clock, quarter
                        ) VALUES %s
                    """).format(sql.Identifier(schema_name), sql.Identifier(table_name)), rows)
                    conn.commit()
                    print(f"[{slug}] Inserted {len(rows)} rows into {schema_name}.{table_name} at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            except Exception as e:
                print(f"[{slug}] Database error, saving locally: {e}")
                save_market_and_score_data_locally(slug, current_time, markets_data, score_data)

            time.sleep(interval_seconds)

    except Exception as e:
        print(f"[{slug}] Error in monitor_event_with_scores: {e}")

    finally:
        release_connection(conn)
        print(f"[{slug}] Released database connection")


def monitor_nba_events_continuously(interval_seconds=60, refresh_interval_minutes=5):
    """
    Continuously monitors NBA events on Polymarket with score data.
    Every refresh_interval_minutes, fetches the current set of NBA events and monitors any new ones.
    """
    print(f"[{datetime.now(ET_TIMEZONE)}] Starting continuous NBA event monitoring with scores...")
    active_threads = {}

    try:
        while True:
            print(f"[{datetime.now(ET_TIMEZONE)}] Refreshing NBA events list...")
            events = get_nba_events(today_only=True)
            print(f"Found {len(events)} active NBA events")

            for event in events:
                slug = event.get('slug')
                
                # Start a new thread if not already active
                if slug not in active_threads or not active_threads[slug].is_alive():
                    print(f"Starting monitor for new event: {slug}")
                    thread = threading.Thread(target=monitor_event_with_scores, args=(slug, interval_seconds))
                    thread.daemon = True  # Make thread exit when main thread exits
                    active_threads[slug] = thread
                    thread.start()

            # Clean up finished threads
            active_threads = {slug: t for slug, t in active_threads.items() if t.is_alive()}
            time.sleep(refresh_interval_minutes * 60)

    except KeyboardInterrupt:
        print("\nInterrupted. Stopping continuous NBA monitor...")

if __name__ == "__main__":
    interval = 60  # Monitor update interval
    refresh_minutes = 5  # How often to check for new events

    monitor_nba_events_continuously(interval_seconds=interval, refresh_interval_minutes=refresh_minutes)
