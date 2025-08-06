
📊 Github Poly Data
This repository collects a tool-chain for gathering prediction-market and related financial data from multiple sources. It consists of a variety of self-contained Python scripts that monitor, download, and record data from Polymarket, Deribit, and more. The goal is to create structured tables (in PostgreSQL) that can be used for quantitative analysis of crypto markets, sporting events, and option pricing.

📁 Directory layout
Github_poly_data/
├── README.md              ← original readme with high-level event list
├── Crypto/                ← scripts for Polymarket crypto markets
├── Deribit_Option/        ← daily Deribit BTC option chain loader
├── MLB/                   ← automatic tracking of MLB prediction markets
├── NBA/                   ← automatic tracking of NBA prediction markets
├── poly_deribit/          ← link Polymarket and Deribit data
├── utilities/             ← shared database/API utilities
└── ... (logs, tests, etc.)
⚡ Crypto – hourly / weekly / monthly markets
hourly_crypto.py – Launches a process every hour (in ET) to monitor the “Bitcoin/Ethereum up or down” yes/no markets for that hour. It generates the current slug (e.g. bitcoin-up-or-down-august-6-2pm-et), creates a table in the hourly_crypto schema, and logs yes/no bid/ask quotes every minute.

monthly_crypto.py – Monitors monthly price-target markets such as “what price will bitcoin hit in month”; it creates threads for the current, previous and next month for each asset (btc, xrp, eth).

weekly_crypto.py – Tracks weekly price markets (e.g. “bitcoin price on July-14”); it keeps a cache of active ETH events and refreshes the list hourly, then launches threads to monitor the previous, current, and next weekly events.

poly_interval_loader.py – Discovers all active scalar interval markets on Polymarket that mention btc/eth. Each qualifying event spawns its own process that creates a table in the polymarket_interval_only schema and writes minute-level snapshots for all price brackets (low/high bounds, yes/no bid/ask).

test_hourly_crypto.py – Small test harness that prints the slugs and expiry calculation for the hourly tracker.

These scripts rely on the shared database helper in utilities/db_utils.py to obtain a connection from a pool and to create per-event tables on the fly. They connect to the Polymarket Gamma API for event metadata and prices, disable SSL verification, and employ automatic retries. Log files are written under Crypto/logs/.

📈 Deribit Option Chain
daily_deribit.py – Uses ccxt to fetch the entire BTC option chain from Deribit. It parses various expiry formats (e.g. 4JUL25, 250624), filters options expiring within the next six months, and returns three pandas DataFrames: main contracts, synthetic contracts, and skipped rows. It can be run stand-alone to produce CSVs for further analysis.

See also utilities/deribit/ for reusable functions to fetch BTC and ETH option chains and write them to CSV with additional greeks (delta, gamma, vega, theta).

⚾ MLB – baseball market monitoring
MLB_Auto.py – Scans active Polymarket events tagged with mlb, parses the market question to extract the two competing teams, and launches a separate process per game. Each process writes a table in the MLB schema recording the timestamp, the teams, best bid/ask for both sides, and current inning and score (added in v14). It sleeps until just before first pitch and stops when the market closes.

test_MLB.py, test_keyword.py, test_ongoing.py – Test harnesses that validate the event filtering logic and verify that active games are correctly recognised as “upcoming” or “in progress”. They query the live API once and check that required price fields (bestBid, bestAsk) are present.

🏀 NBA – basketball market monitoring
NBA_Auto.py – Automatically discovers NBA series markets (e.g. nba-bos-lal-2025-06-08), extracts the two teams from the question, and monitors markets for each game. It writes to the NBA schema and records period (quarter), score, and a boolean is_live flag, in addition to bid/ask quotes.

nba_combined_data.py – Combines Polymarket data with ESPN’s scoreboard API to enrich markets with official scores. It fetches today’s NBA events, calls ESPN to get real-time scores, matches teams via slugs or mascot names, and saves the merged snapshots to Postgres or local CSV.

nba_sportsdataio.py – Example script for using the SportsDataIO replay API. It polls play-by-play and live odds for a specific game (configured via GAME_ID and API_KEY) every 15 seconds, aligns them on the time axis, forward-fills odds, and writes the combined dataset to a CSV file.

Nba_test.py – Simple test wrapper for the auto-tracker.

🔗 Poly–Deribit link
daily_poly_deribit_loader.py – Discovers active Polymarket crypto events whose title includes bitcoin/btc and a dollar price (e.g. “Will Bitcoin be above $70 000 on June 30”). For each such event it infers a strike and direction, matches it against the Deribit option chain to find the closest call/put option with the same expiry, and then continuously logs both Polymarket yes/no prices and Deribit option quotes into a dynamic table under the deribit_polymarket schema.

deribit_poly.py – Command-line tool that performs the above linking for a single Polymarket event; it requires the slug as an argument.

deribit_poly_interval.py – Collects all price intervals of a given Polymarket event into a common table polymarket_only.pm_intervals, recording low/high bounds and yes/no prices every minute.

🛠️ Shared utilities
db_utils.py – Initializes a threaded PostgreSQL connection pool. It defines helper functions to get and release connections, determine a schema name from a slug, create tables if they do not exist, and insert market data. Database credentials (dbname, user, password, host, port) are configured at the top of this file; adjust them before running scripts.

deribit/ – Scripts to download BTC or ETH option chains and daily loaders to persist them to CSV, including calculation of greeks.

kalshi/ – Provides a client for the Kalshi trading API (kalshi_api.py) with support for RSA key signing, and a downloader (kalshi_download_free.py) that fetches market data and writes to Postgres using db_utils. A sample kalshi_private_key.txt is included as a placeholder; insert your own RSA private key for authenticated calls.

polymarket/ – Functions to monitor a single Polymarket event (monitor_event.py), download events via Apify (polymarket_download.py), or use only the public Gamma API (polymarket_download_free.py). These modules are used by the higher-level scripts but can also be run stand-alone.

📦 Requirements
Scripts require Python 3.9+ and the following packages:

Bash

pip install requests ccxt psycopg2 pandas numpy pytz zoneinfo cryptography apify-client
Notes:

Polymarket data is publicly accessible via the Gamma API (no key required).

Database – edit DB_CONFIG in utilities/db_utils.py.

SportsDataIO – set your API key in nba_sportsdataio.py (API_KEY) and pick the desired GAME_ID.

Kalshi – provide an RSA private key (kalshi_private_key.txt) and key ID.

Apify – set APIFY_API_TOKEN in polymarket_download.py or via environment variables.

▶️ Running the scripts
Create a virtual env & install deps

Bash

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt  # or install packages listed above
Configure PostgreSQL (utilities/db_utils.py)

Run what you need

Bash

# hourly bitcoin/ethereum markets
cd Crypto
python3 hourly_crypto.py

# Deribit BTC option chain
cd ../Deribit_Option
python3 daily_deribit.py

# MLB markets
cd ../MLB
python3 MLB_Auto.py

# Single Polymarket/Deribit link (replace slug)
cd ../poly_deribit
python3 deribit_poly.py bitcoin-price-on-july-31
Logs are saved under each module’s logs/ directory. Tables are created in PostgreSQL under schemas such as hourly_crypto, MLB, NBA, deribit_polymarket, etc. If DB connectivity fails, data is backed up to CSV files in local_backup/.

🤝 Contributing
This project is primarily for internal data collection and research.
