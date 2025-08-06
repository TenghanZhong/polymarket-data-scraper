

# 📊 Github Poly Data

This repository contains a **tool-chain for gathering prediction-market and related financial data** from multiple sources. It consists of a variety of self-contained Python scripts that monitor, download, and record data from **Polymarket, Deribit, Kalshi, ESPN**, and **SportsDataIO**.

The goal is to create structured tables (in PostgreSQL or local CSV files) that can be used for quantitative analysis of crypto markets, sporting events, and option pricing.

-----

## ✨ Features

  * **Multi-Source Aggregation**: Pulls data from Polymarket, Deribit, Kalshi, ESPN, and SportsDataIO.
  * **Automated Tracking**: Scripts automatically discover and monitor active markets for crypto, MLB, and NBA events.
  * **Flexible Storage**: Saves structured data to a PostgreSQL database or falls back to local CSV files.
  * **Data Enrichment**: Combines data from multiple sources, like enriching Polymarket odds with real-time scores from ESPN.
  * **Modular Design**: Each data source and task is handled by a dedicated, self-contained module.
  * **Resilient**: Implements automatic retries and connection pooling for robust data collection.

-----

## 📁 Directory Layout

\<details open\>
\<summary\>Click to view repository structure\</summary\>

```text
Github_poly_data/
├── README.md            ← You are here
├── Crypto/              ← Scripts for Polymarket crypto markets
├── Deribit_Option/      ← Daily Deribit BTC option chain loader
├── MLB/                 ← Automatic tracking of MLB prediction markets
├── NBA/                 ← Automatic tracking of NBA prediction markets
├── poly_deribit/        ← Link Polymarket and Deribit data
├── utilities/           ← Shared database/API utilities
└── ... (logs, tests, requirements.txt, etc.)
```

\</details\>

-----

## 🛠️ Modules Overview

This project is organized into several modules, each responsible for a specific data collection task.

### 1\. ⚡ Crypto Markets

Scripts for monitoring hourly, weekly, and monthly crypto prediction markets on Polymarket.

  * `hourly_crypto.py`: Monitors hourly "up or down" markets for BTC/ETH. It logs bid/ask quotes every minute.
  * `monthly_crypto.py`: Tracks monthly price-target markets for assets like BTC, XRP, and ETH.
  * `weekly_crypto.py`: Follows weekly price-target markets for ETH and other assets.
  * `poly_interval_loader.py`: Discovers all active scalar interval markets (e.g., "BTC price between $X and $Y") and logs minute-level snapshots for all price brackets.

### 2\. 📈 Deribit Option Chain

Fetches and processes the entire BTC option chain from the Deribit exchange.

  * `daily_deribit.py`: Uses `ccxt` to fetch the BTC option chain, filters for options expiring within six months, and outputs the data to pandas DataFrames or CSV files.
  * `utilities/deribit/`: Contains reusable functions for fetching BTC/ETH option chains and calculating greeks (delta, gamma, vega, theta).

### 3\. ⚾ MLB Markets

Automatically tracks and records data for Major League Baseball prediction markets on Polymarket.

  * `MLB_Auto.py`: Scans for active MLB markets, extracts the competing teams, and records time-stamped bid/ask quotes along with the current inning and score for each game.

### 4\. 🏀 NBA Markets

Automatically discovers and monitors NBA game and series markets.

  * `NBA_Auto.py`: Finds NBA markets, extracts teams, and logs quotes, period (quarter), score, and a live status flag.
  * `nba_combined_data.py`: Enriches Polymarket data by fetching real-time scores from ESPN's API and merging the datasets.
  * `nba_sportsdataio.py`: Uses the SportsDataIO API to poll play-by-play data and live odds for a specific game, aligning and saving the combined data.

### 5\. 🔗 Poly-Deribit Link

Connects Polymarket crypto events to corresponding options on Deribit.

  * `daily_poly_deribit_loader.py`: Finds Polymarket crypto price-target events, infers the strike price, matches it to the closest option on Deribit with the same expiry, and logs prices from both sources side-by-side.

### 6\. 🛠️ Shared Utilities

Core modules providing shared functionality used across the tool-chain.

  * `db_utils.py`: Manages a threaded PostgreSQL connection pool and includes helpers for dynamic table creation and data insertion. **Database credentials must be configured here.**
  * `kalshi/`: A client for the Kalshi trading API, including RSA key signing and a data downloader.
  * `polymarket/`: Low-level functions for interacting with the Polymarket Gamma API and Apify.

-----

## 🚀 Getting Started

Follow these steps to set up the environment and run the data collection scripts.

### Prerequisites

  * Python 3.9+
  * PostgreSQL Database

### Installation & Configuration

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/your-username/Github_poly_data.git
    cd Github_poly_data
    ```

2.  **Create and activate a virtual environment:**

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    # On Windows, use: venv\Scripts\activate
    ```

3.  **Install the required packages:**

    ```bash
    pip install -r requirements.txt
    ```

    Alternatively, you can install them manually:

    ```bash
    pip install requests ccxt psycopg2 pandas numpy pytz zoneinfo cryptography apify-client
    ```

4.  **Configure services:**

      * **Database**: Open `utilities/db_utils.py` and edit the `DB_CONFIG` dictionary with your PostgreSQL credentials (host, port, dbname, user, password).
      * **SportsDataIO**: Set your `API_KEY` and the desired `GAME_ID` in `nba_sportsdataio.py`.
      * **Kalshi**: Place your RSA private key in `utilities/kalshi/kalshi_private_key.txt` and set your key ID for authenticated calls.
      * **Apify**: Set your `APIFY_API_TOKEN` in `utilities/polymarket/polymarket_download.py` or as an environment variable.

### Usage

Navigate into a module's directory and run the desired Python script.

```bash
# Example: Track hourly crypto markets
cd Crypto/
python3 hourly_crypto.py

# Example: Fetch the daily Deribit BTC option chain
cd ../Deribit_Option/
python3 daily_deribit.py

# Example: Monitor MLB markets continuously
cd ../MLB/
python3 MLB_Auto.py
```

Logs are written to a `logs/` sub-folder within each module. Data is stored in your configured PostgreSQL database, with tables created dynamically under schemas like `hourly_crypto`, `MLB`, `NBA`, etc. If the database connection fails, scripts will fall back to writing data to a `local_backup/` directory.

-----

## 🤝 Contributing

This project is primarily for internal data collection and research. However, contributions are welcome\! If you'd like to add a new data source or improve the tool-chain, please open an issue to discuss your idea or submit a pull request.
