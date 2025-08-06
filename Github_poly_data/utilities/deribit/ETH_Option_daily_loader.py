import time
import schedule
import logging
import pandas as pd
from datetime import datetime

from psycopg2 import sql
from psycopg2.extras import execute_values

from utilities.db_utils import get_connection, release_connection
from utilities.deribit.ETH_Option_Chain import ETH_Option_Chain

SCHEMA_NAME = "Crypto_Option"
TABLE_MAIN = "deribit_eth_options"
TABLE_SYN = "deribit_eth_options_syn"
RUN_TIME_UTC = "00:10"

# ▼▼▼ 关键改动：在表结构中增加所有新字段 ▼▼▼
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    run_ts        TIMESTAMPTZ,
    spot_px       DOUBLE PRECISION,
    symbol        TEXT,
    type          TEXT,
    strike        DOUBLE PRECISION,
    bid_coin      DOUBLE PRECISION,
    ask_coin      DOUBLE PRECISION,
    bid_usd       DOUBLE PRECISION,
    ask_usd       DOUBLE PRECISION,
    expiry        TIMESTAMPTZ,
    iv            DOUBLE PRECISION,
    underlying    TEXT,
    contractSize  DOUBLE PRECISION,
    volume_coin   DOUBLE PRECISION,
    volume_usd    DOUBLE PRECISION,
    delta         DOUBLE PRECISION,
    gamma         DOUBLE PRECISION,
    vega          DOUBLE PRECISION,
    theta         DOUBLE PRECISION,
    PRIMARY KEY (run_ts, symbol)
);
"""

# ▼▼▼ 关键改动：在插入语句中增加所有新字段 ▼▼▼
INSERT_SQL = """
INSERT INTO {schema}.{table} (
    run_ts, spot_px, symbol, type, strike, expiry,
    iv, underlying, contractSize,
    bid_coin, ask_coin, bid_usd, ask_usd,
    volume_coin, volume_usd,
    delta, gamma, vega, theta
) VALUES %s
ON CONFLICT DO NOTHING;
"""

def pg_batch_insert(df, table_base_name, symbol_name="eth"):
    if df.empty:
        return
    df = df.copy()

    df['run_ts'] = pd.to_datetime(df['utc_ts'])
    table_name = f"{table_base_name}_{df['run_ts'].iloc[0].strftime('%Y%m%d')}"

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA_NAME)))
            cur.execute(
                sql.SQL(CREATE_SQL).format(schema=sql.Identifier(SCHEMA_NAME), table=sql.Identifier(table_name)))

            # ▼▼▼ 关键改动：在待插入列中增加所有新字段 ▼▼▼
            columns_to_insert = [
                "run_ts", "spot_px", "symbol", "type", "strike", "expiry",
                "iv", "underlying", "contractSize",
                "bid_coin", "ask_coin", "bid_usd", "ask_usd",
                "volume_coin", "volume_usd",
                "delta", "gamma", "vega", "theta"
            ]

            if not all(col in df.columns for col in columns_to_insert):
                missing_cols = [col for col in columns_to_insert if col not in df.columns]
                logging.error(f"DataFrame is missing required columns for table {table_name}: {missing_cols}")
                return

            execute_values(
                cur,
                sql.SQL(INSERT_SQL).format(schema=sql.Identifier(SCHEMA_NAME), table=sql.Identifier(table_name)),
                df[columns_to_insert].to_records(index=False).tolist()
            )
        conn.commit()
    finally:
        release_connection(conn)


def daily_job():
    logging.info("⏳  Fetching Deribit ETH option chain (with volume & greeks)...")
    try:
        df_main, df_syn, df_skip = ETH_Option_Chain()
        pg_batch_insert(df_main, TABLE_MAIN, symbol_name="eth")
        pg_batch_insert(df_syn, TABLE_SYN, symbol_name="eth")
        logging.info(f"✅  main={len(df_main):4d}  syn={len(df_syn):4d}  skipped={len(df_skip):4d}")
    except Exception as e:
        logging.error(f"❌  An error occurred during the daily job: {e}", exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    daily_job()
    schedule.every().day.at(RUN_TIME_UTC).do(daily_job)

    logging.info(f"🕑  Scheduler started — will run daily at {RUN_TIME_UTC} UTC")

    while True:
        schedule.run_pending()
        time.sleep(60)