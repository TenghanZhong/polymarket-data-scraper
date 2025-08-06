import psycopg2
from psycopg2 import sql, pool

import threading
import logging 

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DatabasePool:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._instance.init_pool(*args, **kwargs)
        return cls._instance

    def init_pool(self, minconn=5, maxconn=20, **db_params):
        self.pool = pool.ThreadedConnectionPool(minconn, maxconn, **db_params)
        logging.info(f"Database connection pool initialized with min {minconn} and max {maxconn} connections")

    def get_connection(self):
        return self.pool.getconn()

    def release_connection(self, conn):
        self.pool.putconn(conn)

    def close_all_connections(self):
        self.pool.closeall()
        logging.info("Closed all database connections")

# Old database
# DB_CONFIG = {
#     "dbname": "pgvectordb",
#     "user": "admin",
#     "password": "xsf77$axsjlz0",
#     "host": "128.97.86.114",
#     "port": "30882",
# }

# new database
DB_CONFIG = {
    "dbname": "pgvector",
    "user": "postgres",
    "password": "G1Amb10kDD2lW15C2wJB0Q==",
    "host": "100.81.189.125",
    "port": "4876",
}

# Initialize the global database pool (shared across scripts)
db_pool = DatabasePool(
    minconn=5,
    maxconn=30,
    **DB_CONFIG
)

def get_connection():
    return db_pool.get_connection()

def release_connection(conn):
    db_pool.release_connection(conn)

def get_schema_from_slug(slug: str) -> str:
    slug = slug.lower()
    if slug.startswith("kxhigh"):
        return "kalshi_temperature"
    elif slug.startswith("elon") or slug.startswith("elonmusk"):
        return "polymarket_tweets"
    elif slug.startswith("highest_temperature"):
        return "polymarket_temperature"
    elif slug.startswith("nba"):
        return "sports"
    elif slug.startswith("what_price_will_") or slug.startswith("bitcoin_price") or slug.startswith("kxeth") or slug.startswith("kxbtc"):
        return "crypto"
    return "public"

def ensure_table_exists(conn, slug):
    table_name = slug.replace("-", "_")
    schema_name = get_schema_from_slug(table_name)

    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP,
                question TEXT,
                best_bid FLOAT,
                best_ask FLOAT
            );
        """).format(sql.Identifier(schema_name), sql.Identifier(table_name)))
        conn.commit()

def insert_market_data(conn, slug, timestamp, markets_data):
    table_name = slug.replace("-", "_")
    schema_name = get_schema_from_slug(table_name)

    with conn.cursor() as cur:
        for market in markets_data:
            cur.execute(sql.SQL("""
                INSERT INTO {}.{} (timestamp, question, best_bid, best_ask)
                VALUES (%s, %s, %s, %s)
            """).format(sql.Identifier(schema_name), sql.Identifier(table_name)),
            (timestamp, market['question'], market['best_bid'], market['best_ask']))
        conn.commit()


def insert_kalshi_market_data(conn, slug, timestamp, markets_data):
    table_name = slug.replace("-", "_")
    schema_name = get_schema_from_slug(table_name)

    print(f"[INFO] Inserting into schema '{schema_name}' | table '{table_name}'")  # Debug log

    with conn.cursor() as cur:
        for market in markets_data:
            cur.execute(sql.SQL("""
                INSERT INTO {}.{} (timestamp, question, best_bid, best_ask)
                VALUES (%s, %s, %s, %s)
            """).format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name)
            ),
            (timestamp, market['ticker'], market['best_bid'], market['best_ask']))
        conn.commit()
