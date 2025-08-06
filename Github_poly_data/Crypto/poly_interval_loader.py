#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
polymarket_interval_auto.py (Final Version with Yes/No Prices, No Prefix)

功能:
  自动发现并为每一个符合条件的 Polymarket "区间预测" (scalar) 事件启动一个
  独立的跟踪进程。每个进程会为对应的事件创建一张专属的数据库表，然后
  持续采集该事件下 *所有* 价格区间 (market) 的行情数据（包括Yes和No盘口），
  并写入该表中，直到市场过期后自动停止。

数据库架构:
  每个事件一张表。表名格式: polymarket_interval.<event_slug>
  表内为长数据格式，每行代表一个价格区间在一个时间点的状态。
"""
from __future__ import annotations
import re
import sys
import time
import logging
import requests
from datetime import datetime, timezone, time as dt_time
from multiprocessing import Process
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Set
from logging.handlers import RotatingFileHandler

# DB imports
import psycopg2
from psycopg2 import sql, extras
from utilities.db_utils import get_connection, release_connection

# ───── 通用配置 ─────────────────────────
GAMMA_API = "https://gamma-api.polymarket.com/events"
PAGE_SIZE = 200
SAMPLE_SECS = 60
DISCOVER_SEC = 3600 * 24
KEYWORDS = ("btc", "bitcoin", "eth", "ethereum")
MIN_INTERVALS = 3
SCHEMA = "polymarket_interval_only"

# ───── 日志 ─────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)


def setup_logger(name: str) -> logging.Logger:
    log_name = name.split('/')[-1]
    logfile = LOG_DIR / f"{log_name}.log"
    logger = logging.getLogger(name)
    if not logger.handlers:
        fmt = "%(asctime)s %(levelname)s %(message)s"
        fh = RotatingFileHandler(logfile, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
        sh = logging.StreamHandler(sys.stdout)
        for h in (fh, sh): h.setFormatter(logging.Formatter(fmt))
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
    return logger


# ───── interval 辅助函数 ──────────────────
PRICE_RE = re.compile(r"\$\s*(\d[\d,]*\.?\d*)([kKmMbB]?)\b")
SUFFIX = {"": 1, "K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
CUT_DATE_RE = re.compile(r"\s+on\s+\w+\s+\d{1,2}", flags=re.IGNORECASE)
LOW_WORDS = ("<", "less", "under", "below", "at most", "dip")
HIGH_WORDS = (">", "greater", "above", "over", "at least", "up to")


def extract_numbers(label: str) -> List[float]:
    tokens = PRICE_RE.findall(label)
    last_suf = next((s for _, s in reversed(tokens) if s), '')
    return [float(n.replace(",", "")) * SUFFIX[s.upper()] for n, s in tokens]


def parse_interval(label: str) -> Tuple[Optional[float], Optional[float]]:
    label = CUT_DATE_RE.sub("", label)
    ltxt = label.lower()
    nums = extract_numbers(label)
    if not nums: return None, None
    if any(w in ltxt for w in LOW_WORDS): return None, nums[0]
    if any(w in ltxt for w in HIGH_WORDS): return nums[0], None
    if len(nums) >= 2: return tuple(sorted(nums[:2]))
    return nums[0], nums[0]


# ───── 数据库 Schema & Helpers ──────────────────
COLS = ("ts_utc,event_slug,market_id,market_label,low_bound,high_bound,pm_expiry,"
        "yes_bid,yes_ask,no_bid,no_ask")

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {{schema}}.{{table_name}} (
  ts_utc          TIMESTAMPTZ,
  event_slug      TEXT,
  market_id       BIGINT,
  market_label    TEXT,
  low_bound       DOUBLE PRECISION,
  high_bound      DOUBLE PRECISION,
  pm_expiry       TIMESTAMPTZ,
  yes_bid         DOUBLE PRECISION,
  yes_ask         DOUBLE PRECISION,
  no_bid          DOUBLE PRECISION,
  no_ask          DOUBLE PRECISION,
  PRIMARY KEY (ts_utc, market_id)
);
"""


def ensure_dynamic_table(table_name: str):
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA)))
            cur.execute(sql.SQL(CREATE_SQL).format(
                schema=sql.Identifier(SCHEMA),
                table_name=sql.Identifier(table_name)
            ))
    finally:
        release_connection(conn)


def insert_rows(table_name: str, rows: List[Tuple]):
    if not rows: return
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            extras.execute_values(
                cur,
                sql.SQL("INSERT INTO {schema}.{table_name} ({cols}) VALUES %s ON CONFLICT DO NOTHING").format(
                    schema=sql.Identifier(SCHEMA),
                    table_name=sql.Identifier(table_name),
                    cols=sql.SQL(COLS)
                ),
                rows
            )
        conn.commit()
    except psycopg2.Error as e:
        logging.error("DB insert error for table %s: %s", table_name, e)
        conn.rollback()
    finally:
        release_connection(conn)


def fetch_event_details(slug: str) -> Optional[Dict]:
    try:
        params = {"slug": slug, "includeMarkets": "true"}
        r = requests.get(GAMMA_API, params=params, timeout=10)
        r.raise_for_status()
        results = r.json()
        return results[0] if results else None
    except (requests.RequestException, IndexError) as e:
        logging.warning("API call failed for slug '%s': %s", slug, e)
        return None


# ───── 拉取 & 过滤 ───────────────────────
DATE_IN_SLUG = re.compile(
    r'-(?P<month>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)-(?P<day>\d{1,2})',
    flags=re.IGNORECASE)
MONTH_MAP = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10,
             "nov": 11, "dec": 12}


def fetch_all_events() -> List[Dict]:
    events, offset = [], 0
    while True:
        params = {"archived": False, "active": True, "tag_slug": "crypto", "includeMarkets": True, "limit": PAGE_SIZE,
                  "offset": offset}
        try:
            r = requests.get(GAMMA_API, params=params, timeout=10)
            r.raise_for_status()
            page = r.json()
            if not page: break
            events.extend(page)
            offset += PAGE_SIZE
            time.sleep(0.2)
        except requests.RequestException as e:
            logging.warning(f"API fetch failed at offset {offset}: {e}")
            time.sleep(5)
    return events


def filter_interval_events(events: List[Dict]) -> List[Tuple[str, datetime]]:
    out: List[Tuple[str, datetime]] = []
    now = datetime.now(timezone.utc)
    for ev in events:
        title = (ev.get("title") or "").lower()
        if not any(k in title for k in KEYWORDS): continue
        markets = ev.get("markets", [])
        ivs = {parse_interval(m.get("title", "") or m.get("question", "")) for m in markets}
        ivs.discard((None, None))
        if len(ivs) < MIN_INTERVALS: continue
        end_dt: Optional[datetime] = None
        end_str = ev.get("endTime") or ev.get("closeTime")
        if end_str:
            try:
                tmp = datetime.fromisoformat(end_str.replace("Z", "+00:00")).astimezone(timezone.utc)
                if tmp > now: end_dt = tmp
            except ValueError:
                pass
        if end_dt is None:
            m = DATE_IN_SLUG.search(ev.get("slug", ""))
            if m:
                mon, day = m.group("month")[:3].lower(), int(m.group("day"))
                try:
                    slug_dt = datetime.combine(datetime(now.year, MONTH_MAP[mon], day).date(), dt_time(23, 59, 59),
                                               tzinfo=timezone.utc)
                    if slug_dt > now: end_dt = slug_dt
                except ValueError:
                    pass
        if end_dt: out.append((ev["slug"], end_dt))
    return out


# ───── 跟踪 & 写库 ──────────────────
def track_one(event_slug: str, expiry_dt: datetime):
    logger = setup_logger(event_slug)

    # 【改动】去掉 "pm_" 前缀
    table_name = re.sub(r'[^a-z0-9_]', '_', event_slug.lower())

    conn = None
    try:
        ensure_dynamic_table(table_name)
        logger.info("Tracking all intervals for %s, writing to %s.%s, expires at %s",
                    event_slug, SCHEMA, table_name, expiry_dt.strftime('%Y-%m-%d %H:%M'))
        conn = get_connection()
        while True:
            now = datetime.now(timezone.utc)
            rows_to_insert = []
            event_data = fetch_event_details(event_slug)
            if not event_data or "markets" not in event_data:
                logger.warning("Failed to fetch valid market data, skipping cycle.")
            else:
                for market in event_data["markets"]:
                    label = market.get("title") or market.get("question") or ""
                    low_bound, high_bound = parse_interval(label)
                    if low_bound is None and high_bound is None: continue

                    yes_bid = float(market.get("bestBid")) if market.get("bestBid") is not None else None
                    yes_ask = float(market.get("bestAsk")) if market.get("bestAsk") is not None else None

                    no_bid = 1.0 - yes_ask if yes_ask is not None else None
                    no_ask = 1.0 - yes_bid if yes_bid is not None else None

                    row = (
                        now, event_slug, int(market["id"]), label,
                        low_bound, high_bound, expiry_dt,
                        yes_bid, yes_ask, no_bid, no_ask
                    )
                    rows_to_insert.append(row)

                if rows_to_insert:
                    insert_rows(table_name, rows_to_insert)
                    logger.info("Inserted %d market rows into %s at %s", len(rows_to_insert), table_name,
                                now.strftime("%H:%M:%S"))

            if now > expiry_dt:
                logger.info("Market has expired. Stopping tracker after final insert.")
                break
            time.sleep(SAMPLE_SECS)
    except Exception as e:
        logger.exception("An unexpected error occurred in the tracker loop for %s: %s", event_slug, e)
    finally:
        if conn and not conn.closed:
            release_connection(conn)
        logger.info("Tracker for %s stopped. DB connection released.", event_slug)


# ───── 发现 & 启动 ─────────────────
def discover_and_start(tracked: Set[str]):
    logging.info("Discovering new events...")
    events = fetch_all_events()
    events_to_track = filter_interval_events(events)
    logging.info("Found %d events passing filters.", len(events_to_track))
    for slug, expiry_dt in events_to_track:
        if slug not in tracked:
            tracked.add(slug)
            p = Process(target=track_one, args=(slug, expiry_dt))
            p.daemon = True
            p.start()
            logging.info("Launched tracker for %s", slug)


# ───── 主入口 ────────────────────────────────
def main():
    setup_logger("main_runner")
    tracked: Set[str] = set()
    try:
        discover_and_start(tracked)
        while True:
            logging.info("Main process sleeping for %.1f hours...", DISCOVER_SEC / 3600)
            time.sleep(DISCOVER_SEC)
            discover_and_start(tracked)
    except KeyboardInterrupt:
        logging.info("Main process shutting down.")
        sys.exit(0)


if __name__ == "__main__":
    main()