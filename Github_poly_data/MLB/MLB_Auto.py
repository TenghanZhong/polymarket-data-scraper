#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
polymarket_mlb_auto.py (v14 - Added Inning and Score Tracking)

功能:
  在数据采集中增加了对比赛当前局数(inning)和实时比分(score)的跟踪。
  数据库表结构已更新，增加了相应字段。
"""
from __future__ import annotations
import re
import sys
import time
import logging
import fcntl
import os
import random
import requests
from datetime import datetime, timezone, timedelta
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
PAGE_SIZE = 100
SAMPLE_SECS = 60
DISCOVER_SEC = 3600
TAG_SLUG = "mlb"
SCHEMA = "MLB"

# ───── 日志 & 锁文件 ─────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOCK_FILE = LOG_DIR / "mlb_auto.lock"


def setup_logger(name: str) -> logging.Logger:
    log_name = name.replace("/", "_")
    logfile = LOG_DIR / f"{log_name}.log"
    logger = logging.getLogger(name)
    if not logger.handlers:
        fmt = "%(asctime)s %(levelname)s %(message)s"
        fh = RotatingFileHandler(logfile, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
        sh = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(fmt)
        for h in (fh, sh):
            h.setFormatter(formatter)
            logger.addHandler(h)
        logger.setLevel(logging.INFO)
    return logger


# ───── MLB 市场辅助函数 ──────────────────
TEAM_RE_PATTERNS = [
    re.compile(r"Will the (.+?) win against the (.+?)\?", flags=re.IGNORECASE),
    re.compile(r"Will the (.+?) beat the (.+?)\?", flags=re.IGNORECASE),
    re.compile(r"(.+?)\s+vs\.?\s+(.+?)(?:\s+on|\s+Game|\s*-\s*\d{4}-\d{2}-\d{2}|$)", flags=re.IGNORECASE),
    re.compile(r"(.+?)\s+@\s+(.+?)(?:\s+on|\s+Game|\s*-\s*\d{4}-\d{2}-\d{2}|$)", flags=re.IGNORECASE),
]


def parse_mlb_teams(question: str) -> Optional[Tuple[str, str]]:
    clean_question = re.sub(
        r"^(MLB:|AL Wildcard:|NL Wildcard:|ALDS:|NLDS:|ALCS:|NLCS:|World Series:)\s*",
        "", question, flags=re.IGNORECASE
    ).strip()
    for pattern in TEAM_RE_PATTERNS:
        match = pattern.search(clean_question)
        if match:
            team_a = match.group(1).strip()
            team_b = match.group(2).strip()
            if team_a and team_b:
                return team_a, team_b
    return None


# ───── 数据库 Schema & Helpers (已修改) ──────────────────
# 【关键修改】增加了 inning 和 score 字段
COLS = ("ts_utc,event_slug,market_id,market_question,game_start_time_utc,"
        "team_in_question,opponent,team_in_question_yes_bid,team_in_question_yes_ask,"
        "opponent_yes_bid,opponent_yes_ask,inning,score")

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {{schema}}.{{table_name}} (
  ts_utc                    TIMESTAMPTZ,
  event_slug                TEXT,
  market_id                 BIGINT,
  market_question           TEXT,
  game_start_time_utc       TIMESTAMPTZ,
  team_in_question          TEXT,
  opponent                  TEXT,
  team_in_question_yes_bid  DOUBLE PRECISION,
  team_in_question_yes_ask  DOUBLE PRECISION,
  opponent_yes_bid          DOUBLE PRECISION,
  opponent_yes_ask          DOUBLE PRECISION,
  inning                    TEXT,
  score                     TEXT,
  PRIMARY KEY (ts_utc, market_id)
);
"""


def ensure_dynamic_table(conn: psycopg2.extensions.connection, table_name: str):
    with conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA)))
        cur.execute(sql.SQL(CREATE_SQL).format(
            schema=sql.Identifier(SCHEMA),
            table_name=sql.Identifier(table_name)
        ))


def insert_rows(conn: psycopg2.extensions.connection, table_name: str, rows: List[Tuple]):
    if not rows: return
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


def fetch_event_details(slug: str) -> Optional[Dict]:
    try:
        params = {"slug": slug, "includeMarkets": "true"}
        r = requests.get(GAMMA_API, params=params, timeout=10)
        r.raise_for_status()
        results = r.json()
        return results[0] if results else None
    except (requests.RequestException, IndexError):
        return None


# ───── 拉取 & 过滤 ───────────────────────
def fetch_all_events() -> List[Dict]:
    events, offset = [], 0
    while True:
        params = {
            "archived": False, "active": True, "tag_slug": TAG_SLUG,
            "includeMarkets": True, "limit": PAGE_SIZE, "offset": offset
        }
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


def filter_mlb_events(events: List[Dict]) -> List[Tuple[str, datetime, datetime]]:
    out: List[Tuple[str, datetime, datetime]] = []
    now = datetime.now(timezone.utc)
    for ev in events:
        if not ev.get("markets"): continue
        market = ev["markets"][0]
        question = market.get("question", "")
        if not parse_mlb_teams(question): continue
        start_time_str = ev.get("startTime")
        if not start_time_str: continue
        try:
            start_dt = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
            close_time_str = ev.get("closedTime")
            if close_time_str:
                expiry_dt = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
            else:
                expiry_dt = start_dt + timedelta(hours=8)
            if expiry_dt > now:
                out.append((ev["slug"], start_dt, expiry_dt))
        except (ValueError, TypeError):
            continue
    return out


# ───── 跟踪 & 写库 (已修改) ──────────────────
def track_mlb_game(event_slug: str, start_dt: datetime, expiry_dt: datetime):
    logger = setup_logger(event_slug)
    table_name = re.sub(r'[^a-z0-9_]', '_', event_slug.lower())
    db_conn = None
    try:
        logger.info("Tracker process started, creating dedicated database connection...")
        db_conn = get_connection()
        logger.info("Database connection established.")
        ensure_dynamic_table(db_conn, table_name)
        logger.info(
            "Tracker initialized for %s. Game start: %s UTC. Will track until %s UTC.",
            event_slug, start_dt.strftime('%Y-%m-%d %H:%M'), expiry_dt.strftime('%Y-%m-%d %H:%M')
        )
        wait_seconds = (start_dt - datetime.now(timezone.utc)).total_seconds()
        if wait_seconds > 0:
            logger.info(f"Waiting for {wait_seconds:.0f} seconds until game start.")
            time.sleep(wait_seconds)
        logger.info("Game tracking active. Beginning data collection.")
        while datetime.now(timezone.utc) < expiry_dt:
            now_ts = datetime.now(timezone.utc)
            event_data = None
            max_retries = 3
            for attempt in range(max_retries):
                event_data = fetch_event_details(event_slug)
                if event_data: break
                if attempt < max_retries - 1: time.sleep(5)
            if not event_data or not event_data.get("markets"):
                logger.warning("Failed to fetch valid market data after %d retries, skipping cycle.", max_retries)
                time.sleep(SAMPLE_SECS)
                continue

            # 【关键修改】从事件顶层获取 inning 和 score
            inning = event_data.get("period")
            score = event_data.get("score")

            market = event_data["markets"][0]
            question = market.get("question", "")
            teams = parse_mlb_teams(question)
            if not teams:
                logger.error("Could not parse teams from question: '%s'. Stopping tracker.", question)
                break
            team_in_question, opponent = teams
            yes_bid_val = market.get("bestBid")
            yes_ask_val = market.get("bestAsk")
            yes_bid = float(yes_bid_val) if yes_bid_val is not None else None
            yes_ask = float(yes_ask_val) if yes_ask_val is not None else None
            opponent_bid = 1.0 - yes_ask if yes_ask is not None else None
            opponent_ask = 1.0 - yes_bid if yes_bid is not None else None

            # 【关键修改】将 inning 和 score 添加到要插入的行中
            row = (
                now_ts, event_slug, int(market.get("id", 0)), question, start_dt,
                team_in_question, opponent,
                yes_bid, yes_ask,
                opponent_bid, opponent_ask,
                inning, score
            )
            insert_rows(db_conn, table_name, [row])
            logger.info("Inserted 1 row into %s at %s (Inning: %s, Score: %s)",
                        table_name, now_ts.strftime("%H:%M:%S"), inning, score)
            time.sleep(SAMPLE_SECS)
    except Exception as e:
        logger.exception("An unexpected error occurred in the tracker for %s: %s", event_slug, e)
    finally:
        if db_conn:
            release_connection(db_conn)
            logger.info("Dedicated database connection released.")
        logger.info("Market %s has expired or an error occurred. Stopping tracker.", event_slug)


# ───── 发现 & 启动 ─────────────────
def discover_and_start(tracked: Set[str]):
    logging.info("Discovering new MLB events...")
    events = fetch_all_events()
    logging.info(f"Fetched {len(events)} total events from API.")

    all_relevant_games = filter_mlb_events(events)
    now = datetime.now(timezone.utc)

    in_progress_games = []
    upcoming_games = []

    for slug, start_dt, expiry_dt in all_relevant_games:
        if start_dt <= now:
            in_progress_games.append((slug, start_dt, expiry_dt))
        else:
            upcoming_games.append((slug, start_dt, expiry_dt))

    logging.info(f"Found {len(in_progress_games)} games currently in-progress.")
    logging.info(f"Found {len(upcoming_games)} upcoming games (will be tracked when they start).")

    # 只为正在进行的比赛启动进程
    for slug, start_dt, expiry_dt in in_progress_games:
        if slug not in tracked:
            tracked.add(slug)
            p = Process(target=track_mlb_game, args=(slug, start_dt, expiry_dt))
            p.daemon = True
            p.start()
            logging.info(
                "Launched tracker for IN-PROGRESS game: %s (Started at: %s UTC)",
                slug, start_dt.strftime('%Y-%m-%d %H:%M')
            )
            time.sleep(random.uniform(0.5, 2.0))


# ───── 主入口 ────────────────────────────────
def main():
    setup_logger("main_runner")

    try:
        lock_file_handle = open(LOCK_FILE, 'w')
        fcntl.lockf(lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        logging.info("Successfully acquired lock. Starting main process.")
    except IOError:
        logging.warning("Another instance is already running. Exiting.")
        sys.exit(0)

    tracked: Set[str] = set()
    try:
        discover_and_start(tracked)
        while True:
            logging.info("Main process sleeping for %.1f hours...", DISCOVER_SEC / 3600)
            time.sleep(DISCOVER_SEC)
            discover_and_start(tracked)
    except KeyboardInterrupt:
        logging.info("Main process shutting down.")
    except Exception as e:
        logging.exception("An error occurred in the main loop: %s", e)
    finally:
        fcntl.lockf(lock_file_handle, fcntl.LOCK_UN)
        lock_file_handle.close()
        os.remove(LOCK_FILE)
        logging.info("Lock released and file removed. Shutdown complete.")
        sys.exit(0)


if __name__ == "__main__":
    main()
