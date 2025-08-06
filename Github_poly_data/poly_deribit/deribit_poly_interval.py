#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
polymarket_intervals.py —— 每分钟抓取 *全部区间* 的 Polymarket 行情
写入同一张表 polymarket_only.pm_intervals：

ts_utc | slug | mk_id | label | lo_bound | hi_bound | pm_expiry
       | yes_bid | yes_ask | no_bid | no_ask
"""

from __future__ import annotations
import re, sys, time, logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from psycopg2 import sql
from utilities.db_utils import get_connection, release_connection

# ───────────────── Logger ─────────────────
def make_logger(name: str) -> logging.Logger:
    Path("logs").mkdir(exist_ok=True)
    lg = logging.getLogger(name)
    if not lg.handlers:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        fh  = logging.FileHandler(Path("logs")/f"{name}.log", encoding="utf-8")
        sh  = logging.StreamHandler(sys.stdout)
        fh.setFormatter(fmt); sh.setFormatter(fmt)
        lg.addHandler(fh); lg.addHandler(sh)
        lg.setLevel(logging.INFO); lg.propagate = False
    return lg

# ─────────────── Const ────────────────────
GAMMA_API   = "https://gamma-api.polymarket.com/events"
SAMPLE_SECS = 60
SCHEMA      = "polymarket_only"
TABLE_NAME  = "pm_intervals"

# 数字，带可选 $，k/m/b 后缀
PRICE_RE = re.compile(r"(?:\$)?\s*([0-9][\d,]*\.?\d*)([kKmMbB]?)")
SUFFIX   = {"":1, "K":1_000, "M":1_000_000, "B":1_000_000_000}

# 砍掉 “… on July 25”“on Aug 1”
CUT_DATE_RE = re.compile(r"\s+on\s+\w+\s+\d{1,2}", flags=re.I)

COLS = ("ts_utc,slug,mk_id,label,lo_bound,hi_bound,pm_expiry,"
        "yes_bid,yes_ask,no_bid,no_ask")

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {{schema}}.{TABLE_NAME} (
  ts_utc    TIMESTAMPTZ,
  slug      TEXT,
  mk_id     BIGINT,
  label     TEXT,
  lo_bound  DOUBLE PRECISION,
  hi_bound  DOUBLE PRECISION,
  pm_expiry TIMESTAMPTZ,
  yes_bid   DOUBLE PRECISION,
  yes_ask   DOUBLE PRECISION,
  no_bid    DOUBLE PRECISION,
  no_ask    DOUBLE PRECISION,
  PRIMARY KEY (ts_utc, mk_id)
);
"""

LOW_WORDS  = ("<", "less", "under", "below", "at most")
HIGH_WORDS = (">", "greater", "above", "over", "at least")

# ─────────────── Helpers ───────────────────
def dollars(num: str, suf: str) -> float:
    return float(num.replace(",", "")) * SUFFIX[suf.upper()]

def extract_numbers(label: str) -> List[float]:
    """
    若左端缺后缀（114‑116k），自动用右端后缀补齐。
    """
    tokens = PRICE_RE.findall(label)
    last_suf = next((s for _,_,s in reversed(tokens) if s), '')
    out=[]
    for _,num,suf in tokens:
        suf = suf or last_suf
        out.append(dollars(num,suf))
    return out

def parse_interval(label: str) -> Tuple[Optional[float], Optional[float]]:
    label = CUT_DATE_RE.split(label,1)[0]          # 去掉日期尾巴
    ltxt  = label.lower()
    nums  = extract_numbers(label)

    if any(w in ltxt for w in LOW_WORDS)  and nums:
        return (None, nums[0])
    if any(w in ltxt for w in HIGH_WORDS) and nums:
        return (nums[0], None)
    if len(nums) >= 2:
        lo, hi = sorted(nums[:2])
        return (lo, hi)
    return (nums[0], nums[0]) if nums else (None, None)

def get_event(slug: str) -> Optional[dict]:
    try:
        r = requests.get(GAMMA_API, params={"slug": slug, "archived": False, "includeMarkets":"true"}, timeout=8)
        if r.ok and r.json():
            return r.json()[0]
    except Exception as e:
        logging.warning("Polymarket API error %s", e)
    return None

# ─────────────── DB ────────────────────────
def ensure_table():
    conn = get_connection()
    with conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA)))
        cur.execute(sql.SQL(CREATE_SQL).format(schema=sql.Identifier(SCHEMA)))
    conn.commit(); release_connection(conn)

def insert_rows(rows: List[Tuple]):
    if not rows:
        return
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            args_str = b",".join(
                cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row)
                for row in rows
            )
            cur.execute(
                sql.SQL(f"INSERT INTO {{schema}}.{TABLE_NAME} ({COLS}) VALUES ").format(schema=sql.Identifier(SCHEMA))
                + args_str +
                sql.SQL(" ON CONFLICT DO NOTHING")
            )
        conn.commit()
    finally:
        release_connection(conn)

# ─────────────── Main ───────────────────────
def main(slug: str):
    lg = make_logger(slug.replace("/", "_"))
    ensure_table()

    first_ev = get_event(slug)
    if not first_ev:
        lg.error("event not found"); sys.exit(2)

    pm_expiry = datetime.fromisoformat(first_ev["endDate"].replace("Z", "+00:00")).astimezone(timezone.utc)

    mk_info: Dict[int, dict] = {}
    for mk in first_ev["markets"]:
        label = mk.get("title") or mk.get("question") or ""
        lo, hi = parse_interval(label)
        mk_info[mk["id"]] = {"label": label, "lo": lo, "hi": hi}

    lg.info("tracking %d markets in %s", len(mk_info), slug)

    while True:
        ev = get_event(slug)
        if not ev:
            time.sleep(5); continue

        id2mk = {m["id"]: m for m in ev["markets"]}
        ts    = datetime.now(timezone.utc)
        rows  = []

        for mk_id, info in mk_info.items():
            mk = id2mk.get(mk_id)
            if not mk:
                continue
            yes_bid = float(mk["bestBid"]) if mk.get("bestBid") else None
            yes_ask = float(mk["bestAsk"]) if mk.get("bestAsk") else None
            no_bid  = 1 - yes_ask if yes_ask is not None else None
            no_ask  = 1 - yes_bid if yes_bid is not None else None

            rows.append((
                ts, slug, mk_id, info["label"], info["lo"], info["hi"], pm_expiry,
                yes_bid, yes_ask, no_bid, no_ask
            ))

        insert_rows(rows)
        time.sleep(SAMPLE_SECS)

# ─────────────── CLI ────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: python polymarket_intervals.py <event-slug>")
    main(sys.argv[1])
