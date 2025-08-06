#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
polymarket_hourly_tracker_auto.py

功能:
  1. 读取本地时间 (UTC) 并转换到 America/New_York (EDT)
  2. 生成当前小时对应的两个事件 slug:
     - bitcoin-up-or-down-<month>-<day>-<hour><am/pm>-et
     - ethereum-up-or-down-<month>-<day>-<hour><am/pm>-et
  3. 拉取每个事件详情并启动跟踪，只跟踪该小时内的 Yes/No 盘口
  4. 复用 HTTP Session、到期前退出、并减少日志开销
  5. 禁用 SSL 验证 & 自动重试
  6. 首次拉取失败也依据 slug 计算 expiry 并启动跟踪
  7. 不再存 low_bound/high_bound，只写 yes_bid, yes_ask, no_bid, no_ask
"""
from __future__ import annotations
import re, sys, time, logging
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from multiprocessing import Process
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from logging.handlers import RotatingFileHandler

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
from psycopg2 import sql, extras
from utilities.db_utils import get_connection, release_connection

# ───── 全局配置 ─────────────────────────────
GAMMA_API   = "https://gamma-api.polymarket.com/events"
SAMPLE_SECS = 60    # 每 60 秒拉取一次
SCHEMA      = "hourly_crypto"
LOG_DIR     = Path("logs"); LOG_DIR.mkdir(exist_ok=True)

# ───── HTTP Session with retry & no-verify ────
session = requests.Session()
session.verify = False  # 忽略 SSL 验证
retries = Retry(total=5, backoff_factor=0.3,
                status_forcelist=[500,502,503,504],
                allowed_methods=["GET"])
session.mount("https://", HTTPAdapter(max_retries=retries))
requests.packages.urllib3.disable_warnings()

# ───── slug → expiry 回退解析 ─────────────────
_MONTH_MAP = {
    "january":1, "february":2, "march":3, "april":4,
    "may":5, "june":6, "july":7, "august":8,
    "september":9, "october":10, "november":11, "december":12
}
_SLUG_RE = re.compile(
    r"-(?P<month>[a-z]+)-(?P<day>\d+)-(?P<hour>\d+)(?P<ampm>am|pm)-et$",
    flags=re.IGNORECASE
)

def get_expiry_from_slug(slug: str) -> datetime:
    """
    根据 slug 直接算本小时下一个整点（ET），然后转 UTC。
    """
    m = _SLUG_RE.search(slug)
    if not m:
        return datetime.now(timezone.utc) + timedelta(hours=1)
    mon, day = m.group("month").lower(), int(m.group("day"))
    h12, ampm = int(m.group("hour")), m.group("ampm").lower()
    h24 = (h12 % 12) + (12 if ampm=="pm" else 0)
    year = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York")).year
    exp_et = datetime(year, _MONTH_MAP.get(mon,1), day,
                      (h24+1)%24, 0, 0,
                      tzinfo=ZoneInfo("America/New_York"))
    return exp_et.astimezone(timezone.utc)

# ───── slug 解析辅助 ─────────────────────────
def generate_current_hour_slugs(now_utc: datetime|None=None) -> List[str]:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
    frag = f"{now_et.strftime('%B').lower()}-{now_et.day}-" \
           f"{(now_et.hour%12 or 12)}" \
           f"{'am' if now_et.hour<12 else 'pm'}-et"
    return [
        f"bitcoin-up-or-down-{frag}",
        f"ethereum-up-or-down-{frag}"
    ]

# ───── 日志 ──────────────────────────────
def setup_logger(name: str) -> logging.Logger:
    logfile = LOG_DIR/f"{name}.log"
    logger = logging.getLogger(name)
    if not logger.handlers:
        fmt = "%(asctime)s %(levelname)s %(message)s"
        fh = RotatingFileHandler(logfile, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
        sh = logging.StreamHandler(sys.stdout)
        for h in (fh, sh):
            h.setFormatter(logging.Formatter(fmt))
            logger.addHandler(h)
        logger.setLevel(logging.INFO)
    return logger

# ───── DB Helpers ───────────────────────────
# 移除了 low_bound, high_bound
COLS = (
    "ts_utc,event_slug,market_id,market_label,pm_expiry,"
    "yes_bid,yes_ask,no_bid,no_ask"
)
CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {{schema}}.{{table_name}} (
  ts_utc          TIMESTAMPTZ,
  event_slug      TEXT,
  market_id       BIGINT,
  market_label    TEXT,
  pm_expiry       TIMESTAMPTZ,
  yes_bid         DOUBLE PRECISION,
  yes_ask         DOUBLE PRECISION,
  no_bid          DOUBLE PRECISION,
  no_ask          DOUBLE PRECISION,
  PRIMARY KEY(ts_utc,market_id)
);
"""
def ensure_dynamic_table(table_name: str):
    conn = get_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(
                sql.Identifier(SCHEMA)))
            cur.execute(sql.SQL(CREATE_SQL).format(
                schema=sql.Identifier(SCHEMA),
                table_name=sql.Identifier(table_name)
            ))
    finally:
        release_connection(conn)

def insert_rows(table_name: str, rows: List[Tuple]):
    if not rows:
        return
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            extras.execute_values(cur,
                sql.SQL("INSERT INTO {schema}.{table_name} ({cols}) "
                        "VALUES %s ON CONFLICT DO NOTHING").format(
                    schema=sql.Identifier(SCHEMA),
                    table_name=sql.Identifier(table_name),
                    cols=sql.SQL(COLS)
                ),
                rows
            )
        conn.commit()
    except Exception as e:
        logging.error("DB insert error for %s: %s", table_name, e)
        conn.rollback()
    finally:
        release_connection(conn)

# ───── 事件详情拉取 ───────────────────────────
def fetch_event_details(slug: str) -> Optional[Dict]:
    try:
        r = session.get(GAMMA_API,
                        params={"slug":slug,"includeMarkets":"true"},
                        timeout=10)
        r.raise_for_status()
        data = r.json()
        return data[0] if data else None
    except Exception as e:
        logging.getLogger("fetch").warning("Failed fetch %s: %s", slug, e)
        return None

def get_expiry_from_event(ev: Dict) -> Optional[datetime]:
    for key in ("endTime","closeTime","end_date","endDate"):
        s = ev.get(key)
        if s:
            try:
                return datetime.fromisoformat(s.replace("Z","+00:00"))\
                               .astimezone(timezone.utc)
            except ValueError:
                pass
    return None

# ───── 单事件跟踪 ─────────────────────────────
def track_one(slug: str, expiry: datetime):
    logger = setup_logger(slug)
    tbl = re.sub(r"[^a-z0-9_]", "_", slug.lower())
    ensure_dynamic_table(tbl)

    # 等市场真正上线
    logger.info("Waiting for markets to appear: %s", slug)
    while True:
        now = datetime.now(timezone.utc)
        if now >= expiry:
            logger.info("Expiry before markets live, abort: %s", slug)
            return
        ev = fetch_event_details(slug)
        if ev and ev.get("markets"):
            logger.info("Markets live for %s, start sampling.", slug)
            break
        time.sleep(5)

    # 正式采样
    first = True
    while True:
        now = datetime.now(timezone.utc)
        if now >= expiry:
            logger.info("Expiry reached, stopping: %s", slug)
            break

        ev = fetch_event_details(slug) or {}
        rows = []
        for m in ev.get("markets", []):
            lbl = m.get("title") or m.get("question") or ""
            yb = float(m.get("bestBid", 0))
            ya = float(m.get("bestAsk", 0))
            nb, na = 1-ya, 1-yb
            rows.append((
                now, slug, int(m["id"]), lbl,
                expiry, yb, ya, nb, na
            ))
        if rows:
            insert_rows(tbl, rows)
            if first:
                logger.info("Inserted %d rows for %s at %s",
                            len(rows), slug, now.strftime("%H:%M:%S"))
                first = False
            else:
                logger.info("Inserted %d rows for %s", len(rows), slug)

        # 对齐 SAMPLE_SECS 边界
        delta = SAMPLE_SECS - (time.time() % SAMPLE_SECS)
        time.sleep(delta)

# ───── 主流程 ────────────────────────────────
def main():
    runner = setup_logger("main_runner")
    runner.info("Starting hourly slug tracker loop…")

    # 用一个列表来持有所有启动的进程，以便未来可以进行管理
    all_processes: List[Process] = []

    while True:
        # 清理已经结束的旧进程 (可选但推荐)
        # p.join() 在进程已结束时会立即返回
        all_processes = [p for p in all_processes if p.is_alive()]
        runner.info("Active trackers: %d", len(all_processes))

        # 1. 启动当前小时的追踪器
        now_utc = datetime.now(timezone.utc)
        now_et = now_utc.astimezone(ZoneInfo("America/New_York"))

        # 避免在整点切换的瞬间重复启动
        if now_utc.minute > 5:
            runner.info("Not the top of the hour, skipping launch phase.")
        else:
            slugs = generate_current_hour_slugs(now_utc)
            runner.info("Current ET %s → slugs: %s",
                        now_et.strftime("%Y-%m-%d %I %p"), slugs)

            for slug in slugs:
                ev = fetch_event_details(slug)
                expiry = get_expiry_from_event(ev) if ev else get_expiry_from_slug(slug)

                p = Process(target=track_one, args=(slug, expiry))
                p.start()
                all_processes.append(p)  # 将新进程加入管理列表
                runner.info("Launched tracker for %s (pid: %s) until %s", slug, p.pid, expiry)

        # 2. 计算到下一个小时整点需要休眠的时间
        # time.time() 返回的是 UTC 秒数
        # 3600 秒 = 1 小时
        now_secs = time.time()
        sleep_secs = 3600 - (now_secs % 3600) + 5  # 加 5 秒余量确保跨过整点

        next_hour = now_utc.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        runner.info("Sleeping for %.2f seconds, next run at %s UTC",
                    sleep_secs, next_hour.strftime("%Y-%m-%d %H:%M:%S"))

        time.sleep(sleep_secs)

        # 循环会在这里结束后，立即进入下一次迭代，开始下一个小时的任务

if __name__ == "__main__":
    main()
