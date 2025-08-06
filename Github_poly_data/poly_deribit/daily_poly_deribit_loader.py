#!/usr/bin/env python3
# 自动发现并持续跟踪相关 Polymarket 事件，与 Deribit 期权行情联动抓取

import re, time, sys, requests, ccxt, psycopg2, logging
from pathlib import Path
from datetime import datetime, timezone
from psycopg2 import sql
from utilities.db_utils import get_connection, release_connection
from multiprocessing import Process

# ── 常量 ────────────────────────────────────────────────────────────
GAMMA_API   = "https://gamma-api.polymarket.com/events"
EXCHANGE    = ccxt.deribit({"enableRateLimit": True})
PRICE_RE    = re.compile(r"\$(\d[\d,]*(?:\.\d+)?)([kKmMbB]?)")
DATE_RE     = re.compile(
    r"on\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"\s+\d{1,2}", re.IGNORECASE
)
LOOKAHEAD   = 7
SAMPLE_SECS = 60
SCHEMA      = "deribit_polymarket"   # schema 固定；表名动态

# ── 日志 ────────────────────────────────────────────────────────────
def setup_logger(slug: str):
    Path("logs").mkdir(exist_ok=True)
    logfile = Path("logs") / f"{slug}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(logfile, encoding="utf-8"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("Logger ready → %s", logfile)

# ── 标题解析 ───────────────────────────────────────────────────────
SUFFIX = {"": 1, "K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
dollars = lambda n, s: float(n.replace(',', '')) * SUFFIX[s.upper()]

def parse_title(title: str):
    lo = any(x in title.lower() for x in ["less", "below", "under", "<"])
    hi = any(x in title.lower() for x in ["greater", "above", "over", ">"])
    direction = "lt" if lo and not hi else "gt" if hi and not lo else None
    nums = [dollars(*m) for m in PRICE_RE.findall(title)]
    strike = sum(nums) / len(nums) if nums else None
    return strike, direction

# ── Polymarket ────────────────────────────────────────────────────
def get_event(slug: str):
    r = requests.get(GAMMA_API, params={"slug": slug, "archived": False}, timeout=8)
    return r.json()[0] if r.ok and r.json() else None

def fetch_all_events():
    """
    拉取 Polymarket 上所有未归档且当前可交易的 crypto 类事件（分页）
    """
    all_events = []
    limit  = 100
    offset = 0
    while True:
        params = {
            "archived": False,
            "active":   True,
            "tag_slug": "crypto",
            "limit":    limit,
            "offset":   offset,
        }
        r = requests.get(GAMMA_API, params=params, timeout=10)
        r.raise_for_status()
        page = r.json()
        if not page:
            break
        all_events.extend(page)
        offset += limit
    return all_events

def filter_relevant_events(events):
    """
    筛选出：
      1) 标题包含 bitcoin 或 btc
      2) 标题包含 $xxx 数字
      3) 标题中出现 “on <month> <day>”
    """
    relevant = []
    for ev in events:
        title = ev.get("title", "")
        low   = title.lower()
        if (("bitcoin" in low or "btc" in low)
                and PRICE_RE.search(title)
                and DATE_RE.search(title)):
            relevant.append(ev)
    return relevant

# ── Deribit helpers ───────────────────────────────────────────────
MONTH = {m: i for i,m in enumerate(
    ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG",
     "SEP","OCT","NOV","DEC"], 1)
}

def exp_from_token(tok: str):
    if re.fullmatch(r"\d{1,2}[A-Z]{3}\d{2}", tok):
        d,m,y = re.match(r"(\d{1,2})([A-Z]{3})(\d{2})", tok).groups()
        return datetime(2000+int(y), MONTH[m], int(d), tzinfo=timezone.utc)
    tok = tok.zfill(6)
    return datetime(2000+int(tok[:2]), int(tok[2:4]), int(tok[4:]), tzinfo=timezone.utc)

def deribit_chain():
    flat = EXCHANGE.fetch_option_chain("BTC")
    return [{"symbol": s.split(":")[-1], **d, **d["info"]} for s,d in flat.items()]

def match_deribit(strike, pm_exp, opt_type):
    letter = "P" if opt_type=="put" else "C"
    pool = [
        r for r in deribit_chain()
        if r["symbol"].split("-")[3].upper()==letter
           and abs(float(r["symbol"].split("-")[2]) - strike) <= 1
    ]
    same = [
        r for r in pool
        if exp_from_token(r["symbol"].split("-")[1]).date() == pm_exp.date()
    ]
    if not same:
        logging.warning("No SAME-DAY Deribit %s", letter)
        return None
    chosen = sorted(same, key=lambda x: x["symbol"])[0]
    logging.info("use SAME-DAY %s", chosen["symbol"])
    return chosen

# ── Postgres helper (表名=slug 清洗) ───────────────────────────────
def table_from_slug(slug: str) -> str:
    clean = re.sub(r"[^a-z0-9_]", "_", slug.lower())
    if not clean.startswith("pm_"):
        clean = "pm_" + clean
    return clean

COLS = (
    "ts_utc,slug,pm_expiry,pm_yes_bid,pm_yes_ask,pm_no_bid,pm_no_ask,"
    "symbol,type,strike,expiry,bid_coin,ask_coin,"
    "bid_usd,ask_usd,iv,underlying,underlying_px"
)
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
  ts_utc TIMESTAMPTZ,
  slug TEXT,
  pm_expiry TIMESTAMPTZ,
  pm_yes_bid DOUBLE PRECISION,
  pm_yes_ask DOUBLE PRECISION,
  pm_no_bid  DOUBLE PRECISION,
  pm_no_ask  DOUBLE PRECISION,
  symbol TEXT,
  type   TEXT,
  strike DOUBLE PRECISION,
  expiry TIMESTAMPTZ,
  bid_coin DOUBLE PRECISION,
  ask_coin DOUBLE PRECISION,
  bid_usd  DOUBLE PRECISION,
  ask_usd  DOUBLE PRECISION,
  iv       DOUBLE PRECISION,
  underlying TEXT,
  underlying_px DOUBLE PRECISION,
  PRIMARY KEY (ts_utc, slug, symbol)
);"""

def ensure_table(table: str):
    conn = get_connection()
    with conn,conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA)))
        cur.execute(sql.SQL(CREATE_SQL).format(
            schema=sql.Identifier(SCHEMA), table=sql.Identifier(table)
        ))
    conn.commit(); release_connection(conn)

def insert_row(table: str, row: tuple):
    conn = get_connection()
    try:
        with conn,conn.cursor() as cur:
            cur.execute(sql.SQL(
                f"INSERT INTO {{schema}}.{{table}} ({COLS}) "
                f"VALUES ({','.join(['%s']*18)}) ON CONFLICT DO NOTHING;"
            ).format(schema=sql.Identifier(SCHEMA), table=sql.Identifier(table)), row)
    except psycopg2.Error as e:
        logging.error("PG error %s", e.pgerror.strip())
    finally:
        release_connection(conn)

# ── 单事件跟踪主函数 ──────────────────────────────────────────────
def track_one_event(slug: str):
    setup_logger(slug)
    table = table_from_slug(slug)
    logging.info("table → %s.%s", SCHEMA, table)

    ev = get_event(slug)
    if not ev:
        logging.error("event not found"); return

    strike, dirn = parse_title(ev["title"])
    logging.info("strike=%s dir=%s", strike, dirn)
    if strike is None or dirn is None: return

    pm_expiry = datetime.strptime(ev["endDate"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    opt_type  = "put" if dirn=="lt" else "call"
    opt       = match_deribit(strike, pm_expiry, opt_type)
    if not opt: return

    symbol    = opt["symbol"]
    expiry_dt = exp_from_token(symbol.split("-")[1]).replace(hour=8)

    ensure_table(table)

    while True:
        # Polymarket
        try:
            ev  = get_event(slug)
            mks = ev["markets"]
            yes = next((m for m in mks if "yes" in m["question"].lower()), mks[0])
            no  = next((m for m in mks if "no"  in m["question"].lower()), None)
            pyb = float( yes.get("bestBid")       or yes.get("bestBidPrice") or 0 )
            pya = float( yes.get("bestAsk")       or yes.get("bestAskPrice") or 0 )
            pnb = float( no.get("bestBid")        or no.get("bestBidPrice")  or 0 ) if no else None
            pna = float( no.get("bestAsk")        or no.get("bestAskPrice")  or 0 ) if no else None
        except Exception as e:
            logging.warning("Poly err %s", e); time.sleep(5); continue

        # Deribit
        try:
            under_px = EXCHANGE.fetch_ticker("BTC-PERPETUAL")["last"]
            tkr      = EXCHANGE.fetch_ticker(symbol)
            bid_c, ask_c = tkr["bid"], tkr["ask"]
            bid_u       = bid_c * under_px if bid_c else None
            ask_u       = ask_c * under_px if ask_c else None
            iv          = (tkr.get("mark_iv") or tkr.get("impliedVolatility")
                           or tkr["info"].get("markIv"))
            iv          = float(iv) if iv else None
        except Exception as e:
            logging.warning("Deribit err %s", e); time.sleep(5); continue

        ts = datetime.now(timezone.utc)
        insert_row(table, (
            ts, slug, pm_expiry, pyb, pya, pnb, pna,
            symbol, opt_type, strike, expiry_dt,
            bid_c, ask_c, bid_u, ask_u, iv,
            opt.get("underlying_index","BTC"), under_px
        ))
        logging.info("%s YES=%s NO=%s %s bid=%s",
                     ts.strftime('%H:%M:%S'), pyb, pnb, symbol, bid_c)
        time.sleep(SAMPLE_SECS)

# ── 启动 & 每 24h 发现一次 ────────────────────────────────────────
def _discover_and_start(tracked_slugs):
    events   = fetch_all_events()
    relevant = filter_relevant_events(events)
    new_ev   = [ev for ev in relevant if ev["slug"] not in tracked_slugs]

    for ev in new_ev:
        slug = ev["slug"]
        tracked_slugs.add(slug)
        p = Process(target=track_one_event, args=(slug,))
        p.daemon = True
        p.start()
        logging.info("Started tracking: %s", slug)

def daily_event_discovery():
    tracked_slugs = set()

    # 启动时立即发现并启动
    _discover_and_start(tracked_slugs)

    # 然后每24小时再跑一次
    while True:
        time.sleep(86400)
        _discover_and_start(tracked_slugs)

# ── 主入口 ─────────────────────────────────────────────────────
if __name__=="__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")
    daily_event_discovery()
