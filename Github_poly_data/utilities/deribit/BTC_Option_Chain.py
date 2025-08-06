#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import ccxt
import pandas as pd
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

# ─── 配置 ───
EXCHANGE_ID, UNDERLYING = "deribit", "BTC"
SPOT_TICKER = "BTC/USDC"
LOOKAHEAD_DAYS = 183

PATH_MAIN = f"{EXCHANGE_ID}_{UNDERLYING}_opt_chain.csv"
PATH_SYN = f"{EXCHANGE_ID}_{UNDERLYING}_opt_chain_SYN.csv"
PATH_SKIPPED = f"{EXCHANGE_ID}_{UNDERLYING}_opt_chain_SKIPPED.csv"

MONTH_MAP = {m.upper(): i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}


# ---------- 工具 ----------
def safe_float(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def parse_expiry(tok: str) -> Optional[datetime]:
    if re.fullmatch(r"\d{1,2}[A-Z]{3}\d{2}", tok):
        d, mon, yy = re.match(r"(\d{1,2})([A-Z]{3})(\d{2})", tok).groups()
        return datetime(2000 + int(yy), MONTH_MAP[mon], int(d), hour=8, tzinfo=timezone.utc)
    if tok.isdigit() and 5 <= len(tok) <= 6:
        tok = tok.zfill(6)
        return datetime(2000 + int(tok[:2]), int(tok[2:4]), int(tok[4:6]), hour=8, tzinfo=timezone.utc)
    return None


def flatten(raw):  # dict→list
    return [{"symbol_full": s, **d, **d.get("info", {})} for s, d in raw.items()]


def is_target_option(inst: str) -> bool:
    return inst.startswith(f"{UNDERLYING}-") and inst.count("-") >= 3


# ---------- 主函数 ----------
def BTC_Option_Chain():
    ex = getattr(ccxt, EXCHANGE_ID)({"enableRateLimit": True})
    now = datetime.now(timezone.utc)
    cutoff_low, cutoff_high = now, now + timedelta(days=LOOKAHEAD_DAYS)

    try:
        underlying_px = safe_float(ex.fetch_ticker(f"{UNDERLYING}-PERPETUAL")["last"])
    except Exception as e:
        print(f"⚠️ 获取 {UNDERLYING}-PERPETUAL 价格失败: {e}")
        underlying_px = None

    try:
        spot_px = safe_float(ex.fetch_ticker(SPOT_TICKER)["last"])
    except Exception as e:
        print(f"⚠️ 获取 {SPOT_TICKER} 现货价格失败: {e}")
        spot_px = None

    flat = flatten(ex.fetch_option_chain(UNDERLYING))
    rows_main, rows_syn, rows_skip = [], [], []

    for r in flat:
        inst = r["symbol_full"].split(":")[-1]
        if not is_target_option(inst):
            rows_skip.append({"reason": "not_target_option", **r})
            continue

        _, tok, strike, opt_letter = inst.split("-")[:4]
        expiry_dt = parse_expiry(tok)
        if not expiry_dt:
            rows_skip.append({"reason": "unparseable_expiry", "bad": tok, **r})
            continue
        if expiry_dt < cutoff_low:
            rows_skip.append({"reason": "already_expired", "exp": expiry_dt, **r})
            continue
        if expiry_dt > cutoff_high:
            rows_skip.append({"reason": "beyond_6m", "exp": expiry_dt, **r})
            continue

        bid_coin = safe_float(r.get("bid_price"))
        ask_coin = safe_float(r.get("ask_price"))
        greeks = r.get("greeks", {}) # 提前获取greeks字典，方便复用

        try:
            ticker = ex.fetch_ticker(r["symbol_full"])
            volume_coin = safe_float(ticker.get("baseVolume"))
            volume_usd = safe_float(ticker.get("quoteVolume"))
        except Exception as e:
            print(f"⚠️ 获取 {inst} 成交量失败: {e}")
            volume_coin = None
            volume_usd = None

        rec = {
            "utc_ts":             now.isoformat(timespec="seconds"),
            "underlying_px":      underlying_px,
            "spot_px":            spot_px,
            "symbol":             inst,
            "type":               "call" if opt_letter.upper() == "C" else "put",
            "strike":             safe_float(strike),
            "expiry_iso":         expiry_dt.isoformat(),
            "expiry":             expiry_dt,
            "bid_coin":           bid_coin,
            "ask_coin":           ask_coin,
            "bid_usd":            bid_coin * underlying_px if underlying_px else None,
            "ask_usd":            ask_coin * underlying_px if underlying_px else None,
            "iv":                 safe_float(r.get("mark_iv") or r.get("impliedVolatility")),
            "underlying":         r.get("underlying_index"),
            "contractSize":       safe_float(r.get("contract_size"), 1.0),
            "volume_coin":        volume_coin,
            "volume_usd":         volume_usd,
            # ▼▼▼ 新增Greeks ▼▼▼
            "delta":              safe_float(greeks.get("delta")),
            "gamma":              safe_float(greeks.get("gamma")),
            "vega":               safe_float(greeks.get("vega")),
            "theta":              safe_float(greeks.get("theta")),
        }

        target_list = rows_syn if str(r.get("underlying_index", "")).startswith("SYN.") else rows_main
        target_list.append(rec)

    pd.DataFrame(rows_main).sort_values("expiry_iso").to_csv(PATH_MAIN, index=False, encoding="utf-8")
    pd.DataFrame(rows_syn).sort_values("expiry_iso").to_csv(PATH_SYN, index=False, encoding="utf-8")
    pd.DataFrame(rows_skip).to_csv(PATH_SKIPPED, index=False, encoding="utf-8")

    print(f"\n✅  非-SYN : {len(rows_main):4d} rows → {PATH_MAIN}")
    print(f"✅  SYN    : {len(rows_syn):4d} rows → {PATH_SYN}")
    print(f"🚫  跳过   : {len(rows_skip):4d} rows → {PATH_SKIPPED}")

    return pd.DataFrame(rows_main), pd.DataFrame(rows_syn), pd.DataFrame(rows_skip)


if __name__ == "__main__":
    BTC_Option_Chain()