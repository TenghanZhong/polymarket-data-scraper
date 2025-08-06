# fetch_deribit_btc_chain.py  —— core loader (main / syn / skipped)

import ccxt, re, pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict

EXCHANGE_ID, UNDERLYING = "deribit", "BTC"
LOOKAHEAD_DAYS          = 183          # 半年

MONTH_MAP = {m.upper(): i for i, m in enumerate(
    ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"], 1)
}

# ---------- utils ----------
def safe_float(x, default=0.0):
    try:  return float(x)
    except (TypeError, ValueError): return default

def parse_expiry(token: str) -> Optional[datetime]:
    """
    ❶ 文字月  : 4JUL25 / 25JUL25
    ❷ 六位数字: 250624 (= 2025-06-24 → ddmmyy)
    ❸ 五位数字: 40625  (= 2025-06-04 → d mmyy)
    """
    if re.fullmatch(r"\d{1,2}[A-Z]{3}\d{2}", token):
        d, mon, yy = re.match(r"(\d{1,2})([A-Z]{3})(\d{2})", token).groups()
        return datetime(2000+int(yy), MONTH_MAP[mon], int(d), tzinfo=timezone.utc)

    if token.isdigit():
        if len(token) == 6:              # ddmmyy
            dd, mm, yy = token[:2], token[2:4], token[4:6]
        elif len(token) == 5:            # d mmyy
            dd, mm, yy = token[0], token[1:3], token[3:5]
        else:
            return None
        return datetime(2000+int(yy), int(mm), int(dd), tzinfo=timezone.utc)

    return None

def is_real_option(symbol: str) -> bool:
    return symbol.startswith(f"{UNDERLYING}-") and symbol.count("-") >= 3

def flatten_deribit_chain(raw: Dict) -> List[Dict]:
    flat = []
    for sym, data in raw.items():
        row = {"symbol_full": sym}
        row.update(data)
        row.update(data.get("info", {}))
        flat.append(row)
    return flat
# ---------------------------

def fetch_chain() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """返回 (main_df, syn_df, skipped_df) — 三个 DataFrame"""
    ex   = getattr(ccxt, EXCHANGE_ID)({"enableRateLimit": True})
    raw  = ex.fetch_option_chain(UNDERLYING)
    flat = flatten_deribit_chain(raw)

    now, cutoff = datetime.now(timezone.utc), datetime.now(timezone.utc)+timedelta(days=LOOKAHEAD_DAYS)

    rows_main, rows_syn, rows_skip = [], [], []

    for r in flat:
        inst = r["symbol_full"].split(":")[-1]

        if not is_real_option(inst):
            rows_skip.append({"reason": "not_BTC_option", **r})
            continue

        _, date_token, strike_str, opt_letter = inst.split("-")[:4]
        expiry_dt = parse_expiry(date_token)
        if not expiry_dt:
            rows_skip.append({"reason": "unparseable_expiry", "bad_token": date_token, **r})
            continue
        if expiry_dt < now:
            rows_skip.append({"reason": "already_expired", "parsed_expiry": expiry_dt, **r})
            continue
        if expiry_dt > cutoff:
            rows_skip.append({"reason": "beyond_6m", "parsed_expiry": expiry_dt, **r})
            continue

        row_std = {
            "symbol"       : inst,
            "type"         : "call" if opt_letter.upper() == "C" else "put",
            "strike"       : safe_float(strike_str),
            "expiry"       : expiry_dt,
            "bid"          : safe_float(r.get("bid_price")),
            "ask"          : safe_float(r.get("ask_price")),
            "iv"           : safe_float(r.get("mark_iv") or r.get("impliedVolatility")),
            "underlying"   : r.get("underlying_index"),
            "contractSize" : safe_float(r.get("contract_size"), 1.0),
        }

        (rows_syn if str(r.get("underlying_index","")).startswith("SYN.")
                   else rows_main).append(row_std)

    return (
        pd.DataFrame(rows_main).sort_values("expiry"),
        pd.DataFrame(rows_syn ).sort_values("expiry"),
        pd.DataFrame(rows_skip)
    )

# --------------- demo ---------------
if __name__ == "__main__":
    df_main, df_syn, df_skip = fetch_chain()
    print("Main  rows :", len(df_main))
    print("SYN   rows :", len(df_syn))
    print("Skipped   :", len(df_skip))
    # df_main.to_csv("main.csv", index=False)  # 如需落地，自行解除注释
