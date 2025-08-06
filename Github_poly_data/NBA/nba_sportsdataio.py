"""
SportsDataIO Replay · OKC @ IND G6 (2025‑06‑19) – score‑event axis
-----------------------------------------------------------------
× 15 s poll play‑by‑play & odds
× new row whenever the score changes
× score columns never expire; odds forward‑fill
× keep score_ts (event) & odds_ts (snapshot)
"""
import time, csv, requests, pandas as pd
from datetime import timedelta
from pathlib import Path
from requests.adapters import HTTPAdapter, Retry
from typing import Union

# ─── manual config ──────────────────────────────────────────────
API_KEY  = "dc84ad19c28a4e7ba18032bcf8c24e50"
GAME_ID  = 22398
OUT_CSV  = Path(r"C:\Users\26876\Desktop\RA_Summer\prediction-market-code-main",
                "nba_2025_finals_game6_pbp_odds.csv")
POLL_SEC = 15
# ────────────────────────────────────────────────────────────────

BASE      = "https://replay.sportsdata.io/api"
TIME_TOL  = timedelta(seconds=60)
TZ        = "America/New_York"
UA        = "ReplayLoop/score-axis/1.2"

# ---------- requests ----------
sess = requests.Session()
sess.headers.update({"User-Agent": UA})
sess.mount("https://", HTTPAdapter(max_retries=Retry(
    total=3, backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"])))

_json        = lambda url, **p: sess.get(url, params={**p, "key": API_KEY}, timeout=20).json()
meta         = lambda: _json(f"{BASE}/metadata")
replay_done  = lambda: meta().get("Status") == "Finished"

# ---------- helpers ----------
def _to_est(obj) -> Union[pd.Series, pd.DatetimeIndex]:
    ts = pd.to_datetime(obj, errors="coerce")
    if isinstance(ts, pd.DatetimeIndex):
        ts = ts.tz_localize(TZ) if ts.tz is None else ts
        return ts.tz_convert(TZ)
    ts = pd.Series(ts)
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize(TZ)
    return ts.dt.tz_convert(TZ)

def _prefer_consensus(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["__pref__"] = (df["Sportsbook"] != "Consensus").astype(int)
    return (df.sort_values(["odds_ts", "__pref__", "Sportsbook"])
              .groupby("odds_ts", as_index=False)
              .first()
              .drop(columns="__pref__"))

# ---------- play‑by‑play ----------
def grab_pbp() -> pd.DataFrame:
    j = _json(f"{BASE}/v3/nba/pbp/json/playbyplay/{GAME_ID}")
    plays = (j if isinstance(j, list) else
             j.get("Plays") or
             [p for q in j.get("Quarters", []) for p in q.get("Plays", [])])
    if not plays:
        return pd.DataFrame()

    ts_key = next(k for k in ("Updated", "Created", "DateTime", "Timestamp") if k in plays[0])

    def _clock(p):
        """assemble mm:ss -> '11:42'; return None if minutes missing"""
        if p.get("TimeRemainingMinutes") is None:
            return None
        return f"{int(p['TimeRemainingMinutes']):02d}:{int(p['TimeRemainingSeconds']):02d}"

    df = (pd.DataFrame({
            "score_ts"   : _to_est([p[ts_key]              for p in plays]),
            "period"     : [p.get("QuarterName")           for p in plays],  # ← 修正
            "clock"      : [_clock(p)                      for p in plays],  # ← 修正
            "home_score" : [p.get("HomeTeamScore")         for p in plays],
            "away_score" : [p.get("AwayTeamScore")         for p in plays],
         })
         .sort_values("score_ts"))

    changed = df[["home_score", "away_score"]].diff().fillna(1).ne(0).any(axis=1)
    return df.loc[changed].reset_index(drop=True)

# ---------- odds ----------
def grab_odds() -> pd.DataFrame:
    games = _json(f"{BASE}/v3/nba/odds/json/livegameoddslinemovement/{GAME_ID}")
    odds  = [o for g in games for o in g.get("LiveOdds") or []] if games else []
    if not odds:
        return pd.DataFrame()

    ts_key = next(k for k in ("Updated", "UpdatedUtc", "Created", "DateTime", "Timestamp") if k in odds[0])
    df = pd.DataFrame({
        "odds_ts"          : _to_est([o[ts_key]           for o in odds]),
        "Sportsbook"       : [o["Sportsbook"]             for o in odds],
        "ML_Home"          : [o["HomeMoneyLine"]          for o in odds],
        "ML_Away"          : [o["AwayMoneyLine"]          for o in odds],
        "spread_ptsHome"   : [o["HomePointSpread"]        for o in odds],
        "spread_ptsAway"   : [o["AwayPointSpread"]        for o in odds],
        "spread_oddsHome"  : [o["HomePointSpreadPayout"]  for o in odds],
        "spread_oddsAway"  : [o["AwayPointSpreadPayout"]  for o in odds],
        "total_pts"        : [o["OverUnder"]              for o in odds],
        "O_odds"           : [o["OverPayout"]             for o in odds],
        "U_odds"           : [o["UnderPayout"]            for o in odds],
    })
    return _prefer_consensus(df.sort_values("odds_ts"))

# ---------- load / init ----------
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
if OUT_CSV.exists():
    master = pd.read_csv(OUT_CSV, encoding="utf-8-sig")
    master["score_ts"] = _to_est(master["score_ts"])
    master["odds_ts"]  = _to_est(master["odds_ts"])
else:
    master = pd.DataFrame()

# ---------- main loop ----------
try:
    while True:
        pbp_new  = grab_pbp()
        odds_now = grab_odds()

        if pbp_new.empty:
            print("🕑 no fresh PBP yet …")
            if replay_done():
                print("✅ replay finished.")
                break
            time.sleep(POLL_SEC)
            continue

        if not master.empty:
            pbp_new = pbp_new[pbp_new["score_ts"] > master["score_ts"].max()]
        if pbp_new.empty:
            time.sleep(POLL_SEC); continue

        merged = pd.merge_asof(
            pbp_new.sort_values("score_ts"),
            odds_now.sort_values("odds_ts"),
            left_on="score_ts", right_on="odds_ts",
            direction="backward", tolerance=TIME_TOL)

        master = pd.concat([master, merged], ignore_index=True)
        master.sort_values("score_ts", inplace=True, ignore_index=True)

        odds_cols = [c for c in master.columns
                     if c.startswith(("ML_", "spread_", "total_", "O_odds", "U_odds", "Sportsbook", "odds_ts"))]
        master[odds_cols] = master[odds_cols].ffill()

        master.to_csv(OUT_CSV, index=False,
                      quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")

        # ── enhanced progress line ──
        last = pbp_new.iloc[-1]
        node = f"Q{last['period']} {last['clock'] or '--:--'}"
        print(f"✅ +{len(pbp_new):2} rows | total {len(master):4} | {node} → {last['home_score']}-{last['away_score']}")

        if replay_done():
            print("🏁 replay complete – exiting.")
            break
        time.sleep(POLL_SEC)

except KeyboardInterrupt:
    print("\n⏹ manually stopped; data saved.")
except Exception as e:
    print(f"\n❌ {e} – data still saved to {OUT_CSV}")
