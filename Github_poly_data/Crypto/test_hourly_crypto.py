#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from hourly_crypto import (
    generate_current_hour_slugs,
    fetch_event_details,
    get_expiry_from_event,
    get_expiry_from_slug
)

def main():
    # 1) 生成当前要跟踪的两个 slug
    slugs = generate_current_hour_slugs()
    print("当前 ET 小时要追踪的两个 slug：")
    for s in slugs:
        print(" •", s)

    # 2) 试着拉 API、解析到期（优先 API，再回退 slug 计算）
    print("\n尝试拉取详情并解析到期时间：")
    for slug in slugs:
        ev = fetch_event_details(slug)
        if not ev:
            print(f" ❌ {slug} — API 拉不到事件，回退用 slug 计算到期")
            expiry_utc = get_expiry_from_slug(slug)
        else:
            expiry_utc = get_expiry_from_event(ev)
            if not expiry_utc:
                print(f" ⚠️ {slug} — API 无到期字段，回退用 slug 计算")
                expiry_utc = get_expiry_from_slug(slug)

        # 最终有了 expiry_utc
        expiry_et = expiry_utc.astimezone(ZoneInfo("America/New_York"))
        print(f" ✅ {slug} — 到期 UTC: {expiry_utc.isoformat()}  |  到期 ET: {expiry_et.strftime('%Y-%m-%d %I:%M %p %Z')}")

if __name__ == "__main__":
    main()
