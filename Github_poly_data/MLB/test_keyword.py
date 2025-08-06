# test_final_structure.py
import sys
from collections import defaultdict

# 从主脚本中导入我们需要测试和使用的函数
try:
    # 确保你的主脚本文件名为 MLB_Auto.py
    from MLB_Auto import fetch_all_events, filter_mlb_events
except ImportError:
    print("错误：无法导入所需函数。")
    print("请确保 test_final_structure.py 和 MLB_Auto.py 文件在同一个目录下。")
    sys.exit(1)


def validate_price_features(event: dict) -> list:
    """
    检查单个事件对象是否包含价格相关的关键字段。
    返回一个包含所有缺失字段名称的列表。
    """
    missing_keys = []

    # 只需要检查 markets 内部的价格字段
    if 'markets' in event and isinstance(event.get('markets'), list) and len(event['markets']) > 0:
        market = event['markets'][0]
        # bestBid 和 bestAsk 可以为 None，但 key 必须存在
        if 'bestBid' not in market:
            missing_keys.append("bestBid")
        if 'bestAsk' not in market:
            missing_keys.append("bestAsk")
    else:
        missing_keys.append("markets 列表无效")

    return missing_keys


if __name__ == "__main__":
    print("🚀 **Polymarket MLB 最终结构验证测试** 🚀")
    print("本测试将先按主程序逻辑筛选比赛，再对通过的比赛进行价格字段的结构验证。")
    print("-" * 50)
    print("正在连接到 Polymarket API 以获取实时 MLB 市场数据...")

    # 1. 从真实 API 获取一次性完整数据
    live_events = fetch_all_events()

    if not live_events:
        print("未能从 API 获取到任何 MLB 事件，测试中止。")
        sys.exit(0)

    print(f"成功从 API 获取到 {len(live_events)} 个活跃的 MLB 事件。")
    print("现在开始模拟主程序筛选...")
    print("-" * 50)

    # 2. 【第一步筛选】完全使用主程序的筛选逻辑，找出所有“候选”比赛
    candidate_events_tuples = filter_mlb_events(live_events)
    candidate_slugs = {event_tuple[0] for event_tuple in candidate_events_tuples}

    # 为了方便查找，创建一个从 slug 到原始 event 对象的映射
    event_map = {ev.get('slug'): ev for ev in live_events if ev.get('slug')}

    print(f"主程序筛选出 {len(candidate_slugs)} 个即将开始的比赛作为候选。")
    print("现在对这些候选比赛进行最终的价格字段结构验证...")
    print("-" * 50)

    # 3. 【第二步验证】对每一个候选比赛进行结构验证
    ready_to_track = []
    missing_features = defaultdict(list)

    for slug in candidate_slugs:
        event_obj = event_map.get(slug)
        if not event_obj:
            continue

        missing = validate_price_features(event_obj)
        if not missing:
            ready_to_track.append(slug)
        else:
            reason = ", ".join(missing)
            missing_features[reason].append(slug)

    # 4. 打印最终的检查报告
    print("\n**最终验证报告**")
    print("=" * 35)
    print(f"候选比赛总数: {len(candidate_slugs)}")
    print(f"✅ 结构完整，可以跟踪: {len(ready_to_track)} 个")
    print(f"❌ 缺少价格字段，无法跟踪: {len(candidate_slugs) - len(ready_to_track)} 个")
    print("=" * 35)

    if ready_to_track:
        print("\n--- ✅ 结构完整，可以被主程序成功跟踪的比赛 ---")
        for slug in sorted(ready_to_track):
            print(f"  - {slug}")

    if missing_features:
        print("\n--- ❌ 因缺少价格字段而最终会失败的比赛 ---")
        for reason, slugs in missing_features.items():
            print(f"\n  ▼ 原因: 缺少字段 -> {reason} ({len(slugs)} 个事件)")
            for slug in sorted(slugs)[:5]:  # 只打印前5个例子
                print(f"    - {slug}")
            if len(slugs) > 5:
                print(f"    ... 以及其他 {len(slugs) - 5} 个。")

    print("\n" + "-" * 50)
    print("检查完成。")

