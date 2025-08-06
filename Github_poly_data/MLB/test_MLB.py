# test_MLB.py (Final Validation Script)
import sys
from datetime import datetime, timezone

# 从主脚本中导入我们需要测试和使用的函数
try:
    # 确保你的主脚本文件名为 MLB_Auto.py
    from MLB_Auto import fetch_all_events, filter_mlb_events
except ImportError:
    print("错误：无法导入所需函数。")
    print("请确保 test_MLB.py 和 MLB_Auto.py 文件在同一个目录下。")
    sys.exit(1)

if __name__ == "__main__":
    print("🚀 **Polymarket MLB 筛选逻辑最终验证测试** 🚀")
    print("-" * 50)
    print("正在连接到 Polymarket API 以获取实时 MLB 市场数据...")

    # 1. 从真实 API 获取一次性完整数据
    live_events = fetch_all_events()

    if not live_events:
        print("未能从 API 获取到任何 MLB 事件，测试中止。")
        sys.exit(0)

    print(f"成功从 API 获取到 {len(live_events)} 个活跃的 MLB 事件。")
    print("现在直接应用主程序的筛选逻辑...")
    print("-" * 50)

    # 2. 直接调用主程序的 filter_mlb_events 函数进行筛选
    # 这个函数现在包含了所有正确的逻辑
    passed_events = filter_mlb_events(live_events)

    # 3. 计算被拒绝的事件，用于生成报告
    passed_slugs = {event[0] for event in passed_events}
    rejected_events_count = len(live_events) - len(passed_events)

    # 4. 打印清晰的最终报告
    print(f"\n**最终验证报告 (UTC 时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')})**")
    print("=" * 35)
    print(f"总计事件: {len(live_events)}")
    print(f"✅ 通过筛选: {len(passed_events)}")
    print(f"❌ 被过滤掉: {rejected_events_count}")
    print("=" * 35)

    print("\n--- ✅ 通过筛选的比赛 (主程序将会跟踪这些) ---")
    if not passed_events:
        print("当前没有符合条件的、即将开始的比赛。")
    else:
        # 按开始时间排序后打印
        for slug, start_t, expiry_t in sorted(passed_events, key=lambda x: x[1]):
            print(f"  - {slug}")
            print(f"    ↳ 官方开始时间 (UTC): {start_t.strftime('%Y-%m-%d %H:%M')}")
            print(f"    ↳ 预计跟踪结束 (UTC): {expiry_t.strftime('%Y-%m-%d %H:%M')}")

    # 打印一些被拒绝的例子，帮助理解
    print("\n--- ❌ 部分被过滤掉的比赛示例 (原因可能是格式不符或已开赛) ---")
    rejected_examples = 0
    for event in live_events:
        if event.get('slug') not in passed_slugs:
            print(f"  - {event.get('slug')} | Title: {event.get('title', 'N/A')[:40]}...")
            rejected_examples += 1
            if rejected_examples >= 10:  # 只显示最多10个例子
                break

    if rejected_examples == 0 and rejected_events_count > 0:
        print("所有事件均已通过筛选。")
    elif rejected_examples < rejected_events_count:
        print(f"  ... 以及其他 {rejected_events_count - rejected_examples} 个被过滤的事件。")

    print("\n" + "-" * 50)
    print("最终验证完成。如果 ✅ 通过筛选 列表符合预期，则主程序工作正常。")
