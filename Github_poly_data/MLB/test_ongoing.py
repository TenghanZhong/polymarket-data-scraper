# test_live_logic.py
import sys
from datetime import datetime, timezone

# 从主脚本中导入我们需要测试和使用的函数
try:
    # 确保本文件与您的主脚本 (MLB_Auto.py) 在同一个目录下
    from MLB_Auto import fetch_all_events, filter_mlb_events
except ImportError:
    print("错误：无法导入所需函数。")
    print("请确保 test_live_logic.py 和 MLB_Auto.py 文件在同一个目录下。")
    sys.exit(1)

if __name__ == "__main__":
    print("🚀 **实时API验证测试: 跟踪正在进行和即将开始的比赛** 🚀")
    print("-" * 60)

    # 设定一个固定的“当前时间”，用于本次测试的所有比较
    NOW = datetime.now(timezone.utc)
    print(f"当前脚本运行时间 (UTC): {NOW.strftime('%Y-%m-%d %H:%M:%S')}")
    print("正在连接到 Polymarket API 以获取实时 MLB 市场数据...")

    # 1. 从真实 API 获取一次性完整数据
    live_events = fetch_all_events()

    if not live_events:
        print("未能从 API 获取到任何 MLB 事件，测试中止。")
        sys.exit(0)

    print(f"成功从 API 获取到 {len(live_events)} 个活跃的 MLB 事件。")
    print("现在开始应用主程序的筛选逻辑...")
    print("-" * 60)

    # 2. 调用主程序的筛选函数
    events_to_track = filter_mlb_events(live_events)

    print(f"\n✅ 主程序筛选出了 {len(events_to_track)} 个比赛进行跟踪。")
    print("-" * 60)

    if not events_to_track:
        print("当前没有符合条件的、正在进行或即将开始的比赛。")
        sys.exit(0)

    # 3. 逐一分析筛选结果，验证逻辑
    print("--- 筛选结果详细分析 ---")
    in_progress_count = 0
    upcoming_count = 0

    # 按开始时间排序后打印
    for slug, start_dt, expiry_dt in sorted(events_to_track, key=lambda x: x[1]):
        print(f"\n分析对象: {slug}")
        print(f"  - 官方开始时间 (UTC): {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")

        # 模拟主脚本中的等待逻辑
        wait_seconds = (start_dt - NOW).total_seconds()

        if wait_seconds > 0:
            upcoming_count += 1
            print(f"  - \033[92m[状态: 即将开始]\033[0m")
            print(f"  - 验证: 脚本将会等待 {wait_seconds:.0f} 秒。")
        else:
            in_progress_count += 1
            print(f"  - \033[93m[状态: 正在进行]\033[0m")
            print(f"  - 验证: 脚本检测到比赛已开始，将立即采集数据。")

    print("\n" + "-" * 60)
    print("🎉 **测试完成!**")
    print(f"脚本成功识别了 {in_progress_count} 场正在进行的比赛和 {upcoming_count} 场即将开始的比赛。")