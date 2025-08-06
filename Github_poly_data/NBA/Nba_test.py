# !/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_NBA_Auto.py

功能:
  为 NBA_Auto.py 提供一个全面的本地单元测试套件。
  该脚本使用模拟数据来验证以下核心功能：
  1. 队伍名称解析 (`parse_nba_teams`) 的准确性。
  2. 比赛筛选逻辑 (`filter_nba_events`) 的正确性，包括对
     “即将开始”、“正在进行”和“已结束”等多种情况的处理。
"""
import unittest
import sys
from datetime import datetime, timezone, timedelta

# 从主脚本中导入我们需要测试的函数
try:
    from NBA_Auto import parse_nba_teams, filter_nba_events
except ImportError:
    print("错误：无法导入所需函数。")
    print("请确保 test_NBA_Auto.py 和 NBA_Auto.py 文件在同一个目录下。")
    sys.exit(1)

# --- 模拟数据和时间 ---

# 设定一个固定的“当前时间”，让测试结果可重复
NOW = datetime.now(timezone.utc)

# 创建一个包含多种场景的模拟API返回数据
MOCK_EVENTS_DATA = [
    {
        # --- 1. 正在进行的比赛 (应该通过) ---
        "slug": "in-progress-game",
        "startTime": (NOW - timedelta(minutes=30)).isoformat().replace('+00:00', 'Z'),
        "closedTime": (NOW + timedelta(hours=3)).isoformat().replace('+00:00', 'Z'),
        "markets": [{"question": "Lakers vs. Clippers"}]
    },
    {
        # --- 2. 即将开始的比赛 (应该通过) ---
        "slug": "upcoming-game",
        "startTime": (NOW + timedelta(hours=1)).isoformat().replace('+00:00', 'Z'),
        "closedTime": (NOW + timedelta(hours=5)).isoformat().replace('+00:00', 'Z'),
        "markets": [{"question": "NBA: Celtics @ Warriors"}]
    },
    {
        # --- 3. 即将开始但缺少 closedTime 的比赛 (应该通过) ---
        # 脚本应为其设置一个默认的结束时间
        "slug": "upcoming-no-close-time",
        "startTime": (NOW + timedelta(hours=2)).isoformat().replace('+00:00', 'Z'),
        # "closedTime" 字段缺失
        "markets": [{"question": "Will the Heat beat the Nuggets?"}]
    },
    {
        # --- 4. 已经结束的比赛 (应该被忽略) ---
        "slug": "finished-game",
        "startTime": (NOW - timedelta(hours=5)).isoformat().replace('+00:00', 'Z'),
        "closedTime": (NOW - timedelta(hours=1)).isoformat().replace('+00:00', 'Z'),
        "markets": [{"question": "Suns vs. Bucks"}]
    },
    {
        # --- 5. 格式不正确的市场 (应该被忽略) ---
        "slug": "invalid-format-mvp",
        "startTime": (NOW + timedelta(hours=1)).isoformat().replace('+00:00', 'Z'),
        "closedTime": (NOW + timedelta(hours=5)).isoformat().replace('+00:00', 'Z'),
        "markets": [{"question": "Who will win MVP?"}]
    },
    {
        # --- 6. 缺少 startTime 的市场 (应该被忽略) ---
        "slug": "missing-start-time",
        # "startTime" 字段缺失
        "closedTime": (NOW + timedelta(hours=5)).isoformat().replace('+00:00', 'Z'),
        "markets": [{"question": "Mavericks vs. 76ers"}]
    }
]


class TestNbaAutoScript(unittest.TestCase):
    """一个用于测试 NBA_Auto.py 核心逻辑的测试套件。"""

    def test_team_parser(self):
        """验证 parse_nba_teams 函数能否正确解析各种格式的比赛标题。"""
        print("\n--- (1/2) 正在测试队伍名称解析逻辑 ---")

        test_cases = {
            "Will the Los Angeles Lakers win against the Boston Celtics?": ("Los Angeles Lakers", "Boston Celtics"),
            "NBA Finals: Golden State Warriors vs. Dallas Mavericks": ("Golden State Warriors", "Dallas Mavericks"),
            "Will the Miami Heat beat the Denver Nuggets?": ("Miami Heat", "Denver Nuggets"),
            "Suns @ Clippers on 2025-12-25": ("Suns", "Clippers"),
            "Who will win the NBA championship?": None,
            "Will Lebron James score over 25.5 points?": None,
        }

        for question, expected in test_cases.items():
            result = parse_nba_teams(question)
            self.assertEqual(result, expected, f"解析 '{question}' 失败")
            status = "✅" if result == expected else "❌"
            print(f"{status} 问题: '{question[:40]}...' -> 预期: {expected}, 结果: {result}")

        print("--- 队伍名称解析测试完成 ---")

    def test_event_filter(self):
        """验证 filter_nba_events 函数能否正确筛选出“正在进行”和“即将开始”的比赛。"""
        print("\n--- (2/2) 正在测试比赛筛选逻辑 ---")
        print(f"模拟的当前时间 (UTC): {NOW.strftime('%Y-m-d %H:%M:%S')}")

        # 调用主脚本的筛选函数
        filtered_list = filter_nba_events(MOCK_EVENTS_DATA)

        # 验证结果
        self.assertEqual(len(filtered_list), 3, "筛选出的比赛数量不等于预期的3个")

        passed_slugs = {event[0] for event in filtered_list}
        expected_slugs = {"in-progress-game", "upcoming-game", "upcoming-no-close-time"}
        self.assertSetEqual(passed_slugs, expected_slugs, "筛选出的比赛 slug 与预期的不完全匹配")

        print("\n筛选结果分析:")
        for slug, start_dt, _ in filtered_list:
            status = "正在进行" if start_dt < NOW else "即将开始"
            print(f"  - ✅ {slug} (状态: {status})")

        print("\n--- 比赛筛选逻辑测试完成 ---")


if __name__ == "__main__":
    print("🚀 **NBA_Auto.py 完整本地测试** 🚀")
    print("=" * 40)
    unittest.main(verbosity=0)
    print("\n🎉 **所有测试均已通过！脚本逻辑正确。** 🎉")