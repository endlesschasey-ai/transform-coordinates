#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速测试脚本 - 只测试前10个坐标点
"""

import asyncio
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# 添加 src 目录到路径
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from transform import AsyncCoordinateTransformer, APIKeyManager

# 加载环境变量
load_dotenv()

async def quick_test():
    """快速测试坐标转换"""
    # 从环境变量读取 API 密钥（支持多个 KEY，用逗号分隔）
    maptiler_api_key_str = os.getenv('MAPTILER_API_KEY', '')

    # 解析 API KEY 列表
    api_keys = []
    if maptiler_api_key_str:
        api_keys = [key.strip() for key in maptiler_api_key_str.split(',') if key.strip()]

    if not api_keys:
        print("❌ 未找到 API KEY")
        return

    print("=" * 60)
    print("🧪 快速测试 - 转换10个坐标点")
    print("=" * 60)

    # 测试坐标列表
    test_coords = [
        (430000, 418000),
        (444000, 520000),
        (382000, 403000),
        (441000, 405000),
        (462000, 397000),
        (430000, 558000),
        (432000, 470000),
        (383000, 328000),
        (390000, 350000),
        (531000, 157000)
    ]

    # 创建 API 密钥管理器和转换器
    api_key_manager = APIKeyManager(api_keys)
    transformer = AsyncCoordinateTransformer(api_key_manager, max_concurrent=5)

    # 批量转换
    result_map = await transformer.transform_batch(test_coords)

    print("\n" + "=" * 60)
    print("📊 转换结果")
    print("=" * 60)

    for (x, y) in test_coords:
        if (x, y) in result_map:
            lon, lat = result_map[(x, y)]
            print(f"✅ ({x}, {y}) -> ({lon:.6f}, {lat:.6f})")
        else:
            print(f"❌ ({x}, {y}) -> 转换失败")

    print("\n" + "=" * 60)
    print(f"✅ 测试完成! 成功率: {len(result_map)}/{len(test_coords)} ({len(result_map)/len(test_coords)*100:.1f}%)")
    print("=" * 60)

if __name__ == '__main__':
    asyncio.run(quick_test())
