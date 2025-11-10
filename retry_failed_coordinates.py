#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重新转换失败的坐标点 - 临时脚本

功能说明：
1. 从 transformed_data.csv 中找出转换失败的坐标（longitude/latitude 为空）
2. 使用新的 API KEY 重新批量转换
3. 将成功转换的结果更新回原 CSV 文件

使用方法：
1. 在 .env 文件中更新 MAPTILER_API_KEY
2. 运行: python retry_failed_coordinates.py

作者: Claude Code
日期: 2025-11-11
"""

import pandas as pd
import asyncio
import aiohttp
import os
from typing import Tuple, Optional, List, Dict
from pathlib import Path
import logging
from dotenv import load_dotenv
from collections import defaultdict

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class APIKeyManager:
    """API 密钥管理器 - 支持多个 API KEY 并自动切换"""

    def __init__(self, api_keys: List[str]):
        """
        初始化 API 密钥管理器

        参数:
            api_keys: API 密钥列表
        """
        if not api_keys:
            raise ValueError("至少需要提供一个 API KEY")

        self.api_keys = api_keys
        self.current_index = 0
        self.stats = defaultdict(lambda: {
            'requests': 0,
            'success': 0,
            'rate_limit': 0,
            'errors': 0
        })
        logger.info(f"✅ 已加载 {len(api_keys)} 个 API KEY")

    def get_current_key(self) -> str:
        """获取当前使用的 API KEY"""
        return self.api_keys[self.current_index]

    def switch_to_next_key(self) -> bool:
        """
        切换到下一个 API KEY

        返回:
            是否成功切换（False 表示已经是最后一个 KEY）
        """
        if self.current_index < len(self.api_keys) - 1:
            old_index = self.current_index
            self.current_index += 1
            logger.warning(f"🔄 切换 API KEY: [{old_index}] -> [{self.current_index}]")
            return True
        else:
            logger.error(f"⚠️ 所有 {len(self.api_keys)} 个 API KEY 都已达到速率限制!")
            return False

    def record_request(self, key: str):
        """记录请求"""
        self.stats[key]['requests'] += 1

    def record_success(self, key: str):
        """记录成功"""
        self.stats[key]['success'] += 1

    def record_rate_limit(self, key: str):
        """记录速率限制"""
        self.stats[key]['rate_limit'] += 1

    def record_error(self, key: str):
        """记录错误"""
        self.stats[key]['errors'] += 1

    def print_stats(self):
        """打印使用统计"""
        logger.info("\n=== 📊 API KEY 使用统计 ===")
        for idx, key in enumerate(self.api_keys):
            stats = self.stats[key]
            key_preview = f"{key[:10]}...{key[-4:]}" if len(key) > 14 else key
            logger.info(f"KEY [{idx}] ({key_preview}):")
            logger.info(f"  总请求: {stats['requests']}")
            logger.info(f"  成功: {stats['success']}")
            logger.info(f"  速率限制: {stats['rate_limit']}")
            logger.info(f"  其他错误: {stats['errors']}")


class AsyncCoordinateTransformer:
    """异步坐标转换器类 - 使用 MapTiler API 进行坐标系转换"""

    # MapTiler API 配置
    API_BASE_URL = "https://api.maptiler.com/coordinates/transform"
    SOURCE_CRS = 27700  # 英国国家网格坐标系 EPSG代码
    TARGET_CRS = 4326   # WGS84 经纬度坐标系 EPSG代码

    def __init__(self, api_key_manager: APIKeyManager, max_concurrent: int = 10):
        """
        初始化异步坐标转换器

        参数:
            api_key_manager: API 密钥管理器
            max_concurrent: 最大并发请求数
        """
        self.api_key_manager = api_key_manager
        self.max_concurrent = max_concurrent
        self.request_count = 0
        self.failed_requests = 0
        self.successful_requests = 0
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limit_backoff = 1.0  # 速率限制退避时间（秒）

    async def transform_coordinate(
        self,
        session: aiohttp.ClientSession,
        x: float,
        y: float,
        max_retries: int = 3
    ) -> Tuple[Tuple[float, float], Optional[Tuple[float, float]]]:
        """
        异步转换单个坐标点，支持 API KEY 自动切换

        参数:
            session: aiohttp 会话对象
            x: 东向坐标 (Easting)
            y: 北向坐标 (Northing)
            max_retries: 最大重试次数（遇到 429 错误时）

        返回:
            ((x, y), (lon, lat)) 元组，失败则返回 ((x, y), None)
        """
        async with self.semaphore:  # 限制并发数
            for retry in range(max_retries):
                try:
                    # 获取当前 API KEY
                    current_key = self.api_key_manager.get_current_key()

                    # 构建 API 请求 URL
                    url = f"{self.API_BASE_URL}/{x},{y}.json"
                    params = {
                        's_srs': self.SOURCE_CRS,
                        't_srs': self.TARGET_CRS,
                        'key': current_key
                    }

                    # 发送异步请求
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        self.request_count += 1
                        self.api_key_manager.record_request(current_key)

                        if response.status == 200:
                            data = await response.json()
                            # API 返回格式: {"results": [{"x": lon, "y": lat}]}
                            if 'results' in data and len(data['results']) > 0:
                                result = data['results'][0]
                                lon = result['x']
                                lat = result['y']
                                self.successful_requests += 1
                                self.api_key_manager.record_success(current_key)
                                logger.debug(f"✓ ({x}, {y}) -> ({lon:.6f}, {lat:.6f})")
                                return ((x, y), (lon, lat))
                            else:
                                logger.warning(f"⚠ Invalid API response for ({x}, {y})")
                                self.api_key_manager.record_error(current_key)
                                self.failed_requests += 1
                                return ((x, y), None)

                        elif response.status == 429:
                            # 遇到速率限制，使用指数退避策略
                            self.api_key_manager.record_rate_limit(current_key)

                            # 首次或前几次重试：等待后重试（不切换 KEY）
                            if retry < max_retries - 1:
                                wait_time = self.rate_limit_backoff * (2 ** retry)  # 指数退避
                                logger.warning(f"⚠ 速率限制 (429) for ({x}, {y})，等待 {wait_time:.1f}s 后重试 ({retry + 1}/{max_retries})")
                                await asyncio.sleep(wait_time)
                                continue  # 重试请求
                            else:
                                # 多次重试后仍然 429，尝试切换 API KEY
                                logger.warning(f"⚠ 持续速率限制 (429) for ({x}, {y})，尝试切换 API KEY")
                                if self.api_key_manager.switch_to_next_key():
                                    logger.info(f"🔄 切换到新的 API KEY 并重试坐标 ({x}, {y})")
                                    await asyncio.sleep(1.0)  # 切换后等待
                                    continue  # 重试请求
                                else:
                                    # 所有 API KEY 都已达到限制
                                    logger.error(f"❌ 所有 API KEY 都已达到速率限制，坐标 ({x}, {y}) 转换失败")
                                    self.failed_requests += 1
                                    return ((x, y), None)

                        else:
                            logger.error(f"✗ API status {response.status} for ({x}, {y})")
                            self.api_key_manager.record_error(current_key)
                            self.failed_requests += 1
                            return ((x, y), None)

                except asyncio.TimeoutError:
                    logger.error(f"⏱ Timeout for ({x}, {y})")
                    current_key = self.api_key_manager.get_current_key()
                    self.api_key_manager.record_error(current_key)
                    self.failed_requests += 1
                    return ((x, y), None)
                except Exception as e:
                    logger.error(f"❌ Error for ({x}, {y}): {str(e)}")
                    current_key = self.api_key_manager.get_current_key()
                    self.api_key_manager.record_error(current_key)
                    self.failed_requests += 1
                    return ((x, y), None)

            # 所有重试都失败
            return ((x, y), None)

    async def transform_batch(
        self,
        coordinates_list: List[Tuple[float, float]]
    ) -> Dict[Tuple[float, float], Tuple[float, float]]:
        """
        异步批量转换坐标

        参数:
            coordinates_list: 坐标列表 [(x1, y1), (x2, y2), ...]

        返回:
            坐标映射字典 {(x, y): (lon, lat), ...}
        """
        total = len(coordinates_list)
        logger.info(f"🚀 开始异步批量转换 {total} 个坐标点 (并发数: {self.max_concurrent})...")

        # 创建异步 HTTP 会话
        async with aiohttp.ClientSession() as session:
            # 创建所有异步任务
            tasks = [
                self.transform_coordinate(session, x, y)
                for x, y in coordinates_list
            ]

            # 并发执行所有任务并显示进度
            results = []
            batch_size = 100
            for i in range(0, len(tasks), batch_size):
                batch = tasks[i:i + batch_size]
                batch_results = await asyncio.gather(*batch)
                results.extend(batch_results)

                # 显示进度
                progress = min(i + batch_size, total)
                logger.info(f"📊 进度: {progress}/{total} ({progress/total*100:.1f}%)")

        # 构建坐标映射字典
        coord_map = {}
        for (x, y), result in results:
            if result:
                coord_map[(x, y)] = result

        success_count = len(coord_map)
        success_rate = success_count / total * 100 if total > 0 else 0
        logger.info(f"✅ 批量转换完成!")
        logger.info(f"   成功: {self.successful_requests}/{total} ({success_rate:.2f}%)")
        logger.info(f"   失败: {self.failed_requests}")

        # 打印 API KEY 使用统计
        self.api_key_manager.print_stats()

        return coord_map


async def retry_failed_coordinates():
    """
    重新转换失败的坐标点主函数
    """
    # ========== 配置参数 ==========
    project_root = Path(__file__).parent
    DATA_FILE = project_root / 'data/transformed_data.csv'

    # 从环境变量读取 API 密钥（支持多个 KEY，用逗号分隔）
    maptiler_api_key_str = os.getenv('MAPTILER_API_KEY', '')

    # 解析 API KEY 列表
    api_keys = []
    if maptiler_api_key_str:
        # 按逗号分隔，并去除空格
        api_keys = [key.strip() for key in maptiler_api_key_str.split(',') if key.strip()]

    if not api_keys:
        logger.error("❌ 错误: 未找到 MAPTILER_API_KEY 环境变量")
        logger.info("💡 请在 .env 文件中设置新的 MAPTILER_API_KEY")
        logger.info("💡 支持多个 API KEY（用逗号分隔）: KEY1,KEY2,KEY3")
        return

    # 最大并发请求数
    MAX_CONCURRENT = 10

    logger.info("=" * 60)
    logger.info("🔄 开始重新转换失败的坐标点")
    logger.info("=" * 60)

    # ========== 第一步：读取数据并找出失败的坐标 ==========
    logger.info(f"📂 正在读取文件: {DATA_FILE}")
    df = pd.read_csv(DATA_FILE, low_memory=False)
    logger.info(f"✓ 共读取 {len(df)} 条记录")

    # 找出转换失败的记录（longitude 或 latitude 为空）
    failed_mask = df['longitude'].isna() | df['latitude'].isna()
    failed_df = df[failed_mask]
    failed_count = len(failed_df)

    if failed_count == 0:
        logger.info("✅ 没有失败的坐标需要重新转换!")
        return

    logger.info(f"🔍 发现 {failed_count} 条记录需要重新转换")

    # 提取唯一的失败坐标点
    unique_failed_coords = failed_df[['x', 'y']].drop_duplicates()
    unique_count = len(unique_failed_coords)
    logger.info(f"🎯 共有 {unique_count} 个唯一的失败坐标点")

    # ========== 第二步：重新转换失败的坐标 ==========
    logger.info("🌍 开始重新转换坐标系 (EPSG:27700 -> EPSG:4326)...")

    # 创建 API 密钥管理器
    api_key_manager = APIKeyManager(api_keys)

    # 初始化异步坐标转换器
    transformer = AsyncCoordinateTransformer(api_key_manager, max_concurrent=MAX_CONCURRENT)

    # 异步批量转换
    coords_list = [(row['x'], row['y']) for _, row in unique_failed_coords.iterrows()]
    coord_map = await transformer.transform_batch(coords_list)

    if not coord_map:
        logger.warning("⚠ 没有坐标转换成功，无需更新文件")
        return

    # ========== 第三步：更新原数据 ==========
    logger.info("📝 正在更新坐标转换结果...")

    updated_count = 0
    for idx, row in df.iterrows():
        # 只更新之前失败的记录
        if pd.isna(row['longitude']) or pd.isna(row['latitude']):
            key = (row['x'], row['y'])
            if key in coord_map:
                lon, lat = coord_map[key]
                df.at[idx, 'longitude'] = lon
                df.at[idx, 'latitude'] = lat
                updated_count += 1

    logger.info(f"✅ 成功更新 {updated_count} 条记录")

    # ========== 第四步：保存更新后的文件 ==========
    logger.info(f"💾 正在保存更新后的数据到: {DATA_FILE}")
    df.to_csv(DATA_FILE, index=False)
    logger.info("✅ 文件保存成功!")

    # ========== 第五步：显示最终统计 ==========
    logger.info("\n" + "=" * 60)
    logger.info("📊 最终统计")
    logger.info("=" * 60)

    # 统计当前成功率
    total_records = len(df)
    successful_records = df['longitude'].notna().sum()
    success_rate = successful_records / total_records * 100

    logger.info(f"总记录数: {total_records}")
    logger.info(f"成功转换: {successful_records} ({success_rate:.2f}%)")
    logger.info(f"仍然失败: {total_records - successful_records}")

    # 显示数据预览
    logger.info("\n=== 📊 更新后的数据预览 ===")
    print(df.head(10))

    logger.info("\n" + "=" * 60)
    logger.info("✅ 重新转换任务完成!")
    logger.info("=" * 60)


def main():
    """主函数"""
    try:
        asyncio.run(retry_failed_coordinates())
    except Exception as e:
        logger.error(f"\n❌ 转换失败: {str(e)}")
        raise


if __name__ == '__main__':
    main()
