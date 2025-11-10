#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
坐标数据异步转换脚本

功能说明：
1. 将宽格式的用户坐标数据转换为长格式
2. 使用异步方式将英国国家网格坐标 (EPSG:27700) 转换为 WGS84 经纬度坐标 (EPSG:4326)
3. 从 .env 文件读取 API 密钥

作者: Claude Code
日期: 2025
"""

import pandas as pd
import asyncio
import aiohttp
import os
from datetime import datetime
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
        logger.info(f"✅ 批量转换完成! 成功: {success_count}/{total} ({success_rate:.2f}%), 失败: {self.failed_requests}")

        # 打印 API KEY 使用统计
        self.api_key_manager.print_stats()

        return coord_map


async def transform_data_async(
    input_file: str = '../data.csv',
    output_file: str = '../transformed_data.csv',
    api_keys: Optional[List[str]] = None,
    convert_to_latlon: bool = False,
    max_concurrent: int = 10
) -> pd.DataFrame:
    """
    异步转换坐标数据主函数

    参数:
        input_file: 输入 CSV 文件路径
        output_file: 输出 CSV 文件路径
        api_keys: MapTiler API 密钥列表（支持多个 KEY 自动切换）
        convert_to_latlon: 是否转换为经纬度坐标
        max_concurrent: 最大并发请求数

    返回:
        转换后的 DataFrame
    """
    # ========== 第一步：读取原始数据 ==========
    logger.info(f"📂 正在读取文件: {input_file}")
    df = pd.read_csv(input_file, low_memory=False)
    logger.info(f"✓ 共读取 {len(df)} 条用户数据")

    # ========== 第二步：宽格式转长格式 ==========
    logger.info("🔄 开始转换数据格式（宽格式 -> 长格式）...")
    transformed_rows = []

    # 遍历每个用户
    for row_idx, row in df.iterrows():
        p_id = row['id']  # 用户ID

        # 查找所有数据点（a0, a1, a2, ...）
        for col in df.columns:
            if col.startswith('p32220_a'):
                # 提取数据点编号（如 'a0', 'a1'）
                a_id = col.split('_')[1]

                # 获取对应的时间、x、y 列
                time_col = f'p32220_{a_id}'
                x_col = f'p32223_{a_id}'
                y_col = f'p32224_{a_id}'

                time_value = row[time_col]
                x_value = row[x_col]
                y_value = row[y_col]

                # 跳过空值
                if pd.isna(time_value) or pd.isna(x_value) or pd.isna(y_value):
                    continue

                # 解析时间字段
                try:
                    date_obj = datetime.strptime(str(time_value), '%Y/%m/%d')
                    year = date_obj.year
                    month = date_obj.month
                    day = date_obj.day
                except Exception:
                    logger.warning(f"⚠ 无法解析时间 '{time_value}' (用户ID={p_id}, 数据点={a_id})")
                    continue

                # 添加转换后的记录
                transformed_rows.append({
                    'p_id': p_id,
                    'a_id': a_id,
                    'year': year,
                    'month': month,
                    'day': day,
                    'x': int(x_value),
                    'y': int(y_value)
                })

        # 显示进度
        if (row_idx + 1) % 10000 == 0:
            logger.info(f"⏳ 已处理 {row_idx + 1}/{len(df)} 个用户...")

    # 创建转换后的 DataFrame
    transformed_df = pd.DataFrame(transformed_rows)
    logger.info(f"✅ 格式转换完成! 生成 {len(transformed_df)} 条记录")

    # ========== 第三步：异步坐标系转换（可选）==========
    if convert_to_latlon:
        if not api_keys or len(api_keys) == 0:
            logger.error("❌ 错误: 需要提供 MapTiler API 密钥来转换坐标系")
            logger.info("💡 请在 .env 文件中设置 MAPTILER_API_KEY (支持多个 KEY，用逗号分隔)")
            raise ValueError("API key is required for coordinate transformation")

        logger.info("🌍 开始异步转换坐标系 (EPSG:27700 -> EPSG:4326)...")

        # 创建 API 密钥管理器
        api_key_manager = APIKeyManager(api_keys)

        # 初始化异步坐标转换器
        transformer = AsyncCoordinateTransformer(api_key_manager, max_concurrent=max_concurrent)

        # 提取唯一的坐标点（避免重复转换）
        unique_coords = transformed_df[['x', 'y']].drop_duplicates()
        logger.info(f"🔍 发现 {len(unique_coords)} 个唯一坐标点")

        # 异步批量转换
        coords_list = [(row['x'], row['y']) for _, row in unique_coords.iterrows()]
        coord_map = await transformer.transform_batch(coords_list)

        # 应用转换结果
        logger.info("📝 正在应用坐标转换结果...")
        transformed_df['longitude'] = None
        transformed_df['latitude'] = None

        for idx, row in transformed_df.iterrows():
            key = (row['x'], row['y'])
            if key in coord_map:
                lon, lat = coord_map[key]
                transformed_df.at[idx, 'longitude'] = lon
                transformed_df.at[idx, 'latitude'] = lat

        # 统计转换成功率
        success_count = transformed_df['longitude'].notna().sum()
        success_rate = success_count / len(transformed_df) * 100
        logger.info(f"✅ 坐标转换完成! 成功率: {success_rate:.2f}% ({success_count}/{len(transformed_df)})")

    # ========== 第四步：保存结果 ==========
    transformed_df.to_csv(output_file, index=False)
    logger.info(f"💾 数据已保存至: {output_file}")

    # 显示数据预览
    logger.info("\n=== 📊 数据预览 ===")
    print(transformed_df.head(10))

    # 显示数据统计
    logger.info("\n=== 📈 数据统计 ===")
    logger.info(f"总记录数: {len(transformed_df)}")
    logger.info(f"用户数: {transformed_df['p_id'].nunique()}")
    logger.info(f"数据点分布:")
    print(transformed_df['a_id'].value_counts().sort_index().head(10))

    return transformed_df


def main():
    """主函数 - 运行异步转换流程"""
    # ========== 配置参数 ==========
    # 获取脚本所在目录
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    INPUT_FILE = project_root / 'data/origin_data.csv'
    OUTPUT_FILE = project_root / 'data/transformed_data.csv'

    # 从环境变量读取 API 密钥（支持多个 KEY，用逗号分隔）
    maptiler_api_key_str = os.getenv('MAPTILER_API_KEY', '')

    # 解析 API KEY 列表
    api_keys = []
    if maptiler_api_key_str:
        # 按逗号分隔，并去除空格
        api_keys = [key.strip() for key in maptiler_api_key_str.split(',') if key.strip()]

    if not api_keys:
        logger.warning("⚠ 未找到 MAPTILER_API_KEY 环境变量")
        logger.info("💡 将只执行格式转换，不进行坐标系转换")
        logger.info("💡 如需转换坐标系，请在 .env 文件中设置 MAPTILER_API_KEY")
        logger.info("💡 支持多个 API KEY（用逗号分隔）: KEY1,KEY2,KEY3")
    else:
        logger.info(f"✅ 已从环境变量加载 {len(api_keys)} 个 API KEY")

    # 是否转换为经纬度坐标
    CONVERT_TO_LATLON = len(api_keys) > 0  # 有 API KEY 则自动启用

    # 最大并发请求数
    MAX_CONCURRENT = 10

    # ========== 执行异步转换 ==========
    logger.info("=" * 60)
    logger.info("🚀 启动坐标数据异步转换程序")
    logger.info("=" * 60)

    try:
        # 运行异步任务
        asyncio.run(
            transform_data_async(
                input_file=str(INPUT_FILE),
                output_file=str(OUTPUT_FILE),
                api_keys=api_keys,
                convert_to_latlon=CONVERT_TO_LATLON,
                max_concurrent=MAX_CONCURRENT
            )
        )
        logger.info("\n" + "=" * 60)
        logger.info("✅ 所有转换任务完成!")
        logger.info("=" * 60)
    except Exception as e:
        logger.error(f"\n❌ 转换失败: {str(e)}")
        raise


if __name__ == '__main__':
    main()
