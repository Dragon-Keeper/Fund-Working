#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 爬取开放式基金数据
"""
基金数据爬取与HDF5存储系统
用于从东方财富网爬取基金数据并存储到HDF5数据库中
支持多线程爬取、错误处理和数据验证
"""

import os
import sys
import argparse
import time
import requests
import re
import json
import h5py
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

# 设置日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("fund_crawl.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


# 检查并安装必要的依赖
def check_and_install_dependencies():
    """检查并安装必要的依赖"""
    required_packages = ["pandas", "numpy", "h5py", "requests"]
    missing_packages = []

    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        logger.info(f"缺少必要的依赖包: {', '.join(missing_packages)}")
        logger.info("正在安装...")
        os.system(f"{sys.executable} -m pip install {' '.join(missing_packages)}")
        logger.info("安装完成，请重新运行程序")
        sys.exit(0)

    logger.info("所有依赖包已安装")


# 获取HDF5文件路径
def get_hdf5_path():
    """获取HDF5文件路径"""
    # 获取当前脚本所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 构建HDF5文件路径
    hdf5_dir = os.path.join(current_dir, "data")
    # 确保目录存在
    if not os.path.exists(hdf5_dir):
        os.makedirs(hdf5_dir)
    # 构建HDF5文件完整路径
    hdf5_path = os.path.join(hdf5_dir, "Fetch_Fund_Data.h5")
    return hdf5_path


# 获取总页数
def get_total_pages():
    """从东方财富网获取总页数"""
    url = "https://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=10&lx=1&letter=&gsid=&text=&sort=rzdf,desc&page=1,200"
    try:
        response = requests.get(url, timeout=10)
        response.encoding = "utf-8"

        # 使用正则表达式提取总页数
        pages_match = re.search(r"pages:\"(\d+)\"", response.text)
        if pages_match:
            total_pages = int(pages_match.group(1))
            logger.info(f"成功获取总页数: {total_pages}")
            return total_pages
        else:
            logger.error("未能找到总页数信息")
            return 113  # 默认返回113页，根据用户提供的信息
    except Exception as e:
        logger.error(f"获取总页数时发生错误: {e}")
        return 113  # 默认返回113页


# 解析基金数据
def parse_fund_data(page_content):
    """解析页面内容，提取基金数据"""
    try:
        # 1. 提取数据部分 - 使用多种正则表达式模式
        logger.info("开始提取数据部分...")

        # 尝试多种正则表达式模式
        patterns = [
            r"var db=\{\s*chars:\[.*?\]\s*,\s*datas:\[(.*?)\]\s*\}",  # 完整模式
            r"datas:\[(.*?)\]",  # 简化模式
            r'\[\["[^"]*",[^\]]*\]\]',  # 数据数组模式
        ]

        data_str = None
        for i, pattern in enumerate(patterns):
            logger.debug(f"尝试正则表达式模式 {i+1}: {pattern}")
            match = re.search(pattern, page_content, re.DOTALL)
            if match:
                data_str = match.group(1)
                logger.info(f"使用模式 {i+1} 成功提取数据部分")
                break

        if not data_str:
            logger.error("未能找到基金数据")
            return []

        logger.debug(f"提取到的数据部分 (前200字符): {data_str[:200]}")

        # 2. 解析基金数据项
        logger.info("开始解析基金数据项...")
        funds_data = []
        matches = []

        # 方法1: 使用精确的正则表达式匹配每个基金数据项
        logger.info("方法1: 使用精确的正则表达式匹配每个基金数据项")
        # 这是一个更精确的正则表达式，考虑到了转义字符和各种特殊情况
        fund_pattern = re.compile(
            r'\["([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)","([^"\\]*(?:\\.[^"\\]*)*)"\]'
        )

        matches = fund_pattern.findall(data_str)
        logger.info(f"方法1: 找到 {len(matches)} 个基金数据项")

        # 如果方法1失败，尝试方法2
        if len(matches) < 100:
            logger.info("方法1效果不佳，尝试方法2: 手动解析数据字符串")
            # 方法2: 手动解析数据字符串
            # 首先清理首尾的可能存在的括号
            cleaned_data = data_str.strip("[]")

            # 寻找所有基金数据项的开始和结束位置
            start_positions = []
            end_positions = []
            in_quotes = False
            bracket_count = 0

            for i, char in enumerate(cleaned_data):
                if char == '"' and (i == 0 or cleaned_data[i - 1] != "\\"):
                    in_quotes = not in_quotes
                elif not in_quotes:
                    if char == "[" and bracket_count == 0:
                        start_positions.append(i)
                    elif char == "]" and bracket_count == 1:
                        end_positions.append(i + 1)

                    if char == "[":
                        bracket_count += 1
                    elif char == "]":
                        bracket_count -= 1

            # 提取每个基金数据项
            manual_matches = []
            for start, end in zip(start_positions, end_positions):
                fund_str = cleaned_data[start:end]
                # 分割数据项
                items = []
                current_item = ""
                in_quotes = False

                for char in fund_str[1:-1]:  # 去掉首尾的 [ 和 ]
                    if char == '"' and (not current_item or current_item[-1] != "\\"):
                        in_quotes = not in_quotes
                        current_item += char
                    elif char == "," and not in_quotes:
                        items.append(current_item)
                        current_item = ""
                    else:
                        current_item += char

                # 添加最后一个item
                if current_item:
                    items.append(current_item)

                # 清理引号
                items = [item.strip('"') for item in items]

                if len(items) >= 23:
                    manual_matches.append(tuple(items[:23]))

            logger.info(f"方法2: 手动解析找到 {len(manual_matches)} 个基金数据项")

            # 如果手动解析更好，使用手动解析的结果
            if len(manual_matches) > len(matches):
                matches = manual_matches

        # 如果前面的方法都失败，尝试方法3：按23个字段分割
        if len(matches) < 100:
            logger.info("方法1和方法2都效果不佳，尝试方法3: 按23个字段分割")
            # 清理首尾的括号和引号
            cleaned_data = data_str.strip("[]")
            # 使用更安全的分割方法，考虑引号内的逗号
            items = []
            current_item = ""
            in_quotes = False

            for char in cleaned_data:
                if char == '"' and (not current_item or current_item[-1] != "\\"):
                    in_quotes = not in_quotes
                    current_item += char
                elif char == "," and not in_quotes:
                    items.append(current_item)
                    current_item = ""
                else:
                    current_item += char

            # 添加最后一个item
            if current_item:
                items.append(current_item)

            # 清理引号
            items = [item.strip('"') for item in items]

            # 假设每个基金数据项有23个字段，尝试按这个长度分割
            if len(items) >= 23:
                fund_count = len(items) // 23
                logger.info(f"方法3: 按23个字段分割，找到 {fund_count} 个基金数据项")

                matches = []
                for i in range(fund_count):
                    start_idx = i * 23
                    end_idx = start_idx + 23
                    fund_items = items[start_idx:end_idx]
                    matches.append(tuple(fund_items))

        # 处理找到的匹配项
        for items in matches:
            # 确保有足够的数据项
            if len(items) >= 23:
                try:
                    # 根据用户提供的字段映射关系解析数据
                    fund_info = {
                        "fund_code": items[0],  # 基金代码
                        "fund_name": items[1],  # 基金简称
                        "current_unit_nav": (
                            float(items[3]) if items[3] and items[3] != "" else None
                        ),  # 最新交易日的单位净值
                        "current_accumulated_nav": (
                            float(items[4]) if items[4] and items[4] != "" else None
                        ),  # 最新交易日的累计净值
                        "previous_unit_nav": (
                            float(items[5]) if items[5] and items[5] != "" else None
                        ),  # 上一个交易日的单位净值
                        "previous_accumulated_nav": (
                            float(items[6]) if items[6] and items[6] != "" else None
                        ),  # 上一个交易日的累计净值
                        "daily_growth_value": (
                            float(items[7]) if items[7] and items[7] != "" else None
                        ),  # 日增长值
                        "daily_growth_rate": (
                            float(items[8]) if items[8] and items[8] != "" else None
                        ),  # 日增长率
                        "purchase_status": items[9],  # 申购状态
                        "redemption_status": items[10],  # 赎回状态
                        "actual_fee_rate": (
                            items[17] if items[17] and items[17] != "" else None
                        ),  # 实际的手续费
                        "original_fee_rate": (
                            items[20] if items[20] and items[20] != "" else None
                        ),  # 原来的手续费
                        "current_date": items[21],  # 最新交易日的日期
                        "previous_date": items[22],  # 上一个交易日的日期
                    }
                    funds_data.append(fund_info)
                except Exception as e:
                    logger.warning(f"解析单个基金数据项时出错: {e}")
                    logger.debug(f"出错的数据项: {items[:5]}...")

        logger.info(f"成功解析 {len(funds_data)} 只基金数据")
        return funds_data
    except Exception as e:
        logger.error(f"解析基金数据时发生错误: {e}")
        import traceback

        traceback.print_exc()
        return []


# 获取原始页面内容
def get_raw_page_content(page_num):
    """获取指定页面的原始内容"""
    url = f"https://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx?t=10&lx=1&letter=&gsid=&text=&sort=rzdf,desc&page={page_num},200"
    retries = 3
    delay = 2

    for attempt in range(retries):
        try:
            logger.info(
                f"正在获取第 {page_num} 页原始内容 (尝试 {attempt + 1}/{retries})"
            )
            response = requests.get(url, timeout=15)

            # 尝试使用不同的编码
            response.encoding = "utf-8"  # 先尝试UTF-8
            if response.status_code == 200:
                # 检查是否有乱码，尝试GBK编码
                if "娴峰瘜閫氭垚" in response.text or "寮€鏀剧敵璐" in response.text:
                    logger.info("检测到乱码，尝试使用GBK编码重新解析")
                    response.encoding = "gbk"

                logger.info(f"成功获取第 {page_num} 页原始内容")
                return response.text
            else:
                logger.warning(
                    f"第 {page_num} 页请求失败，状态码: {response.status_code}"
                )
        except Exception as e:
            logger.warning(f"第 {page_num} 页获取异常: {e}")

        # 重试前等待
        if attempt < retries - 1:
            logger.info(f"{delay}秒后重试...")
            time.sleep(delay)
            delay *= 1.5  # 指数退避

    logger.error(f"第 {page_num} 页原始内容获取失败，已达到最大重试次数")
    return None


# 获取单页基金数据
def fetch_page_data(page_num):
    """获取指定页面的基金数据"""
    # 先获取原始页面内容
    page_content = get_raw_page_content(page_num)
    if not page_content:
        return page_num, []

    # 解析页面内容
    fund_data = parse_fund_data(page_content)
    logger.info(f"第 {page_num} 页数据获取完成，共 {len(fund_data)} 条记录")
    return page_num, fund_data


# 将基金数据存储到HDF5
def store_fund_data_to_hdf5(fund_data, hdf5_path):
    """将基金数据存储到HDF5文件中"""
    try:
        # 如果文件不存在，创建新文件；如果存在，追加数据
        mode = "a" if os.path.exists(hdf5_path) else "w"

        with h5py.File(hdf5_path, mode) as hf:
            # 按基金代码组织数据
            for fund in fund_data:
                fund_code = fund["fund_code"]

                # 如果基金组不存在，创建它
                if fund_code not in hf:
                    group = hf.create_group(fund_code)
                    # 存储属性信息
                    for key, value in fund.items():
                        if key != "fund_code":  # 基金代码已经是组名
                            if isinstance(value, str):
                                group.attrs[key] = value
                            elif value is not None:
                                group.attrs[key] = value

                    # 记录存储时间
                    group.attrs["last_updated"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    logger.debug(f"基金 {fund_code} {fund['fund_name']} 数据已存储")
                else:
                    # 如果基金已存在，更新数据
                    group = hf[fund_code]
                    for key, value in fund.items():
                        if key != "fund_code":
                            if isinstance(value, str):
                                group.attrs[key] = value
                            elif value is not None:
                                group.attrs[key] = value

                    # 更新存储时间
                    group.attrs["last_updated"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    logger.debug(f"基金 {fund_code} {fund['fund_name']} 数据已更新")

        logger.info(f"成功存储 {len(fund_data)} 条基金数据到HDF5文件")
        return True
    except Exception as e:
        logger.error(f"存储基金数据到HDF5时发生错误: {e}")
        return False


# 根据基金代码查询数据
def query_fund_by_code(fund_code):
    """根据基金代码查询基金数据"""
    hdf5_path = get_hdf5_path()

    # 检查文件是否存在
    if not os.path.exists(hdf5_path):
        logger.warning(f"HDF5文件不存在: {hdf5_path}")
        print("\n错误：基金数据库文件不存在，请先下载数据！")
        return False

    try:
        with h5py.File(hdf5_path, "r") as hf:
            if fund_code not in hf:
                logger.warning(f"基金代码 {fund_code} 不存在于数据库中")
                print(f"\n错误：未找到基金代码 {fund_code} 的数据！")
                return False

            # 获取基金数据
            fund_group = hf[fund_code]
            fund_data = {}

            # 读取所有属性
            for key in fund_group.attrs:
                fund_data[key] = fund_group.attrs[key]

            # 格式化显示基金数据
            print("\n===== 基金数据详情 ======")
            print(f"基金代码: {fund_data.get('fund_code', '未知')}")
            print(f"基金简称: {fund_data.get('fund_name', '未知')}")
            print(f"最新单位净值: {fund_data.get('current_unit_nav', '未知')}")
            print(f"最新累计净值: {fund_data.get('current_accumulated_nav', '未知')}")
            print(f"上一交易日单位净值: {fund_data.get('previous_unit_nav', '未知')}")
            print(
                f"上一交易日累计净值: {fund_data.get('previous_accumulated_nav', '未知')}"
            )
            print(f"日增长值: {fund_data.get('daily_growth_value', '未知')}")
            print(f"日增长率: {fund_data.get('daily_growth_rate', '未知')}%")
            print(f"申购状态: {fund_data.get('purchase_status', '未知')}")
            print(f"赎回状态: {fund_data.get('redemption_status', '未知')}")
            print(f"实际手续费率: {fund_data.get('actual_fee_rate', '未知')}")
            print(f"原始手续费率: {fund_data.get('original_fee_rate', '未知')}")
            print(f"最新交易日期: {fund_data.get('current_date', '未知')}")
            print(f"数据更新时间: {fund_data.get('last_updated', '未知')}")
            print("========================\n")

            return True
    except Exception as e:
        logger.error(f"查询基金数据时发生错误: {e}")
        print(f"\n错误：查询基金数据时发生异常: {e}")
        return False

# 获取所有基金代码
def get_all_fund_codes():
    """获取HDF5文件中所有基金代码"""
    hdf5_path = get_hdf5_path()
    
    if not os.path.exists(hdf5_path):
        print(f"错误：HDF5文件不存在: {hdf5_path}")
        return []
    
    try:
        with h5py.File(hdf5_path, "r") as f:
            # 返回根目录下所有组名（即基金代码）
            return list(f.keys())
    except Exception as e:
        print(f"读取基金代码时发生错误: {e}")
        return []

# 显示所有基金代码
def show_all_fund_codes():
    """显示所有基金代码"""
    all_fund_codes = get_all_fund_codes()
    
    if not all_fund_codes:
        print("数据库中没有基金数据")
        return
    
    print(f"\n共有 {len(all_fund_codes)} 只基金:")
    # 分页显示基金代码
    page_size = 20
    exit_view = False
    for i in range(0, len(all_fund_codes), page_size):
        if exit_view:
            break
            
        page_codes = all_fund_codes[i:i+page_size]
        for code in page_codes:
            print(code, end='  ')
        print()
        
        if i + page_size < len(all_fund_codes):
            user_input = input("按Enter键查看下一页... 或按'q'退出查看: ").strip().lower()
            if user_input == 'q':
                exit_view = True


# 显示菜单
def show_menu():
    """显示主菜单"""
    print("\n===== 开放式基金数据管理系统 ======")
    print("1. 下载所有页面的开放式基金数据")
    print("2. 查询开放式基金数据")
    print("3. 查看所有基金代码")
    print("0. 退出系统")
    print("========================\n")


# 使用多线程批量获取基金数据
def batch_fetch_fund_data(total_pages, max_threads=8):
    """使用多线程批量获取所有页面的基金数据"""
    all_fund_data = []
    success_pages = 0
    failed_pages = 0

    # 计算最优线程数
    optimal_threads = min(max_threads, total_pages, os.cpu_count() or 4)
    logger.info(f"将使用 {optimal_threads} 个线程进行数据爬取")

    # 创建进度条显示
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=optimal_threads) as executor:
        # 提交所有页面的爬取任务
        future_to_page = {
            executor.submit(fetch_page_data, page_num): page_num
            for page_num in range(1, total_pages + 1)
        }

        # 处理完成的任务
        for future in as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                _, fund_data = future.result()
                if fund_data:
                    all_fund_data.extend(fund_data)
                    success_pages += 1
                else:
                    failed_pages += 1
                    logger.warning(f"第 {page_num} 页未返回有效数据")

                # 显示进度
                elapsed_time = time.time() - start_time
                progress = (success_pages + failed_pages) / total_pages * 100
                logger.info(
                    f"进度: {success_pages + failed_pages}/{total_pages} ({progress:.2f}%) - 已成功: {success_pages} 页 - 已失败: {failed_pages} 页 - 耗时: {elapsed_time:.2f}秒"
                )

                # 每获取几页数据就保存一次，避免数据丢失
                if (success_pages + failed_pages) % 10 == 0:
                    hdf5_path = get_hdf5_path()
                    store_fund_data_to_hdf5(
                        all_fund_data[-2000:], hdf5_path
                    )  # 保存最近的2000条数据
            except Exception as e:
                failed_pages += 1
                logger.error(f"处理第 {page_num} 页数据时发生异常: {e}")

    total_time = time.time() - start_time
    logger.info(
        f"所有页面爬取完成 - 成功: {success_pages} 页 - 失败: {failed_pages} 页 - 总耗时: {total_time:.2f}秒"
    )

    return all_fund_data


# 验证数据一致性
def verify_fund_data(fund_data):
    """验证爬取的基金数据完整性"""
    # 检查数据数量
    logger.info(f"共爬取 {len(fund_data)} 条基金数据")

    # 检查是否包含示例数据中的基金
    sample_fund = next(
        (fund for fund in fund_data if fund["fund_code"] == "004041"), None
    )
    if sample_fund:
        logger.info(
            f"示例基金 004041 验证成功: 单位净值={sample_fund['current_unit_nav']}, 累计净值={sample_fund['current_accumulated_nav']}"
        )
    else:
        logger.warning("未找到示例基金 004041")

    # 统计数据质量
    valid_count = sum(1 for fund in fund_data if fund["current_unit_nav"] is not None)
    invalid_count = len(fund_data) - valid_count
    logger.info(f"有效数据: {valid_count} 条, 无效数据: {invalid_count} 条")

    # 返回验证结果
    return valid_count > 0


# 主函数
def main():
    """主函数，协调整个爬取和存储过程，提供菜单驱动的用户界面"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='开放式基金数据管理系统')
    parser.add_argument('--auto', action='store_true', help='自动模式：仅执行数据下载操作')
    args = parser.parse_args()
    
    logger.info("===== 基金数据爬取与存储系统启动 =====")

    try:
        # 检查依赖
        check_and_install_dependencies()

        if args.auto:
            # 自动模式：直接执行下载操作
            try:
                # 获取总页数
                total_pages = get_total_pages()
                if total_pages <= 0:
                    logger.error("无法获取总页数，操作取消")
                    print("\n错误：无法获取总页数，请检查网络连接或网站是否可访问！")
                    return

                logger.info(f"将爬取 {total_pages} 页基金数据")
                print(f"\n检测到总页数: {total_pages} 页")

                # 批量获取基金数据
                logger.info("开始爬取基金数据...")
                print("\n开始爬取基金数据，请稍候...")
                all_fund_data = batch_fetch_fund_data(total_pages)

                if not all_fund_data:
                    logger.error("未获取到任何基金数据")
                    print("\n错误：未获取到任何基金数据！")
                    return

                # 验证数据
                logger.info("开始验证数据...")
                print("\n开始验证数据...")
                if verify_fund_data(all_fund_data):
                    # 存储所有数据
                    hdf5_path = get_hdf5_path()
                    logger.info(f"开始将数据存储到HDF5文件: {hdf5_path}")
                    print(f"\n开始将数据存储到HDF5文件: {hdf5_path}")
                    store_fund_data_to_hdf5(all_fund_data, hdf5_path)
                    logger.info("数据存储完成")
                    print("\n数据存储完成")

                    # 显示最终统计信息
                    logger.info(f"===== 任务完成 =====")
                    logger.info(f"总页数: {total_pages}")
                    logger.info(f"成功爬取基金数量: {len(all_fund_data)}")
                    logger.info(f"数据已存储到: {hdf5_path}")
                    print(f"\n===== 任务完成 =====")
                    print(f"总页数: {total_pages}")
                    print(f"成功爬取基金数量: {len(all_fund_data)}")
                    print(f"数据已存储到: {hdf5_path}")
                else:
                    logger.error("数据验证失败")
                    print("\n错误：数据验证失败，可能需要检查爬取逻辑！")
            except Exception as e:
                logger.error(f"下载数据时发生错误: {e}")
                print(f"\n错误：下载数据时发生异常: {e}")
        else:
            # 正常模式：显示交互式菜单
            while True:
                # 显示菜单
                show_menu()

                # 获取用户选择
                choice = input("请输入您的选择 (0-2): ")

                # 根据选择执行相应操作
                if choice == "1":
                    # 下载所有页面的开放式基金数据
                    try:
                        # 获取总页数
                        total_pages = get_total_pages()
                        if total_pages <= 0:
                            logger.error("无法获取总页数，操作取消")
                            print("\n错误：无法获取总页数，请检查网络连接或网站是否可访问！")
                            continue

                        logger.info(f"将爬取 {total_pages} 页基金数据")
                        print(f"\n检测到总页数: {total_pages} 页")

                        # 获取用户确认
                        confirm = input(
                            f"即将爬取 {total_pages} 页基金数据，这可能需要一些时间。是否继续？(y/n): "
                        )
                        if confirm.lower() != "y":
                            logger.info("用户取消操作")
                            print("\n操作已取消")
                            continue

                        # 批量获取基金数据
                        logger.info("开始爬取基金数据...")
                        print("\n开始爬取基金数据，请稍候...")
                        all_fund_data = batch_fetch_fund_data(total_pages)

                        if not all_fund_data:
                            logger.error("未获取到任何基金数据")
                            print("\n错误：未获取到任何基金数据！")
                            continue

                        # 验证数据
                        logger.info("开始验证数据...")
                        print("\n开始验证数据...")
                        if verify_fund_data(all_fund_data):
                            # 存储所有数据
                            hdf5_path = get_hdf5_path()
                            logger.info(f"开始将数据存储到HDF5文件: {hdf5_path}")
                            print(f"\n开始将数据存储到HDF5文件: {hdf5_path}")
                            store_fund_data_to_hdf5(all_fund_data, hdf5_path)
                            logger.info("数据存储完成")
                            print("\n数据存储完成")

                            # 显示最终统计信息
                            logger.info(f"===== 任务完成 =====")
                            logger.info(f"总页数: {total_pages}")
                            logger.info(f"成功爬取基金数量: {len(all_fund_data)}")
                            logger.info(f"数据已存储到: {hdf5_path}")
                            print(f"\n===== 任务完成 =====")
                            print(f"总页数: {total_pages}")
                            print(f"成功爬取基金数量: {len(all_fund_data)}")
                            print(f"数据已存储到: {hdf5_path}")
                        else:
                            logger.error("数据验证失败")
                            print("\n错误：数据验证失败，可能需要检查爬取逻辑！")
                    except Exception as e:
                        logger.error(f"下载数据时发生错误: {e}")
                        print(f"\n错误：下载数据时发生异常: {e}")

                elif choice == "2":
                    # 查询开放式基金数据
                    try:
                        fund_code = input("请输入基金代码: ").strip()
                        if not fund_code:
                            print("错误：基金代码不能为空")
                            continue

                        query_fund_by_code(fund_code)
                    except Exception as e:
                        logger.error(f"查询基金数据时发生错误: {e}")
                        print(f"\n错误：查询过程中发生异常: {e}")

                elif choice == "3":
                    # 查看所有基金代码
                    try:
                        show_all_fund_codes()
                    except Exception as e:
                        logger.error(f"显示基金代码时发生错误: {e}")
                        print(f"\n错误：显示基金代码时发生异常: {e}")

                elif choice == "0":
                    # 退出程序
                    logger.info("用户选择退出程序")
                    print("\n感谢使用基金数据管理系统，再见！")
                    break

                else:
                    # 无效选择
                    print("错误：请输入有效的选项 (0-2)")

    except KeyboardInterrupt:
        logger.info("用户中断操作")
        print("\n程序已被用户中断")
    except Exception as e:
        logger.error(f"程序运行时发生未预期的错误: {e}")
        print(f"\n错误：程序发生未预期的错误: {e}")
    finally:
        logger.info("===== 基金数据爬取与存储系统结束 =====")


if __name__ == "__main__":
    main()
