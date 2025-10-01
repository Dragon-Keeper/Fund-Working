# 提取开放式基金的排名数据
import asyncio
import re
import h5py
import os
import pandas as pd
import asyncio
import requests
import json
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

# 全局配置
HDF5_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "Open_Fund_Ranking_Data.h5"
)
URL = "http://fund.eastmoney.com/data/fundranking.html"


# 确保数据目录存在
def ensure_data_directory():
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)


# 初始化HDF5文件
def init_hdf5_file():
    ensure_data_directory()
    # 如果文件不存在，创建一个新的
    if not os.path.exists(HDF5_PATH):
        with h5py.File(HDF5_PATH, "w") as f:
            # 创建一个组来存储基金数据
            f.create_group("funds")
            # 存储元数据
            f.attrs["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.attrs["version"] = "1.0"


# 存储基金数据到HDF5文件
def store_fund_data_to_hdf5(fund_data_list):
    init_hdf5_file()

    with h5py.File(HDF5_PATH, "a") as f:
        # 清空现有的基金数据
        if "funds" in f:
            del f["funds"]

        # 创建基金数据组
        funds_group = f.create_group("funds")

        # 存储每只基金的数据
        for fund_data in fund_data_list:
            fund_code = fund_data["fund_code"]
            if fund_code in funds_group:
                del funds_group[fund_code]

            # 创建基金数据组
            fund_group = funds_group.create_group(fund_code)

            # 存储基金属性
            for key, value in fund_data.items():
                # 处理不同类型的数据
                if isinstance(value, str):
                    # 统一将字符串编码为UTF-8，避免中文字符问题
                    fund_group.attrs[key] = value.encode("utf-8")
                elif isinstance(value, float) or isinstance(value, int):
                    fund_group.attrs[key] = value
                elif isinstance(value, dict):
                    # 如果是嵌套字典，创建子组
                    sub_group = fund_group.create_group(key)
                    for sub_key, sub_value in value.items():
                        if isinstance(sub_value, str):
                            sub_group.attrs[sub_key] = sub_value.encode("utf-8")
                        else:
                            sub_group.attrs[sub_key] = sub_value

        # 更新元数据
        f.attrs["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.attrs["fund_count"] = len(fund_data_list)


# 查询基金数据
def query_fund_by_code(fund_code):
    init_hdf5_file()

    with h5py.File(HDF5_PATH, "r") as f:
        if "funds" not in f or fund_code not in f["funds"]:
            return None

        fund_group = f["funds"][fund_code]
        fund_data = {}

        # 读取基金属性
        for key, value in fund_group.attrs.items():
            # 解码UTF-8编码的字符串
            if isinstance(value, bytes):
                try:
                    fund_data[key] = value.decode("utf-8")
                except UnicodeDecodeError:
                    fund_data[key] = str(value)
            else:
                fund_data[key] = value

        # 读取子组数据
        for subgroup_name in fund_group:
            subgroup = fund_group[subgroup_name]
            subgroup_data = {}
            for key, value in subgroup.attrs.items():
                if isinstance(value, bytes):
                    try:
                        subgroup_data[key] = value.decode("utf-8")
                    except UnicodeDecodeError:
                        subgroup_data[key] = str(value)
                else:
                    subgroup_data[key] = value
            fund_data[subgroup_name] = subgroup_data

        return fund_data


# 获取所有基金代码
def get_all_fund_codes():
    init_hdf5_file()

    with h5py.File(HDF5_PATH, "r") as f:
        if "funds" not in f:
            return []

        return list(f["funds"].keys())


# 解析百分比数据
def parse_percentage_data(text):
    if text == "---":
        return "---"
    try:
        # 去除百分号并转换为浮点数
        if text.endswith("%"):
            return float(text[:-1])
        else:
            return float(text)
    except:
        return text


# 解析数值数据
def parse_numeric_data(text):
    if text == "---":
        return "---"
    try:
        return float(text)
    except:
        return text


# 使用东方财富API获取基金数据
def fetch_fund_data_api(page=1, page_size=50):
    # 设置日期范围（最近一年）
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    # 构造请求URL
    url = "http://fund.eastmoney.com/data/rankhandler.aspx"
    params = {
        "op": "ph",
        "dt": "kf",  # 开放基金
        "ft": "all",  # 全部类型
        "rs": "",
        "gs": "0",
        "sc": "1nz",  # 按近一年收益率排序
        "st": "desc",
        "sd": start_date,
        "ed": end_date,
        "qdii": "",
        "tabSubtype": ",,,,",
        "pi": page,  # 页码
        "pn": page_size,
        "dx": "1",
    }

    # 模拟浏览器请求头
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "http://fund.eastmoney.com/data/fundranking.html",
    }

    try:
        # 发送GET请求
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = "utf-8"

        # 尝试更宽松的正则表达式提取rankData.datas数组
        match = re.search(
            r"var rankData\s*=\s*\{datas\s*:\s*\[(.*?)\]", response.text, re.DOTALL
        )
        if not match:
            # 如果没找到rankData结构，尝试直接查找数组内容
            match = re.search(
                r"var rankDatas\s*=\s*\[(.*?)\];", response.text, re.DOTALL
            )
            if not match:
                raise ValueError("未找到基金数据")

        # 提取数组内容并处理
        data_array_str = match.group(1)

        # 分割数组元素（每个基金数据）
        fund_entries = data_array_str.split('","')
        # 处理第一个和最后一个元素的引号
        fund_entries[0] = fund_entries[0].lstrip('"')
        fund_entries[-1] = fund_entries[-1].rstrip('"')

        # 解析总页数信息
        total_pages_match = re.search(r"allPages:(\d+)", response.text)
        total_pages = int(total_pages_match.group(1)) if total_pages_match else 3

        return fund_entries, total_pages

    except Exception as e:
        print(f"API请求失败: {e}")
        return [], 3


# 使用API获取开放型基金数据
async def fetch_open_fund_data():
    fund_data_list = []
    try:
        # 设置初始页面
        page_num = 1
        total_processed = 0

        # 循环处理所有页面，直到获取不到数据
        while True:
            print(f"正在处理第 {page_num} 页...")

            # 使用API获取数据
            fund_entries, total_pages = fetch_fund_data_api(page=page_num)

            if not fund_entries:
                print(f"第 {page_num} 页未获取到数据，结束处理")
                break

            print(
                f"从API成功获取第 {page_num} 页数据，共 {len(fund_entries)} 条基金记录"
            )
            print(f"总页数: {total_pages}")

            processed_rows = 0

            # 处理每条基金数据
            for entry in fund_entries:
                try:
                    # 按逗号分割数据字段
                    fields = entry.split(",")

                    if len(fields) < 18:
                        continue

                    # 构建基金数据字典，严格按照用户提供的固定顺序映射
                    # 固定顺序: [基金代码, 基金简称, 数据获取日期, 最新单位净值, 最新累计净值, 日增长率, 近1周增长率, 近1月增长率, 近3月增长率, 近6月增长率, 近1年增长率, 近2年增长率, 近3年增长率, 今年来增长率, 成立来增长率, 自定义, 手续费信息]
                    fund_data = {
                        "fund_code": fields[0],  # 基金代码
                        "fund_name": fields[1],  # 基金简称
                        "data_date": fields[3][5:],  # 数据获取日期 (截取月日)
                        "unit_nav": parse_numeric_data(fields[4]),  # 最新单位净值
                        "accum_nav": parse_numeric_data(fields[5]),  # 最新累计净值
                        "day_growth": (
                            parse_percentage_data(fields[6])
                            if fields[6] and fields[6] != "-"
                            else "---"
                        ),  # 日增长率
                        "week_growth": (
                            parse_percentage_data(fields[7])
                            if len(fields) > 7
                            and fields[7]
                            and fields[7] != "-"
                            and not re.search(r"\d{4}-\d{2}-\d{2}", fields[7])
                            else "---"
                        ),  # 近1周增长率
                        "month_growth": (
                            parse_percentage_data(fields[8])
                            if len(fields) > 8
                            and fields[8]
                            and fields[8] != "-"
                            and not re.search(r"\d{4}-\d{2}-\d{2}", fields[8])
                            else "---"
                        ),  # 近1月增长率
                        "quarter_growth": (
                            parse_percentage_data(fields[9])
                            if len(fields) > 9
                            and fields[9]
                            and fields[9] != "-"
                            and not re.search(r"\d{4}-\d{2}-\d{2}", fields[9])
                            else "---"
                        ),  # 近3月增长率
                        "half_year_growth": (
                            parse_percentage_data(fields[10])
                            if len(fields) > 10
                            and fields[10]
                            and fields[10] != "-"
                            and not re.search(r"\d{4}-\d{2}-\d{2}", fields[10])
                            else "---"
                        ),  # 近6月增长率
                        "year_growth": (
                            parse_percentage_data(fields[11])
                            if len(fields) > 11
                            and fields[11]
                            and fields[11] != "-"
                            and not re.search(r"\d{4}-\d{2}-\d{2}", fields[11])
                            else "---"
                        ),  # 近1年增长率
                        "two_year_growth": (
                            parse_percentage_data(fields[12])
                            if len(fields) > 12
                            and fields[12]
                            and fields[12] != "-"
                            and not re.search(r"\d{4}-\d{2}-\d{2}", fields[12])
                            else "---"
                        ),  # 近2年增长率
                        "three_year_growth": (
                            parse_percentage_data(fields[13])
                            if len(fields) > 13
                            and fields[13]
                            and fields[13] != "-"
                            and not re.search(r"\d{4}-\d{2}-\d{2}", fields[13])
                            else "---"
                        ),  # 近3年增长率
                        "year_to_date_growth": (
                            parse_percentage_data(fields[14])
                            if len(fields) > 14
                            and fields[14]
                            and fields[14] != "-"
                            and not re.search(r"\d{4}-\d{2}-\d{2}", fields[14])
                            else "---"
                        ),  # 今年来增长率
                        "since_establishment_growth": (
                            parse_percentage_data(fields[15])
                            if len(fields) > 15
                            and fields[15]
                            and fields[15] != "-"
                            and not re.search(r"\d{4}-\d{2}-\d{2}", fields[15])
                            else "---"
                        ),  # 成立来增长率
                        "fetch_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }

                    fund_data_list.append(fund_data)
                    processed_rows += 1

                except Exception as e:
                    print(f"处理基金数据时出错: {e}")
                    continue

            total_processed += processed_rows
            print(
                f"第 {page_num} 页处理完成，共处理 {processed_rows} 条有效基金数据，累计处理 {total_processed} 条"
            )

            # 检查是否还有下一页
            if page_num >= total_pages:
                print(f"已处理完所有 {total_pages} 页数据")
                break

            # 处理下一页
            page_num += 1

    except Exception as e:
        print(f"获取数据时出错: {e}")

    return fund_data_list


# 下载所有开放型基金数据
def download_all_open_funds():
    print("正在获取开放型基金数据...")

    # 运行异步函数
    fund_data_list = asyncio.run(fetch_open_fund_data())

    if fund_data_list:
        print(f"成功获取 {len(fund_data_list)} 只开放型基金数据")

        # 存储数据到HDF5文件
        store_fund_data_to_hdf5(fund_data_list)
        print(f"数据已成功保存到 {HDF5_PATH}")
    else:
        print("未获取到任何开放型基金数据")


# 查询开放型基金数据
def query_open_fund():
    fund_code = input("请输入开放型基金代码: ").strip()

    fund_data = query_fund_by_code(fund_code)

    if fund_data:
        print("\n基金数据查询结果:")
        print(f"基金代码: {fund_data['fund_code']}")
        print(f"基金简称: {fund_data['fund_name']}")
        print(f"数据获取日期: {fund_data['data_date']}")
        print(f"最新单位净值: {fund_data['unit_nav']}")
        print(f"最新累计净值: {fund_data['accum_nav']}")
        print(f"日增长率: {fund_data['day_growth']}%")
        print(f"近1周增长率: {fund_data['week_growth']}%")
        print(f"近1月增长率: {fund_data['month_growth']}%")
        print(f"近3月增长率: {fund_data['quarter_growth']}%")
        print(f"近6月增长率: {fund_data['half_year_growth']}%")
        print(f"近1年增长率: {fund_data['year_growth']}%")
        print(f"近2年增长率: {fund_data['two_year_growth']}%")
        print(f"近3年增长率: {fund_data['three_year_growth']}%")
        print(f"今年来增长率: {fund_data['year_to_date_growth']}%")
        print(f"成立来增长率: {fund_data['since_establishment_growth']}%")
        print(f"数据获取时间: {fund_data['fetch_time']}")
    else:
        print(f"未找到基金代码为 {fund_code} 的开放型基金数据")


# 显示菜单
def show_menu():
    while True:
        print("\n===== 开放式基金数据管理系统 ======")
        print("1. 下载所有开放式基金数据")
        print("2. 查询开放式基金数据")
        print("0. 退出系统")
        print("========================\n")

        choice = input("请选择功能 (0-2): ").strip()

        if choice == "1":
            download_all_open_funds()
        elif choice == "2":
            query_open_fund()
        elif choice == "0":
            print("感谢使用，再见！")
            break
        else:
            print("无效的选择，请重新输入")


# 主函数
if __name__ == "__main__":
    show_menu()
