# 提取货币型基金的万份收益和7日年化%数据

import asyncio
import re
import h5py
import os
import sys
import argparse
import pandas as pd
from datetime import datetime
from playwright.async_api import async_playwright

# 全局配置
HDF5_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "Currency_Fund_Data.h5"
)
URL = "http://fund.eastmoney.com/HBJJ_dwsy.html"


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

        return fund_data


# 获取所有基金代码
def get_all_fund_codes():
    init_hdf5_file()

    with h5py.File(HDF5_PATH, "r") as f:
        if "funds" not in f:
            return []

        return list(f["funds"].keys())


# 使用Playwright获取货币基金数据
async def fetch_currency_fund_data():
    fund_data_list = []

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,  # 无头模式，可以改为False查看浏览器操作
                args=["--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage"],
            )

            page = await browser.new_page()

            # 导航到目标网页
            await page.goto(URL, wait_until="networkidle")

            # 等待表格加载完成
            await page.wait_for_selector("table.dbtable#oTable")

            # 提取表格内容
            table_html = await page.inner_html("table.dbtable#oTable")

            # 使用正则表达式解析表格内容
            # 首先获取表头，确定日期
            date_match = re.search(r"<nobr>(\d{4}-\d{2}-\d{2})</nobr>", table_html)
            latest_date = (
                date_match.group(1)
                if date_match
                else datetime.now().strftime("%Y-%m-%d")
            )

            # 解析每一行基金数据
            fund_rows = re.findall(
                r'<tr height="20" bgcolor="(?:#F5FFFF|#FFFFFF)"[^>]*>(.*?)</tr>',
                table_html,
            )

            print(f"找到 {len(fund_rows)} 行基金数据")
            # 只打印前几行用于调试
            for i, row in enumerate(fund_rows):
                if i < 2:  # 只打印前2行用于调试
                    print(f"\n行 {i+1} 内容:")
                    print(row)
                    print("-" * 50)
                # 提取基金代码
                fund_code_match = re.search(r"<td>(\d{6})</td>", row)
                if not fund_code_match:
                    continue
                fund_code = fund_code_match.group(1)

                # 提取基金名称 - 根据实际HTML结构调整
                fund_name_match = re.search(
                    r'<td class="jc"><nobr><a href="http://fund\.eastmoney\.com/\d+\.html">([^<]+)</a>',
                    row,
                )
                if not fund_name_match:
                    fund_name_match = re.search(
                        r'<td class="jc"><nobr><a href=""[^>]*>([^<]+)</a>', row
                    )
                fund_name = fund_name_match.group(1) if fund_name_match else ""

                # 提取最新万份收益 - 处理两种可能的位置
                latest_yield_match = re.search(
                    r'<td bgcolor="#EBF3FB"><span class="ping">([\d.]+)</span></td>',
                    row,
                )
                if not latest_yield_match:
                    latest_yield_match = re.search(
                        r'<span class="ping">([\d.]+)</span>', row
                    )
                latest_yield = (
                    float(latest_yield_match.group(1)) if latest_yield_match else 0.0
                )

                # 提取最新7日年化 - 处理两种可能的位置
                latest_annual_match = re.search(r"<td>([\d.]+)%</td>", row)
                if not latest_annual_match:
                    latest_annual_match = re.search(
                        r'<td bgcolor="#EBF3FB">([\d.]+)%</td>', row
                    )
                latest_annual = (
                    float(latest_annual_match.group(1)) if latest_annual_match else 0.0
                )

                # 提取成立日期
                establish_date_match = re.search(r"<td>(\d{4}-\d{2}-\d{2})</td>", row)
                establish_date = (
                    establish_date_match.group(1) if establish_date_match else ""
                )

                # 提取基金经理 - 根据实际HTML结构调整
                manager_match = re.search(
                    r'<td><a href="http://fundf10\.eastmoney\.com/jjjl_\d+\.html">([^<]+)</a></td>',
                    row,
                )
                if not manager_match:
                    manager_match = re.search(
                        r'<td><a href=""[^>]*>([^<]+)</a></td>', row
                    )
                manager = manager_match.group(1) if manager_match else ""

                # 提取手续费
                fee_match = re.search(r'<span class="red">(\d+)</span>', row)
                fee = float(fee_match.group(1)) if fee_match else 0.0

                # 创建基金数据字典
                fund_data = {
                    "fund_code": fund_code,
                    "fund_name": fund_name,  # 基金简称
                    "latest_yield": latest_yield,
                    "latest_annual": latest_annual,
                    "establish_date": establish_date,
                    "manager": manager,
                    "fee": fee,
                    "update_date": latest_date,
                    "fetch_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                fund_data_list.append(fund_data)

            await browser.close()

    except Exception as e:
        print(f"获取数据时出错: {e}")

    return fund_data_list


# 下载所有货币基金数据
def download_all_currency_funds():
    print("正在获取货币基金数据...")

    # 运行异步函数
    fund_data_list = asyncio.run(fetch_currency_fund_data())

    if fund_data_list:
        print(f"成功获取 {len(fund_data_list)} 只货币基金数据")

        # 存储数据到HDF5文件
        store_fund_data_to_hdf5(fund_data_list)
        print(f"数据已成功保存到 {HDF5_PATH}")
    else:
        print("未获取到任何货币基金数据")


# 查询货币基金数据
def query_currency_fund():
    fund_code = input("请输入货币基金代码: ").strip()
    
    # 验证基金代码格式（6位数字）
    if not re.match(r"^\d{6}$", fund_code):
        print("错误：基金代码格式不正确，请输入6位数字的基金代码")
        return
    
    fund_data = query_fund_by_code(fund_code)

    if fund_data:
        print("\n基金数据查询结果:")
        print(f"基金代码: {fund_data.get('fund_code', '---')}")
        print(f"基金简称: {fund_data.get('fund_name', '---')}")
        print(f"最新万份收益: {fund_data.get('latest_yield', '---')}")
        print(f"最新7日年化%: {fund_data.get('latest_annual', '---')}%")
        print(f"成立日期: {fund_data.get('establish_date', '---')}")
        print(f"基金经理: {fund_data.get('manager', '---')}")
        print(f"手续费: {fund_data.get('fee', '---')}")
        print(f"数据更新日期: {fund_data.get('update_date', '---')}")
        print(f"数据获取时间: {fund_data.get('fetch_time', '---')}")
    else:
        print(f"未找到基金代码为 {fund_code} 的货币基金数据")

# 显示所有基金代码
def show_all_fund_codes():
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
    while True:
        print("\n===== 货币基金数据管理系统 ======")
        print("1. 下载所有货币基金数据")
        print("2. 查询货币基金数据")
        print("3. 查看所有基金代码")
        print("0. 退出系统")

        choice = input("请选择功能 (0-3): ").strip()

        if choice == "1":
            download_all_currency_funds()
        elif choice == "2":
            query_currency_fund()
        elif choice == "3":
            show_all_fund_codes()
        elif choice == "0":
            print("感谢使用，再见！")
            break
        else:
            print("无效的选择，请重新输入")


# 主函数
if __name__ == "__main__":
    show_menu()

# 为了被quant_orchestrator调用而添加的main函数
def main():
    """被量化调度器调用的主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='货币基金数据管理系统')
    parser.add_argument('--auto', action='store_true', help='自动模式：仅执行数据下载操作')
    args = parser.parse_args()
    
    if args.auto:
        # 自动模式：直接执行下载操作
        download_all_currency_funds()
    else:
        # 正常模式：显示菜单
        show_menu()
