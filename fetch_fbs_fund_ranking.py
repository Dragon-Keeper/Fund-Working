# 提取场内交易基金的排名数据
import asyncio
import re
import h5py
import os
import pandas as pd
import json
from datetime import datetime, timedelta
from playwright.async_api import async_playwright

# 全局配置
HDF5_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "FBS_Fund_Ranking_Data.h5"
)


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


# 解析百分比数据
def parse_percentage_data(text):
    if text == "---" or not text or text == "-" or text == "":
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
    if text == "---" or not text or text == "-" or text == "":
        return "---"
    try:
        return float(text)
    except:
        return text


# 使用Playwright获取场内交易基金数据
async def fetch_fbs_fund_data():
    fund_data_list = []
    try:
        # 设置初始页面
        page_num = 1
        total_pages = 1  # 初始值，后续会从网页中获取实际值
        total_processed = 0
        base_url = "https://fund.eastmoney.com/data/fbsfundranking.html"
        current_url = f"{base_url}#tct;c0;r;s1nzf;ddesc;pn10000;"

        # 使用Playwright启动浏览器
        async with async_playwright() as p:
            print("正在启动浏览器...")
            # 配置浏览器启动参数
            browser = await p.chromium.launch(
                headless=True,  # 无头模式，不显示浏览器界面
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--window-size=1920,1080",
                ],
            )

            # 在创建页面时设置user agent
            page = await browser.new_page(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )

            # 设置页面加载超时
            page.set_default_timeout(60000)  # 60秒，注意这不是异步方法，不需要await

            try:
                # 循环处理所有页面
                while True:
                    # 如果不是第一页，构造新的URL
                    if page_num > 1:
                        current_url = (
                            f"{base_url}#tct;c{page_num-1};r;s1nzf;ddesc;pn10000;"
                        )

                    print(f"正在处理第 {page_num} 页，URL: {current_url}")

                    # 导航到目标页面
                    await page.goto(current_url)
                    await page.wait_for_load_state("networkidle")  # 等待网络空闲

                    # 等待表格加载完成
                    try:
                        await page.wait_for_selector(
                            "#dbtable > tbody > tr", timeout=10000
                        )
                        print("成功找到基金数据表格")
                    except:
                        print(
                            f"第 {page_num} 页未找到基金数据表格，可能没有更多数据或页面结构已更改"
                        )
                        break

                    # 获取总页数
                    if page_num == 1:
                        try:
                            # 从分页控件获取总页数
                            pagination_text = await page.evaluate(
                                "() => document.querySelector('.pnum').textContent"
                            )
                            match = re.search(r"/\s*(\d+)", pagination_text)
                            if match:
                                total_pages = int(match.group(1))
                                print(f"获取到总页数: {total_pages}")
                        except Exception as e:
                            print(f"获取总页数时出错: {e}")
                            # 如果无法获取总页数，保守估计为1页
                            total_pages = 1

                    # 获取所有行数据
                    rows = await page.query_selector_all("#dbtable > tbody > tr")
                    print(f"第 {page_num} 页共找到 {len(rows)} 条基金数据")

                    processed_rows = 0

                    # 处理每行数据
                    for row in rows:
                        try:
                            # 获取所有单元格
                            cells = await row.query_selector_all("td")

                            if len(cells) < 18:  # 确保有足够的字段
                                print(f"行数据字段不足，跳过该行")
                                continue

                            # 获取数据日期（数据日期）
                            # 根据用户要求：数据日期指的是最新交易日数据产生的日期，
                            # 从表格单元格中获取，具体为取基金类型的下一位数据（cells[5]）
                            data_date = ""  # 初始化为空字符串

                            # 安全地从表格单元格中获取数据日期
                            try:
                                if (
                                    len(cells) >= 6
                                ):  # 确保有足够的单元格获取数据日期（基金类型下一位）
                                    latest_date_text = await cells[5].text_content()
                                    latest_date_text = latest_date_text.strip()
                                    # 处理日期格式，可能是 MM-DD 或 YYYY-MM-DD
                                    if (
                                        len(latest_date_text) == 5
                                        and latest_date_text[2] == "-"
                                    ):
                                        # MM-DD 格式，添加当前年份
                                        current_year = datetime.now().year
                                        data_date = f"{current_year}-{latest_date_text}"
                                    elif (
                                        len(latest_date_text) == 10
                                        and latest_date_text[4] == "-"
                                        and latest_date_text[7] == "-"
                                    ):
                                        # YYYY-MM-DD 格式
                                        data_date = latest_date_text
                            except:
                                # 异常情况下保持data_date为空
                                pass

                            # 如果未能获取到有效的日期，则标记为无效
                            if not data_date:
                                print(f"未能从表格获取有效日期，跳过该行")
                                continue

                            # 提取各个字段数据 - 使用安全的方式访问单元格
                            fund_code = "---"
                            fund_name = "---"
                            fund_type = "---"
                            unit_nav = "---"
                            accum_nav = "---"
                            week_growth = "---"
                            month_growth = "---"
                            quarter_growth = "---"
                            half_year_growth = "---"
                            year_growth = "---"
                            two_year_growth = "---"
                            three_year_growth = "---"
                            year_to_date_growth = "---"
                            since_establishment_growth = "---"
                            establishment_date = "---"

                            # 安全地访问各个字段，根据实际网页结构调整索引
                            try:
                                # 首先打印单元格数量，帮助调试
                                print(f"当前行有 {len(cells)} 个单元格")

                                # 正确提取基金代码的逻辑
                                # 基金代码通常在链接中或特定元素中
                                if len(cells) >= 3:  # 尝试从第3个单元格获取基金代码链接
                                    try:
                                        # 查找单元格中的<a>标签
                                        fund_link = await cells[2].query_selector("a")
                                        if fund_link:
                                            # 获取链接的href属性
                                            href = await fund_link.get_attribute("href")
                                            # 从链接中提取基金代码（通常是6位数字）
                                            fund_code_match = re.search(r"\d{6}", href)
                                            if fund_code_match:
                                                fund_code = fund_code_match.group(0)
                                    except:
                                        # 如果无法从链接中获取，尝试直接获取文本
                                        if len(cells) >= 3:
                                            fund_code = await cells[2].text_content()

                                # 如果上面的方法没有获取到基金代码，尝试使用第2个单元格的文本
                                if fund_code == "---" and len(cells) >= 2:
                                    fund_code_text = await cells[1].text_content()
                                    # 检查是否是6位数字的基金代码格式
                                    fund_code_match = re.search(
                                        r"\d{6}", fund_code_text
                                    )
                                    if fund_code_match:
                                        fund_code = fund_code_match.group(0)

                                # 基金简称
                                if len(cells) >= 4:  # 基金简称
                                    fund_name = await cells[3].text_content()

                                # 基金类型
                                if len(cells) >= 5:  # 基金类型
                                    fund_type = await cells[4].text_content()

                                # 最新单位净值
                                if len(cells) >= 7:  # 最新单位净值
                                    unit_nav_text = await cells[6].text_content()
                                    unit_nav = parse_numeric_data(unit_nav_text.strip())

                                # 最新累计净值
                                if len(cells) >= 8:  # 最新累计净值
                                    accum_nav_text = await cells[7].text_content()
                                    accum_nav = parse_numeric_data(
                                        accum_nav_text.strip()
                                    )

                                # 近1周增长率
                                if len(cells) >= 9:  # 近1周增长率
                                    week_growth_text = await cells[8].text_content()
                                    week_growth = parse_percentage_data(
                                        week_growth_text.strip()
                                    )

                                # 近1月增长率
                                if len(cells) >= 10:  # 近1月增长率
                                    month_growth_text = await cells[9].text_content()
                                    month_growth = parse_percentage_data(
                                        month_growth_text.strip()
                                    )

                                # 近3月增长率
                                if len(cells) >= 11:  # 近3月增长率
                                    quarter_growth_text = await cells[10].text_content()
                                    quarter_growth = parse_percentage_data(
                                        quarter_growth_text.strip()
                                    )

                                # 近6月增长率
                                if len(cells) >= 12:  # 近6月增长率
                                    half_year_growth_text = await cells[
                                        11
                                    ].text_content()
                                    half_year_growth = parse_percentage_data(
                                        half_year_growth_text.strip()
                                    )

                                # 近1年增长率
                                if len(cells) >= 13:  # 近1年增长率
                                    year_growth_text = await cells[12].text_content()
                                    year_growth = parse_percentage_data(
                                        year_growth_text.strip()
                                    )

                                # 近2年增长率
                                if len(cells) >= 14:  # 近2年增长率
                                    two_year_growth_text = await cells[
                                        13
                                    ].text_content()
                                    two_year_growth = parse_percentage_data(
                                        two_year_growth_text.strip()
                                    )

                                # 近3年增长率
                                if len(cells) >= 15:  # 近3年增长率
                                    three_year_growth_text = await cells[
                                        14
                                    ].text_content()
                                    three_year_growth = parse_percentage_data(
                                        three_year_growth_text.strip()
                                    )

                                # 今年来增长率
                                if len(cells) >= 16:  # 今年来增长率
                                    year_to_date_growth_text = await cells[
                                        15
                                    ].text_content()
                                    year_to_date_growth = parse_percentage_data(
                                        year_to_date_growth_text.strip()
                                    )

                                # 成立来增长率
                                if len(cells) >= 17:  # 成立来增长率
                                    since_establishment_growth_text = await cells[
                                        16
                                    ].text_content()
                                    since_establishment_growth = parse_percentage_data(
                                        since_establishment_growth_text.strip()
                                    )

                                # 成立日期信息
                                if len(cells) >= 18:  # 成立日期信息
                                    establishment_date_text = await cells[
                                        17
                                    ].text_content()
                                    establishment_date = (
                                        establishment_date_text.strip()
                                        if establishment_date_text.strip()
                                        and establishment_date_text.strip() != "---"
                                        else "---"
                                    )
                            except Exception as e:
                                print(f"提取字段数据时出错: {e}")

                            # 构建基金数据字典，严格按照用户提供的固定顺序映射
                            # 固定顺序: [基金代码, 基金简称, 基金类型, 数据获取日期, 最新单位净值, 最新累计净值, 近1周增长率, 近1月增长率, 近3月增长率, 近6月增长率, 近1年增长率, 近2年增长率, 近3年增长率, 今年来增长率, 成立来增长率, 成立日期信息]
                            fund_data = {
                                "fund_code": (
                                    fund_code.strip()
                                    if fund_code.strip() and fund_code.strip() != "---"
                                    else "---"
                                ),
                                "fund_name": (
                                    fund_name.strip()
                                    if fund_name.strip() and fund_name.strip() != "---"
                                    else "---"
                                ),
                                "fund_type": (
                                    fund_type.strip()
                                    if fund_type.strip() and fund_type.strip() != "---"
                                    else "---"
                                ),
                                "data_date": data_date,  # 数据获取日期，使用完整日期格式
                                "unit_nav": unit_nav,
                                "accum_nav": accum_nav,
                                "week_growth": week_growth,
                                "month_growth": month_growth,
                                "quarter_growth": quarter_growth,
                                "half_year_growth": half_year_growth,
                                "year_growth": year_growth,
                                "two_year_growth": two_year_growth,
                                "three_year_growth": three_year_growth,
                                "year_to_date_growth": year_to_date_growth,
                                "since_establishment_growth": since_establishment_growth,
                                "establishment_date": establishment_date,
                                "fetch_time": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                            }

                            # 添加到结果列表
                            fund_data_list.append(fund_data)
                            processed_rows += 1

                            # 每处理100条数据打印一次进度
                            if processed_rows % 100 == 0:
                                print(f"第 {page_num} 页已处理 {processed_rows} 条数据")

                        except Exception as e:
                            print(f"处理某条基金数据时出错: {e}")
                            continue

                    # 更新累计处理数量
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

                    # 添加适当的延迟，避免请求过于频繁
                    await page.wait_for_timeout(2000)  # 等待2秒

            except Exception as e:
                print(f"使用Playwright获取数据时出错: {e}")
                import traceback

                traceback.print_exc()
            finally:
                # 关闭浏览器
                await browser.close()
                print("浏览器已关闭")

    except Exception as e:
        print(f"获取场内交易基金数据时发生总错误: {e}")
        import traceback

        traceback.print_exc()

    return fund_data_list


# 下载所有场内交易基金数据
def download_all_fbs_funds():
    print("正在获取场内交易基金数据...")

    # 运行异步函数
    fund_data_list = asyncio.run(fetch_fbs_fund_data())

    if fund_data_list:
        print(f"成功获取 {len(fund_data_list)} 只场内交易基金数据")

        # 存储数据到HDF5文件
        store_fund_data_to_hdf5(fund_data_list)
        print(f"数据已成功保存到 {HDF5_PATH}")
    else:
        print("未获取到任何场内交易基金数据")


# 获取所有基金代码
def get_all_fund_codes():
    hdf5_path = HDF5_PATH
    
    if not os.path.exists(hdf5_path):
        print(f"错误：HDF5文件不存在: {hdf5_path}")
        return []
    
    with h5py.File(hdf5_path, "r") as f:
        if "funds" not in f:
            return []
        
        return list(f["funds"].keys())

# 查询场内交易基金数据
def query_fbs_fund():
    fund_code = input("请输入场内交易基金代码: ").strip()
    
    # 验证基金代码格式（6位数字）
    if not re.match(r"^\d{6}$", fund_code):
        print("错误：基金代码格式不正确，请输入6位数字的基金代码")
        return
    
    fund_data = query_fund_by_code(fund_code)
    
    if fund_data:
        print("\n查询结果：")
        print(f"基金代码: {fund_data.get('fund_code', '---')}")
        print(f"基金简称: {fund_data.get('fund_name', '---')}")
        print(f"基金类型: {fund_data.get('fund_type', '---')}")
        print(f"最新单位净值: {fund_data.get('unit_nav', '---')}")
        print(f"最新累计净值: {fund_data.get('accum_nav', '---')}")
        print(f"近1周增长率: {fund_data.get('week_growth', '---')}%")
        print(f"近1月增长率: {fund_data.get('month_growth', '---')}%")
        print(f"近3月增长率: {fund_data.get('quarter_growth', '---')}%")
        print(f"近6月增长率: {fund_data.get('half_year_growth', '---')}%")
        print(f"近1年增长率: {fund_data.get('year_growth', '---')}%")
        print(f"近2年增长率: {fund_data.get('two_year_growth', '---')}%")
        print(f"近3年增长率: {fund_data.get('three_year_growth', '---')}%")
        print(f"今年来增长率: {fund_data.get('year_to_date_growth', '---')}%")
        print(f"成立来增长率: {fund_data.get('since_establishment_growth', '---')}%")
        print(f"成立日期: {fund_data.get('establishment_date', '---')}")
        print(f"数据获取时间: {fund_data.get('fetch_time', '---')}")
    else:
        print(f"未找到基金代码为 {fund_code} 的数据")

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
        print("\n===== 场内交易基金数据管理系统 ======")
        print("1. 下载所有场内交易基金数据")
        print("2. 查询场内交易基金数据")
        print("3. 查看所有基金代码")
        print("0. 退出")
        print("========================\n")

        choice = input("请选择功能 (0-3): ").strip()

        if choice == "1":
            download_all_fbs_funds()
        elif choice == "2":
            query_fbs_fund()
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
    show_menu()
