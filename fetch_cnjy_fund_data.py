# 提取场内交易基金的各项数据
import asyncio
import re
import h5py
import os
import pandas as pd
from datetime import datetime
from playwright.async_api import async_playwright

# 全局配置
HDF5_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "CNJY_Fund_Data.h5"
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

# 解析百分比数据
def parse_percentage_data(text):
    if text == "---" or not text or text == "-" or text.strip() == "":
        return "---"
    try:
        # 去除百分号并转换为浮点数
        if text.endswith("%"):
            return float(text[:-1])
        else:
            return float(text)
    except:
        return "---"

# 解析数值数据
def parse_numeric_data(text):
    if text == "---" or not text or text == "-" or text.strip() == "":
        return "---"
    try:
        return float(text)
    except:
        return "---"

# 使用Playwright获取场内交易基金数据
async def fetch_cnjy_fund_data():
    fund_data_list = []
    try:
        # 设置初始页面
        page_num = 1
        total_pages = 1  # 初始值，后续会从网页中获取实际值
        total_processed = 0
        base_url = "https://fund.eastmoney.com/cnjy_jzzzl.html"
        
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
            page.set_default_timeout(60000)  # 60秒

            try:
                # 直接处理当前页面，不需要分页
                current_url = base_url
                print(f"正在处理页面，URL: {current_url}")

                # 导航到目标页面
                await page.goto(current_url)
                await page.wait_for_load_state("networkidle")  # 等待网络空闲

                # 等待表格加载完成
                try:
                    # 尝试多种可能的表格选择器
                    table_selectors = [
                        "table > tbody > tr",  # 原始选择器
                        ".dataList > tbody > tr",  # 可能的类名选择器
                        "#dbtable > tbody > tr",  # 可能的ID选择器
                        "table.dataList > tbody > tr"  # 更具体的选择器
                    ]
                    
                    found_table = False
                    for selector in table_selectors:
                        try:
                            await page.wait_for_selector(selector, timeout=5000)
                            print(f"使用选择器 '{selector}' 成功找到基金数据表格")
                            found_table = True
                            break
                        except:
                            continue
                    
                    if not found_table:
                        # 作为最后的尝试，获取所有tr元素
                        all_trs = await page.query_selector_all("tr")
                        if len(all_trs) > 5:  # 如果找到了多个tr元素，假设表格存在
                            print("找到多个tr元素，假设表格存在")
                            found_table = True
                        else:
                            print("未找到基金数据表格，可能页面结构已更改")
                            # 使用return而不是break
                            return []
                except Exception as e:
                    print(f"查找表格时出错: {e}")
                    # 使用return而不是break
                    return []

                # 获取所有行数据
                rows = []
                # 尝试多种可能的行选择器
                row_selectors = [
                    "table > tbody > tr",  # 原始选择器
                    ".dataList > tbody > tr",  # 可能的类名选择器
                    "#dbtable > tbody > tr",  # 可能的ID选择器
                    "table.dataList > tbody > tr",  # 更具体的选择器
                    "tr"  # 最后的备选，获取所有tr元素
                ]
                
                for selector in row_selectors:
                    try:
                        rows = await page.query_selector_all(selector)
                        # 过滤掉可能的表头行
                        rows = [row for row in rows if len(await row.query_selector_all("td")) > 0]
                        if len(rows) > 0:
                            print(f"使用选择器 '{selector}' 定位到 {len(rows)} 条基金数据行")
                            break
                    except:
                        continue
                
                if len(rows) == 0:
                    print("未找到有效的基金数据行")
                    # 使用return而不是break
                    return []

                processed_rows = 0

                # 处理每行数据
                for row in rows:
                    try:
                        # 获取所有单元格
                        cells = await row.query_selector_all("td")

                        if len(cells) < 14:  # 确保有足够的字段（14个单元格）
                            continue

                        # 从第4个单元格开始提取数据（索引为3），提取到第14个单元格（索引为13）
                        # 字段顺序：基金代码、基金简称、基金类型、最新单位净值、最新累计净值、上个交易日单位净值、上个交易日累计净值、增长值、增长率、市价、折价率
                        
                        # 初始化所有字段为默认值
                        fund_code = "---"
                        fund_name = "---"
                        fund_type = "---"
                        latest_unit_nav = "---"  # 最新单位净值
                        latest_accum_nav = "---"  # 最新累计净值
                        prev_unit_nav = "---"  # 上个交易日单位净值
                        prev_accum_nav = "---"  # 上个交易日累计净值
                        growth_value = "---"  # 增长值
                        growth_rate = "---"  # 增长率
                        market_price = "---"  # 市价
                        discount_rate = "---"  # 折价率

                        # 安全地提取字段数据
                        try:
                            # 基金代码（第4个单元格，索引3）
                            fund_code_text = await cells[3].text_content()
                            fund_code_text = fund_code_text.strip()
                            # 检查是否是6位数字的基金代码格式
                            fund_code_match = re.search(r"\d{6}", fund_code_text)
                            if fund_code_match:
                                fund_code = fund_code_match.group(0)
                            else:
                                # 尝试从链接中提取基金代码
                                fund_link = await cells[3].query_selector("a")
                                if fund_link:
                                    href = await fund_link.get_attribute("href")
                                    fund_code_match = re.search(r"\d{6}", href)
                                    if fund_code_match:
                                        fund_code = fund_code_match.group(0)
                                
                                # 如果仍未获取到有效代码，直接使用文本（可能包含非数字）
                                if fund_code == "---" and fund_code_text:
                                    fund_code = fund_code_text

                            # 基金简称（第5个单元格，索引4）
                            fund_name_text = await cells[4].text_content()
                            fund_name = fund_name_text.strip() if fund_name_text.strip() else "---"

                            # 基金类型（第6个单元格，索引5）
                            fund_type_text = await cells[5].text_content()
                            fund_type = fund_type_text.strip() if fund_type_text.strip() else "---"

                            # 最新单位净值（第7个单元格，索引6）
                            latest_unit_nav_text = await cells[6].text_content()
                            latest_unit_nav = parse_numeric_data(latest_unit_nav_text.strip())

                            # 最新累计净值（第8个单元格，索引7）
                            latest_accum_nav_text = await cells[7].text_content()
                            latest_accum_nav = parse_numeric_data(latest_accum_nav_text.strip())

                            # 上个交易日单位净值（第9个单元格，索引8）
                            prev_unit_nav_text = await cells[8].text_content()
                            prev_unit_nav = parse_numeric_data(prev_unit_nav_text.strip())

                            # 上个交易日累计净值（第10个单元格，索引9）
                            prev_accum_nav_text = await cells[9].text_content()
                            prev_accum_nav = parse_numeric_data(prev_accum_nav_text.strip())

                            # 增长值（第11个单元格，索引10）
                            growth_value_text = await cells[10].text_content()
                            growth_value = parse_numeric_data(growth_value_text.strip())

                            # 增长率（第12个单元格，索引11）
                            growth_rate_text = await cells[11].text_content()
                            growth_rate = parse_percentage_data(growth_rate_text.strip())

                            # 市价（第13个单元格，索引12）
                            market_price_text = await cells[12].text_content()
                            market_price = parse_numeric_data(market_price_text.strip())

                            # 折价率（第14个单元格，索引13）
                            discount_rate_text = await cells[13].text_content()
                            discount_rate = parse_percentage_data(discount_rate_text.strip())

                        except Exception:
                            continue

                        # 构建基金数据字典，严格按照指定顺序映射
                        # 字段顺序：
                        # 1. fund_code (基金代码)
                        # 2. fund_name (基金简称)
                        # 3. fund_type (基金类型)
                        # 4. latest_unit_nav (最新单位净值)
                        # 5. latest_accum_nav (最新累计净值)
                        # 6. prev_unit_nav (上个交易日单位净值)
                        # 7. prev_accum_nav (上个交易日累计净值)
                        # 8. growth_value (增长值)
                        # 9. growth_rate (增长率)
                        # 10. market_price (市价)
                        # 11. discount_rate (折价率)
                        fund_data = {
                            "fund_code": fund_code,
                            "fund_name": fund_name,
                            "fund_type": fund_type,
                            "latest_unit_nav": latest_unit_nav,
                            "latest_accum_nav": latest_accum_nav,
                            "prev_unit_nav": prev_unit_nav,
                            "prev_accum_nav": prev_accum_nav,
                            "growth_value": growth_value,
                            "growth_rate": growth_rate,
                            "market_price": market_price,
                            "discount_rate": discount_rate,
                            "fetch_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }

                        # 添加到基金数据列表
                        fund_data_list.append(fund_data)
                        processed_rows += 1
                        total_processed += 1

                        # 每处理100条记录显示一次进度
                        if total_processed % 100 == 0:
                            print(f"已处理 {total_processed} 条基金数据")

                    except Exception:
                        continue

                print(f"页面处理完成，共处理 {processed_rows} 条记录")
                print("数据提取完成")

            except Exception as e:
                print(f"爬虫过程中发生错误: {e}")

            finally:
                # 关闭浏览器
                await browser.close()

    except Exception as e:
        print(f"程序执行出错: {e}")

    print(f"总共处理了 {len(fund_data_list)} 条基金数据")
    return fund_data_list

# 下载所有场内交易基金数据
def download_all_cnjy_funds():
    print("开始下载所有场内交易基金数据...")
    try:
        # 运行异步函数
        fund_data_list = asyncio.run(fetch_cnjy_fund_data())
        
        # 存储到HDF5文件
        if fund_data_list:
            print(f"将 {len(fund_data_list)} 条基金数据存储到HDF5文件...")
            store_fund_data_to_hdf5(fund_data_list)
            print("数据存储完成！")
        else:
            print("未获取到基金数据，无法存储")
    except Exception as e:
        print(f"下载基金数据时出错: {e}")

# 查询场内交易基金
def query_cnjy_fund():
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
        print(f"最新单位净值: {fund_data.get('latest_unit_nav', '---')}")
        print(f"最新累计净值: {fund_data.get('latest_accum_nav', '---')}")
        print(f"上个交易日单位净值: {fund_data.get('prev_unit_nav', '---')}")
        print(f"上个交易日累计净值: {fund_data.get('prev_accum_nav', '---')}")
        print(f"增长值: {fund_data.get('growth_value', '---')}")
        print(f"增长率: {fund_data.get('growth_rate', '---')}")
        print(f"市价: {fund_data.get('market_price', '---')}")
        print(f"折价率: {fund_data.get('discount_rate', '---')}")
        print(f"数据获取时间: {fund_data.get('fetch_time', '---')}")
    else:
        print(f"未找到基金代码为 {fund_code} 的数据")

# 显示菜单
def show_menu():
    while True:
        print("\n===== 场内交易基金数据管理系统 =====")
        print("1. 下载所有场内交易基金数据")
        print("2. 查询场内交易基金数据")
        print("0. 退出系统")
        
        choice = input("请输入您的选择 (0-2): ").strip()
        
        if choice == "1":
            download_all_cnjy_funds()
        elif choice == "2":
            query_cnjy_fund()
        elif choice == "0":
            print("感谢使用，再见！")
            break
        else:
            print("无效的选择，请重新输入")

# 主函数
if __name__ == "__main__":
    try:
        show_menu()
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序运行出错: {e}")