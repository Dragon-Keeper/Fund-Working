# 提取货币基金的排名数据
import asyncio
import re
import h5py
import os
from datetime import datetime
from playwright.async_api import async_playwright

# 全局配置
HDF5_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "HBX_Fund_Ranking_Data.h5"
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


# 使用Playwright获取货币基金数据
async def fetch_currency_fund_data():
    fund_data_list = []
    try:
        # 设置初始页面
        page_num = 1
        total_pages = 1  # 初始值，后续会从网页中获取实际值
        total_processed = 0
        base_url = "https://fund.eastmoney.com/data/hbxfundranking.html"
        current_url = f"{base_url}#t;c0;r;sSYL_1N;ddesc;pn10000;mg;os1;"

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
                        current_url = f"{base_url}#t;c{page_num-1};r;sSYL_1N;ddesc;pn10000;mg;os1;"

                    print(f"正在处理第 {page_num} 页，URL: {current_url}")

                    # 导航到目标页面
                    await page.goto(current_url)
                    await page.wait_for_load_state("networkidle")  # 等待网络空闲

                    # 等待表格加载完成
                    try:
                        await page.wait_for_selector("#dbtable > tbody > tr", timeout=10000)
                        print("成功找到基金数据表格")
                    except:
                        print(f"第 {page_num} 页未找到基金数据表格，可能没有更多数据或页面结构已更改")
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

                            # 货币基金表格每行应有21个单元格
                            if len(cells) < 20:  # 确保有足够的字段
                                print(f"行数据字段不足，当前有{len(cells)}个单元格，跳过该行")
                                continue

                            # 提取各个字段数据 - 严格按照用户要求的顺序从第三个单元格开始提取
                            # 固定顺序: [基金代码、基金简称、数据日期、万份收益、7日年化收益率、14日年化收益率、28日年化收益率、基金净值、近1月增长率、近3月增长率、近6月增长率、近1年增长率、近2年增长率、近3年增长率、近5年增长率、今年来增长率、成立来增长率、手续费]
                            
                            # 初始化所有字段为默认值
                            fund_code = "---"
                            fund_name = "---"
                            data_date = "---"
                            per_10k_return = "---"
                            seven_day_annualized = "---"
                            fourteen_day_annualized = "---"
                            twenty_eight_day_annualized = "---"
                            net_value = "---"
                            month_growth = "---"
                            quarter_growth = "---"
                            half_year_growth = "---"
                            year_growth = "---"
                            two_year_growth = "---"
                            three_year_growth = "---"
                            five_year_growth = "---"
                            year_to_date_growth = "---"
                            since_establishment_growth = "---"
                            fee = "---"

                            # 安全地访问各个字段，严格按照指定顺序映射
                            try:
                                # 打印单元格数量，帮助调试
                                print(f"当前行有 {len(cells)} 个单元格")

                                # 基金代码 (第3个单元格，索引为2)
                                if len(cells) >= 3:
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
                                        fund_code_text = await cells[2].text_content()
                                        fund_code_match = re.search(r"\d{6}", fund_code_text)
                                        if fund_code_match:
                                            fund_code = fund_code_match.group(0)

                                # 基金简称 (第4个单元格，索引为3)
                                if len(cells) >= 4:
                                    fund_name = await cells[3].text_content()
                                    fund_name = fund_name.strip()

                                # 数据日期 (第5个单元格，索引为4)
                                if len(cells) >= 5:
                                    date_text = await cells[4].text_content()
                                    date_text = date_text.strip()
                                    # 处理日期格式，可能是 MM-DD 或 YYYY-MM-DD
                                    if len(date_text) == 5 and date_text[2] == "-":
                                        # MM-DD 格式，添加当前年份
                                        current_year = datetime.now().year
                                        data_date = f"{current_year}-{date_text}"
                                    elif len(date_text) == 10 and date_text[4] == "-" and date_text[7] == "-":
                                        # YYYY-MM-DD 格式
                                        data_date = date_text
                                    else:
                                        data_date = date_text

                                # 万份收益 (第6个单元格，索引为5)
                                if len(cells) >= 6:
                                    per_10k_return_text = await cells[5].text_content()
                                    per_10k_return = parse_numeric_data(per_10k_return_text.strip())

                                # 7日年化收益率 (第7个单元格，索引为6)
                                if len(cells) >= 7:
                                    seven_day_text = await cells[6].text_content()
                                    seven_day_annualized = parse_percentage_data(seven_day_text.strip())

                                # 14日年化收益率 (第8个单元格，索引为7)
                                if len(cells) >= 8:
                                    fourteen_day_text = await cells[7].text_content()
                                    fourteen_day_annualized = parse_percentage_data(fourteen_day_text.strip())

                                # 28日年化收益率 (第9个单元格，索引为8)
                                if len(cells) >= 9:
                                    twenty_eight_day_text = await cells[8].text_content()
                                    twenty_eight_day_annualized = parse_percentage_data(twenty_eight_day_text.strip())

                                # 基金净值 (第10个单元格，索引为9)
                                if len(cells) >= 10:
                                    net_value_text = await cells[9].text_content()
                                    net_value = parse_numeric_data(net_value_text.strip())

                                # 近1月增长率 (第11个单元格，索引为10)
                                if len(cells) >= 11:
                                    month_growth_text = await cells[10].text_content()
                                    month_growth = parse_percentage_data(month_growth_text.strip())

                                # 近3月增长率 (第12个单元格，索引为11)
                                if len(cells) >= 12:
                                    quarter_growth_text = await cells[11].text_content()
                                    quarter_growth = parse_percentage_data(quarter_growth_text.strip())

                                # 近6月增长率 (第13个单元格，索引为12)
                                if len(cells) >= 13:
                                    half_year_growth_text = await cells[12].text_content()
                                    half_year_growth = parse_percentage_data(half_year_growth_text.strip())

                                # 近1年增长率 (第14个单元格，索引为13)
                                if len(cells) >= 14:
                                    year_growth_text = await cells[13].text_content()
                                    year_growth = parse_percentage_data(year_growth_text.strip())

                                # 近2年增长率 (第15个单元格，索引为14)
                                if len(cells) >= 15:
                                    two_year_growth_text = await cells[14].text_content()
                                    two_year_growth = parse_percentage_data(two_year_growth_text.strip())

                                # 近3年增长率 (第16个单元格，索引为15)
                                if len(cells) >= 16:
                                    three_year_growth_text = await cells[15].text_content()
                                    three_year_growth = parse_percentage_data(three_year_growth_text.strip())

                                # 近5年增长率 (第17个单元格，索引为16)
                                if len(cells) >= 17:
                                    five_year_growth_text = await cells[16].text_content()
                                    five_year_growth = parse_percentage_data(five_year_growth_text.strip())

                                # 今年来增长率 (第18个单元格，索引为17)
                                if len(cells) >= 18:
                                    year_to_date_growth_text = await cells[17].text_content()
                                    year_to_date_growth = parse_percentage_data(year_to_date_growth_text.strip())

                                # 成立来增长率 (第19个单元格，索引为18)
                                if len(cells) >= 19:
                                    since_establishment_growth_text = await cells[18].text_content()
                                    since_establishment_growth = parse_percentage_data(since_establishment_growth_text.strip())

                                # 手续费 (第20个单元格，索引为19)
                                if len(cells) >= 20:
                                    fee_text = await cells[19].text_content()
                                    fee = fee_text.strip() if fee_text.strip() and fee_text.strip() != "---" else "---"

                            except Exception as e:
                                print(f"提取字段数据时出错: {e}")

                            # 构建基金数据字典，严格按照用户提供的固定顺序映射
                            fund_data = {
                                "fund_code": fund_code.strip() if fund_code.strip() and fund_code.strip() != "---" else "---",
                                "fund_name": fund_name.strip() if fund_name.strip() and fund_name.strip() != "---" else "---",
                                "data_date": data_date,
                                "per_10k_return": per_10k_return,
                                "seven_day_annualized": seven_day_annualized,
                                "fourteen_day_annualized": fourteen_day_annualized,
                                "twenty_eight_day_annualized": twenty_eight_day_annualized,
                                "net_value": net_value,
                                "month_growth": month_growth,
                                "quarter_growth": quarter_growth,
                                "half_year_growth": half_year_growth,
                                "year_growth": year_growth,
                                "two_year_growth": two_year_growth,
                                "three_year_growth": three_year_growth,
                                "five_year_growth": five_year_growth,
                                "year_to_date_growth": year_to_date_growth,
                                "since_establishment_growth": since_establishment_growth,
                                "fee": fee,
                                "fetch_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
                    print(f"第 {page_num} 页处理完成，共处理 {processed_rows} 条有效基金数据，累计处理 {total_processed} 条")

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
        print(f"获取货币基金数据时发生总错误: {e}")
        import traceback
        traceback.print_exc()

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
    # 检查基金代码格式是否正确
    if not fund_code.isdigit() or len(fund_code) != 6:
        print("错误：基金代码应为6位数字")
        return

    fund_data = query_fund_by_code(fund_code)

    if fund_data:
        print("\n基金数据查询结果:")
        print(f"基金代码: {fund_data['fund_code']}")
        print(f"基金简称: {fund_data['fund_name']}")
        print(f"数据日期: {fund_data['data_date']}")
        print(f"万份收益: {fund_data['per_10k_return']}")
        print(f"7日年化收益率: {fund_data['seven_day_annualized']}%")
        print(f"14日年化收益率: {fund_data['fourteen_day_annualized']}%")
        print(f"28日年化收益率: {fund_data['twenty_eight_day_annualized']}%")
        print(f"基金净值: {fund_data['net_value']}")
        print(f"近1月增长率: {fund_data['month_growth']}%")
        print(f"近3月增长率: {fund_data['quarter_growth']}%")
        print(f"近6月增长率: {fund_data['half_year_growth']}%")
        print(f"近1年增长率: {fund_data['year_growth']}%")
        print(f"近2年增长率: {fund_data['two_year_growth']}%")
        print(f"近3年增长率: {fund_data['three_year_growth']}%")
        print(f"近5年增长率: {fund_data['five_year_growth']}%")
        print(f"今年来增长率: {fund_data['year_to_date_growth']}%")
        print(f"成立来增长率: {fund_data['since_establishment_growth']}%")
        print(f"手续费: {fund_data['fee']}")
        print(f"数据获取时间: {fund_data['fetch_time']}")
    else:
        print(f"未找到基金代码为 {fund_code} 的货币基金数据")


# 获取所有基金代码
def get_all_fund_codes():
    """获取HDF5文件中所有基金代码"""
    if not os.path.exists(HDF5_PATH):
        print(f"错误：HDF5文件不存在: {HDF5_PATH}")
        return []
    
    try:
        with h5py.File(HDF5_PATH, "r") as f:
            if "funds" not in f:
                return []
            # 返回funds组下所有子组名（即基金代码）
            return list(f["funds"].keys())
    except Exception as e:
        print(f"读取基金代码时发生错误: {e}")
        return []

# 显示所有基金代码
def show_all_fund_codes():
    """显示所有基金代码，支持分页查看"""
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
        print("0. 退出")

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
    show_menu()