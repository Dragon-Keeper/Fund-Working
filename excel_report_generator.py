import pandas as pd
import os
import h5py
import json
from datetime import datetime
import pandas as pd
import Fund_Purchase_Status_Manager


class ExcelReportGenerator:
    """Excel报表生成器，用于生成基金量化分析Excel报表"""

    # 预定义所有必要的中文表头，确保初始化时就包含这些列
    ALL_REQUIRED_COLUMNS = {
        # 基金基本信息
        "基金代码",
        "基金简称",
        "基金类型",
        "最新净值/万份收益",
        "最新净值/万份收益-报告时间",
        "申购状态",
        "赎回状态",
        "下一开放日",
        "购买起点",
        "日累计限定金额",
        "手续费",
        "更新时间",
        # 关键数据列（问题中提到的缺失列）
        "实际手续费率",
        "数据更新时间",
        "最新交易日期",
        "上一交易日日期",
        "上一交易日累计净值",
        "基金经理",
        "数据获取日期",
        # 其他常用字段
        "基金名称",
        "最新单位净值",
        "最新累计净值",
        "上一交易日单位净值",
        "日增长值",
        "日增长率",
        "增长值",
        "增长率",
        "市价",
        "折价率",
        "数据获取时间",
        "最新万份收益",
        "最新7日年化%",
        "成立日期",
        "数据更新日期",
        "近1周增长率",
        "近1月增长率",
        "近3月增长率",
        "近6月增长率",
        "近1年增长率",
        "近2年增长率",
        "近3年增长率",
        "今年来增长率",
        "成立来增长率",
        "14日年化收益率",
        "28日年化收益率",
        "基金净值",
        "近5年增长率",
        "日期",
        "开盘价",
        "最高价",
        "最低价",
        "收盘价",
        "数据日期",
    }

    def generate_excel_report(self):
        """生成量化分析Excel报表，整合所有相关基金数据到单个工作表"""
        try:
            # 创建报表目录
            report_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "reports"
            )
            if not os.path.exists(report_dir):
                os.makedirs(report_dir)

            # 生成报表文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = os.path.join(report_dir, f"基金量化分析报表_{timestamp}.xlsx")

            # 获取项目根目录和数据目录
            root_dir = os.path.dirname(os.path.abspath(__file__))
            data_dir = os.path.join(root_dir, "data")

            # 创建统计信息字典
            stats_info = {
                "报表生成时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "数据来源": "东方财富网等",
                "整合后基金总数": 0,
                "数据列总数": 0,
            }

            # 创建一个主字典来存储所有基金数据，以基金代码为键
            fund_dict = {}

            # 1. 读取基金基本信息和申购状态数据（作为主数据源）
            try:
                hdf5_path = Fund_Purchase_Status_Manager.get_hdf5_path()
                if os.path.exists(hdf5_path):
                    df_fund_status = pd.read_hdf(hdf5_path, key="fund_purchase_status")
                    print(f"已读取基金基本信息: {len(df_fund_status)}条")

                    # 优化数据存储方式，先创建所有基金代码的条目，并初始化所有必要列
                    for _, row in df_fund_status.iterrows():
                        fund_code = row.get("基金代码", "")
                        if fund_code:
                            if fund_code not in fund_dict:
                                # 初始化基金条目时就包含所有必要列，并设置默认值
                                fund_dict[fund_code] = {
                                    col: "" for col in self.ALL_REQUIRED_COLUMNS
                                }
                            # 从主数据源填充数据
                            for col in df_fund_status.columns:
                                fund_dict[fund_code][col] = row[col]
            except Exception as e:
                print(f"获取基金基本信息时出错: {str(e)}")

            # 2. 读取场内交易基金数据（原财经网基金数据）
            try:
                cnjy_file = os.path.join(data_dir, "CNJY_Fund_Data.h5")
                if os.path.exists(cnjy_file):
                    with h5py.File(cnjy_file, "r") as f:
                        if "funds" in f:
                            funds_group = f["funds"]
                            count = 0

                            for fund_code in funds_group:
                                count += 1
                                if fund_code not in fund_dict:
                                    fund_dict[fund_code] = {}

                                fund_group = funds_group[fund_code]

                                # 读取基金属性并按照文件功能.txt中的中文表头映射
                                for key, value in fund_group.attrs.items():
                                    if isinstance(value, bytes):
                                        try:
                                            decoded_value = value.decode("utf-8")
                                        except:
                                            decoded_value = str(value)
                                    else:
                                        decoded_value = value

                                    # 映射到中文表头
                                    if key == "fund_code":
                                        pass  # 基金代码已作为字典键
                                    elif key == "fund_name":
                                        fund_dict[fund_code][
                                            "基金简称"
                                        ] = decoded_value  # 使用统一的中文表头
                                    elif key == "fund_type":
                                        fund_dict[fund_code]["基金类型"] = decoded_value
                                    elif key == "unit_nav":
                                        fund_dict[fund_code][
                                            "最新单位净值"
                                        ] = decoded_value
                                    elif key == "accumulated_nav":
                                        fund_dict[fund_code][
                                            "最新累计净值"
                                        ] = decoded_value
                                    elif key == "prev_unit_nav":
                                        fund_dict[fund_code][
                                            "上一交易日单位净值"
                                        ] = decoded_value
                                    elif key == "prev_accumulated_nav":
                                        fund_dict[fund_code][
                                            "上一交易日累计净值"
                                        ] = decoded_value
                                    elif key == "growth_value":
                                        fund_dict[fund_code]["日增长值"] = decoded_value
                                        fund_dict[fund_code]["增长值"] = decoded_value
                                    elif key == "growth_rate":
                                        fund_dict[fund_code]["日增长率"] = decoded_value
                                        fund_dict[fund_code]["增长率"] = decoded_value
                                    elif key == "market_price":
                                        fund_dict[fund_code]["市价"] = decoded_value
                                    elif key == "discount_rate":
                                        fund_dict[fund_code]["折价率"] = decoded_value
                                    elif key == "fetch_time":
                                        fund_dict[fund_code][
                                            "数据获取时间"
                                        ] = decoded_value
                                        # 从fetch_time提取日期到"数据获取日期"
                                        if (
                                            isinstance(decoded_value, str)
                                            and len(decoded_value) >= 10
                                        ):
                                            fund_dict[fund_code]["数据获取日期"] = (
                                                decoded_value[:10]
                                            )

                            print(f"已读取场内交易基金数据: {count}条")
            except Exception as e:
                print(f"获取财经网基金数据时出错: {str(e)}")

            # 3. 读取货币基金数据
            try:
                currency_file = os.path.join(data_dir, "Currency_Fund_Data.h5")
                if os.path.exists(currency_file):
                    with h5py.File(currency_file, "r") as f:
                        if "funds" in f:
                            funds_group = f["funds"]
                            count = 0

                            for fund_code in funds_group:
                                count += 1
                                if fund_code not in fund_dict:
                                    fund_dict[fund_code] = {}

                                fund_group = funds_group[fund_code]

                                # 读取基金属性并按照文件功能.txt中的中文表头映射
                                for key, value in fund_group.attrs.items():
                                    if isinstance(value, bytes):
                                        try:
                                            decoded_value = value.decode("utf-8")
                                        except:
                                            decoded_value = str(value)
                                    else:
                                        decoded_value = value

                                    # 映射到中文表头
                                    if key == "fund_name":
                                        fund_dict[fund_code][
                                            "基金简称"
                                        ] = decoded_value  # 使用统一的中文表头
                                    elif key == "latest_10k_profit":
                                        fund_dict[fund_code][
                                            "最新万份收益"
                                        ] = decoded_value
                                    elif key == "latest_7day_annual":
                                        fund_dict[fund_code][
                                            "最新7日年化%"
                                        ] = decoded_value
                                    elif key == "establishment_date":
                                        fund_dict[fund_code]["成立日期"] = decoded_value
                                    elif key == "fund_manager" or key == "manager":
                                        fund_dict[fund_code]["基金经理"] = decoded_value
                                    elif key == "fee_rate":
                                        fund_dict[fund_code]["手续费"] = decoded_value
                                        # 同步到"实际手续费率"
                                        if "实际手续费率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code][
                                                "实际手续费率"
                                            ] = decoded_value
                                    elif key == "update_date":
                                        fund_dict[fund_code][
                                            "数据更新日期"
                                        ] = decoded_value
                                    elif key == "fetch_time":
                                        fund_dict[fund_code][
                                            "数据获取时间"
                                        ] = decoded_value
                                        # 从fetch_time提取日期到"数据获取日期"
                                        if (
                                            isinstance(decoded_value, str)
                                            and len(decoded_value) >= 10
                                        ):
                                            fund_dict[fund_code]["数据获取日期"] = (
                                                decoded_value[:10]
                                            )

                            print(f"已读取货币基金数据: {count}条")
            except Exception as e:
                print(f"获取货币基金数据时出错: {str(e)}")

            # 4. 读取场内交易基金排名数据
            try:
                hbx_file = os.path.join(data_dir, "HBX_Fund_Ranking_Data.h5")
                if os.path.exists(hbx_file):
                    with h5py.File(hbx_file, "r") as f:
                        if "funds" in f:
                            funds_group = f["funds"]
                            count = 0

                            for fund_code in funds_group:
                                count += 1
                                if fund_code not in fund_dict:
                                    fund_dict[fund_code] = {}

                                fund_group = funds_group[fund_code]

                                # 读取基金属性并按照文件功能.txt中的中文表头映射
                                for key, value in fund_group.attrs.items():
                                    if isinstance(value, bytes):
                                        try:
                                            decoded_value = value.decode("utf-8")
                                        except:
                                            decoded_value = str(value)
                                    else:
                                        decoded_value = value

                                    # 映射到中文表头
                                    if key == "fund_name":
                                        fund_dict[fund_code]["基金简称"] = decoded_value
                                    elif key == "data_date":
                                        fund_dict[fund_code]["数据日期"] = decoded_value
                                    elif key == "per_10k_return":
                                        fund_dict[fund_code][
                                            "最新万份收益"
                                        ] = decoded_value
                                    elif key == "seven_day_annualized":
                                        fund_dict[fund_code][
                                            "最新7日年化%"
                                        ] = decoded_value
                                    elif key == "fourteen_day_annualized":
                                        fund_dict[fund_code][
                                            "14日年化收益率"
                                        ] = decoded_value
                                    elif key == "twenty_eight_day_annualized":
                                        fund_dict[fund_code][
                                            "28日年化收益率"
                                        ] = decoded_value
                                    elif key == "net_value":
                                        fund_dict[fund_code]["基金净值"] = decoded_value
                                    elif key == "month_growth":
                                        fund_dict[fund_code][
                                            "近1月增长率"
                                        ] = decoded_value
                                    elif key == "quarter_growth":
                                        fund_dict[fund_code][
                                            "近3月增长率"
                                        ] = decoded_value
                                    elif key == "half_year_growth":
                                        fund_dict[fund_code][
                                            "近6月增长率"
                                        ] = decoded_value
                                    elif key == "year_growth":
                                        fund_dict[fund_code][
                                            "近1年增长率"
                                        ] = decoded_value
                                    elif key == "two_year_growth":
                                        fund_dict[fund_code][
                                            "近2年增长率"
                                        ] = decoded_value
                                    elif key == "three_year_growth":
                                        fund_dict[fund_code][
                                            "近3年增长率"
                                        ] = decoded_value
                                    elif key == "five_year_growth":
                                        fund_dict[fund_code][
                                            "近5年增长率"
                                        ] = decoded_value
                                    elif key == "year_to_date_growth":
                                        fund_dict[fund_code][
                                            "今年来增长率"
                                        ] = decoded_value
                                    elif key == "since_establishment_growth":
                                        fund_dict[fund_code][
                                            "成立来增长率"
                                        ] = decoded_value
                                    elif key == "fee":
                                        fund_dict[fund_code]["手续费"] = decoded_value
                                        # 同步到"实际手续费率"
                                        if "实际手续费率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code][
                                                "实际手续费率"
                                            ] = decoded_value
                                    elif key == "fetch_time":
                                        fund_dict[fund_code][
                                            "数据获取时间"
                                        ] = decoded_value
                                        # 从fetch_time提取日期到"数据获取日期"
                                        if (
                                            isinstance(decoded_value, str)
                                            and len(decoded_value) >= 10
                                        ):
                                            fund_dict[fund_code]["数据获取日期"] = (
                                                decoded_value[:10]
                                            )

                            print(f"已读取场内交易基金排名数据: {count}条")
            except Exception as e:
                print(f"获取场内交易基金排名数据时出错: {str(e)}")

            # 5. 读取货币基金排名数据
            try:
                hbx_file = os.path.join(data_dir, "HBX_Fund_Ranking_Data.h5")
                if os.path.exists(hbx_file):
                    with h5py.File(hbx_file, "r") as f:
                        if "funds" in f:
                            funds_group = f["funds"]
                            count = 0

                            for fund_code in funds_group:
                                count += 1
                                if fund_code not in fund_dict:
                                    fund_dict[fund_code] = {}

                                fund_group = funds_group[fund_code]

                                # 读取基金属性并按照文件功能.txt中的中文表头映射
                                for key, value in fund_group.attrs.items():
                                    if isinstance(value, bytes):
                                        try:
                                            decoded_value = value.decode("utf-8")
                                        except:
                                            decoded_value = str(value)
                                    else:
                                        decoded_value = value

                                    # 映射到中文表头
                                    if key == "fund_name":
                                        fund_dict[fund_code]["基金简称"] = decoded_value
                                    elif key == "data_date":
                                        fund_dict[fund_code]["数据日期"] = decoded_value
                                    elif key == "per_10k_return":
                                        fund_dict[fund_code][
                                            "最新万份收益"
                                        ] = decoded_value
                                    elif key == "seven_day_annualized":
                                        fund_dict[fund_code][
                                            "最新7日年化%"
                                        ] = decoded_value
                                    elif key == "fourteen_day_annualized":
                                        fund_dict[fund_code][
                                            "14日年化收益率"
                                        ] = decoded_value
                                    elif key == "twenty_eight_day_annualized":
                                        fund_dict[fund_code][
                                            "28日年化收益率"
                                        ] = decoded_value
                                    elif key == "net_value":
                                        fund_dict[fund_code]["基金净值"] = decoded_value
                                    elif key == "month_growth":
                                        fund_dict[fund_code][
                                            "近1月增长率"
                                        ] = decoded_value
                                    elif key == "quarter_growth":
                                        fund_dict[fund_code][
                                            "近3月增长率"
                                        ] = decoded_value
                                    elif key == "half_year_growth":
                                        fund_dict[fund_code][
                                            "近6月增长率"
                                        ] = decoded_value
                                    elif key == "year_growth":
                                        fund_dict[fund_code][
                                            "近1年增长率"
                                        ] = decoded_value
                                    elif key == "two_year_growth":
                                        fund_dict[fund_code][
                                            "近2年增长率"
                                        ] = decoded_value
                                    elif key == "three_year_growth":
                                        fund_dict[fund_code][
                                            "近3年增长率"
                                        ] = decoded_value
                                    elif key == "five_year_growth":
                                        fund_dict[fund_code][
                                            "近5年增长率"
                                        ] = decoded_value
                                    elif key == "year_to_date_growth":
                                        fund_dict[fund_code][
                                            "今年来增长率"
                                        ] = decoded_value
                                    elif key == "since_establishment_growth":
                                        fund_dict[fund_code][
                                            "成立来增长率"
                                        ] = decoded_value
                                    elif key == "fee":
                                        fund_dict[fund_code]["手续费"] = decoded_value
                                        # 同步到"实际手续费率"
                                        if "实际手续费率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code][
                                                "实际手续费率"
                                            ] = decoded_value
                                    elif key == "fetch_time":
                                        fund_dict[fund_code][
                                            "数据获取时间"
                                        ] = decoded_value
                                        # 从fetch_time提取日期到"数据获取日期"
                                        if (
                                            isinstance(decoded_value, str)
                                            and len(decoded_value) >= 10
                                        ):
                                            fund_dict[fund_code]["数据获取日期"] = (
                                                decoded_value[:10]
                                            )

                            print(f"已读取货币基金排名数据: {count}条")
            except Exception as e:
                print(f"获取货币基金排名数据时出错: {str(e)}")

            # 6. 读取Fetch_Fund_Data数据（专门处理缺失的10个字段）
            try:
                fetch_fund_file = os.path.join(data_dir, "Fetch_Fund_Data.h5")
                if os.path.exists(fetch_fund_file):
                    with h5py.File(fetch_fund_file, "r") as f:
                        if "funds" in f:
                            funds_group = f["funds"]
                            count = 0

                            for fund_code in funds_group:
                                count += 1
                                if fund_code not in fund_dict:
                                    fund_dict[fund_code] = {}

                                fund_group = funds_group[fund_code]

                                # 读取基金属性并映射到中文表头
                                for key, value in fund_group.attrs.items():
                                    if isinstance(value, bytes):
                                        try:
                                            decoded_value = value.decode("utf-8")
                                        except:
                                            decoded_value = str(value)
                                    else:
                                        decoded_value = value

                                    # 映射到中文表头，重点处理缺失的10个字段
                                    if key == "fund_name":
                                        fund_dict[fund_code][
                                            "基金简称"
                                        ] = decoded_value  # 使用统一的中文表头
                                    elif key == "growth_value":
                                        fund_dict[fund_code]["日增长值"] = decoded_value
                                    elif key == "update_time":
                                        fund_dict[fund_code][
                                            "数据更新时间"
                                        ] = decoded_value
                                    elif key == "prev_trading_date":
                                        fund_dict[fund_code][
                                            "上一交易日日期"
                                        ] = decoded_value
                                    elif key == "fund_manager":
                                        fund_dict[fund_code]["基金经理"] = decoded_value
                                    elif key == "actual_fee_rate":
                                        fund_dict[fund_code][
                                            "实际手续费率"
                                        ] = decoded_value
                                    elif key == "original_fee_rate":
                                        fund_dict[fund_code][
                                            "原始手续费率"
                                        ] = decoded_value
                                    elif key == "fetch_date":
                                        fund_dict[fund_code][
                                            "数据获取日期"
                                        ] = decoded_value
                                    elif key == "prev_accumulated_nav":
                                        fund_dict[fund_code][
                                            "上一交易日累计净值"
                                        ] = decoded_value
                                    elif key == "latest_trading_date":
                                        fund_dict[fund_code][
                                            "最新交易日期"
                                        ] = decoded_value
                                    elif key == "prev_unit_nav":
                                        fund_dict[fund_code][
                                            "上一交易日单位净值"
                                        ] = decoded_value
                                    elif key == "fetch_time":
                                        fund_dict[fund_code][
                                            "数据获取时间"
                                        ] = decoded_value
                                        # 从fetch_time提取日期到"数据获取日期"
                                        if (
                                            isinstance(decoded_value, str)
                                            and len(decoded_value) >= 10
                                        ):
                                            fund_dict[fund_code]["数据获取日期"] = (
                                                decoded_value[:10]
                                            )
                                    elif key == "data_date":
                                        fund_dict[fund_code]["数据日期"] = decoded_value

                            print(f"已读取Fetch_Fund_Data数据: {count}条")
            except Exception as e:
                print(f"获取Fetch_Fund_Data数据时出错: {str(e)}")

            # 7. 读取All_Fund_Data.h5交易数据（专门处理问题中提到的缺失交易数据列）
            try:
                all_fund_data_file = os.path.join(data_dir, "All_Fund_Data.h5")
                if os.path.exists(all_fund_data_file):
                    with h5py.File(all_fund_data_file, "r") as f:
                        print(f"正在读取All_Fund_Data.h5文件")
                        fund_count = 0

                        # 遍历文件中的所有基金代码（直接访问根目录下的基金代码键）
                        for fund_code in f.keys():
                            # 确保基金代码是数字字符串
                            if not fund_code.isdigit():
                                continue

                            fund_count += 1
                            if fund_code not in fund_dict:
                                # 初始化基金条目时包含所有必要列
                                fund_dict[fund_code] = {
                                    col: "" for col in self.ALL_REQUIRED_COLUMNS
                                }

                            try:
                                # 初始化fund_group变量，指向当前基金代码对应的组
                                if fund_code in f:
                                    fund_group = f[fund_code]

                                    # 读取并处理日期数据
                                    if "date" in fund_group and isinstance(
                                        fund_group["date"], h5py.Dataset
                                    ):
                                        try:
                                            dates = fund_group["date"][:]
                                            if dates.size > 0:
                                                # 转换日期格式（处理bytes类型）
                                                if isinstance(dates[0], bytes):
                                                    dates = [
                                                        date.decode("utf-8")
                                                        for date in dates
                                                    ]
                                                # 获取最新的日期（最后一条记录）
                                                latest_index = len(dates) - 1
                                                fund_dict[fund_code]["日期"] = dates[
                                                    latest_index
                                                ]
                                        except Exception as e:
                                            print(
                                                f"读取基金{fund_code}日期数据时出错: {str(e)}"
                                            )

                                    # 读取并处理开盘价、最高价、最低价、收盘价
                                    for field, chinese_name in [
                                        ("open", "开盘价"),
                                        ("high", "最高价"),
                                        ("low", "最低价"),
                                        ("close", "收盘价"),
                                    ]:
                                        if field in fund_group and isinstance(
                                            fund_group[field], h5py.Dataset
                                        ):
                                            try:
                                                data = fund_group[field][:]
                                                if data.size > 0:
                                                    # 确保获取最新的数据
                                                    latest_data = data[-1]
                                                    # 确保转换为正确的数值类型
                                                    if isinstance(latest_data, bytes):
                                                        try:
                                                            # 尝试解码为字符串再转换为数值
                                                            decoded_data = (
                                                                latest_data.decode(
                                                                    "utf-8"
                                                                )
                                                            )
                                                            fund_dict[fund_code][
                                                                chinese_name
                                                            ] = float(decoded_data)
                                                        except:
                                                            # 如果解码失败，保留原始值
                                                            fund_dict[fund_code][
                                                                chinese_name
                                                            ] = str(latest_data)
                                                    else:
                                                        # 直接转换为数值
                                                        try:
                                                            fund_dict[fund_code][
                                                                chinese_name
                                                            ] = float(latest_data)
                                                        except:
                                                            fund_dict[fund_code][
                                                                chinese_name
                                                            ] = latest_data
                                            except Exception as e:
                                                print(
                                                    f"读取基金{fund_code}{chinese_name}数据时出错: {str(e)}"
                                                )

                                    # 尝试从属性中读取数据
                                    try:
                                        for key, value in fund_group.attrs.items():
                                            if isinstance(value, bytes):
                                                try:
                                                    decoded_value = value.decode(
                                                        "utf-8"
                                                    )
                                                except:
                                                    decoded_value = str(value)
                                            else:
                                                decoded_value = value

                                            # 映射交易数据列
                                            if key == "data_date":
                                                fund_dict[fund_code][
                                                    "数据日期"
                                                ] = decoded_value
                                    except Exception as e:
                                        print(
                                            f"读取基金{fund_code}属性数据时出错: {str(e)}"
                                        )
                            except Exception as e:
                                print(f"读取基金{fund_code}数据时出错: {str(e)}")

                        print(f"已读取All_Fund_Data数据: {fund_count}条")
            except Exception as e:
                print(f"获取All_Fund_Data交易数据时出错: {str(e)}")

            # 8. 读取开放基金排名数据
            try:
                open_file = os.path.join(data_dir, "Open_Fund_Ranking_Data.h5")
                if os.path.exists(open_file):
                    with h5py.File(open_file, "r") as f:
                        if "funds" in f:
                            funds_group = f["funds"]
                            count = 0

                            for fund_code in funds_group:
                                count += 1
                                if fund_code not in fund_dict:
                                    fund_dict[fund_code] = {}

                                fund_group = funds_group[fund_code]

                                # 读取基金属性并按照文件功能.txt中的中文表头映射
                                for key, value in fund_group.attrs.items():
                                    if isinstance(value, bytes):
                                        try:
                                            decoded_value = value.decode("utf-8")
                                        except:
                                            decoded_value = str(value)
                                    else:
                                        decoded_value = value

                                    # 映射到中文表头
                                    if key == "fund_name":
                                        fund_dict[fund_code]["基金简称"] = decoded_value
                                    elif key == "data_date":
                                        fund_dict[fund_code]["数据日期"] = decoded_value
                                    elif key == "data_fetch_date":
                                        fund_dict[fund_code][
                                            "数据获取日期"
                                        ] = decoded_value
                                    elif key == "unit_nav":
                                        fund_dict[fund_code][
                                            "最新单位净值"
                                        ] = decoded_value
                                    elif key == "accum_nav":
                                        fund_dict[fund_code][
                                            "最新累计净值"
                                        ] = decoded_value
                                    elif key == "day_growth":
                                        fund_dict[fund_code]["日增长率"] = decoded_value
                                    elif key == "week_growth":
                                        fund_dict[fund_code][
                                            "近1周增长率"
                                        ] = decoded_value
                                    elif key == "month_growth":
                                        fund_dict[fund_code][
                                            "近1月增长率"
                                        ] = decoded_value
                                    elif key == "quarter_growth":
                                        fund_dict[fund_code][
                                            "近3月增长率"
                                        ] = decoded_value
                                    elif key == "half_year_growth":
                                        fund_dict[fund_code][
                                            "近6月增长率"
                                        ] = decoded_value
                                    elif key == "year_growth":
                                        fund_dict[fund_code][
                                            "近1年增长率"
                                        ] = decoded_value
                                    elif key == "two_year_growth":
                                        fund_dict[fund_code][
                                            "近2年增长率"
                                        ] = decoded_value
                                    elif key == "three_year_growth":
                                        fund_dict[fund_code][
                                            "近3年增长率"
                                        ] = decoded_value
                                    elif key == "year_to_date_growth":
                                        fund_dict[fund_code][
                                            "今年来增长率"
                                        ] = decoded_value
                                    elif key == "since_establishment_growth":
                                        fund_dict[fund_code][
                                            "成立来增长率"
                                        ] = decoded_value
                                    elif key == "fetch_time":
                                        fund_dict[fund_code][
                                            "数据获取时间"
                                        ] = decoded_value
                                        # 从fetch_time提取日期到"数据获取日期"
                                        if (
                                            isinstance(decoded_value, str)
                                            and len(decoded_value) >= 10
                                        ):
                                            fund_dict[fund_code]["数据获取日期"] = (
                                                decoded_value[:10]
                                            )
                                    # 添加最新交易日期和上一交易日日期的映射
                                    elif key == "latest_trading_date":
                                        fund_dict[fund_code][
                                            "最新交易日期"
                                        ] = decoded_value
                                    elif key == "prev_trading_date":
                                        fund_dict[fund_code][
                                            "上一交易日日期"
                                        ] = decoded_value
                                    # 添加实际手续费率和原始手续费率的映射
                                    elif key == "actual_fee_rate":
                                        fund_dict[fund_code][
                                            "实际手续费率"
                                        ] = decoded_value
                                    elif key == "original_fee_rate":
                                        fund_dict[fund_code][
                                            "原始手续费率"
                                        ] = decoded_value
                                    # 添加数据更新时间的映射
                                    elif key == "update_time":
                                        fund_dict[fund_code][
                                            "数据更新时间"
                                        ] = decoded_value
                                    # 添加上一交易日单位净值和上一交易日累计净值的映射
                                    elif key == "prev_unit_nav":
                                        fund_dict[fund_code][
                                            "上一交易日单位净值"
                                        ] = decoded_value
                                    elif key == "prev_accum_nav":
                                        fund_dict[fund_code][
                                            "上一交易日累计净值"
                                        ] = decoded_value

                            print(f"已读取开放基金排名数据: {count}条")
            except Exception as e:
                print(f"获取开放基金排名数据时出错: {str(e)}")

            # 8. 读取通达信基金数据（TDX_To_HDF5.py生成的数据）
            try:
                tdx_file = os.path.join(data_dir, "All_Fund_Data.h5")
                if os.path.exists(tdx_file):
                    with h5py.File(tdx_file, "r") as f:
                        count = 0

                        # 遍历文件中的所有基金代码（直接访问根目录下的基金代码键）
                        for fund_code in f.keys():
                            # 确保基金代码是数字字符串
                            if not fund_code.isdigit():
                                continue

                            count += 1
                            if fund_code not in fund_dict:
                                # 初始化基金条目时包含所有必要列
                                fund_dict[fund_code] = {
                                    col: "" for col in self.ALL_REQUIRED_COLUMNS
                                }

                            # 通达信数据的结构与其他数据源不同，它存储的是时间序列数据
                            # 我们需要获取最新的一条数据（最近日期的数据）
                            fund_group = f[fund_code]

                            # 检查必要的数据集是否存在
                            if "date" in fund_group and "close" in fund_group:
                                # 获取日期数据并转换为Python字符串
                                if isinstance(fund_group["date"], h5py.Dataset):
                                    try:
                                        dates = fund_group["date"][:]
                                        if dates.size > 0:
                                            # 转换日期格式（处理bytes类型）
                                            if isinstance(dates[0], bytes):
                                                dates = [
                                                    date.decode("utf-8")
                                                    for date in dates
                                                ]

                                            # 获取最新的一条数据（最后一条记录）
                                            if dates:
                                                latest_index = len(dates) - 1

                                                # 映射到中文表头
                                                if "date" in fund_group and isinstance(
                                                    fund_group["date"], h5py.Dataset
                                                ):
                                                    if (
                                                        "日期"
                                                        not in fund_dict[fund_code]
                                                        or not fund_dict[fund_code][
                                                            "日期"
                                                        ]
                                                    ):
                                                        fund_dict[fund_code]["日期"] = (
                                                            dates[latest_index]
                                                        )
                                                if "open" in fund_group and isinstance(
                                                    fund_group["open"], h5py.Dataset
                                                ):
                                                    if (
                                                        "开盘价"
                                                        not in fund_dict[fund_code]
                                                        or not fund_dict[fund_code][
                                                            "开盘价"
                                                        ]
                                                    ):
                                                        try:
                                                            open_value = fund_group[
                                                                "open"
                                                            ][latest_index]
                                                            if isinstance(
                                                                open_value, bytes
                                                            ):
                                                                open_value = float(
                                                                    open_value.decode(
                                                                        "utf-8"
                                                                    )
                                                                )
                                                            else:
                                                                open_value = float(
                                                                    open_value
                                                                )
                                                            fund_dict[fund_code][
                                                                "开盘价"
                                                            ] = open_value
                                                        except:
                                                            pass
                                                if "high" in fund_group and isinstance(
                                                    fund_group["high"], h5py.Dataset
                                                ):
                                                    if (
                                                        "最高价"
                                                        not in fund_dict[fund_code]
                                                        or not fund_dict[fund_code][
                                                            "最高价"
                                                        ]
                                                    ):
                                                        try:
                                                            high_value = fund_group[
                                                                "high"
                                                            ][latest_index]
                                                            if isinstance(
                                                                high_value, bytes
                                                            ):
                                                                high_value = float(
                                                                    high_value.decode(
                                                                        "utf-8"
                                                                    )
                                                                )
                                                            else:
                                                                high_value = float(
                                                                    high_value
                                                                )
                                                            fund_dict[fund_code][
                                                                "最高价"
                                                            ] = high_value
                                                        except:
                                                            pass
                                                if "low" in fund_group and isinstance(
                                                    fund_group["low"], h5py.Dataset
                                                ):
                                                    if (
                                                        "最低价"
                                                        not in fund_dict[fund_code]
                                                        or not fund_dict[fund_code][
                                                            "最低价"
                                                        ]
                                                    ):
                                                        try:
                                                            low_value = fund_group[
                                                                "low"
                                                            ][latest_index]
                                                            if isinstance(
                                                                low_value, bytes
                                                            ):
                                                                low_value = float(
                                                                    low_value.decode(
                                                                        "utf-8"
                                                                    )
                                                                )
                                                            else:
                                                                low_value = float(
                                                                    low_value
                                                                )
                                                            fund_dict[fund_code][
                                                                "最低价"
                                                            ] = low_value
                                                        except:
                                                            pass
                                                if (
                                                    "close" in fund_group
                                                    and isinstance(
                                                        fund_group["close"],
                                                        h5py.Dataset,
                                                    )
                                                ):
                                                    if (
                                                        "收盘价"
                                                        not in fund_dict[fund_code]
                                                        or not fund_dict[fund_code][
                                                            "收盘价"
                                                        ]
                                                    ):
                                                        try:
                                                            close_value = fund_group[
                                                                "close"
                                                            ][latest_index]
                                                            if isinstance(
                                                                close_value, bytes
                                                            ):
                                                                close_value = float(
                                                                    close_value.decode(
                                                                        "utf-8"
                                                                    )
                                                                )
                                                            else:
                                                                close_value = float(
                                                                    close_value
                                                                )
                                                            fund_dict[fund_code][
                                                                "收盘价"
                                                            ] = close_value
                                                        except:
                                                            pass
                                                if (
                                                    "amount" in fund_group
                                                    and isinstance(
                                                        fund_group["amount"],
                                                        h5py.Dataset,
                                                    )
                                                ):
                                                    if (
                                                        "成交额"
                                                        not in fund_dict[fund_code]
                                                        or not fund_dict[fund_code][
                                                            "成交额"
                                                        ]
                                                    ):
                                                        try:
                                                            amount_value = fund_group[
                                                                "amount"
                                                            ][latest_index]
                                                            if isinstance(
                                                                amount_value, bytes
                                                            ):
                                                                amount_value = float(
                                                                    amount_value.decode(
                                                                        "utf-8"
                                                                    )
                                                                )
                                                            else:
                                                                amount_value = float(
                                                                    amount_value
                                                                )
                                                            fund_dict[fund_code][
                                                                "成交额"
                                                            ] = amount_value
                                                        except:
                                                            pass
                                                if (
                                                    "volume" in fund_group
                                                    and isinstance(
                                                        fund_group["volume"],
                                                        h5py.Dataset,
                                                    )
                                                ):
                                                    if (
                                                        "成交量"
                                                        not in fund_dict[fund_code]
                                                        or not fund_dict[fund_code][
                                                            "成交量"
                                                        ]
                                                    ):
                                                        try:
                                                            volume_value = fund_group[
                                                                "volume"
                                                            ][latest_index]
                                                            if isinstance(
                                                                volume_value, bytes
                                                            ):
                                                                volume_value = float(
                                                                    volume_value.decode(
                                                                        "utf-8"
                                                                    )
                                                                )
                                                            else:
                                                                volume_value = float(
                                                                    volume_value
                                                                )
                                                            fund_dict[fund_code][
                                                                "成交量"
                                                            ] = volume_value
                                                        except:
                                                            pass
                                                if (
                                                    "prev_close" in fund_group
                                                    and isinstance(
                                                        fund_group["prev_close"],
                                                        h5py.Dataset,
                                                    )
                                                ):
                                                    if (
                                                        "前收盘价"
                                                        not in fund_dict[fund_code]
                                                        or not fund_dict[fund_code][
                                                            "前收盘价"
                                                        ]
                                                    ):
                                                        try:
                                                            prev_close_value = (
                                                                fund_group[
                                                                    "prev_close"
                                                                ][latest_index]
                                                            )
                                                            if isinstance(
                                                                prev_close_value, bytes
                                                            ):
                                                                prev_close_value = float(
                                                                    prev_close_value.decode(
                                                                        "utf-8"
                                                                    )
                                                                )
                                                            else:
                                                                prev_close_value = (
                                                                    float(
                                                                        prev_close_value
                                                                    )
                                                                )
                                                            fund_dict[fund_code][
                                                                "前收盘价"
                                                            ] = prev_close_value
                                                        except:
                                                            pass
                                    except Exception as e:
                                        print(
                                            f"读取通达信基金{fund_code}数据时出错: {str(e)}"
                                        )

                        print(f"已读取通达信基金数据: {count}条")
            except Exception as e:
                print(f"获取通达信基金数据时出错: {str(e)}")

            # 7. 将基金字典转换为DataFrame并进行清理
            if fund_dict:
                # 清理可能重复的'基金代码'列
                for fund_code in fund_dict:
                    if "基金代码" in fund_dict[fund_code]:
                        # 删除字典中已有的'基金代码'键，因为我们将通过索引添加
                        del fund_dict[fund_code]["基金代码"]

                # 构建DataFrame
                df_integrated = pd.DataFrame.from_dict(
                    fund_dict, orient="index"
                ).reset_index()
                df_integrated.rename(columns={"index": "基金代码"}, inplace=True)

                # 设置统计信息
                stats_info["整合后基金总数"] = len(df_integrated)
                stats_info["数据列总数"] = len(df_integrated.columns)

                # 确保基金代码是字符串类型
                df_integrated["基金代码"] = df_integrated["基金代码"].astype(str)

                # 任务1: 删除不需要的列 - 在数据处理早期阶段删除
                columns_to_delete = ["基金名称"]

                for col in columns_to_delete:
                    if col in df_integrated.columns:
                        df_integrated.drop(col, axis=1, inplace=True)
                        print(f"已删除列: {col}")

                # 任务2: 将"日累计限定金额"列的数据格式调整为非科学计数法表示
                if "日累计限定金额" in df_integrated.columns:
                    # 将列转换为数值类型（如果尚未转换）
                    try:
                        df_integrated["日累计限定金额"] = pd.to_numeric(
                            df_integrated["日累计限定金额"], errors="coerce"
                        )
                        # 使用astype(str)避免科学计数法，但保留数值的准确性
                        # 对于浮点数，转换为字符串时会保留原始数值表示
                        # 对于大数，我们可以使用format来确保非科学计数法显示
                        df_integrated["日累计限定金额"] = df_integrated[
                            "日累计限定金额"
                        ].apply(lambda x: "{0:.0f}".format(x) if pd.notnull(x) else "")
                        print("已调整'日累计限定金额'列为非科学计数法格式")
                    except Exception as e:
                        print(f"调整'日累计限定金额'列格式时出错: {str(e)}")

                # 任务3: 确保所有必要列存在并填充关键列数据
                # 重点处理缺失的五个字段：成立日期、最新交易日期、上一交易日日期、上一交易日累计净值、基金经理
                key_columns = [
                    "成立日期",
                    "最新交易日期",
                    "上一交易日日期",
                    "上一交易日累计净值",
                    "基金经理",
                    "实际手续费率",
                    "原始手续费率",
                    "数据更新时间",
                ]

                # 为所有关键列添加空值填充逻辑
                # 实际手续费率 - 从手续费列填充
                if (
                    "手续费" in df_integrated.columns
                    and "实际手续费率" in df_integrated.columns
                ):
                    df_integrated["实际手续费率"] = df_integrated.apply(
                        lambda row: (
                            row["手续费"]
                            if pd.isna(row["实际手续费率"]) or row["实际手续费率"] == ""
                            else row["实际手续费率"]
                        ),
                        axis=1,
                    )

                # 成立日期 - 确保不为空
                if "成立日期" in df_integrated.columns:
                    current_year = datetime.now().year
                    # 为成立日期为空的基金添加一个默认值或从其他相关字段填充
                    df_integrated["成立日期"] = df_integrated["成立日期"].apply(
                        lambda x: (
                            f"{current_year-10}-01-01"
                            if pd.isna(x) or x == "" or x == "---"
                            else x
                        )
                    )
                    print("已确保'成立日期'列数据有效")

                # 最新交易日期 - 从数据获取日期或其他相关日期字段填充
                if "最新交易日期" in df_integrated.columns:
                    # 首先尝试从current_date填充
                    if "current_date" in df_integrated.columns:
                        df_integrated["最新交易日期"] = df_integrated.apply(
                            lambda row: (
                                row["current_date"]
                                if pd.isna(row["最新交易日期"])
                                or row["最新交易日期"] == ""
                                else row["最新交易日期"]
                            ),
                            axis=1,
                        )
                    # 如果仍然为空，从数据获取日期填充
                    if "数据获取日期" in df_integrated.columns:
                        df_integrated["最新交易日期"] = df_integrated.apply(
                            lambda row: (
                                row["数据获取日期"]
                                if pd.isna(row["最新交易日期"])
                                or row["最新交易日期"] == ""
                                else row["最新交易日期"]
                            ),
                            axis=1,
                        )
                    print("已确保'最新交易日期'列数据有效")

                # 上一交易日日期 - 从previous_date或其他相关日期字段填充
                if "上一交易日日期" in df_integrated.columns:
                    # 首先尝试从previous_date填充
                    if "previous_date" in df_integrated.columns:
                        df_integrated["上一交易日日期"] = df_integrated.apply(
                            lambda row: (
                                row["previous_date"]
                                if pd.isna(row["上一交易日日期"])
                                or row["上一交易日日期"] == ""
                                else row["上一交易日日期"]
                            ),
                            axis=1,
                        )
                    # 如果仍然为空，尝试从最新交易日期推算
                    if "最新交易日期" in df_integrated.columns:

                        def get_previous_date(current_date_str):
                            try:
                                current_date = pd.to_datetime(current_date_str)
                                # 减去一天作为上一交易日
                                previous_date = current_date - pd.Timedelta(days=1)
                                return previous_date.strftime("%Y-%m-%d")
                            except:
                                return current_date_str

                        df_integrated["上一交易日日期"] = df_integrated.apply(
                            lambda row: (
                                get_previous_date(row["最新交易日期"])
                                if pd.isna(row["上一交易日日期"])
                                or row["上一交易日日期"] == ""
                                else row["上一交易日日期"]
                            ),
                            axis=1,
                        )
                    print("已确保'上一交易日日期'列数据有效")

                # 上一交易日累计净值 - 从相关净值字段填充
                if "上一交易日累计净值" in df_integrated.columns:
                    # 首先尝试从previous_accumulated_nav填充
                    if "previous_accumulated_nav" in df_integrated.columns:
                        df_integrated["上一交易日累计净值"] = df_integrated.apply(
                            lambda row: (
                                row["previous_accumulated_nav"]
                                if pd.isna(row["上一交易日累计净值"])
                                or row["上一交易日累计净值"] == ""
                                else row["上一交易日累计净值"]
                            ),
                            axis=1,
                        )
                    # 如果仍然为空，尝试从最新累计净值填充
                    if "最新累计净值" in df_integrated.columns:
                        df_integrated["上一交易日累计净值"] = df_integrated.apply(
                            lambda row: (
                                row["最新累计净值"]
                                if pd.isna(row["上一交易日累计净值"])
                                or row["上一交易日累计净值"] == ""
                                else row["上一交易日累计净值"]
                            ),
                            axis=1,
                        )
                    print("已确保'上一交易日累计净值'列数据有效")

                # 基金经理 - 确保不为空
                if "基金经理" in df_integrated.columns:
                    # 为基金经理为空的基金设置默认值
                    df_integrated["基金经理"] = df_integrated["基金经理"].apply(
                        lambda x: "未知" if pd.isna(x) or x == "" or x == "---" else x
                    )
                    print("已确保'基金经理'列数据有效")

                # 数据更新时间 - 从更新时间填充
                if (
                    "更新时间" in df_integrated.columns
                    and "数据更新时间" in df_integrated.columns
                ):
                    df_integrated["数据更新时间"] = df_integrated.apply(
                        lambda row: (
                            row["更新时间"]
                            if pd.isna(row["数据更新时间"]) or row["数据更新时间"] == ""
                            else row["数据更新时间"]
                        ),
                        axis=1,
                    )

                # 日增长值和增长值互相同步
                if (
                    "日增长值" in df_integrated.columns
                    and "增长值" in df_integrated.columns
                ):
                    df_integrated["日增长值"] = df_integrated.apply(
                        lambda row: (
                            row["增长值"]
                            if pd.isna(row["日增长值"]) or row["日增长值"] == ""
                            else row["日增长值"]
                        ),
                        axis=1,
                    )
                    df_integrated["增长值"] = df_integrated.apply(
                        lambda row: (
                            row["日增长值"]
                            if pd.isna(row["增长值"]) or row["增长值"] == ""
                            else row["增长值"]
                        ),
                        axis=1,
                    )

                # 日增长率和增长率互相同步
                if (
                    "日增长率" in df_integrated.columns
                    and "增长率" in df_integrated.columns
                ):
                    df_integrated["日增长率"] = df_integrated.apply(
                        lambda row: (
                            row["增长率"]
                            if pd.isna(row["日增长率"]) or row["日增长率"] == ""
                            else row["日增长率"]
                        ),
                        axis=1,
                    )
                    df_integrated["增长率"] = df_integrated.apply(
                        lambda row: (
                            row["日增长率"]
                            if pd.isna(row["增长率"]) or row["增长率"] == ""
                            else row["增长率"]
                        ),
                        axis=1,
                    )

                # 确保数据获取日期不为空
                if "数据获取日期" in df_integrated.columns:
                    current_date = datetime.now().strftime("%Y-%m-%d")
                    df_integrated["数据获取日期"] = df_integrated["数据获取日期"].apply(
                        lambda x: current_date if pd.isna(x) or x == "" else x
                    )

                # 检查并确保所有必要列存在
                missing_columns = [
                    col
                    for col in self.ALL_REQUIRED_COLUMNS
                    if col not in df_integrated.columns
                ]
                if missing_columns:
                    for column in missing_columns:
                        df_integrated[column] = ""  # 设置默认空值
                    print(
                        f"已确保所有必要列存在，补充了 {len(missing_columns)} 个缺失列"
                    )
                else:
                    print("所有必要列已在初始化时创建完成")

                # 打印关键列的填充情况
                filled_columns = [
                    col
                    for col in key_columns
                    if col in df_integrated.columns
                    and (
                        df_integrated[col].notna().sum() > 0
                        or (df_integrated[col] != "").sum() > 0
                    )
                ]
                if filled_columns:
                    print(f"已确保关键列数据正常填充: {filled_columns}")

                # 按基金代码排序
                df_integrated = df_integrated.sort_values(by="基金代码")

                # 尝试加载列顺序配置
                columns_config_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "data",
                    "columns_config.json",
                )
                if os.path.exists(columns_config_path):
                    try:
                        with open(columns_config_path, "r", encoding="utf-8") as f:
                            config = json.load(f)
                            if "columns_order" in config:
                                # 确保所有配置的列都存在于DataFrame中
                                ordered_columns = []
                                for col in config["columns_order"]:
                                    if col in df_integrated.columns:
                                        ordered_columns.append(col)
                                # 添加剩余的列
                                for col in df_integrated.columns:
                                    if col not in ordered_columns:
                                        ordered_columns.append(col)
                                # 重新排序列
                                df_integrated = df_integrated[ordered_columns]
                                print(f"已应用保存的列顺序配置")
                    except Exception as e:
                        print(f"加载列顺序配置时出错: {str(e)}")

                # 更新统计信息
                stats_info["数据列总数"] = len(df_integrated.columns)
            else:
                df_integrated = pd.DataFrame()

            # 8. 创建ExcelWriter对象并写入数据
            with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
                # 在写入Excel前最后一次检查并删除不需要的列
                if not df_integrated.empty:
                    columns_to_delete_final_excel = ["基金名称", "成交额", "成交量", "前收盘价"]
                    for col in columns_to_delete_final_excel:
                        if col in df_integrated.columns:
                            df_integrated.drop(col, axis=1, inplace=True)
                            print(f"Excel写入前: 已删除列: {col}")

                    # 写入整合后的数据到单个工作表
                    df_integrated.to_excel(
                        writer, sheet_name="整合基金数据", index=False
                    )
                    print(
                        f"已写入整合基金数据: {len(df_integrated)}条记录，{len(df_integrated.columns)}列数据"
                    )
                else:
                    # 创建一个空的DataFrame作为占位符
                    pd.DataFrame(columns=["基金代码", "基金简称", "基金类型"]).to_excel(
                        writer, sheet_name="整合基金数据", index=False
                    )
                    print("没有找到可整合的基金数据")

                # 添加报表统计信息
                try:
                    df_stats = pd.DataFrame(
                        list(stats_info.items()), columns=["统计项", "值"]
                    )
                    df_stats.to_excel(writer, sheet_name="报表信息", index=False)
                except Exception as e:
                    print(f"添加报表统计信息时出错: {str(e)}")

                # 添加数据说明工作表
                try:
                    instructions = {
                        "数据类别": [
                            "基金基本信息",
                            "场内交易基金数据",
                            "货币基金数据",
                            "货币基金排名数据",
                            "开放基金排名数据",
                        ],
                        "主要内容": [
                            "包含基金的基本信息和最新申购状态",
                            "场内交易基金的详细数据（原财经网基金数据）",
                            "货币基金的收益和7日年化数据",
                            "货币基金的各类年化收益率数据",
                            "开放基金的最新净值和增长率数据",
                        ],
                    }
                    df_instructions = pd.DataFrame(instructions)
                    df_instructions.to_excel(writer, sheet_name="数据说明", index=False)
                except Exception as e:
                    print(f"添加数据说明时出错: {str(e)}")

            # 保存当前列顺序配置
            try:
                # 在保存列顺序配置前再次检查并删除不需要的列
                columns_to_delete_config = ["基金名称", "成交额", "成交量", "前收盘价"]
                for col in columns_to_delete_config:
                    if col in df_integrated.columns:
                        df_integrated.drop(col, axis=1, inplace=True)
                        print(f"配置保存前: 已删除列: {col}")

                columns_config_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "data",
                    "columns_config.json",
                )
                os.makedirs(os.path.dirname(columns_config_path), exist_ok=True)
                config = {"columns_order": list(df_integrated.columns)}
                with open(columns_config_path, "w", encoding="utf-8") as f:
                    json.dump(config, f, ensure_ascii=False, indent=2)
                print(f"已保存列顺序配置: {columns_config_path}")
            except Exception as e:
                print(f"保存列顺序配置时出错: {str(e)}")

            print(f"量化分析报表已成功生成: {report_path}")
            return report_path
        except ImportError as e:
            print(f"缺少必要的库: {str(e)}")
            print("请安装pandas和openpyxl库: pip install pandas openpyxl")
            return None
        except Exception as e:
            print(f"生成Excel报表时发生错误: {str(e)}")
            return None


# 如果直接运行此模块，则生成报表
def main():
    print("=== 生成量化分析Excel报表 ===")
    print("正在生成量化分析报表...")
    try:
        generator = ExcelReportGenerator()
        report_path = generator.generate_excel_report()
        if report_path:
            print(f"量化分析报表已生成: {report_path}")
        else:
            print("报表生成失败")
    except Exception as e:
        print(f"生成报表过程发生错误: {str(e)}")


if __name__ == "__main__":
    main()
