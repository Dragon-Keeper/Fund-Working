import pandas as pd
import os
import h5py
from datetime import datetime
import Fund_Purchase_Status_Manager

class ExcelReportGenerator:
    """Excel报表生成器，用于生成基金量化分析Excel报表"""
    
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

                    # 将数据存入主字典
                    for _, row in df_fund_status.iterrows():
                        fund_code = row.get("基金代码", "")
                        if fund_code:
                            if fund_code not in fund_dict:
                                fund_dict[fund_code] = {}
                            for col in df_fund_status.columns:
                                fund_dict[fund_code][col] = row[col]
            except Exception as e:
                print(f"获取基金基本信息时出错: {str(e)}")

            # 2. 读取财经网基金数据
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
                                        if "基金简称" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["基金简称"] = decoded_value  # 使用统一的中文表头
                                    elif key == "fund_type":
                                        if "基金类型" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["基金类型"] = decoded_value
                                    elif key == "unit_nav":
                                        if "最新单位净值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["最新单位净值"] = decoded_value
                                    elif key == "accumulated_nav":
                                        if "最新累计净值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["最新累计净值"] = decoded_value
                                    elif key == "prev_unit_nav":
                                        if "上一交易日单位净值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["上一交易日单位净值"] = decoded_value
                                    elif key == "prev_accumulated_nav":
                                        if "上一交易日累计净值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["上一交易日累计净值"] = decoded_value
                                    elif key == "growth_value":
                                        if "日增长值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["日增长值"] = decoded_value
                                    elif key == "growth_rate":
                                        if "日增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["日增长率"] = decoded_value
                                    elif key == "market_price":
                                        if "市价" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["市价"] = decoded_value
                                    elif key == "discount_rate":
                                        if "折价率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["折价率"] = decoded_value
                                    elif key == "fetch_time":
                                        if "数据获取时间" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据获取时间"] = decoded_value

                            print(f"已读取财经网基金数据: {count}条")
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
                                        if "基金名称" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["基金名称"] = decoded_value
                                    elif key == "latest_10k_profit":
                                        if "最新万份收益" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["最新万份收益"] = decoded_value
                                    elif key == "latest_7day_annual":
                                        if "最新7日年化%" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["最新7日年化%"] = decoded_value
                                    elif key == "establishment_date":
                                        if "成立日期" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["成立日期"] = decoded_value
                                    elif key == "fund_manager":
                                        if "基金经理" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["基金经理"] = decoded_value
                                    elif key == "fee_rate":
                                        if "手续费" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["手续费"] = decoded_value
                                    elif key == "update_date":
                                        if "数据更新日期" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据更新日期"] = decoded_value
                                    elif key == "fetch_time":
                                        if "数据获取时间" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据获取时间"] = decoded_value

                            print(f"已读取货币基金数据: {count}条")
            except Exception as e:
                print(f"获取货币基金数据时出错: {str(e)}")

            # 4. 读取场内交易基金排名数据
            try:
                fbs_file = os.path.join(data_dir, "FBS_Fund_Ranking_Data.h5")
                if os.path.exists(fbs_file):
                    with h5py.File(fbs_file, "r") as f:
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
                                        if "基金简称" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["基金简称"] = decoded_value
                                    elif key == "fund_type":
                                        if "基金类型" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["基金类型"] = decoded_value
                                    elif key == "data_date":
                                        if "数据日期" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据日期"] = decoded_value
                                    elif key == "unit_nav":
                                        if "最新单位净值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["最新单位净值"] = decoded_value
                                    elif key == "accumulated_nav":
                                        if "最新累计净值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["最新累计净值"] = decoded_value
                                    elif key == "week_growth":
                                        if "近1周增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近1周增长率"] = decoded_value
                                    elif key == "month_growth":
                                        if "近1月增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近1月增长率"] = decoded_value
                                    elif key == "quarter_growth":
                                        if "近3月增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近3月增长率"] = decoded_value
                                    elif key == "half_year_growth":
                                        if "近6月增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近6月增长率"] = decoded_value
                                    elif key == "year_growth":
                                        if "近1年增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近1年增长率"] = decoded_value
                                    elif key == "two_year_growth":
                                        if "近2年增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近2年增长率"] = decoded_value
                                    elif key == "three_year_growth":
                                        if "近3年增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近3年增长率"] = decoded_value
                                    elif key == "year_to_date_growth":
                                        if "今年来增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["今年来增长率"] = decoded_value
                                    elif key == "since_establishment_growth":
                                        if "成立来增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["成立来增长率"] = decoded_value
                                    elif key == "establishment_date":
                                        if "成立日期" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["成立日期"] = decoded_value
                                    elif key == "fetch_time":
                                        if "数据获取时间" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据获取时间"] = decoded_value

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
                                        if "基金简称" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["基金简称"] = decoded_value
                                    elif key == "data_date":
                                        if "数据日期" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据日期"] = decoded_value
                                    elif key == "per_10k_return":
                                        if "最新万份收益" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["最新万份收益"] = decoded_value
                                    elif key == "seven_day_annualized":
                                        if "最新7日年化%" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["最新7日年化%"] = decoded_value
                                    elif key == "fourteen_day_annualized":
                                        if "14日年化收益率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["14日年化收益率"] = decoded_value
                                    elif key == "twenty_eight_day_annualized":
                                        if "28日年化收益率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["28日年化收益率"] = decoded_value
                                    elif key == "net_value":
                                        if "基金净值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["基金净值"] = decoded_value
                                    elif key == "month_growth":
                                        if "近1月增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近1月增长率"] = decoded_value
                                    elif key == "quarter_growth":
                                        if "近3月增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近3月增长率"] = decoded_value
                                    elif key == "half_year_growth":
                                        if "近6月增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近6月增长率"] = decoded_value
                                    elif key == "year_growth":
                                        if "近1年增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近1年增长率"] = decoded_value
                                    elif key == "two_year_growth":
                                        if "近2年增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近2年增长率"] = decoded_value
                                    elif key == "three_year_growth":
                                        if "近3年增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近3年增长率"] = decoded_value
                                    elif key == "five_year_growth":
                                        if "近5年增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近5年增长率"] = decoded_value
                                    elif key == "year_to_date_growth":
                                        if "今年来增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["今年来增长率"] = decoded_value
                                    elif key == "since_establishment_growth":
                                        if "成立来增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["成立来增长率"] = decoded_value
                                    elif key == "fee":
                                        if "手续费" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["手续费"] = decoded_value
                                    elif key == "fetch_time":
                                        if "数据获取时间" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据获取时间"] = decoded_value

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
                                        if "基金名称" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["基金名称"] = decoded_value
                                    elif key == "growth_value":
                                        if "日增长值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["日增长值"] = decoded_value
                                    elif key == "update_time":
                                        if "数据更新时间" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据更新时间"] = decoded_value
                                    elif key == "prev_trading_date":
                                        if "上一交易日日期" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["上一交易日日期"] = decoded_value
                                    elif key == "fund_manager":
                                        if "基金经理" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["基金经理"] = decoded_value
                                    elif key == "actual_fee_rate":
                                        if "实际手续费率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["实际手续费率"] = decoded_value
                                    elif key == "original_fee_rate":
                                        if "原始手续费率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["原始手续费率"] = decoded_value
                                    elif key == "fetch_date":
                                        if "数据获取日期" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据获取日期"] = decoded_value
                                    elif key == "prev_accumulated_nav":
                                        if "上一交易日累计净值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["上一交易日累计净值"] = decoded_value
                                    elif key == "latest_trading_date":
                                        if "最新交易日期" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["最新交易日期"] = decoded_value
                                    elif key == "prev_unit_nav":
                                        if "上一交易日单位净值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["上一交易日单位净值"] = decoded_value
                                    elif key == "fetch_time":
                                        if "数据获取时间" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据获取时间"] = decoded_value
                                    elif key == "data_date":
                                        if "数据日期" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据日期"] = decoded_value

                            print(f"已读取Fetch_Fund_Data数据: {count}条")
            except Exception as e:
                print(f"获取Fetch_Fund_Data数据时出错: {str(e)}")

            # 7. 读取开放基金排名数据
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
                                        if "基金简称" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["基金简称"] = decoded_value
                                    elif key == "data_date":
                                        if "数据日期" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据日期"] = decoded_value
                                    elif key == "data_fetch_date":
                                        if "数据获取日期" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据获取日期"] = decoded_value
                                    elif key == "unit_nav":
                                        if "最新单位净值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["最新单位净值"] = decoded_value
                                    elif key == "accum_nav":
                                        if "最新累计净值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["最新累计净值"] = decoded_value
                                    elif key == "day_growth":
                                        if "日增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["日增长率"] = decoded_value
                                    elif key == "week_growth":
                                        if "近1周增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近1周增长率"] = decoded_value
                                    elif key == "month_growth":
                                        if "近1月增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近1月增长率"] = decoded_value
                                    elif key == "quarter_growth":
                                        if "近3月增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近3月增长率"] = decoded_value
                                    elif key == "half_year_growth":
                                        if "近6月增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近6月增长率"] = decoded_value
                                    elif key == "year_growth":
                                        if "近1年增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近1年增长率"] = decoded_value
                                    elif key == "two_year_growth":
                                        if "近2年增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近2年增长率"] = decoded_value
                                    elif key == "three_year_growth":
                                        if "近3年增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["近3年增长率"] = decoded_value
                                    elif key == "year_to_date_growth":
                                        if "今年来增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["今年来增长率"] = decoded_value
                                    elif key == "since_establishment_growth":
                                        if "成立来增长率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["成立来增长率"] = decoded_value
                                    elif key == "fetch_time":
                                        if "数据获取时间" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据获取时间"] = decoded_value
                                    # 添加最新交易日期和上一交易日日期的映射
                                    elif key == "latest_trading_date":
                                        if "最新交易日期" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["最新交易日期"] = decoded_value
                                    elif key == "prev_trading_date":
                                        if "上一交易日日期" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["上一交易日日期"] = decoded_value
                                    # 添加实际手续费率和原始手续费率的映射
                                    elif key == "actual_fee_rate":
                                        if "实际手续费率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["实际手续费率"] = decoded_value
                                    elif key == "original_fee_rate":
                                        if "原始手续费率" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["原始手续费率"] = decoded_value
                                    # 添加数据更新时间的映射
                                    elif key == "update_time":
                                        if "数据更新时间" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["数据更新时间"] = decoded_value
                                    # 添加上一交易日单位净值和上一交易日累计净值的映射
                                    elif key == "prev_unit_nav":
                                        if "上一交易日单位净值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["上一交易日单位净值"] = decoded_value
                                    elif key == "prev_accum_nav":
                                        if "上一交易日累计净值" not in fund_dict[fund_code]:
                                            fund_dict[fund_code]["上一交易日累计净值"] = decoded_value

                            print(f"已读取开放基金排名数据: {count}条")
            except Exception as e:
                print(f"获取开放基金排名数据时出错: {str(e)}")

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

                # 任务1: 删除不需要的列
                columns_to_delete = [
                    "更新时间",  # 仅删除这个字段，其他字段保留
                ]

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

                # 任务3: 补充未显示的表头信息
                # 从各文件功能.txt中提取的所有可能的中文表头
                all_possible_columns = {
                    # Fund_Purchase_Status_Manager.py - 基金基本信息
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
                    # Fetch_Fund_Data.py - 开放式基金数据
                    "基金名称",
                    "最新单位净值",
                    "最新累计净值",
                    "上一交易日单位净值",
                    "上一交易日累计净值",
                    "日增长值",
                    "日增长率",
                    "实际手续费率",
                    "原始手续费率",
                    "最新交易日期",
                    "上一交易日日期",
                    "数据更新时间",
                    # fetch_cnjy_fund_data.py - 场内交易基金
                    "增长值",
                    "增长率",
                    "市价",
                    "折价率",
                    # fetch_currency_fund_data.py - 货币基金
                    "最新万份收益",
                    "最新7日年化%",
                    "成立日期",
                    "基金经理",
                    # fetch_fbs_fund_ranking.py - 场内交易基金排名
                    "近1周增长率",
                    "近1月增长率",
                    "近3月增长率",
                    "近6月增长率",
                    "近1年增长率",
                    "近2年增长率",
                    "近3年增长率",
                    "今年来增长率",
                    "成立来增长率",
                    # fetch_hbx_fund_ranking.py - 货币基金排名
                    "14日年化收益率",
                    "28日年化收益率",
                    "基金净值",
                    "近5年增长率",
                    # fetch_open_fund_ranking.py - 开放基金排名
                    "数据获取日期",
                    "日增长率",
                }

                # 检查并添加缺失的列
                for column in all_possible_columns:
                    if column not in df_integrated.columns:
                        # 添加空列
                        df_integrated[column] = ""
                        print(f"已添加缺失列: {column}")

                # 按基金代码排序
                df_integrated = df_integrated.sort_values(by="基金代码")

                # 更新统计信息
                stats_info["数据列总数"] = len(df_integrated.columns)
            else:
                df_integrated = pd.DataFrame()

            # 8. 创建ExcelWriter对象并写入数据
            with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
                # 写入整合后的数据到单个工作表
                if not df_integrated.empty:
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
                            "财经网基金数据",
                            "货币基金数据",
                            "场内交易基金排名数据",
                            "货币基金排名数据",
                            "开放基金排名数据",
                        ],
                        "主要内容": [
                            "包含基金的基本信息和最新申购状态",
                            "财经网平台提供的基金详细数据",
                            "货币基金的收益和7日年化数据",
                            "场内交易基金的各类增长率数据",
                            "货币基金的各类年化收益率数据",
                            "开放基金的最新净值和增长率数据",
                        ],
                    }
                    df_instructions = pd.DataFrame(instructions)
                    df_instructions.to_excel(writer, sheet_name="数据说明", index=False)
                except Exception as e:
                    print(f"添加数据说明时出错: {str(e)}")

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