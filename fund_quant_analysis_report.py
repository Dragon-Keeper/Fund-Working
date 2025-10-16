#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
基金量化分析报表生成器
用于生成包含基金基本信息、上涨比例等指标的Excel报表
"""

import os
import sys
import time
import datetime
import pandas as pd
import numpy as np
import h5py
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# 确保中文显示正常
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

class FundQuantAnalysisReport:
    """基金量化分析报表生成器"""
    
    def __init__(self):
        """初始化"""
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(self.base_dir, "data")
        
        # 确保数据目录存在
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        
        # 文件路径
        self.fund_purchase_status_path = os.path.join(self.data_dir, "Fund_Purchase_Status.h5")
        self.fund_fundamental_overview_path = os.path.join(self.data_dir, "Fund_Fundamental_Overview.h5")
        self.all_fund_data_path = os.path.join(self.data_dir, "All_Fund_Data.h5")
        
        # 检查必要文件是否存在
        self._check_required_files()
        
        # 结果数据
        self.report_data = []
    
    def _check_required_files(self):
        """检查必要文件是否存在"""
        missing_files = []
        
        if not os.path.exists(self.fund_purchase_status_path):
            missing_files.append(self.fund_purchase_status_path)
        
        if not os.path.exists(self.fund_fundamental_overview_path):
            missing_files.append(self.fund_fundamental_overview_path)
        
        if not os.path.exists(self.all_fund_data_path):
            missing_files.append(self.all_fund_data_path)
        
        if missing_files:
            print("错误：缺少必要的数据文件：")
            for file in missing_files:
                print(f"  - {file}")
            print("请先运行相关的数据下载脚本获取数据。")
            # 不直接退出，允许程序继续运行，后续处理会跳过缺少数据的基金
    
    def _get_fund_purchase_status_data(self):
        """获取基金申购状态数据"""
        try:
            with pd.HDFStore(self.fund_purchase_status_path, mode='r') as store:
                if 'fund_purchase_status' in store:
                    return store['fund_purchase_status']
                else:
                    print(f"警告：{self.fund_purchase_status_path} 中没有 fund_purchase_status 键")
                    return pd.DataFrame()
        except Exception as e:
            print(f"读取基金申购状态数据时出错: {e}")
            return pd.DataFrame()
    
    def _get_fund_fundamental_overview_data(self):
        """获取基金基本面概况数据"""
        try:
            with pd.HDFStore(self.fund_fundamental_overview_path, mode='r') as store:
                if 'fund_fundamental_overview' in store:
                    return store['fund_fundamental_overview']
                else:
                    print(f"警告：{self.fund_fundamental_overview_path} 中没有 fund_fundamental_overview 键")
                    return pd.DataFrame()
        except Exception as e:
            print(f"读取基金基本面概况数据时出错: {e}")
            return pd.DataFrame()
    
    def _get_fund_nav_data(self, fund_code, start_date, end_date, hf=None):
        """获取基金净值数据
        
        Args:
            fund_code: 基金代码
            start_date: 开始日期
            end_date: 结束日期
            hf: 已打开的HDF5文件句柄，如果为None则新建
        """
        try:
            # 如果提供了文件句柄，直接使用
            if hf is not None:
                if fund_code not in hf:
                    return None
                
                group = hf[fund_code]
                
                # 读取数据并转换为DataFrame
                dates = [d.decode('utf-8') if isinstance(d, bytes) else str(d) for d in group['date'][()]]
                
                df = pd.DataFrame({
                    'date': pd.to_datetime(dates),
                    'open': group['open'][()],
                    'high': group['high'][()],
                    'low': group['low'][()],
                    'close': group['close'][()],
                    'amount': group['amount'][()],
                    'volume': group['volume'][()],
                    'prev_close': group['prev_close'][()]
                })
                
                # 按日期排序
                df = df.sort_values('date')
                
                # 筛选日期范围
                mask = (df['date'] >= start_date) & (df['date'] <= end_date)
                df_filtered = df[mask].copy()
                
                if df_filtered.empty:
                    return None
                
                return df_filtered
            else:
                # 如果没有提供文件句柄，使用传统方式
                with h5py.File(self.all_fund_data_path, 'r') as hf_new:
                    if fund_code not in hf_new:
                        return None
                    
                    group = hf_new[fund_code]
                    
                    # 读取数据并转换为DataFrame
                    dates = [d.decode('utf-8') if isinstance(d, bytes) else str(d) for d in group['date'][()]]
                    
                    df = pd.DataFrame({
                        'date': pd.to_datetime(dates),
                        'open': group['open'][()],
                        'high': group['high'][()],
                        'low': group['low'][()],
                        'close': group['close'][()],
                        'amount': group['amount'][()],
                        'volume': group['volume'][()],
                        'prev_close': group['prev_close'][()]
                    })
                    
                    # 按日期排序
                    df = df.sort_values('date')
                    
                    # 筛选日期范围
                    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
                    df_filtered = df[mask].copy()
                    
                    if df_filtered.empty:
                        return None
                    
                    return df_filtered
        except Exception as e:
            # 静默处理错误，避免过多输出
            # print(f"读取基金 {fund_code} 净值数据时出错: {e}")
            return None
    
    def _calculate_positive_quarters_ratio(self, df):
        """计算上涨季度比例"""
        if len(df) < 2:
            return None
        
        try:
            # 按季分组并计算季收益率（使用新的参数名）
            df_quarterly = df.set_index("date").resample("QE").last()
            df_quarterly["quarterly_return"] = df_quarterly["close"].pct_change(fill_method=None)
            
            # 计算季收益率为正的季度数
            if len(df_quarterly) < 2:
                return None
            
            positive_quarters = (df_quarterly["quarterly_return"] > 0).sum()
            
            return round((positive_quarters / (len(df_quarterly) - 1)) * 100, 2)
        except Exception as e:
            print(f"计算上涨季度比例时出错: {e}")
            return None
    
    def _calculate_positive_months_ratio(self, df):
        """计算上涨月份比例"""
        if len(df) < 2:
            return None
        
        try:
            # 按月分组并计算月收益率（使用新的参数名）
            df_monthly = df.set_index("date").resample("ME").last()
            df_monthly["monthly_return"] = df_monthly["close"].pct_change(fill_method=None)
            
            # 计算月收益率为正的月份数
            if len(df_monthly) < 2:
                return None
            
            positive_months = (df_monthly["monthly_return"] > 0).sum()
            
            return round((positive_months / (len(df_monthly) - 1)) * 100, 2)
        except Exception as e:
            print(f"计算上涨月份比例时出错: {e}")
            return None
    
    def _calculate_positive_weeks_ratio(self, df):
        """计算上涨星期比例"""
        if len(df) < 2:
            return None
        
        try:
            # 按周分组并计算周收益率（添加fill_method参数）
            df_weekly = df.set_index("date").resample("W").last()
            df_weekly["weekly_return"] = df_weekly["close"].pct_change(fill_method=None)
            
            # 计算周收益率为正的周数
            if len(df_weekly) < 2:
                return None
            
            positive_weeks = (df_weekly["weekly_return"] > 0).sum()
            
            return round((positive_weeks / (len(df_weekly) - 1)) * 100, 2)
        except Exception as e:
            print(f"计算上涨星期比例时出错: {e}")
            return None
    
    def _get_establishment_years(self, establishment_date):
        """计算成立年数"""
        try:
            if isinstance(establishment_date, str):
                # 尝试不同的日期格式
                date_formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y年%m月%d日']
                parsed_date = None
                
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.datetime.strptime(establishment_date, fmt)
                        break
                    except ValueError:
                        continue
                
                if parsed_date is None:
                    return None
                    
                today = datetime.datetime.now()
                years = (today - parsed_date).days / 365.25
                return round(years, 2)
            elif isinstance(establishment_date, datetime.datetime):
                today = datetime.datetime.now()
                years = (today - establishment_date).days / 365.25
                return round(years, 2)
            else:
                return None
        except Exception as e:
            print(f"计算成立年数时出错: {e}")
            return None
    
    def _map_fund_purchase_info(self, purchase_status, redemption_status, next_open_date, min_purchase, daily_limit, fee):
        """映射基金买卖信息"""
        info_parts = []
        
        # 申购状态
        if purchase_status:
            info_parts.append(f"申购: {purchase_status}")
        
        # 赎回状态
        if redemption_status:
            info_parts.append(f"赎回: {redemption_status}")
        
        # 下一开放日
        if next_open_date and next_open_date != "" and next_open_date != "---":
            info_parts.append(f"下开: {next_open_date}")
        
        # 购买起点
        if min_purchase and min_purchase != "" and min_purchase != "---":
            info_parts.append(f"起购: {min_purchase}")
        
        # 日累计限定金额
        if daily_limit and daily_limit != "" and daily_limit != "---":
            info_parts.append(f"日限: {daily_limit}")
        
        # 手续费
        if fee and fee != "" and fee != "---":
            info_parts.append(f"手续费: {fee}")
        
        return " | ".join(info_parts) if info_parts else "无"
    
    def _map_closed_type(self, purchase_status, redemption_status, next_open_date):
        """映射封闭类型"""
        if purchase_status == "开放申购" and redemption_status == "开放赎回":
            return "开放式"
        elif purchase_status == "暂停申购" and redemption_status == "开放赎回":
            return "暂停申购"
        elif purchase_status == "开放申购" and redemption_status == "暂停赎回":
            return "暂停赎回"
        elif purchase_status == "暂停申购" and redemption_status == "暂停赎回":
            return "暂停申赎"
        elif next_open_date and next_open_date != "" and next_open_date != "---":
            return "定期开放"
        else:
            return "未知"
    
    def _map_closed_length(self, next_open_date):
        """映射封闭长度"""
        try:
            # 增加对非字符串类型的检查
            if not isinstance(next_open_date, str):
                return "未知"
                
            if next_open_date and next_open_date != "" and next_open_date != "---":
                # 尝试解析日期
                date_formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y年%m月%d日']
                parsed_date = None
                
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.datetime.strptime(next_open_date, fmt)
                        break
                    except ValueError:
                        continue
                
                if parsed_date:
                    today = datetime.datetime.now()
                    days = (parsed_date - today).days
                    if days > 0:
                        return f"{days}天"
                    else:
                        return "即将开放"
                else:
                    return next_open_date
            else:
                return "未知"
        except Exception as e:
            # 静默处理错误，避免过多输出
            # print(f"计算封闭长度时出错: {e}")
            return "未知"
    
    def _map_fund_status(self, purchase_status, redemption_status):
        """映射基金状态"""
        if purchase_status == "开放申购" and redemption_status == "开放赎回":
            return "正常"
        elif purchase_status == "暂停申购" or redemption_status == "暂停赎回":
            return "限制交易"
        else:
            return "未知"
    
    def _process_single_fund(self, fund_code, fund_purchase_df, fund_fundamental_df, start_date, end_date, hf=None):
        """处理单个基金数据"""
        try:
            # 获取基金申购状态数据
            purchase_row = fund_purchase_df[fund_purchase_df['基金代码'] == fund_code]
            if purchase_row.empty:
                return None
            
            purchase_data = purchase_row.iloc[0]
            
            # 获取基金基本面数据
            fundamental_row = fund_fundamental_df[fund_fundamental_df['基金代码'] == fund_code]
            
            # 获取基金净值数据
            nav_data = self._get_fund_nav_data(fund_code, start_date, end_date, hf)
            
            # 计算上涨比例指标
            positive_quarters_ratio = None
            positive_months_ratio = None
            positive_weeks_ratio = None
            
            if nav_data is not None:
                positive_quarters_ratio = self._calculate_positive_quarters_ratio(nav_data)
                positive_months_ratio = self._calculate_positive_months_ratio(nav_data)
                positive_weeks_ratio = self._calculate_positive_weeks_ratio(nav_data)
            
            # 构建基金数据字典
            fund_info = {
                '基金代码': fund_code,
                '基金简称': purchase_data.get('基金简称', ''),
                '基金类型': purchase_data.get('基金类型', ''),
                '期初日期': start_date.strftime('%Y-%m-%d'),
                '期末日期': end_date.strftime('%Y-%m-%d'),
                '基金规模': None,
                '成立年数': None,
                '上涨季度比例': positive_quarters_ratio,
                '上涨月份比例': positive_months_ratio,
                '上涨星期比例': positive_weeks_ratio,
                '封闭类型': self._map_closed_type(
                    purchase_data.get('申购状态', ''),
                    purchase_data.get('赎回状态', ''),
                    purchase_data.get('下一开放日', '')
                ),
                '封闭长度': self._map_closed_length(purchase_data.get('下一开放日', '')),
                '状态': self._map_fund_status(
                    purchase_data.get('申购状态', ''),
                    purchase_data.get('赎回状态', '')
                ),
                '基金买卖信息': self._map_fund_purchase_info(
                    purchase_data.get('申购状态', ''),
                    purchase_data.get('赎回状态', ''),
                    purchase_data.get('下一开放日', ''),
                    purchase_data.get('购买起点', ''),
                    purchase_data.get('日累计限定金额', ''),
                    purchase_data.get('手续费', '')
                ),
                '基金经理人': None,
                '成立来分红': None,
                '最近更新日期': None
            }
            
            # 补充基本面数据
            if not fundamental_row.empty:
                fundamental_data = fundamental_row.iloc[0]
                fund_info['基金规模'] = fundamental_data.get('资产规模', '')
                fund_info['基金经理人'] = fundamental_data.get('基金经理人', '')
                fund_info['成立来分红'] = fundamental_data.get('成立来分红', '')
                fund_info['最近更新日期'] = fundamental_data.get('数据日期', '')
                
                # 计算成立年数
                establishment_date = fundamental_data.get('成立日期', '')
                if establishment_date:
                    fund_info['成立年数'] = self._get_establishment_years(establishment_date)
            
            return fund_info
            
        except Exception as e:
            print(f"处理基金 {fund_code} 时出错: {e}")
            return None
    
    def generate_report(self, start_date=None, end_date=None, output_file=None, max_workers=4):
        """生成量化分析报表
        
        Args:
            start_date: 期初日期，默认为1年前
            end_date: 期末日期，默认为今天
            output_file: 输出文件名，默认为当前日期
            max_workers: 最大工作线程数
        """
        # 设置日期
        if end_date is None:
            end_date = datetime.datetime.now()
        elif isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        
        if start_date is None:
            start_date = end_date - datetime.timedelta(days=365)
        elif isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        
        print(f"\n=== 开始生成基金量化分析报表 ===")
        print(f"分析期间: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
        
        # 读取数据
        print("\n读取基金数据...")
        fund_purchase_df = self._get_fund_purchase_status_data()
        if fund_purchase_df.empty:
            print("错误：无法读取基金申购状态数据")
            return False
        
        fund_fundamental_df = self._get_fund_fundamental_overview_data()
        
        # 获取所有基金代码
        fund_codes = fund_purchase_df['基金代码'].unique()
        print(f"共找到 {len(fund_codes)} 只基金")
        
        # 检查All_Fund_Data.h5文件是否存在
        if not os.path.exists(self.all_fund_data_path):
            print(f"警告：{self.all_fund_data_path} 文件不存在，将无法计算上涨比例")
            hf = None
        else:
            # 优化：一次性打开HDF5文件
            try:
                hf = h5py.File(self.all_fund_data_path, 'r')
                print("已打开All_Fund_Data.h5文件用于批量读取")
            except Exception as e:
                print(f"打开All_Fund_Data.h5文件时出错: {e}")
                hf = None
        
        # 多线程处理基金数据
        print("\n处理基金数据...")
        self.report_data = []
        
        # 限制处理的基金数量，用于测试
        # 生产环境可以注释掉这一行
        fund_codes = fund_codes[:100]  # 仅处理前100只基金用于测试
        
        with ThreadPoolExecutor(max_workers=min(max_workers, 2)) as executor:  # 减少线程数避免过多的IO操作
            # 创建任务
            future_to_fund = {
                executor.submit(
                    self._process_single_fund, 
                    fund_code, 
                    fund_purchase_df, 
                    fund_fundamental_df, 
                    start_date, 
                    end_date,
                    hf  # 传递打开的HDF5文件句柄
                ): fund_code for fund_code in fund_codes
            }
            
            # 进度条
            with tqdm(total=len(fund_codes)) as pbar:
                for future in as_completed(future_to_fund):
                    fund_code = future_to_fund[future]
                    try:
                        result = future.result()
                        if result:
                            self.report_data.append(result)
                    except Exception as e:
                        print(f"处理基金 {fund_code} 时发生异常: {e}")
                    finally:
                        pbar.update(1)
        
        # 关闭HDF5文件
        if hf is not None:
            try:
                hf.close()
                print("已关闭All_Fund_Data.h5文件")
            except:
                pass
        
        print(f"\n成功处理 {len(self.report_data)} 只基金的数据")
        
        # 生成Excel报表
        if not self.report_data:
            print("警告：没有可用的基金数据，可能是因为数据文件不存在或格式不正确")
            return False
        
        return self._save_to_excel(start_date, end_date, output_file)
    
    def _save_to_excel(self, start_date, end_date, output_file=None):
        """保存数据到Excel文件"""
        try:
            # 创建DataFrame
            df = pd.DataFrame(self.report_data)
            
            # 设置列顺序
            columns_order = [
                '基金代码', '基金简称', '基金类型', '期初日期', '期末日期',
                '基金规模', '成立年数', '上涨季度比例', '上涨月份比例', '上涨星期比例',
                '封闭类型', '封闭长度', '状态', '基金买卖信息', '基金经理人',
                '成立来分红', '最近更新日期'
            ]
            
            # 确保所有列都存在
            for col in columns_order:
                if col not in df.columns:
                    df[col] = None
            
            # 重新排序列
            df = df[columns_order]
            
            # 设置输出文件名
            if output_file is None:
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = os.path.join(
                    self.base_dir, 
                    'reports', 
                    f'基金量化分析报表_{timestamp}.xlsx'
                )
            
            # 确保输出目录存在
            output_dir = os.path.dirname(output_file)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # 保存到Excel
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='基金量化分析', index=False)
                
                # 设置列宽
                worksheet = writer.sheets['基金量化分析']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            print(f"\n报表已成功生成: {output_file}")
            return True
            
        except Exception as e:
            print(f"保存Excel报表时出错: {e}")
            return False

    def interactive_menu(self):
        """交互式菜单"""
        print("\n=== 基金量化分析报表生成器 ===")
        
        # 获取用户输入的日期
        start_date_str = input("请输入期初日期 (YYYY-MM-DD，默认为1年前): ").strip()
        end_date_str = input("请输入期末日期 (YYYY-MM-DD，默认为今天): ").strip()
        
        # 验证日期格式
        start_date = None
        end_date = None
        
        if start_date_str:
            try:
                start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
            except ValueError:
                print("警告：期初日期格式错误，将使用默认值")
        
        if end_date_str:
            try:
                end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d')
            except ValueError:
                print("警告：期末日期格式错误，将使用默认值")
        
        # 生成报表
        self.generate_report(start_date=start_date, end_date=end_date)

if __name__ == "__main__":
    # 检查依赖
    try:
        import pandas as pd
        import numpy as np
        import h5py
        from tqdm import tqdm
    except ImportError as e:
        missing_package = str(e).split(" ")[-1]
        print(f"错误：缺少依赖包 {missing_package}")
        print("请运行以下命令安装依赖：")
        print("pip install pandas numpy h5py tqdm openpyxl")
        sys.exit(1)
    
    # 创建分析器实例并运行
    analyzer = FundQuantAnalysisReport()
    
    # 解析命令行参数
    import argparse
    parser = argparse.ArgumentParser(description='基金量化分析报表生成器')
    parser.add_argument('--start-date', type=str, help='期初日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='期末日期 (YYYY-MM-DD)')
    parser.add_argument('--output', type=str, help='输出文件名')
    parser.add_argument('--non-interactive', action='store_true', help='非交互式模式')
    args = parser.parse_args()
    
    if args.non_interactive:
        # 非交互式模式，直接生成报表
        print("使用非交互式模式生成报表...")
        analyzer.generate_report(
            start_date=args.start_date,
            end_date=args.end_date,
            output_file=args.output
        )
    else:
        # 交互式模式
        analyzer.interactive_menu()