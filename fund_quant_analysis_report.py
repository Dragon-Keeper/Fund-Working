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
import math

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
        
        # 测试限制
        self.TEST_LIMIT = None
        
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
                if start_date is not None and end_date is not None:
                    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
                    df_filtered = df[mask].copy()
                    
                    if df_filtered.empty:
                        return None
                    
                    return df_filtered
                else:
                    # 如果没有指定日期范围，返回完整数据
                    return df.copy()
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
                    if start_date is not None and end_date is not None:
                        mask = (df['date'] >= start_date) & (df['date'] <= end_date)
                        df_filtered = df[mask].copy()
                        
                        if df_filtered.empty:
                            return None
                        
                        return df_filtered
                    else:
                        # 如果没有指定日期范围，返回完整数据
                        return df.copy()
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
    
    def _calculate_quarterly_volatility(self, df):
        """计算季度涨跌幅标准差"""
        if len(df) < 2:
            return None
        
        try:
            df_quarterly = df.set_index("date").resample("QE").last()
            df_quarterly["quarterly_return"] = df_quarterly["close"].pct_change(fill_method=None)
            
            if len(df_quarterly) < 2:
                return None
            
            # 计算标准差并年化
            volatility = df_quarterly["quarterly_return"].std() * math.sqrt(4)
            return round(volatility * 100, 2)
        except Exception as e:
            print(f"计算季度涨跌幅标准差时出错: {e}")
            return None
    
    def _calculate_monthly_volatility(self, df):
        """计算月涨跌幅标准差"""
        if len(df) < 2:
            return None
        
        try:
            df_monthly = df.set_index("date").resample("ME").last()
            df_monthly["monthly_return"] = df_monthly["close"].pct_change(fill_method=None)
            
            if len(df_monthly) < 2:
                return None
            
            # 计算标准差并年化
            volatility = df_monthly["monthly_return"].std() * math.sqrt(12)
            return round(volatility * 100, 2)
        except Exception as e:
            print(f"计算月涨跌幅标准差时出错: {e}")
            return None
    
    def _calculate_weekly_volatility(self, df):
        """计算周涨跌幅标准差"""
        if len(df) < 2:
            return None
        
        try:
            df_weekly = df.set_index("date").resample("W").last()
            df_weekly["weekly_return"] = df_weekly["close"].pct_change(fill_method=None)
            
            if len(df_weekly) < 2:
                return None
            
            # 计算标准差并年化
            volatility = df_weekly["weekly_return"].std() * math.sqrt(52)
            return round(volatility * 100, 2)
        except Exception as e:
            print(f"计算周涨跌幅标准差时出错: {e}")
            return None
    
    def _calculate_annualized_return(self, df):
        """计算年化收益率"""
        if len(df) < 2:
            return None
        
        try:
            # 计算总收益率
            total_return = (df["close"].iloc[-1] / df["close"].iloc[0]) - 1
            
            # 计算持仓天数
            days = (df["date"].iloc[-1] - df["date"].iloc[0]).days
            
            if days == 0:
                return None
            
            # 计算年化收益率
            annualized_return = ((1 + total_return) ** (365.25 / days)) - 1
            return round(annualized_return * 100, 2)
        except Exception as e:
            print(f"计算年化收益率时出错: {e}")
            return None
    
    def _calculate_max_drawdown(self, df):
        """计算最大回撤率"""
        if len(df) < 2:
            return None
        
        try:
            # 计算累计最大值
            df["cumulative_max"] = df["close"].cummax()
            
            # 计算回撤
            df["drawdown"] = (df["close"] / df["cumulative_max"]) - 1
            
            # 最大回撤
            max_drawdown = df["drawdown"].min()
            return round(max_drawdown * 100, 2)
        except Exception as e:
            print(f"计算最大回撤率时出错: {e}")
            return None
    
    def _calculate_second_max_drawdown(self, df):
        """计算第二大回撤"""
        if len(df) < 2:
            return None
        
        try:
            # 计算累计最大值
            df["cumulative_max"] = df["close"].cummax()
            
            # 计算回撤
            df["drawdown"] = (df["close"] / df["cumulative_max"]) - 1
            
            # 找到最大回撤的位置
            max_drawdown_idx = df["drawdown"].idxmin()
            
            # 分割数据，避开最大回撤区域
            df_before = df.loc[:max_drawdown_idx - 1]
            df_after = df.loc[max_drawdown_idx + 1:]
            
            # 找出第二大回撤
            second_max_drawdown = None
            
            if not df_before.empty:
                second_max_drawdown = df_before["drawdown"].min()
            
            if not df_after.empty:
                after_drawdown = df_after["drawdown"].min()
                if second_max_drawdown is None or after_drawdown < second_max_drawdown:
                    second_max_drawdown = after_drawdown
            
            return round(second_max_drawdown * 100, 2) if second_max_drawdown is not None else None
        except Exception as e:
            print(f"计算第二大回撤时出错: {e}")
            return None
    
    def _calculate_sharpe_ratio(self, df, risk_free_rate=0.02):
        """计算夏普率"""
        if len(df) < 2:
            return None
        
        try:
            # 计算日收益率
            df["daily_return"] = df["close"].pct_change(fill_method=None)
            
            # 计算平均日收益率和标准差
            avg_daily_return = df["daily_return"].mean()
            daily_volatility = df["daily_return"].std()
            
            if daily_volatility == 0:
                return None
            
            # 计算年化夏普率
            sharpe_ratio = (avg_daily_return * 252 - risk_free_rate) / (daily_volatility * math.sqrt(252))
            return round(sharpe_ratio, 2)
        except Exception as e:
            print(f"计算夏普率时出错: {e}")
            return None
    
    def _calculate_calmar_ratio(self, df):
        """计算卡玛率"""
        if len(df) < 2:
            return None
        
        try:
            # 计算年化收益率
            annualized_return = self._calculate_annualized_return(df)
            if annualized_return is None:
                return None
            annualized_return = annualized_return / 100
            
            # 计算最大回撤
            max_drawdown = self._calculate_max_drawdown(df)
            if max_drawdown is None:
                return None
            max_drawdown = abs(max_drawdown / 100)
            
            if max_drawdown == 0:
                return None
            
            # 计算卡玛率
            calmar_ratio = annualized_return / max_drawdown
            return round(calmar_ratio, 2)
        except Exception as e:
            print(f"计算卡玛率时出错: {e}")
            return None
    
    def _calculate_ols_discrete_coefficient(self, df):
        """计算OLS离散系数"""
        if len(df) < 2:
            return None
        
        try:
            # 计算日收益率
            df["daily_return"] = df["close"].pct_change(fill_method=None).dropna()
            
            if len(df["daily_return"]) < 2:
                return None
            
            # 计算平均值
            mean_return = df["daily_return"].mean()
            
            # 计算标准差
            std_return = df["daily_return"].std()
            
            if mean_return == 0:
                return None
            
            # 计算离散系数
            discrete_coefficient = std_return / abs(mean_return)
            return round(discrete_coefficient, 2)
        except Exception as e:
            print(f"计算OLS离散系数时出错: {e}")
            return None
    
    def _calculate_period_returns(self, df, periods):
        """计算不同时间周期的涨跌幅
        
        Args:
            df: 基金净值数据
            periods: 时间周期列表，如['1M', '2M', '3M', '6M', '1Y', '9M', '12M']
        """
        results = {}
        
        try:
            for period in periods:
                # 获取相应的天数
                if period == '1W':
                    days = 7
                elif period == '1M':
                    days = 30
                elif period == '2M':
                    days = 60
                elif period == '3M':
                    days = 90
                elif period == '6M':
                    days = 180
                elif period == '9M':
                    days = 270
                elif period == '12M':
                    days = 360
                elif period == '1Y':
                    days = 365
                elif period == '2Y':
                    days = 730
                elif period == '3Y':
                    days = 1095
                else:
                    continue
                
                # 筛选时间范围内的数据
                end_date = df["date"].max()
                start_date = end_date - datetime.timedelta(days=days)
                
                period_df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
                
                if len(period_df) < 2:
                    results[period] = None
                    continue
                
                # 计算涨跌幅
                return_pct = (period_df["close"].iloc[-1] / period_df["close"].iloc[0] - 1) * 100
                results[period] = round(return_pct, 2)
        except Exception as e:
            print(f"计算周期涨跌幅时出错: {e}")
        
        return results
    
    def _calculate_yearly_returns(self, df, years):
        """计算特定年份的涨跌幅
        
        Args:
            df: 基金净值数据
            years: 年份列表，如[2024, 2025]
        """
        results = {}
        
        try:
            for year in years:
                # 筛选年份数据
                year_df = df[df["date"].dt.year == year]
                
                if len(year_df) < 2:
                    results[str(year)] = None
                    continue
                
                # 计算涨跌幅
                return_pct = (year_df["close"].iloc[-1] / year_df["close"].iloc[0] - 1) * 100
                results[str(year)] = round(return_pct, 2)
        except Exception as e:
            print(f"计算年份涨跌幅时出错: {e}")
        
        return results
    
    def _calculate_max_monthly_abnormal(self, df):
        """计算月涨跌幅最大异常"""
        if len(df) < 2:
            return None
        
        try:
            # 计算月度收益率
            df_monthly = df.set_index("date").resample("ME").last()
            df_monthly["monthly_return"] = df_monthly["close"].pct_change(fill_method=None)
            
            if len(df_monthly) < 2:
                return None
            
            # 计算标准差
            std = df_monthly["monthly_return"].std()
            
            # 找出最大的异常值（与平均值的偏离）
            mean = df_monthly["monthly_return"].mean()
            df_monthly["deviation"] = abs(df_monthly["monthly_return"] - mean) / std
            
            max_abnormal = df_monthly["deviation"].max()
            return round(max_abnormal, 2)
        except Exception as e:
            print(f"计算月涨跌幅最大异常时出错: {e}")
            return None
    
    def _calculate_monthly_return_range(self, df):
        """计算近一月涨跌幅范围"""
        if len(df) < 2:
            return None
        
        try:
            # 获取最近一个月的数据
            end_date = df["date"].max()
            start_date = end_date - datetime.timedelta(days=30)
            
            month_df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
            
            if len(month_df) < 2:
                return None
            
            # 计算日收益率
            month_df["daily_return"] = month_df["close"].pct_change(fill_method=None).dropna()
            
            if len(month_df["daily_return"]) == 0:
                return None
            
            # 计算最大和最小日收益率
            max_daily_return = month_df["daily_return"].max() * 100
            min_daily_return = month_df["daily_return"].min() * 100
            
            return f"{round(min_daily_return, 2)}% ~ {round(max_daily_return, 2)}%"
        except Exception as e:
            print(f"计算近一月涨跌幅范围时出错: {e}")
            return None
    
    def _calculate_period_metrics(self, df, days):
        """计算指定天数范围内的所有指标
        
        Args:
            df: 完整的基金净值数据
            days: 天数范围
        """
        # 筛选时间范围
        end_date = df["date"].max()
        start_date = end_date - datetime.timedelta(days=days)
        
        period_df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
        
        if len(period_df) < 2:
            return None
        
        # 计算所有指标
        metrics = {
            'positive_quarters_ratio': self._calculate_positive_quarters_ratio(period_df),
            'positive_months_ratio': self._calculate_positive_months_ratio(period_df),
            'positive_weeks_ratio': self._calculate_positive_weeks_ratio(period_df),
            'quarterly_volatility': self._calculate_quarterly_volatility(period_df),
            'monthly_volatility': self._calculate_monthly_volatility(period_df),
            'weekly_volatility': self._calculate_weekly_volatility(period_df),
            'annualized_return': self._calculate_annualized_return(period_df),
            'max_drawdown': self._calculate_max_drawdown(period_df),
            'second_max_drawdown': self._calculate_second_max_drawdown(period_df),
            'sharpe_ratio': self._calculate_sharpe_ratio(period_df),
            'calmar_ratio': self._calculate_calmar_ratio(period_df),
            'ols_discrete_coefficient': self._calculate_ols_discrete_coefficient(period_df)
        }
        
        return metrics
    
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
                print(f"基金 {fund_code} 申购数据为空")
                return None
            
            purchase_data = purchase_row.iloc[0]
            
            # 获取基金基本面数据
            fundamental_row = fund_fundamental_df[fund_fundamental_df['基金代码'] == fund_code]
            
            # 获取完整的基金净值数据（不限制时间范围，以便计算各种时间周期的指标）
            # 注意：这里直接获取完整数据，后面在各个指标计算函数中进行时间筛选
            # 这样可以减少数据库访问次数
            full_nav_data = self._get_fund_nav_data(fund_code, None, None, hf)
            
            # 调试信息
            print(f"\n处理基金: {fund_code} - {purchase_data.get('基金简称', '')}")
            print(f"基金净值数据状态: {'存在' if full_nav_data is not None else '不存在'}")
            if full_nav_data is not None:
                print(f"净值数据行数: {len(full_nav_data)}")
                print(f"净值数据日期范围: {full_nav_data['date'].min()} 至 {full_nav_data['date'].max()}")
            
            # 初始化所有指标为None
            fund_info = {
                '基金代码': str(fund_code),  # 确保基金代码始终为字符串类型
                '基金简称': purchase_data.get('基金简称', ''),
                '基金类型': purchase_data.get('基金类型', ''),
                '期初日期': start_date.strftime('%Y-%m-%d'),
                '期末日期': end_date.strftime('%Y-%m-%d'),
                '基金规模': None,
                '成立年数': None,
                '上涨季度比例': None,
                '上涨月份比例': None,
                '上涨星期比例': None,
                '季涨跌幅标准差': None,
                '月涨跌幅标准差': None,
                '周涨跌幅标准差': None,
                '年化收益率': None,
                '最大回撤率': None,
                '第二大回撤': None,
                '夏普率': None,
                '卡玛率': None,
                'OLS离散系数': None,
                '近3年上涨季度比例': None,
                '近3年上涨月份比例': None,
                '近3年上涨星期比例': None,
                '近3年季涨跌幅标准差': None,
                '近3年月涨跌幅标准差': None,
                '近3年周涨跌幅标准差': None,
                '近3年年化收益率': None,
                '近3年最大回撤率': None,
                '近3年第二大回撤': None,
                '近3年夏普率': None,
                '近3年卡玛率': None,
                '近3年OLS离散系数': None,
                '近1年上涨月份比例': None,
                '近1年上涨星期比例': None,
                '近1年月涨跌幅标准差': None,
                '近1年周涨跌幅标准差': None,
                '近1年年化收益率': None,
                '近1年最大回撤率': None,
                '近1年第二大回撤': None,
                '近1年夏普率': None,
                '近1年卡玛率': None,
                '近1年OLS离散系数': None,
                '前1月涨跌幅': None,
                '前2月涨跌幅': None,
                '前3月涨跌幅': None,
                '前2季涨跌幅': None,
                '前3季涨跌幅': None,
                '前4季涨跌幅': None,
                '近1周涨跌幅': None,
                '近1月涨跌幅': None,
                '近3月涨跌幅': None,
                '近6月涨跌幅': None,
                '近1年涨跌幅': None,
                '2025年涨跌幅': None,
                '2024年涨跌幅': None,
                '月涨跌幅最大异常': None,
                '近一月涨跌幅范围': None,
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
            
            # 如果有净值数据，计算所有指标
            if full_nav_data is not None and len(full_nav_data) > 0:
                # 筛选主时间范围内的数据
                nav_data = full_nav_data[(full_nav_data['date'] >= start_date) & (full_nav_data['date'] <= end_date)]
                print(f"主时间范围内数据行数: {len(nav_data)}")
                
                # 计算主时间范围的指标
                if len(nav_data) >= 2:
                    # 测试单个计算方法
                    quarterly_volatility = self._calculate_quarterly_volatility(nav_data)
                    print(f"季度涨跌幅标准差计算结果: {quarterly_volatility}")
                    
                    # 计算所有指标
                    fund_info['上涨季度比例'] = self._calculate_positive_quarters_ratio(nav_data)
                    fund_info['上涨月份比例'] = self._calculate_positive_months_ratio(nav_data)
                    fund_info['上涨星期比例'] = self._calculate_positive_weeks_ratio(nav_data)
                    fund_info['季涨跌幅标准差'] = quarterly_volatility
                    fund_info['月涨跌幅标准差'] = self._calculate_monthly_volatility(nav_data)
                    fund_info['周涨跌幅标准差'] = self._calculate_weekly_volatility(nav_data)
                    fund_info['年化收益率'] = self._calculate_annualized_return(nav_data)
                    fund_info['最大回撤率'] = self._calculate_max_drawdown(nav_data)
                    fund_info['第二大回撤'] = self._calculate_second_max_drawdown(nav_data)
                    fund_info['夏普率'] = self._calculate_sharpe_ratio(nav_data)
                    fund_info['卡玛率'] = self._calculate_calmar_ratio(nav_data)
                    fund_info['OLS离散系数'] = self._calculate_ols_discrete_coefficient(nav_data)
                    
                    # 输出部分计算结果用于调试
                    print(f"年化收益率: {fund_info['年化收益率']}%")
                    print(f"最大回撤率: {fund_info['最大回撤率']}%")
                
                # 计算近3年指标
                three_year_metrics = self._calculate_period_metrics(full_nav_data, 1095)  # 3年 = 1095天
                print(f"近3年指标计算结果: {'存在' if three_year_metrics else '不存在'}")
                if three_year_metrics:
                    fund_info['近3年上涨季度比例'] = three_year_metrics['positive_quarters_ratio']
                    fund_info['近3年上涨月份比例'] = three_year_metrics['positive_months_ratio']
                    fund_info['近3年上涨星期比例'] = three_year_metrics['positive_weeks_ratio']
                    fund_info['近3年季涨跌幅标准差'] = three_year_metrics['quarterly_volatility']
                    fund_info['近3年月涨跌幅标准差'] = three_year_metrics['monthly_volatility']
                    fund_info['近3年周涨跌幅标准差'] = three_year_metrics['weekly_volatility']
                    fund_info['近3年年化收益率'] = three_year_metrics['annualized_return']
                    fund_info['近3年最大回撤率'] = three_year_metrics['max_drawdown']
                    fund_info['近3年第二大回撤'] = three_year_metrics['second_max_drawdown']
                    fund_info['近3年夏普率'] = three_year_metrics['sharpe_ratio']
                    fund_info['近3年卡玛率'] = three_year_metrics['calmar_ratio']
                    fund_info['近3年OLS离散系数'] = three_year_metrics['ols_discrete_coefficient']
                
                # 计算近1年指标
                one_year_metrics = self._calculate_period_metrics(full_nav_data, 365)  # 1年 = 365天
                print(f"近1年指标计算结果: {'存在' if one_year_metrics else '不存在'}")
                if one_year_metrics:
                    fund_info['近1年上涨月份比例'] = one_year_metrics['positive_months_ratio']
                    fund_info['近1年上涨星期比例'] = one_year_metrics['positive_weeks_ratio']
                    fund_info['近1年月涨跌幅标准差'] = one_year_metrics['monthly_volatility']
                    fund_info['近1年周涨跌幅标准差'] = one_year_metrics['weekly_volatility']
                    fund_info['近1年年化收益率'] = one_year_metrics['annualized_return']
                    fund_info['近1年最大回撤率'] = one_year_metrics['max_drawdown']
                    fund_info['近1年第二大回撤'] = one_year_metrics['second_max_drawdown']
                    fund_info['近1年夏普率'] = one_year_metrics['sharpe_ratio']
                    fund_info['近1年卡玛率'] = one_year_metrics['calmar_ratio']
                    fund_info['近1年OLS离散系数'] = one_year_metrics['ols_discrete_coefficient']
                
                # 计算各种时间周期的涨跌幅
                period_returns = self._calculate_period_returns(full_nav_data, ['1W', '1M', '3M', '6M', '1Y'])
                print(f"时间周期涨跌幅计算结果: {period_returns}")
                fund_info['近1周涨跌幅'] = period_returns.get('1W')
                fund_info['近1月涨跌幅'] = period_returns.get('1M')
                fund_info['近3月涨跌幅'] = period_returns.get('3M')
                fund_info['近6月涨跌幅'] = period_returns.get('6M')
                fund_info['近1年涨跌幅'] = period_returns.get('1Y')
                
                # 计算前N月涨跌幅
                # 这里简化处理，使用近似天数计算
                fund_info['前1月涨跌幅'] = period_returns.get('1M')
                two_month_returns = self._calculate_period_returns(full_nav_data, ['2M'])
                fund_info['前2月涨跌幅'] = two_month_returns.get('2M')
                fund_info['前3月涨跌幅'] = period_returns.get('3M')
                
                # 计算前N季涨跌幅
                try:
                    # 首先尝试使用精确的季度数据计算
                    if full_nav_data is not None and len(full_nav_data) >= 4 * 21:  # 假设每季度约21个交易日
                        # 确保日期列是datetime类型
                        if not pd.api.types.is_datetime64_any_dtype(full_nav_data['date']):
                            full_nav_data['date'] = pd.to_datetime(full_nav_data['date'])
                        
                        # 获取最新日期
                        latest_date = full_nav_data['date'].max()
                        
                        # 计算前2季（约6个月）
                        two_quarters_ago = latest_date - pd.DateOffset(months=6)
                        two_quarter_data = full_nav_data[full_nav_data['date'] >= two_quarters_ago]
                        if len(two_quarter_data) >= 2:
                            two_quarter_return = (two_quarter_data['close'].iloc[-1] / two_quarter_data['close'].iloc[0] - 1) * 100
                            fund_info['前2季涨跌幅'] = round(two_quarter_return, 2)
                        
                        # 计算前3季（约9个月）
                        three_quarters_ago = latest_date - pd.DateOffset(months=9)
                        three_quarter_data = full_nav_data[full_nav_data['date'] >= three_quarters_ago]
                        if len(three_quarter_data) >= 2:
                            three_quarter_return = (three_quarter_data['close'].iloc[-1] / three_quarter_data['close'].iloc[0] - 1) * 100
                            fund_info['前3季涨跌幅'] = round(three_quarter_return, 2)
                        
                        # 计算前4季（约12个月）
                        four_quarters_ago = latest_date - pd.DateOffset(months=12)
                        four_quarter_data = full_nav_data[full_nav_data['date'] >= four_quarters_ago]
                        if len(four_quarter_data) >= 2:
                            four_quarter_return = (four_quarter_data['close'].iloc[-1] / four_quarter_data['close'].iloc[0] - 1) * 100
                            fund_info['前4季涨跌幅'] = round(four_quarter_return, 2)
                except Exception as e:
                    print(f"计算精确季度涨跌幅时出错: {e}，使用备用方法")
                    
                # 确保所有季度涨跌幅数据完整
                six_month_returns = self._calculate_period_returns(full_nav_data, ['6M'])
                nine_month_returns = self._calculate_period_returns(full_nav_data, ['9M'])
                twelve_month_returns = self._calculate_period_returns(full_nav_data, ['12M'])
                
                # 强制使用备用方法确保数据完整性
                fund_info['前2季涨跌幅'] = six_month_returns.get('6M')
                fund_info['前3季涨跌幅'] = nine_month_returns.get('9M')
                fund_info['前4季涨跌幅'] = twelve_month_returns.get('12M')
                
                # 计算年份涨跌幅
                yearly_returns = self._calculate_yearly_returns(full_nav_data, [2024, 2025])
                print(f"年份涨跌幅计算结果: {yearly_returns}")
                fund_info['2024年涨跌幅'] = yearly_returns.get('2024')
                fund_info['2025年涨跌幅'] = yearly_returns.get('2025')
                
                # 计算其他特殊指标
                max_monthly_abnormal = self._calculate_max_monthly_abnormal(full_nav_data)
                monthly_return_range = self._calculate_monthly_return_range(full_nav_data)
                print(f"特殊指标计算结果 - 月涨跌幅最大异常: {max_monthly_abnormal}, 近一月涨跌幅范围: {monthly_return_range}")
                fund_info['月涨跌幅最大异常'] = max_monthly_abnormal
                fund_info['近一月涨跌幅范围'] = monthly_return_range
            
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
        # 确保基金代码为字符串类型
        fund_codes = [str(code) for code in fund_codes]
        print(f"共找到 {len(fund_codes)} 只基金")
        
        # 检查All_Fund_Data.h5文件是否存在
        if not os.path.exists(self.all_fund_data_path):
            print(f"警告：{self.all_fund_data_path} 文件不存在，将无法计算量化指标")
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
        
        # 检查是否为测试报表输出
        is_test_report = False
        if output_file and isinstance(output_file, str) and '测试报表' in output_file:
            is_test_report = True
            # 测试报表限制为5只基金
            fund_codes = fund_codes[:5]
            print(f"测试报表模式：仅处理前 5 只基金")
        # 根据TEST_LIMIT属性决定处理数量
        elif hasattr(self, 'TEST_LIMIT') and self.TEST_LIMIT is not None:
            fund_codes = fund_codes[:self.TEST_LIMIT]
            print(f"测试模式：仅处理前{self.TEST_LIMIT}只基金")
        elif len(fund_codes) > 100:
            # 默认限制100只基金
            fund_codes = fund_codes[:100]
            print("默认限制：仅处理前100只基金")
        
        # 优化：根据系统CPU核心数自动调整最佳线程数
        try:
            cpu_count = os.cpu_count() or 4  # 获取CPU核心数，默认为4
            
            # 对于IO密集型任务，可以设置比CPU核心数更多的线程
            # 但也要避免过多线程导致的上下文切换开销
            # 基本公式：CPU核心数 * 1.5 到 CPU核心数 * 3 之间
            base_workers = max(2, cpu_count)  # 至少2个线程
            max_recommended = max(cpu_count * 3, 8)  # 最多为CPU核心数的3倍或最少8个线程
            
            # 根据要处理的基金数量动态调整线程数
            # 基金数量较少时使用较少线程
            if len(fund_codes) <= 10:
                optimal_workers = min(base_workers, 4)  # 少量基金时最多4个线程
            elif len(fund_codes) <= 50:
                optimal_workers = min(base_workers * 2, max_recommended)  # 中等数量时使用更多线程
            else:
                optimal_workers = min(base_workers * 3, max_recommended)  # 大量基金时使用最多线程
            
            print(f"系统CPU核心数: {cpu_count}")
            print(f"根据系统配置和任务量，自动选择 {optimal_workers} 个工作线程进行处理")
        except Exception as e:
            # 异常情况下使用默认线程数
            optimal_workers = 4
            print(f"获取系统信息时出错: {e}，使用默认的4个工作线程")
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=optimal_workers) as executor:
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
            except Exception:
                print("关闭HDF5文件时发生异常")
        
        processing_time = time.time() - start_time
        print(f"\n成功处理 {len(self.report_data)} 只基金的数据")
        print(f"处理时间: {processing_time:.2f} 秒")
        print(f"平均每只基金处理时间: {processing_time / len(fund_codes):.2f} 秒")
        
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
            
            # 确保基金代码列始终为字符串类型，保留前导零
            if '基金代码' in df.columns:
                df['基金代码'] = df['基金代码'].astype(str)
            
            # 设置列顺序（包含所有新增的表头，并添加单位）
            columns_order = [
                '基金代码', '基金简称', '基金类型', '期初日期', '期末日期',
                '基金规模(亿元)', '成立年数(年)', '上涨季度比例%', '上涨月份比例%', '上涨星期比例%',
                '季涨跌幅标准差%', '月涨跌幅标准差%', '周涨跌幅标准差%', '年化收益率%', '最大回撤率%', 
                '第二大回撤%', '夏普率', '卡玛率', 'OLS离散系数',
                '近3年上涨季度比例%', '近3年上涨月份比例%', '近3年上涨星期比例%', '近3年季涨跌幅标准差%', 
                '近3年月涨跌幅标准差%', '近3年周涨跌幅标准差%', '近3年年化收益率%', '近3年最大回撤率%', 
                '近3年第二大回撤%', '近3年夏普率', '近3年卡玛率', '近3年OLS离散系数',
                '近1年上涨月份比例%', '近1年上涨星期比例%', '近1年月涨跌幅标准差%', '近1年周涨跌幅标准差%', 
                '近1年年化收益率%', '近1年最大回撤率%', '近1年第二大回撤%', '近1年夏普率', 
                '近1年卡玛率', '近1年OLS离散系数',
                '前1月涨跌幅%', '前2月涨跌幅%', '前3月涨跌幅%', '前2季涨跌幅%', '前3季涨跌幅%', '前4季涨跌幅%',
                '近1周涨跌幅%', '近1月涨跌幅%', '近3月涨跌幅%', '近6月涨跌幅%', '近1年涨跌幅%',
                '2025年涨跌幅%', '2024年涨跌幅%', '月涨跌幅最大异常%', '近一月涨跌幅范围%',
                '封闭类型', '封闭长度', '状态', '基金买卖信息', '基金经理人',
                '成立来分红(元)', '最近更新日期'
            ]
            
            # 定义原列名到新列名的映射
            column_mapping = {
                '基金代码': '基金代码',
                '基金简称': '基金简称',
                '基金类型': '基金类型',
                '期初日期': '期初日期',
                '期末日期': '期末日期',
                '基金规模': '基金规模(亿元)',
                '成立年数': '成立年数(年)',
                '上涨季度比例': '上涨季度比例%',
                '上涨月份比例': '上涨月份比例%',
                '上涨星期比例': '上涨星期比例%',
                '季涨跌幅标准差': '季涨跌幅标准差%',
                '月涨跌幅标准差': '月涨跌幅标准差%',
                '周涨跌幅标准差': '周涨跌幅标准差%',
                '年化收益率': '年化收益率%',
                '最大回撤率': '最大回撤率%',
                '第二大回撤': '第二大回撤%',
                '夏普率': '夏普率',
                '卡玛率': '卡玛率',
                'OLS离散系数': 'OLS离散系数',
                '近3年上涨季度比例': '近3年上涨季度比例%',
                '近3年上涨月份比例': '近3年上涨月份比例%',
                '近3年上涨星期比例': '近3年上涨星期比例%',
                '近3年季涨跌幅标准差': '近3年季涨跌幅标准差%',
                '近3年月涨跌幅标准差': '近3年月涨跌幅标准差%',
                '近3年周涨跌幅标准差': '近3年周涨跌幅标准差%',
                '近3年年化收益率': '近3年年化收益率%',
                '近3年最大回撤率': '近3年最大回撤率%',
                '近3年第二大回撤': '近3年第二大回撤%',
                '近3年夏普率': '近3年夏普率',
                '近3年卡玛率': '近3年卡玛率',
                '近3年OLS离散系数': '近3年OLS离散系数',
                '近1年上涨月份比例': '近1年上涨月份比例%',
                '近1年上涨星期比例': '近1年上涨星期比例%',
                '近1年月涨跌幅标准差': '近1年月涨跌幅标准差%',
                '近1年周涨跌幅标准差': '近1年周涨跌幅标准差%',
                '近1年年化收益率': '近1年年化收益率%',
                '近1年最大回撤率': '近1年最大回撤率%',
                '近1年第二大回撤': '近1年第二大回撤%',
                '近1年夏普率': '近1年夏普率',
                '近1年卡玛率': '近1年卡玛率',
                '近1年OLS离散系数': '近1年OLS离散系数',
                '前1月涨跌幅': '前1月涨跌幅%',
                '前2月涨跌幅': '前2月涨跌幅%',
                '前3月涨跌幅': '前3月涨跌幅%',
                '前2季涨跌幅': '前2季涨跌幅%',
                '前3季涨跌幅': '前3季涨跌幅%',
                '前4季涨跌幅': '前4季涨跌幅%',
                '近1周涨跌幅': '近1周涨跌幅%',
                '近1月涨跌幅': '近1月涨跌幅%',
                '近3月涨跌幅': '近3月涨跌幅%',
                '近6月涨跌幅': '近6月涨跌幅%',
                '近1年涨跌幅': '近1年涨跌幅%',
                '2025年涨跌幅': '2025年涨跌幅%',
                '2024年涨跌幅': '2024年涨跌幅%',
                '月涨跌幅最大异常': '月涨跌幅最大异常%',
                '近一月涨跌幅范围': '近一月涨跌幅范围%',
                '封闭类型': '封闭类型',
                '封闭长度': '封闭长度',
                '状态': '状态',
                '基金买卖信息': '基金买卖信息',
                '基金经理人': '基金经理人',
                '成立来分红': '成立来分红(元)',
                '最近更新日期': '最近更新日期'
            }
            
            # 转换列名并处理数值数据
            new_df = pd.DataFrame()
            # 定义需要强制保留两位小数的列名列表 - 按照用户要求的31项
            force_numeric_columns = [
                '季涨跌幅标准差%', '月涨跌幅标准差%', '周涨跌幅标准差%', '年化收益率%', '最大回撤率%', 
                '第二大回撤%', '夏普率', '卡玛率', 'OLS离散系数', 
                '近3年季涨跌幅标准差%', '近3年月涨跌幅标准差%', '近3年周涨跌幅标准差%', '近3年年化收益率%', 
                '近3年最大回撤率%', '近3年第二大回撤%', '近3年夏普率', '近3年卡玛率', '近3年OLS离散系数', 
                '近1年月涨跌幅标准差%', '近1年周涨跌幅标准差%', '近1年年化收益率%', '近1年最大回撤率%', 
                '近1年第二大回撤%', '近1年夏普率', '近1年卡玛率', '近1年OLS离散系数', 
                '前1月涨跌幅%', '前2月涨跌幅%', '前3月涨跌幅%', '前2季涨跌幅%', '前3季涨跌幅%', '前4季涨跌幅%', 
                '近1周涨跌幅%', '近1月涨跌幅%', '近3月涨跌幅%', '近6月涨跌幅%', '近1年涨跌幅%',
                '2025年涨跌幅%', '2024年涨跌幅%', '月涨跌幅最大异常%', '近一月涨跌幅范围%'
            ]
            
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    # 基金代码列特殊处理，保持为字符串类型
                    if old_col == '基金代码':
                        new_df[new_col] = df[old_col].astype(str)
                        continue
                    
                    # 处理所有可能的数值列，确保统一保留两位小数
                    try:
                        # 先尝试直接转换为数值
                        numeric_series = pd.to_numeric(df[old_col], errors='coerce')
                        
                        # 检查是否存在非空的数值
                        if numeric_series.notna().any():
                            # 对数值列应用round(2)
                            new_df[new_col] = numeric_series.round(2)
                        else:
                            # 如果没有成功转换为数值，尝试第二种方法：先转为字符串再处理
                            try:
                                # 尝试处理可能的字符串数值（如带百分号的字符串）
                                str_series = df[old_col].astype(str)
                                # 移除百分号并转换为数值
                                clean_series = str_series.str.replace('%', '').str.strip()
                                numeric_series_alt = pd.to_numeric(clean_series, errors='coerce')
                                
                                if numeric_series_alt.notna().any():
                                    new_df[new_col] = numeric_series_alt.round(2)
                                else:
                                    # 如果仍然无法转换为数值，检查是否在强制数值列表中
                                    if new_col in force_numeric_columns:
                                        # 即使无法转换，也设置为空数值而不是原始值
                                        new_df[new_col] = pd.Series([None] * len(df))
                                    else:
                                        # 非强制数值列，使用原始值
                                        new_df[new_col] = df[old_col]
                            except:
                                # 发生异常时，检查是否在强制数值列表中
                                if new_col in force_numeric_columns:
                                    new_df[new_col] = pd.Series([None] * len(df))
                                else:
                                    new_df[new_col] = df[old_col]
                    except Exception as e:
                        # 发生异常时，检查是否在强制数值列表中
                        print(f"处理列 {old_col} -> {new_col} 时出错: {e}")
                        if new_col in force_numeric_columns:
                            new_df[new_col] = pd.Series([None] * len(df))
                        else:
                            new_df[new_col] = df[old_col]
                else:
                    # 列不存在时，检查是否在强制数值列表中
                    if new_col in force_numeric_columns:
                        new_df[new_col] = pd.Series([None] * len(df))
                    else:
                        new_df[new_col] = None
            
            # 第二遍处理：确保所有force_numeric_columns中的列都正确格式化为两位小数
            for col in force_numeric_columns:
                if col in new_df.columns:
                    try:
                        # 再次尝试确保数值格式
                        if new_df[col].notna().any():
                            # 转换为数值并保留两位小数
                            numeric_series = pd.to_numeric(new_df[col], errors='coerce')
                            # 使用更严格的格式化方法确保两位小数
                            new_df[col] = numeric_series.apply(lambda x: float(f"{x:.2f}") if pd.notna(x) else x)
                        else:
                            # 如果全部为空，保持原样
                            pass
                    except Exception as e:
                        print(f"格式化列 {col} 时出错: {e}")
                        # 保持原值
                        pass
            
            # 重新排序列
            df = new_df[columns_order]
            
            # 设置输出文件名，确保总是有有效的输出路径
            if output_file is None or not output_file or not isinstance(output_file, str):
                # 当output_file为空、None或不是字符串时，使用默认路径
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = os.path.join(
                    self.base_dir, 
                    'reports', 
                    f'基金量化分析报表_{timestamp}.xlsx'
                )
            else:
                # 确保输出到reports文件夹
                if not os.path.isabs(output_file) and not output_file.startswith('reports'):
                    output_file = os.path.join(self.base_dir, 'reports', output_file)
                elif os.path.isabs(output_file):
                    # 绝对路径情况下，如果不是reports文件夹，则移动到reports文件夹
                    if 'reports' not in output_file:
                        # 提取文件名部分
                        filename = os.path.basename(output_file)
                        output_file = os.path.join(self.base_dir, 'reports', filename)
                
                # 测试报表添加时间戳
                if '测试报表' in output_file and '_' not in os.path.basename(output_file).split('.')[0]:
                    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = os.path.basename(output_file)
                    name_without_ext = os.path.splitext(filename)[0]
                    output_file = os.path.join(
                        os.path.dirname(output_file),
                        f'{name_without_ext}_{timestamp}.xlsx'
                    )
                
                # 验证用户指定的路径，确保是有效的Excel文件
                if not output_file.endswith(('.xlsx', '.xls')):
                    # 如果没有指定扩展名，添加.xlsx
                    output_file += '.xlsx'
            
            # 确保输出目录存在
            try:
                output_dir = os.path.dirname(output_file)
                # 如果output_dir为空（即只有文件名没有路径），使用当前目录
                if not output_dir:
                    output_dir = os.getcwd()
                    output_file = os.path.join(output_dir, os.path.basename(output_file))
                
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir, exist_ok=True)
                    print(f"已创建输出目录: {output_dir}")
            except Exception as e:
                print(f"创建输出目录时出错: {e}")
                # 使用当前目录作为备选
                output_dir = os.getcwd()
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = os.path.join(output_dir, f'基金量化分析报表_{timestamp}.xlsx')
                print(f"使用备选输出路径: {output_file}")
            
            # 保存到Excel
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='基金量化分析', index=False)
                
                # 设置列宽和格式
                worksheet = writer.sheets['基金量化分析']
                
                # 设置基金代码列为文本格式，保留前导零
                if '基金代码' in df.columns:
                    fund_code_col = df.columns.get_loc('基金代码') + 1  # +1 因为Excel列从1开始
                    for row in range(2, worksheet.max_row + 1):  # 从第2行开始，跳过标题行
                        cell = worksheet.cell(row=row, column=fund_code_col)
                        cell.number_format = '@'  # 设置为文本格式
                
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
            
            # 验证Excel文件中的基金代码格式
            print("\n验证Excel文件中的基金代码格式...")
            try:
                # 使用字符串类型读取基金代码列
                verify_df = pd.read_excel(output_file, dtype={'基金代码': str})
                print("基金代码列的前5个值:")
                print(verify_df['基金代码'].head())
                print("基金代码列的数据类型:", verify_df['基金代码'].dtype)
                print("\n注意：当使用pandas读取此Excel文件时，请使用以下参数以确保基金代码列保留前导零：")
                print("pd.read_excel('文件路径', dtype={'基金代码': str})")
            except Exception as e:
                print(f"验证Excel文件时出错: {e}")
            
            print(f"\n报表已成功生成: {output_file}")
            return True
            
        except Exception as e:
            print(f"保存Excel报表时出错: {e}")
            return False

    def interactive_menu(self):
        """交互式菜单"""
        print("\n=== 基金量化分析报表生成器 ===")
        
        # 添加报表类型选择
        while True:
            print("\n请选择操作:")
            print("1、生成完整的量化基金分析报表")
            print("2、生成5个基金的量化报表测试程序")
            
            choice = input("请输入选择 (1 或 2): ").strip()
            
            if choice in ['1', '2']:
                # 设置测试限制
                if choice == '1':
                    self.TEST_LIMIT = None  # 不限制数量
                    print("将生成完整的量化基金分析报表")
                else:
                    self.TEST_LIMIT = 5  # 仅处理5只基金用于测试
                    print("将生成5个基金的量化报表测试程序")
                break
            else:
                print("无效的选择，请重新输入")
        
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