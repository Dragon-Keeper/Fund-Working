import numpy as np
import pandas as pd
import os
import h5py
import math
from datetime import datetime, timedelta
from scipy import stats
import warnings
import concurrent.futures
import multiprocessing
import threading

# 忽略警告信息
warnings.filterwarnings("ignore")

class AdvancedQuantAnalyzer:
    """高级量化分析器，用于生成指定格式的基金量化分析结果"""
    
    def __init__(self, hdf5_path=None, start_date_str=None):
        self.results = []
        self.start_date = None
        self.end_date = datetime.now()
        
        # 设置起始日期
        if start_date_str:
            self.start_date = datetime.strptime(start_date_str, "%Y%m%d")
        else:
            # 默认使用最近一年的数据
            self.start_date = self.end_date - timedelta(days=365)
        
        # 设置HDF5文件路径
        if hdf5_path:
            self.hdf5_path = hdf5_path
        else:
            # 默认路径
            self.hdf5_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "data", "All_Fund_Data.h5"
            )
        
        # 设置无风险收益率（用于夏普比率等计算）
        self.risk_free_rate = 0.03  # 假设无风险收益率为3%
        
        # 交易日天数（一年）
        self.trading_days_per_year = 252
        
        # 初始化所有需要的列名
        self.required_columns = [
            "基金代码", "基金简称", "期初日期", "期末日期", "规模-亿元", "年数", 
            "上涨季度比例", "上涨月份比例", "上涨星期比例", 
            "季涨跌幅标准差", "月涨跌幅标准差", "周涨跌幅标准差", 
            "年化收益率", "最大回撤率", "第二大回撤", "夏普率", "卡玛率", "OLS离散系数", 
            "近3年上涨季度比例", "近3年上涨月份比例", "近3年上涨星期比例", 
            "近3年季涨跌幅标准差", "近3年月涨跌幅标准差", "近3年周涨跌幅标准差", 
            "近3年年化收益率", "近3年最大回撤率", "近3年第二大回撤", "近3年夏普率", "近3年卡玛率", "近3年OLS离散系数", 
            "近1年上涨月份比例", "近1年上涨星期比例", 
            "近1年月涨跌幅标准差", "近1年周涨跌幅标准差", 
            "近1年年化收益率", "近1年最大回撤率", "近1年第二大回撤", "近1年夏普率", "近1年卡玛率", "近1年OLS离散系数", 
            "前1月涨跌幅", "前2月涨跌幅", "前3月涨跌幅", 
            "前2季涨跌幅", "前3季涨跌幅", "前4季涨跌幅", 
            "近1周涨跌幅", "近1月涨跌幅", "近3月涨跌幅", "近6月涨跌幅", "近1年涨跌幅", 
            "2025年涨跌幅", "2024年涨跌幅", "月涨跌幅最大异常", 
            "封闭类型", "封闭长度", "状态", "基金买卖信息", "最近更新日期", "类别", 
            "KenChoice", "KenComment", "近一月涨跌幅范围"
        ]
    
    def read_fund_data(self, fund_code):
        """读取基金的完整净值时间序列数据"""
        try:
            with h5py.File(self.hdf5_path, "r") as hf:
                if fund_code not in hf:
                    print(f"基金代码 {fund_code} 不存在于HDF5文件中")
                    return None
    
                group = hf[fund_code]
    
                # 读取数据并转换为DataFrame
                dates = [d.decode("utf-8") for d in group["date"][()]]
    
                # 创建DataFrame
                df = pd.DataFrame(
                    {
                        "date": pd.to_datetime(dates),
                        "open": group["open"][()],
                        "high": group["high"][()],
                        "low": group["low"][()],
                        "close": group["close"][()],
                        "amount": group["amount"][()],
                        "volume": group["volume"][()],
                        "prev_close": group["prev_close"][()],
                    }
                )
    
                # 按日期排序
                df = df.sort_values("date")
    
                # 过滤时间范围
                mask = (df["date"] >= self.start_date) & (df["date"] <= self.end_date)
                df = df[mask].copy()
    
                # 计算日收益率
                df["daily_return"] = df["close"].pct_change()
    
                return df
        except Exception as e:
            print(f"读取基金 {fund_code} 数据时出错: {str(e)}")
            return None
    
    def get_fund_name(self, fund_code):
        """获取基金名称"""
        try:
            with h5py.File(self.hdf5_path, "r") as hf:
                if fund_code in hf and "fund_name" in hf[fund_code].attrs:
                    fund_name = hf[fund_code].attrs["fund_name"]
                    if isinstance(fund_name, bytes):
                        fund_name = fund_name.decode("utf-8")
                    return fund_name
                return fund_code
        except:
            return fund_code
    
    def get_all_fund_codes(self):
        """获取HDF5文件中所有基金的代码"""
        try:
            with h5py.File(self.hdf5_path, "r") as hf:
                return list(hf.keys())
        except Exception as e:
            print(f"获取基金代码列表时出错: {str(e)}")
            return []
    
    def analyze_all_funds(self, thread_mode='auto', custom_thread_count=None):
        """分析所有基金"""
        # 获取所有基金代码
        all_fund_codes = self.get_all_fund_codes()
        total_funds = len(all_fund_codes)
    
        if total_funds == 0:
            print("未找到基金数据，请先确保HDF5文件中包含数据")
            return False
    
        print(f"共发现 {total_funds} 只基金，开始进行量化分析...")
        
        # 如果只有少量基金，直接使用单线程
        if total_funds <= 5:
            print("基金数量较少，使用单线程处理")
            return self._analyze_all_funds_single_thread(all_fund_codes)
        
        # 确定线程数量
        if thread_mode == 'single':
            print("使用单线程模式处理")
            return self._analyze_all_funds_single_thread(all_fund_codes)
        elif thread_mode == 'custom' and custom_thread_count:
            thread_count = max(1, min(custom_thread_count, multiprocessing.cpu_count() * 2))
            print(f"使用自定义线程数模式处理，线程数: {thread_count}")
        else:  # auto
            # 根据CPU核心数自动分配线程数，一般为核心数的1-2倍
            cpu_count = multiprocessing.cpu_count()
            thread_count = min(cpu_count * 2, max(4, cpu_count))
            print(f"使用自动线程分配模式处理，检测到CPU核心数: {cpu_count}，分配线程数: {thread_count}")
        
        # 使用线程池进行多线程处理
        return self._analyze_all_funds_multi_thread(all_fund_codes, thread_count)
    
    def _analyze_all_funds_single_thread(self, all_fund_codes):
        """单线程分析所有基金"""
        total_funds = len(all_fund_codes)
        
        for i, fund_code in enumerate(all_fund_codes, 1):
            print(f"分析基金 {i}/{total_funds}: {fund_code}")
            try:
                fund_result = self.analyze_single_fund(fund_code)
                if fund_result:
                    self.results.append(fund_result)
            except Exception as e:
                print(f"分析基金 {fund_code} 时出错: {str(e)}")
                
        return len(self.results) > 0
    
    def _analyze_all_funds_multi_thread(self, all_fund_codes, thread_count):
        """多线程分析所有基金"""
        total_funds = len(all_fund_codes)
        processed_count = 0
        lock = threading.Lock()  # 用于保护共享资源
        
        # 定义单个基金的分析函数
        def analyze_fund(fund_code):
            nonlocal processed_count
            try:
                # 分析单只基金
                fund_result = self.analyze_single_fund(fund_code)
                
                # 更新处理计数和进度
                with lock:
                    nonlocal processed_count
                    processed_count += 1
                    print(f"分析基金 {processed_count}/{total_funds}: {fund_code}")
                
                return fund_result
            except Exception as e:
                with lock:
                    nonlocal processed_count
                    processed_count += 1
                    print(f"分析基金 {processed_count}/{total_funds}: {fund_code} 时出错: {str(e)}")
                return None
        
        # 使用线程池
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            # 提交所有任务
            futures = [executor.submit(analyze_fund, fund_code) for fund_code in all_fund_codes]
            
            # 收集结果
            for future in concurrent.futures.as_completed(futures):
                fund_result = future.result()
                if fund_result:
                    # 使用锁保护共享资源的更新
                    with lock:
                        self.results.append(fund_result)
        
        return len(self.results) > 0
    
    def analyze_single_fund(self, fund_code):
        """分析单只基金，生成指定格式的结果"""
        # 获取基金名称
        fund_name = self.get_fund_name(fund_code)
        
        # 读取基金数据
        df = self.read_fund_data(fund_code)
        if df is None or len(df) < 2:
            print(f"基金 {fund_code} 数据不足，跳过分析")
            return None
        
        # 初始化结果字典
        result = {col: "" for col in self.required_columns}
        result["基金代码"] = fund_code
        result["基金简称"] = fund_name
        
        # 填充基本信息
        result["期初日期"] = df["date"].min().strftime("%Y-%m-%d")
        result["期末日期"] = df["date"].max().strftime("%Y-%m-%d")
        
        # 计算年数
        years_diff = (df["date"].max() - df["date"].min()).days / 365.0
        result["年数"] = round(years_diff, 2)
        
        # 规模-亿元（暂时用成交量的平均值估算，实际应从其他数据源获取）
        avg_volume = df["volume"].mean()
        result["规模-亿元"] = round(avg_volume / 100000000, 2) if not pd.isna(avg_volume) else 0
        
        # 计算所有时间段的指标
        # 全时期指标
        self._calculate_period_indicators(df, result, "")
        
        # 近3年指标
        three_years_ago = datetime.now() - timedelta(days=3*365)
        df_3y = df[df["date"] >= three_years_ago].copy()
        if len(df_3y) >= 2:
            self._calculate_period_indicators(df_3y, result, "近3年")
        
        # 近1年指标
        one_year_ago = datetime.now() - timedelta(days=365)
        df_1y = df[df["date"] >= one_year_ago].copy()
        if len(df_1y) >= 2:
            self._calculate_period_indicators(df_1y, result, "近1年")
        
        # 计算各种时间段的涨跌幅
        self._calculate_returns_for_periods(df, result)
        
        # 计算2024年和2025年涨跌幅
        self._calculate_yearly_returns(df, result)
        
        # 计算月涨跌幅最大异常
        result["月涨跌幅最大异常"] = self._calculate_max_monthly_return_anomaly(df)
        
        # 填充其他信息（这些信息应从其他数据源获取，这里暂时用默认值）
        result["封闭类型"] = "开放式"  # 默认为开放式
        result["封闭长度"] = ""
        result["状态"] = "正常"
        result["基金买卖信息"] = ""
        result["最近更新日期"] = datetime.now().strftime("%Y-%m-%d")
        result["类别"] = "股票型"  # 默认为股票型
        result["KenChoice"] = ""
        result["KenComment"] = ""
        
        # 计算近一月涨跌幅范围
        result["近一月涨跌幅范围"] = self._calculate_monthly_return_range(df)
        
        return result
    
    def _calculate_period_indicators(self, df, result, prefix=""):
        """计算指定时间段的指标"""
        # 上涨季度比例
        if len(df) >= 60:  # 至少需要2个季度的数据
            result[f"{prefix}上涨季度比例"] = round(self._calculate_positive_quarters_ratio(df), 2)
        
        # 上涨月份比例
        if len(df) >= 20:  # 至少需要2个月的数据
            result[f"{prefix}上涨月份比例"] = round(self._calculate_positive_months_ratio(df), 2)
        
        # 上涨星期比例
        result[f"{prefix}上涨星期比例"] = round(self._calculate_positive_weeks_ratio(df), 2)
        
        # 季涨跌幅标准差
        if len(df) >= 60:
            result[f"{prefix}季涨跌幅标准差"] = round(self._calculate_quarterly_volatility(df), 2)
        
        # 月涨跌幅标准差
        if len(df) >= 20:
            result[f"{prefix}月涨跌幅标准差"] = round(self._calculate_monthly_volatility(df), 2)
        
        # 周涨跌幅标准差
        result[f"{prefix}周涨跌幅标准差"] = round(self._calculate_weekly_volatility(df), 2)
        
        # 年化收益率
        result[f"{prefix}年化收益率"] = round(self._calculate_annualized_return(df), 2)
        
        # 最大回撤率
        result[f"{prefix}最大回撤率"] = round(self._calculate_max_drawdown(df), 2)
        
        # 第二大回撤
        result[f"{prefix}第二大回撤"] = round(self._calculate_second_max_drawdown(df), 2)
        
        # 夏普率
        result[f"{prefix}夏普率"] = round(self._calculate_sharpe_ratio(df), 2)
        
        # 卡玛率
        result[f"{prefix}卡玛率"] = round(self._calculate_calmar_ratio(df), 2)
        
        # OLS离散系数
        result[f"{prefix}OLS离散系数"] = round(self._calculate_ols_dispersion(df), 2)
    
    def _calculate_returns_for_periods(self, df, result):
        """计算各种时间段的涨跌幅"""
        # 计算前N月涨跌幅
        for months in [1, 2, 3]:
            months_ago = datetime.now() - timedelta(days=months*30)
            df_period = df[df["date"] >= months_ago].copy()
            if len(df_period) >= 2:
                start_price = df_period["close"].iloc[0]
                end_price = df_period["close"].iloc[-1]
                return_pct = ((end_price - start_price) / start_price) * 100
                result[f"前{months}月涨跌幅"] = round(return_pct, 2)
        
        # 计算前N季涨跌幅
        for quarters in [2, 3, 4]:
            days_ago = quarters * 90
            period_ago = datetime.now() - timedelta(days=days_ago)
            df_period = df[df["date"] >= period_ago].copy()
            if len(df_period) >= 2:
                start_price = df_period["close"].iloc[0]
                end_price = df_period["close"].iloc[-1]
                return_pct = ((end_price - start_price) / start_price) * 100
                result[f"前{quarters}季涨跌幅"] = round(return_pct, 2)
        
        # 计算近N时间段涨跌幅
        periods = {
            "近1周涨跌幅": 7,
            "近1月涨跌幅": 30,
            "近3月涨跌幅": 90,
            "近6月涨跌幅": 180,
            "近1年涨跌幅": 365
        }
        
        for key, days in periods.items():
            period_ago = datetime.now() - timedelta(days=days)
            df_period = df[df["date"] >= period_ago].copy()
            if len(df_period) >= 2:
                start_price = df_period["close"].iloc[0]
                end_price = df_period["close"].iloc[-1]
                return_pct = ((end_price - start_price) / start_price) * 100
                result[key] = round(return_pct, 2)
    
    def _calculate_yearly_returns(self, df, result):
        """计算2024年和2025年的涨跌幅"""
        # 2024年涨跌幅
        df_2024 = df[(df["date"].dt.year == 2024)].copy()
        if len(df_2024) >= 2:
            start_price = df_2024["close"].iloc[0]
            end_price = df_2024["close"].iloc[-1]
            return_pct = ((end_price - start_price) / start_price) * 100
            result["2024年涨跌幅"] = round(return_pct, 2)
        
        # 2025年涨跌幅
        df_2025 = df[(df["date"].dt.year == 2025)].copy()
        if len(df_2025) >= 2:
            start_price = df_2025["close"].iloc[0]
            end_price = df_2025["close"].iloc[-1]
            return_pct = ((end_price - start_price) / start_price) * 100
            result["2025年涨跌幅"] = round(return_pct, 2)
    
    def _calculate_max_monthly_return_anomaly(self, df):
        """计算月涨跌幅最大异常值"""
        monthly_returns = self._calculate_monthly_returns(df)
        if len(monthly_returns) < 12:  # 至少需要1年的数据
            return 0
        
        # 计算均值和标准差
        mean = monthly_returns.mean()
        std = monthly_returns.std()
        
        if std == 0:
            return 0
        
        # 计算Z-score
        z_scores = (monthly_returns - mean) / std
        
        # 返回最大绝对Z-score
        return round(z_scores.abs().max(), 2)
    
    def _calculate_monthly_return_range(self, df):
        """计算近一月涨跌幅范围"""
        one_month_ago = datetime.now() - timedelta(days=30)
        df_month = df[df["date"] >= one_month_ago].copy()
        
        if len(df_month) < 2:
            return ""
        
        min_return = df_month["daily_return"].min() * 100
        max_return = df_month["daily_return"].max() * 100
        
        return f"{min_return:.2f}%~{max_return:.2f}%"
    
    def _calculate_annualized_return(self, df):
        """计算年化收益率"""
        if len(df) < 2:
            return 0.0
        
        # 计算总收益率
        start_price = df["close"].iloc[0]
        end_price = df["close"].iloc[-1]
        total_return = (end_price - start_price) / start_price
        
        # 计算投资期限（年）
        days_diff = (df["date"].iloc[-1] - df["date"].iloc[0]).days
        years_diff = days_diff / 365.0
        
        # 如果投资期限小于0.1年，返回总收益率
        if years_diff < 0.1:
            return total_return * 100
        
        # 计算年化收益率
        annualized_return = ((1 + total_return) ** (1 / years_diff)) - 1
        
        return annualized_return * 100  # 转换为百分比
    
    def _calculate_positive_quarters_ratio(self, df):
        """计算上涨季度比例"""
        if len(df) < 2:
            return 0.0
        
        # 按季分组并计算季收益率
        df_quarterly = df.set_index("date").resample("Q").last()
        df_quarterly["quarterly_return"] = df_quarterly["close"].pct_change()
        
        # 计算季收益率为正的季度数
        if len(df_quarterly) < 2:
            return 0.0
        
        positive_quarters = (df_quarterly["quarterly_return"] > 0).sum()
        
        return (positive_quarters / (len(df_quarterly) - 1)) * 100
    
    def _calculate_positive_months_ratio(self, df):
        """计算上涨月份比例"""
        if len(df) < 2:
            return 0.0
        
        # 按月分组并计算月收益率
        df_monthly = df.set_index("date").resample("M").last()
        df_monthly["monthly_return"] = df_monthly["close"].pct_change()
        
        # 计算月收益率为正的月份数
        if len(df_monthly) < 2:
            return 0.0
        
        positive_months = (df_monthly["monthly_return"] > 0).sum()
        
        return (positive_months / (len(df_monthly) - 1)) * 100
    
    def _calculate_positive_weeks_ratio(self, df):
        """计算上涨星期比例"""
        if len(df) < 2:
            return 0.0
        
        # 按周分组并计算周收益率
        df_weekly = df.set_index("date").resample("W").last()
        df_weekly["weekly_return"] = df_weekly["close"].pct_change()
        
        # 计算周收益率为正的周数
        if len(df_weekly) < 2:
            return 0.0
        
        positive_weeks = (df_weekly["weekly_return"] > 0).sum()
        
        return (positive_weeks / (len(df_weekly) - 1)) * 100
    
    def _calculate_quarterly_volatility(self, df):
        """计算季涨跌幅标准差"""
        quarterly_returns = self._calculate_quarterly_returns(df)
        if len(quarterly_returns) < 2:
            return 0.0
        
        return quarterly_returns.std() * 100  # 转换为百分比
    
    def _calculate_monthly_volatility(self, df):
        """计算月涨跌幅标准差"""
        monthly_returns = self._calculate_monthly_returns(df)
        if len(monthly_returns) < 2:
            return 0.0
        
        return monthly_returns.std() * 100  # 转换为百分比
    
    def _calculate_weekly_volatility(self, df):
        """计算周涨跌幅标准差"""
        weekly_returns = self._calculate_weekly_returns(df)
        if len(weekly_returns) < 2:
            return 0.0
        
        return weekly_returns.std() * 100  # 转换为百分比
    
    def _calculate_max_drawdown(self, df):
        """计算最大回撤率"""
        if len(df) < 2:
            return 0.0
        
        # 计算累计净值
        cumulative_nav = df["close"] / df["close"].iloc[0]
        
        # 计算累计最大值
        running_max = cumulative_nav.cummax()
        
        # 计算回撤率
        drawdown = (cumulative_nav - running_max) / running_max
        
        # 计算最大回撤率（绝对值）
        max_drawdown = drawdown.min()
        
        return abs(max_drawdown) * 100  # 转换为百分比
    
    def _calculate_second_max_drawdown(self, df):
        """计算第二大回撤率"""
        if len(df) < 2:
            return 0.0
        
        # 计算累计净值
        cumulative_nav = df["close"] / df["close"].iloc[0]
        
        # 计算累计最大值
        running_max = cumulative_nav.cummax()
        
        # 计算回撤率
        drawdown = (cumulative_nav - running_max) / running_max
        
        # 找出所有回撤的结束点（即新的高点出现前）
        peak_indices = []
        current_max = drawdown.iloc[0]
        
        for i in range(1, len(drawdown)):
            if drawdown.iloc[i] > current_max:
                current_max = drawdown.iloc[i]
                peak_indices.append(i - 1)  # 前一个点是回撤的结束点
        
        # 添加最后一个点
        peak_indices.append(len(drawdown) - 1)
        
        # 提取所有回撤谷值
        drawdown_values = []
        for i in range(len(peak_indices) - 1):
            start_idx = peak_indices[i]
            end_idx = peak_indices[i + 1]
            if end_idx > start_idx:
                drawdown_segment = drawdown.iloc[start_idx : end_idx + 1]
                drawdown_values.append(drawdown_segment.min())
        
        # 如果没有足够的回撤段，返回最大回撤的70%
        if len(drawdown_values) < 2:
            max_dd = drawdown.min()
            return abs(max_dd * 0.7) * 100
        
        # 排序并返回第二大的回撤率
        drawdown_values.sort()
        second_max_drawdown = drawdown_values[1] if len(drawdown_values) > 1 else drawdown_values[0]
        
        return abs(second_max_drawdown) * 100  # 转换为百分比
    
    def _calculate_sharpe_ratio(self, df):
        """计算夏普率"""
        if len(df) < 2:
            return 0.0
        
        # 计算日收益率均值
        daily_return_mean = df["daily_return"].mean()
        
        # 计算日收益率标准差
        daily_return_std = df["daily_return"].std()
        
        if daily_return_std == 0:
            return 0.0
        
        # 计算年化夏普率
        sharpe_ratio = (daily_return_mean * self.trading_days_per_year - self.risk_free_rate) / \
                      (daily_return_std * math.sqrt(self.trading_days_per_year))
        
        return sharpe_ratio
    
    def _calculate_calmar_ratio(self, df):
        """计算卡玛比率"""
        if len(df) < 2:
            return 0.0
        
        # 计算年化收益率
        annualized_return = self._calculate_annualized_return(df) / 100  # 转换为小数
        
        # 计算最大回撤率
        max_drawdown = self._calculate_max_drawdown(df) / 100  # 转换为小数
        
        if max_drawdown == 0:
            return 0.0
        
        # 卡玛比率 = 年化收益率 / 最大回撤率
        calmar_ratio = annualized_return / max_drawdown
        
        return calmar_ratio
    
    def _calculate_ols_dispersion(self, df):
        """计算OLS离散系数（回归分析的残差标准差）"""
        if len(df) < 30:  # 至少需要30个数据点进行回归分析
            return 0.0
        
        # 创建时间序列索引（x轴）
        x = np.arange(len(df))
        
        # 对数收益率（用于线性回归更合适）
        log_returns = np.log(df["close"] / df["close"].shift(1)).dropna()
        
        if len(log_returns) < 30:
            return 0.0
        
        # 进行线性回归
        slope, intercept, r_value, p_value, std_err = stats.linregress(x[1:], log_returns)
        
        # 计算预测值
        predictions = intercept + slope * x[1:]
        
        # 计算残差
        residuals = log_returns - predictions
        
        # 计算残差的标准差（OLS离散系数）
        ols_dispersion = residuals.std()
        
        return ols_dispersion * 100  # 转换为百分比
    
    def _calculate_monthly_returns(self, df):
        """计算月度收益率"""
        if len(df) < 2:
            return pd.Series()
        
        # 按月重采样并计算月度收益率
        monthly_prices = df.resample("ME", on="date")["close"].last()
        monthly_returns = monthly_prices.pct_change()
        return monthly_returns
    
    def _calculate_quarterly_returns(self, df):
        """计算季度收益率"""
        if len(df) < 2:
            return pd.Series()
        
        # 按季度重采样并计算季度收益率
        quarterly_prices = df.resample("QE", on="date")["close"].last()
        quarterly_returns = quarterly_prices.pct_change()
        return quarterly_returns
    
    def _calculate_weekly_returns(self, df):
        """计算周收益率"""
        if len(df) < 2:
            return pd.Series()
        
        # 按周重采样并计算周收益率
        weekly_prices = df.resample("W", on="date")["close"].last()
        weekly_returns = weekly_prices.pct_change()
        return weekly_returns
    
    def export_to_excel(self, output_path=None):
        """将量化分析结果导出到Excel文档"""
        # 创建报表目录
        report_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
        if not os.path.exists(report_dir):
            os.makedirs(report_dir)
        
        # 创建Excel文件，使用时间戳作为文件名的一部分
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if output_path:
            excel_file = output_path
        else:
            excel_file = os.path.join(report_dir, f"全面的基金量化分析报表_{timestamp}.xlsx")
        
        print(f"正在生成Excel报表: {excel_file}")
        
        # 创建DataFrame存储结果
        df = pd.DataFrame(self.results, columns=self.required_columns)
        
        # 导出到Excel
        try:
            with pd.ExcelWriter(excel_file, engine="xlsxwriter") as writer:
                df.to_excel(writer, sheet_name="量化分析结果", index=False)
        
                # 获取工作表
                worksheet = writer.sheets["量化分析结果"]
        
                # 设置列宽
                for col_num, col_name in enumerate(df.columns):
                    max_length = max(df[col_name].astype(str).map(len).max(), len(col_name)) + 2
                    worksheet.set_column(col_num, col_num, max_length)
        
                # 添加条件格式
                # 1. 年化收益率为负的单元格标红
                if "年化收益率" in df.columns:
                    annual_return_col = df.columns.get_loc("年化收益率")
                    worksheet.conditional_format(
                        1, annual_return_col, len(df), annual_return_col,
                        {
                            "type": "cell",
                            "criteria": "<",
                            "value": 0,
                            "format": writer.book.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"}),
                        }
                    )
        
                # 2. 夏普率大于1的单元格标绿
                if "夏普率" in df.columns:
                    sharpe_col = df.columns.get_loc("夏普率")
                    worksheet.conditional_format(
                        1, sharpe_col, len(df), sharpe_col,
                        {
                            "type": "cell",
                            "criteria": ">",
                            "value": 1,
                            "format": writer.book.add_format({"bg_color": "#C6EFCE", "font_color": "#006100"}),
                        }
                    )
        
                # 3. 最大回撤率大于20%的单元格标红
                if "最大回撤率" in df.columns:
                    max_drawdown_col = df.columns.get_loc("最大回撤率")
                    worksheet.conditional_format(
                        1, max_drawdown_col, len(df), max_drawdown_col,
                        {
                            "type": "cell",
                            "criteria": ">",
                            "value": 20,
                            "format": writer.book.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"}),
                        }
                    )
        except Exception as e:
            print(f"导出Excel时出错: {str(e)}")
            return None
        
        return excel_file


def main():
    """主函数，用于演示如何使用AdvancedQuantAnalyzer"""
    print("=== 高级基金量化分析系统 ===")
    print("正在初始化系统...")
    
    # 创建高级量化分析器实例
    analyzer = AdvancedQuantAnalyzer()
    
    # 执行分析
    success = analyzer.analyze_all_funds(thread_mode='auto')
    
    if success:
        # 导出结果到Excel
        excel_file = analyzer.export_to_excel()
        if excel_file:
            print(f"\n量化分析完成！结果已导出至: {excel_file}")
    else:
        print("量化分析失败，请检查日志信息")


if __name__ == "__main__":
    main()