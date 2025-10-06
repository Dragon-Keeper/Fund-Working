import numpy as np
import pandas as pd
import os
import h5py
import math
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from scipy import stats
import warnings

# 忽略警告信息
warnings.filterwarnings("ignore")


class QuantAnalyzer:
    """量化分析器，用于基于完整净值时间序列数据计算各种量化指标"""

    def __init__(self, hdf5_path=None, start_date_str=None):
        self.results = {}
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

    def analyze_all_funds(self):
        """分析所有基金"""
        # 获取所有基金代码
        all_fund_codes = self.get_all_fund_codes()
        total_funds = len(all_fund_codes)

        if total_funds == 0:
            print("未找到基金数据，请先确保HDF5文件中包含数据")
            return False

        print(f"共发现 {total_funds} 只基金，开始进行量化分析...")

        # 逐个分析基金
        for i, fund_code in enumerate(all_fund_codes, 1):
            print(f"分析基金 {i}/{total_funds}: {fund_code}")
            try:
                self.analyze_single_fund(fund_code)
            except Exception as e:
                print(f"分析基金 {fund_code} 时出错: {str(e)}")

        return len(self.results) > 0

    def analyze_specific_fund(self, fund_code):
        """分析指定基金"""
        # 尝试分析该基金
        try:
            self.analyze_single_fund(fund_code)
            return True
        except Exception as e:
            print(f"分析基金 {fund_code} 时出错: {str(e)}")
            return False

    def analyze_single_fund(self, fund_code):
        """分析单只基金，计算各项量化指标"""
        # 获取基金名称
        fund_name = self.get_fund_name(fund_code)

        # 读取基金数据
        df = self.read_fund_data(fund_code)
        if df is None or len(df) < 2:
            print(f"基金 {fund_code} 数据不足，跳过分析")
            return

        # 初始化指标字典
        indicators = {}

        # 清理数据，处理异常值
        df_clean = self._clean_data(df)

        # 计算各项指标
        indicators["年化收益率"] = self._calculate_annualized_return(df_clean)
        indicators["上涨日数比例"] = self._calculate_positive_days_ratio(df_clean)
        indicators["上涨月数比例"] = self._calculate_positive_months_ratio(df_clean)
        indicators["上涨季数比例"] = self._calculate_positive_quarters_ratio(df_clean)
        indicators["日涨跌幅标准差"] = self._calculate_daily_volatility(df_clean)
        indicators["月涨跌幅标准差"] = self._calculate_monthly_volatility(df_clean)
        indicators["月涨跌幅最大值比中值倍数"] = (
            self._calculate_max_to_median_monthly_ratio(df_clean)
        )
        indicators["最大回撤率"] = self._calculate_max_drawdown(df_clean)
        indicators["第二大回撤率"] = self._calculate_second_max_drawdown(df_clean)
        indicators["夏普率"] = self._calculate_sharpe_ratio(df_clean)
        indicators["卡玛比率"] = self._calculate_calmar_ratio(df_clean)
        indicators["索提诺比率"] = self._calculate_sortino_ratio(df_clean)
        indicators["信息比率"] = self._calculate_information_ratio(df_clean)

        # 添加时间范围和数据量信息
        indicators["数据起始日期"] = df_clean["date"].min().strftime("%Y-%m-%d")
        indicators["数据结束日期"] = df_clean["date"].max().strftime("%Y-%m-%d")
        indicators["交易日数量"] = len(df_clean)

        # 添加统计显著性检验结果
        significance_results = self._perform_statistical_significance_tests(df_clean)
        indicators.update(significance_results)

        # 将结果保存
        self.results[fund_code] = {"name": fund_name, "indicators": indicators}

    def _clean_data(self, df):
        """清理数据，处理异常值"""
        # 创建副本以避免修改原始数据
        df_clean = df.copy()

        # 处理缺失值
        df_clean = df_clean.dropna(subset=["close", "daily_return"])

        # 使用IQR方法检测和处理异常值
        q1 = df_clean["daily_return"].quantile(0.25)
        q3 = df_clean["daily_return"].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 3 * iqr  # 使用3倍IQR以避免过度过滤
        upper_bound = q3 + 3 * iqr

        # 保留在正常范围内的数据
        df_clean = df_clean[
            (df_clean["daily_return"] >= lower_bound)
            & (df_clean["daily_return"] <= upper_bound)
        ]

        # 重置索引
        df_clean = df_clean.reset_index(drop=True)

        return df_clean

    def _calculate_annualized_return(self, df):
        """基于完整净值时间序列计算年化收益率"""
        # 检查数据是否足够
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

    def _calculate_positive_days_ratio(self, df):
        """计算上涨日数比例"""
        if len(df) == 0:
            return 0.0

        # 计算日收益率为正的天数
        positive_days = (df["daily_return"] > 0).sum()

        return (positive_days / len(df)) * 100

    def _calculate_positive_months_ratio(self, df):
        """计算上涨月数比例"""
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

    def _calculate_positive_quarters_ratio(self, df):
        """计算上涨季数比例"""
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

    def _calculate_daily_volatility(self, df):
        """计算日涨跌幅标准差"""
        if len(df) < 2:
            return 0.0

        return df["daily_return"].std() * 100  # 转换为百分比

    def _calculate_monthly_volatility(self, df):
        """计算月涨跌幅标准差"""
        if len(df) < 2:
            return 0.0

        # 按月分组并计算月收益率
        df_monthly = df.set_index("date").resample("M").last()
        df_monthly["monthly_return"] = df_monthly["close"].pct_change()

        if len(df_monthly) < 2:
            return 0.0

        return df_monthly["monthly_return"].std() * 100  # 转换为百分比

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
        second_max_drawdown = (
            drawdown_values[1] if len(drawdown_values) > 1 else drawdown_values[0]
        )

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
        # (年化收益率 - 无风险收益率) / 年化波动率
        sharpe_ratio = (
            daily_return_mean * self.trading_days_per_year - self.risk_free_rate
        ) / (daily_return_std * math.sqrt(self.trading_days_per_year))

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

    def _calculate_sortino_ratio(self, df):
        """计算索提诺比率"""
        if len(df) < 2:
            return 0.0

        # 计算日收益率均值
        daily_return_mean = df["daily_return"].mean()

        # 计算下行收益率
        downside_returns = df[df["daily_return"] < 0]["daily_return"]

        if len(downside_returns) == 0:
            return 0.0

        # 计算下行波动率
        downside_std = downside_returns.std()

        if downside_std == 0:
            return 0.0

        # 计算年化索提诺比率
        # (年化收益率 - 无风险收益率) / 年化下行波动率
        sortino_ratio = (
            daily_return_mean * self.trading_days_per_year - self.risk_free_rate
        ) / (downside_std * math.sqrt(self.trading_days_per_year))

        return sortino_ratio

    def _calculate_information_ratio(self, df):
        """计算信息比率"""
        if len(df) < 2:
            return 0.0

        # 计算日收益率
        daily_returns = df["daily_return"]

        # 假设基准收益率为0（简化处理）
        # 实际应用中应使用合适的基准指数
        excess_returns = daily_returns

        # 计算超额收益率的均值
        excess_return_mean = excess_returns.mean()

        # 计算跟踪误差（超额收益率的标准差）
        tracking_error = excess_returns.std()

        if tracking_error == 0:
            return 0.0

        # 计算年化信息比率
        information_ratio = (excess_return_mean * self.trading_days_per_year) / (
            tracking_error * math.sqrt(self.trading_days_per_year)
        )

        return information_ratio

    def _perform_statistical_significance_tests(self, df):
        """对计算结果进行统计显著性检验"""
        results = {}

        if len(df) < 30:  # 样本量不足，不进行检验
            results["统计显著性"] = "样本量不足"
            return results

        # 对年化收益率进行t检验，检验是否显著大于0
        daily_returns = df["daily_return"].dropna()

        if len(daily_returns) >= 30:
            # 单样本t检验，检验均值是否显著大于0
            t_stat, p_value = stats.ttest_1samp(daily_returns, 0, alternative="greater")

            results["收益率t统计量"] = t_stat
            results["收益率p值"] = p_value
            results["收益率显著性"] = "显著" if p_value < 0.05 else "不显著"

        # 计算置信区间（95%）
        mean_return = daily_returns.mean()
        std_error = stats.sem(daily_returns)
        confidence_interval = stats.t.interval(
            0.95, len(daily_returns) - 1, loc=mean_return, scale=std_error
        )

        # 转换为年化数据
        annualized_ci_lower = (
            confidence_interval[0] * self.trading_days_per_year
        ) * 100
        annualized_ci_upper = (
            confidence_interval[1] * self.trading_days_per_year
        ) * 100

        results["年化收益率95%置信区间"] = (
            f"[{annualized_ci_lower:.2f}%, {annualized_ci_upper:.2f}%]"
        )

        # 检验数据正态性（使用Shapiro-Wilk检验）
        if len(daily_returns) <= 5000:  # Shapiro-Wilk检验对大样本敏感
            sw_stat, sw_p_value = stats.shapiro(daily_returns)
            results["正态性检验统计量"] = sw_stat
            results["正态性检验p值"] = sw_p_value
            results["数据正态性"] = (
                "符合正态分布" if sw_p_value > 0.05 else "不符合正态分布"
            )

        return results

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

    def _calculate_monthly_volatility(self, df):
        """计算月涨跌幅标准差"""
        monthly_returns = self._calculate_monthly_returns(df)
        if len(monthly_returns) < 2:
            return 0.0

        return monthly_returns.std() * 100  # 转换为百分比

    def _calculate_max_to_median_monthly_ratio(self, df):
        """计算月涨跌幅最大值比中值倍数"""
        monthly_returns = self._calculate_monthly_returns(df)
        if len(monthly_returns) < 2:
            return 0.0

        # 只考虑正收益率用于计算最大值与中值的比率
        positive_monthly_returns = monthly_returns[monthly_returns > 0]
        if len(positive_monthly_returns) == 0:
            return 0.0

        max_return = positive_monthly_returns.max()
        median_return = positive_monthly_returns.median()

        if median_return == 0:
            return 0.0

        return max_return / median_return

    def _get_fund_info_from_multiple_sources(self, fund_code):
        """从多个HDF5数据源获取基金信息，增强版"""
        # 详细记录数据获取过程的日志
        print(f"获取基金代码 {fund_code} 的详细信息...")
        fund_info = {}
        root_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(root_dir, "data")

        # 确保处理所有需要的列，先初始化所有列的值为空字符串
        required_columns = [
            "基金简称",
            "基金类型",
            "万份收益",
            "7日年化收益率",
            "14日年化收益率",
            "28日年化收益率",
            "近1月增长率",
            "近3月增长率",
            "近6月增长率",
            "近1年增长率",
            "近2年增长率",
            "近3年增长率",
            "近5年增长率",
            "今年来增长率",
            "成立来增长率",
        ]

        for col in required_columns:
            fund_info[col] = ""

        # 存储映射关系，用于将不同数据源的字段映射到统一的中文表头
        field_mappings = {
            "per_10k_return": "万份收益",
            "latest_10k_profit": "万份收益",
            "seven_day_annualized": "7日年化收益率",
            "latest_7day_annual": "7日年化收益率",
            "fourteen_day_annualized": "14日年化收益率",
            "twenty_eight_day_annualized": "28日年化收益率",
            "month_growth": "近1月增长率",
            "quarter_growth": "近3月增长率",
            "half_year_growth": "近6月增长率",
            "year_growth": "近1年增长率",
            "two_year_growth": "近2年增长率",
            "three_year_growth": "近3年增长率",
            "five_year_growth": "近5年增长率",
            "year_to_date_growth": "今年来增长率",
            "since_establishment_growth": "成立来增长率",
            "fund_name": "基金简称",
            "fund_type": "基金类型",
        }

        # 1. 从Fund_Purchase_Status_Manager获取数据
        try:
            import Fund_Purchase_Status_Manager

            hdf5_path = Fund_Purchase_Status_Manager.get_hdf5_path()
            if os.path.exists(hdf5_path):
                print(f"  尝试从Fund_Purchase_Status_Manager ({hdf5_path}) 获取数据")
                # 尝试直接使用pandas读取HDF5文件
                df = pd.read_hdf(hdf5_path, key="fund_purchase_status")
                fund_row = df[df["基金代码"] == fund_code]
                if not fund_row.empty:
                    for col in required_columns:
                        if col in fund_row.columns and pd.notna(fund_row.iloc[0][col]):
                            # 不再检查是否为空，确保获取最新数据
                            fund_info[col] = fund_row.iloc[0][col]
        except Exception as e:
            print(f"从Fund_Purchase_Status_Manager获取数据时出错: {str(e)}")

        # 2. 从CNJY_Fund_Data.h5获取数据
        cnjy_file = os.path.join(data_dir, "CNJY_Fund_Data.h5")
        try:
            if os.path.exists(cnjy_file):
                print(f"  尝试从CNJY_Fund_Data.h5 ({cnjy_file}) 获取数据")
                with h5py.File(cnjy_file, "r") as f:
                    if "funds" in f and fund_code in f["funds"]:
                        fund_group = f["funds"][fund_code]
                        for key, value in fund_group.attrs.items():
                            if isinstance(value, bytes):
                                try:
                                    decoded_value = value.decode("utf-8")
                                except:
                                    decoded_value = str(value)
                            else:
                                decoded_value = value

                            # 使用映射关系获取中文表头
                            if key in field_mappings:
                                chinese_column = field_mappings[key]
                                # 不再检查是否为空，确保获取最新数据
                                fund_info[chinese_column] = decoded_value
        except Exception as e:
            print(f"从CNJY_Fund_Data.h5获取数据时出错: {str(e)}")

        # 3. 从Currency_Fund_Data.h5获取数据
        currency_file = os.path.join(data_dir, "Currency_Fund_Data.h5")
        try:
            if os.path.exists(currency_file):
                print(f"  尝试从Currency_Fund_Data.h5 ({currency_file}) 获取数据")
                with h5py.File(currency_file, "r") as f:
                    if "funds" in f and fund_code in f["funds"]:
                        fund_group = f["funds"][fund_code]
                        for key, value in fund_group.attrs.items():
                            if isinstance(value, bytes):
                                try:
                                    decoded_value = value.decode("utf-8")
                                except:
                                    decoded_value = str(value)
                            else:
                                decoded_value = value

                            # 使用映射关系获取中文表头
                            if key in field_mappings:
                                chinese_column = field_mappings[key]
                                # 不再检查是否为空，确保获取最新数据
                                fund_info[chinese_column] = decoded_value
        except Exception as e:
            print(f"从Currency_Fund_Data.h5获取数据时出错: {str(e)}")

        # 4. 从HBX_Fund_Ranking_Data.h5获取数据
        hbx_file = os.path.join(data_dir, "HBX_Fund_Ranking_Data.h5")
        try:
            if os.path.exists(hbx_file):
                print(f"  尝试从HBX_Fund_Ranking_Data.h5 ({hbx_file}) 获取数据")
                with h5py.File(hbx_file, "r") as f:
                    if "funds" in f and fund_code in f["funds"]:
                        fund_group = f["funds"][fund_code]
                        for key, value in fund_group.attrs.items():
                            if isinstance(value, bytes):
                                try:
                                    decoded_value = value.decode("utf-8")
                                except:
                                    decoded_value = str(value)
                            else:
                                decoded_value = value

                            # 使用映射关系获取中文表头
                            if key in field_mappings:
                                chinese_column = field_mappings[key]
                                # 不再检查是否为空，确保获取最新数据
                                fund_info[chinese_column] = decoded_value
        except Exception as e:
            print(f"从HBX_Fund_Ranking_Data.h5获取数据时出错: {str(e)}")

        # 5. 从Fetch_Fund_Data.h5获取数据（额外数据源）
        fetch_fund_file = os.path.join(data_dir, "Fetch_Fund_Data.h5")
        try:
            if os.path.exists(fetch_fund_file):
                print(f"  尝试从Fetch_Fund_Data.h5 ({fetch_fund_file}) 获取数据")
                with h5py.File(fetch_fund_file, "r") as f:
                    if "funds" in f and fund_code in f["funds"]:
                        fund_group = f["funds"][fund_code]
                        for key, value in fund_group.attrs.items():
                            if isinstance(value, bytes):
                                try:
                                    decoded_value = value.decode("utf-8")
                                except:
                                    decoded_value = str(value)
                            else:
                                decoded_value = value

                            # 使用映射关系获取中文表头
                            if key in field_mappings:
                                chinese_column = field_mappings[key]
                                # 不再检查是否为空，确保获取最新数据
                                fund_info[chinese_column] = decoded_value
        except Exception as e:
            print(f"从Fetch_Fund_Data.h5获取数据时出错: {str(e)}")

        # 6. 从Open_Fund_Ranking_Data.h5获取数据（额外数据源）
        open_fund_file = os.path.join(data_dir, "Open_Fund_Ranking_Data.h5")
        try:
            if os.path.exists(open_fund_file):
                print(f"  尝试从Open_Fund_Ranking_Data.h5 ({open_fund_file}) 获取数据")
                with h5py.File(open_fund_file, "r") as f:
                    if "funds" in f and fund_code in f["funds"]:
                        fund_group = f["funds"][fund_code]
                        for key, value in fund_group.attrs.items():
                            if isinstance(value, bytes):
                                try:
                                    decoded_value = value.decode("utf-8")
                                except:
                                    decoded_value = str(value)
                            else:
                                decoded_value = value

                            # 使用映射关系获取中文表头
                            if key in field_mappings:
                                chinese_column = field_mappings[key]
                                # 不再检查是否为空，确保获取最新数据
                                fund_info[chinese_column] = decoded_value
        except Exception as e:
            print(f"从Open_Fund_Ranking_Data.h5获取数据时出错: {str(e)}")

        # 替换空字符串为'N/A'
        for col in fund_info:
            if fund_info[col] == "":
                fund_info[col] = "N/A"

        # 打印获取到的数据摘要
        print(f"  获取到的基金信息: {fund_code} - {fund_info['基金简称']}")
        for col in required_columns[2:]:  # 跳过基金简称和基金类型
            if fund_info[col] != "N/A":
                print(f"    {col}: {fund_info[col]}")

        return fund_info

    def export_to_excel(
        self, output_path=None, is_all_funds=False, specific_fund_code=None
    ):
        """将量化分析结果导出到Excel文档"""
        # 创建报表目录
        report_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
        if not os.path.exists(report_dir):
            os.makedirs(report_dir)

        # 创建Excel文件，根据不同功能生成不同格式的文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if output_path:
            excel_file = output_path
        else:
            if is_all_funds:
                # 分析所有基金时的文件名格式
                excel_file = os.path.join(
                    report_dir, f"All_Fund_量化分析结果_{timestamp}.xlsx"
                )
            elif specific_fund_code:
                # 分析特定基金时的文件名格式
                excel_file = os.path.join(
                    report_dir, f"{specific_fund_code}_量化分析结果_{timestamp}.xlsx"
                )
            else:
                # 默认文件名格式
                excel_file = os.path.join(
                    report_dir, f"基金量化分析报表_{timestamp}.xlsx"
                )

        # 打印当前正在生成的文件名
        print(f"正在生成Excel报表: {excel_file}")

        # 创建DataFrame存储结果
        data = []
        columns = ["基金代码", "基金简称"]

        # 添加用户要求的基金信息列
        fund_info_columns = [
            "基金类型",
            "万份收益",
            "7日年化收益率",
            "14日年化收益率",
            "28日年化收益率",
            "近1月增长率",
            "近3月增长率",
            "近6月增长率",
            "近1年增长率",
            "近2年增长率",
            "近3年增长率",
            "近5年增长率",
            "今年来增长率",
            "成立来增长率",
        ]
        columns.extend(fund_info_columns)

        # 获取所有唯一的指标名称
        all_indicators = set()
        for fund_code, fund_info in self.results.items():
            all_indicators.update(fund_info["indicators"].keys())

        # 按重要性排序指标，并确保新增的指标位置正确
        important_indicators = [
            "年化收益率",
            "夏普率",
            "卡玛比率",
            "最大回撤率",
            "第二大回撤率",
            "上涨日数比例",
            "上涨月数比例",
            "上涨季数比例",
            "日涨跌幅标准差",
            "月涨跌幅标准差",
            "月涨跌幅最大值比中值倍数",
        ]
        sorted_indicators = important_indicators + [
            ind for ind in all_indicators if ind not in important_indicators
        ]
        columns.extend(sorted_indicators)

        # 填充数据
        for fund_code, fund_info in self.results.items():
            # 从多个数据源获取基金信息
            detailed_fund_info = self._get_fund_info_from_multiple_sources(fund_code)

            # 获取基金简称，如果没有则使用基金名称
            fund_short_name = detailed_fund_info.get("基金简称", fund_info["name"])

            row = [fund_code, fund_short_name]

            # 添加基金信息列
            for info_col in fund_info_columns:
                row.append(detailed_fund_info.get(info_col, "N/A"))

            for indicator in sorted_indicators:
                row.append(fund_info["indicators"].get(indicator, "N/A"))
            data.append(row)

        # 创建DataFrame
        df = pd.DataFrame(data, columns=columns)

        # 导出到Excel
        try:
            with pd.ExcelWriter(excel_file, engine="xlsxwriter") as writer:
                df.to_excel(writer, sheet_name="量化分析结果", index=False)

                # 获取工作表
                worksheet = writer.sheets["量化分析结果"]

                # 设置列宽
                for col_num, col_name in enumerate(df.columns):
                    max_length = (
                        max(df[col_name].astype(str).map(len).max(), len(col_name)) + 2
                    )
                    worksheet.set_column(col_num, col_num, max_length)

                # 添加条件格式
                # 1. 年化收益率为负的单元格标红
                if "年化收益率" in df.columns:
                    annual_return_col = df.columns.get_loc("年化收益率")
                    worksheet.conditional_format(
                        1,
                        annual_return_col,
                        len(df),
                        annual_return_col,
                        {
                            "type": "cell",
                            "criteria": "<",
                            "value": 0,
                            "format": writer.book.add_format(
                                {"bg_color": "#FFC7CE", "font_color": "#9C0006"}
                            ),
                        },
                    )

                # 2. 夏普率大于1的单元格标绿
                if "夏普率" in df.columns:
                    sharpe_col = df.columns.get_loc("夏普率")
                    worksheet.conditional_format(
                        1,
                        sharpe_col,
                        len(df),
                        sharpe_col,
                        {
                            "type": "cell",
                            "criteria": ">",
                            "value": 1,
                            "format": writer.book.add_format(
                                {"bg_color": "#C6EFCE", "font_color": "#006100"}
                            ),
                        },
                    )

                # 3. 最大回撤率大于20%的单元格标红
                if "最大回撤率" in df.columns:
                    max_drawdown_col = df.columns.get_loc("最大回撤率")
                    worksheet.conditional_format(
                        1,
                        max_drawdown_col,
                        len(df),
                        max_drawdown_col,
                        {
                            "type": "cell",
                            "criteria": ">",
                            "value": 20,
                            "format": writer.book.add_format(
                                {"bg_color": "#FFC7CE", "font_color": "#9C0006"}
                            ),
                        },
                    )
        except Exception as e:
            print(f"导出Excel时出错: {str(e)}")
            return None

        return excel_file


# 验证日期格式是否为YYYYMMDD
def validate_date_format(date_str):
    """验证日期格式是否为YYYYMMDD"""
    if len(date_str) != 8:
        return False
    try:
        # 尝试将字符串转换为日期对象
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        return False


# 显示量化分析菜单
def show_quant_analysis_menu():
    """显示量化分析菜单"""
    print("\n=== 基金量化分析系统 ===")
    print("请选择操作:")
    print("1. 计算并导出所有基金量化指标")
    print("2. 计算并导出指定基金量化指标")
    print(
        "3. 自定义HDF5文件路径（指向基金净值时间序列数据库，默认路径：data/All_Fund_Data.h5）"
    )
    print("0. 退出系统")


# 处理所有基金的量化分析
def handle_all_funds_quant_analysis(analyzer):
    """处理所有基金的量化分析"""
    # 获取用户输入的时间范围
    start_date_str = input(
        "请输入起始日期（YYYYMMDD格式，不输入则使用默认值）: "
    ).strip()

    # 验证日期格式
    if start_date_str:
        if not validate_date_format(start_date_str):
            print("日期格式不正确，请使用YYYYMMDD格式")
            # 无需等待用户输入，直接返回
    else:
        start_date_str = None

    # 如果指定了起始日期，重新创建分析器实例
    if start_date_str:
        analyzer = QuantAnalyzer(analyzer.hdf5_path, start_date_str)

    print("\n正在准备量化分析...")
    print(
        f"时间范围: {'从' + start_date_str + '开始' if start_date_str else '使用默认时间范围'} 至 {datetime.now().strftime('%Y-%m-%d')}"
    )
    print(f"HDF5文件路径: {analyzer.hdf5_path}")

    try:
        # 执行量化分析
        success = analyzer.analyze_all_funds()

        if success:
            # 导出到Excel，使用All_Fund文件名格式
            excel_file = analyzer.export_to_excel(is_all_funds=True)
            if excel_file:
                print(f"\n量化分析完成！结果已导出至: {excel_file}")
        else:
            print("量化分析失败，请检查日志信息")
    except Exception as e:
        print(f"量化分析过程发生错误: {str(e)}")

    # 无需等待用户输入，直接返回


# 处理指定基金的量化分析
def handle_specific_fund_quant_analysis(analyzer):
    """处理指定基金的量化分析"""
    # 获取用户输入的时间范围
    start_date_str = input(
        "请输入起始日期（YYYYMMDD格式，不输入则使用默认值）: "
    ).strip()

    # 验证日期格式
    if start_date_str:
        if not validate_date_format(start_date_str):
            print("日期格式不正确，请使用YYYYMMDD格式")
            # 无需等待用户输入，直接返回
            return
    else:
        start_date_str = None

    # 获取用户输入的基金代码
    fund_code = input("请输入基金代码: ").strip()
    if not fund_code:
        print("请输入有效的基金代码")
        # 无需等待用户输入，直接返回

    # 无论是否指定了起始日期，都创建一个新的分析器实例（确保results字典为空）
    analyzer = QuantAnalyzer(analyzer.hdf5_path, start_date_str)

    print("\n正在准备量化分析...")
    print(f"基金代码: {fund_code}")
    print(
        f"时间范围: {'从' + start_date_str + '开始' if start_date_str else '使用默认时间范围'} 至 {datetime.now().strftime('%Y-%m-%d')}"
    )

    try:
        # 执行量化分析
        success = analyzer.analyze_specific_fund(fund_code)

        if success:
            # 导出到Excel，使用基金代码文件名格式
            excel_file = analyzer.export_to_excel(specific_fund_code=fund_code)
            if excel_file:
                print(f"\n量化分析完成！结果已导出至: {excel_file}")
        else:
            print("量化分析失败，请检查日志信息")
    except Exception as e:
        print(f"量化分析过程发生错误: {str(e)}")

    pass


# 主程序入口
def main():
    """主程序入口"""
    print("=== 基金量化分析系统 ===")
    print("正在初始化系统...")

    # 创建量化分析器实例
    analyzer = QuantAnalyzer()

    while True:
        show_quant_analysis_menu()
        try:
            choice = input("请输入功能选项: ").strip()

            if choice == "0":
                print("谢谢使用基金量化分析系统，再见!")
                break
            elif choice == "1":
                # 计算并导出所有基金量化指标
                handle_all_funds_quant_analysis(analyzer)
            elif choice == "2":
                # 计算并导出指定基金量化指标
                handle_specific_fund_quant_analysis(analyzer)
            elif choice == "3":
                # 自定义HDF5文件路径
                new_path = input("请输入HDF5文件路径: ").strip()
                if new_path and os.path.exists(new_path):
                    analyzer = QuantAnalyzer(new_path)
                    print(f"HDF5文件路径已更新为: {new_path}")
                else:
                    print("无效的文件路径，请检查文件是否存在")
                # 无需等待用户输入，直接返回
            else:
                print("无效的功能选项，请重新输入")
        except KeyboardInterrupt:
            print("\n程序已被用户中断")
            break
        except Exception as e:
            print(f"程序运行发生错误: {str(e)}")


if __name__ == "__main__":
    main()
