#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基金数据Excel报表生成器
基于多个HDF5数据源，逆向生成KQ4_CHN_FND_20250929.xlsm格式的Excel报表
"""

import os
import sys
import pandas as pd
import numpy as np
import h5py
from datetime import datetime, timedelta
import warnings

# 忽略警告信息
warnings.filterwarnings("ignore")

class FundExcelGenerator:
    """
    基金Excel报表生成器，用于从多个HDF5数据源生成指定格式的Excel报表
    """
    
    def __init__(self):
        # 设置数据文件路径
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.data_dir = os.path.join(self.base_dir, "data")
        
        # HDF5文件路径
        self.cnjy_fund_path = os.path.join(self.data_dir, "CNJY_Fund_Data.h5")
        self.fund_status_path = os.path.join(self.data_dir, "Fund_Purchase_Status.h5")
        self.fbs_ranking_path = os.path.join(self.data_dir, "FBS_Fund_Ranking_Data.h5")
        self.all_fund_data_path = os.path.join(self.data_dir, "All_Fund_Data.h5")
        
        # 确保数据目录存在
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            print(f"创建数据目录: {self.data_dir}")
        
        # 初始化结果数据
        self.fund_data = []
        
        # 定义Excel报表所需的列名
        self.required_columns = [
            "基金代码", "基金名称", "基金类型", "最新净值", "累计净值", "日涨跌幅(%)", 
            "周涨跌幅(%)", "月涨跌幅(%)", "季涨跌幅(%)", "年涨跌幅(%)", "成立以来(%)",
            "规模(亿元)", "成立日期", "基金经理", "基金公司", "申购状态", "赎回状态",
            "近一年收益率(%)", "近二年收益率(%)", "近三年收益率(%)", "风险等级", 
            "夏普比率", "最大回撤(%)", "波动率(%)", "贝塔系数", "R平方", "Alpha"
        ]
    
    def _read_hdf5_data(self, file_path, key=None):
        """
        从HDF5文件中读取数据
        """
        if not os.path.exists(file_path):
            print(f"警告: 文件不存在: {file_path}")
            return None
        
        try:
            if key:
                # 对于使用pandas存储的HDF5文件
                return pd.read_hdf(file_path, key=key)
            else:
                # 对于使用h5py直接存储的HDF5文件
                fund_list = []
                with h5py.File(file_path, 'r') as hf:
                    if 'funds' in hf:
                        for fund_code in hf['funds'].keys():
                            fund_group = hf['funds'][fund_code]
                            fund_info = {}
                            # 读取所有属性
                            for attr_key, attr_value in fund_group.attrs.items():
                                if isinstance(attr_value, bytes):
                                    fund_info[attr_key] = attr_value.decode('utf-8')
                                else:
                                    fund_info[attr_key] = attr_value
                            fund_list.append(fund_info)
                return fund_list
        except Exception as e:
            print(f"读取HDF5文件 {file_path} 时出错: {str(e)}")
            return None
    
    def _read_fund_price_data(self, fund_code):
        """
        从All_Fund_Data.h5读取基金价格数据
        """
        if not os.path.exists(self.all_fund_data_path):
            return None
        
        try:
            with h5py.File(self.all_fund_data_path, 'r') as hf:
                if fund_code not in hf:
                    return None
                
                group = hf[fund_code]
                
                # 读取日期和价格数据
                dates = [d.decode('utf-8') for d in group['date'][()]]
                closes = group['close'][()]
                
                # 创建DataFrame
                df = pd.DataFrame({
                    'date': pd.to_datetime(dates),
                    'close': closes
                })
                
                # 按日期排序
                df = df.sort_values('date')
                
                # 计算收益率
                df['daily_return'] = df['close'].pct_change() * 100
                
                return df
        except Exception as e:
            print(f"读取基金 {fund_code} 价格数据时出错: {str(e)}")
            return None
    
    def _calculate_returns(self, price_df):
        """
        计算各种时间段的收益率
        """
        returns = {
            'latest_nav': 0,
            'cumulative_nav': 0,
            'daily_return': 0,
            'weekly_return': 0,
            'monthly_return': 0,
            'quarterly_return': 0,
            'yearly_return': 0,
            'since_inception_return': 0
        }
        
        if price_df is None or len(price_df) < 2:
            return returns
        
        # 最新净值
        returns['latest_nav'] = price_df['close'].iloc[-1]
        
        # 累计净值（假设起始为1）
        returns['cumulative_nav'] = price_df['close'].iloc[-1] / price_df['close'].iloc[0]
        
        # 日涨跌幅
        if len(price_df) >= 2:
            returns['daily_return'] = price_df['daily_return'].iloc[-1]
        
        # 周涨跌幅
        one_week_ago = datetime.now() - timedelta(days=7)
        df_week = price_df[price_df['date'] >= one_week_ago]
        if len(df_week) >= 2:
            returns['weekly_return'] = ((df_week['close'].iloc[-1] - df_week['close'].iloc[0]) / df_week['close'].iloc[0]) * 100
        
        # 月涨跌幅
        one_month_ago = datetime.now() - timedelta(days=30)
        df_month = price_df[price_df['date'] >= one_month_ago]
        if len(df_month) >= 2:
            returns['monthly_return'] = ((df_month['close'].iloc[-1] - df_month['close'].iloc[0]) / df_month['close'].iloc[0]) * 100
        
        # 季涨跌幅
        one_quarter_ago = datetime.now() - timedelta(days=90)
        df_quarter = price_df[price_df['date'] >= one_quarter_ago]
        if len(df_quarter) >= 2:
            returns['quarterly_return'] = ((df_quarter['close'].iloc[-1] - df_quarter['close'].iloc[0]) / df_quarter['close'].iloc[0]) * 100
        
        # 年涨跌幅
        one_year_ago = datetime.now() - timedelta(days=365)
        df_year = price_df[price_df['date'] >= one_year_ago]
        if len(df_year) >= 2:
            returns['yearly_return'] = ((df_year['close'].iloc[-1] - df_year['close'].iloc[0]) / df_year['close'].iloc[0]) * 100
        
        # 成立以来涨跌幅
        returns['since_inception_return'] = ((price_df['close'].iloc[-1] - price_df['close'].iloc[0]) / price_df['close'].iloc[0]) * 100
        
        return returns
    
    def _calculate_risk_metrics(self, price_df):
        """
        计算风险指标
        """
        metrics = {
            'sharpe_ratio': 0,
            'max_drawdown': 0,
            'volatility': 0,
            'beta': 0,
            'r_squared': 0,
            'alpha': 0
        }
        
        if price_df is None or len(price_df) < 30:
            return metrics
        
        # 计算波动率（标准差）
        daily_returns = price_df['daily_return'].dropna()
        if len(daily_returns) > 0:
            metrics['volatility'] = daily_returns.std() * np.sqrt(252)  # 年化波动率
        
        # 计算最大回撤
        cumulative = price_df['close'] / price_df['close'].iloc[0]
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        metrics['max_drawdown'] = abs(drawdown.min()) * 100
        
        # 计算夏普比率（假设无风险收益率为3%）
        risk_free_rate = 3 / 252  # 日无风险收益率
        if metrics['volatility'] > 0:
            excess_return = daily_returns.mean() - risk_free_rate
            metrics['sharpe_ratio'] = (excess_return / (metrics['volatility'] / np.sqrt(252))) * np.sqrt(252)
        
        # 简化计算的beta、r_squared和alpha（实际应用中应该与基准指数比较）
        # 这里使用简化的实现
        metrics['beta'] = 1.0
        metrics['r_squared'] = 0.5
        metrics['alpha'] = 0.0
        
        return metrics
    
    def collect_fund_data(self):
        """
        从多个数据源收集基金数据
        """
        print("开始收集基金数据...")
        
        # 从各个HDF5文件中读取数据
        cnjy_funds = self._read_hdf5_data(self.cnjy_fund_path)
        fund_status = self._read_hdf5_data(self.fund_status_path, key='fund_purchase_status')
        fbs_ranking = self._read_hdf5_data(self.fbs_ranking_path)
        
        # 整合数据
        # 首先从CNJY_Fund_Data.h5获取基金列表
        if cnjy_funds:
            print(f"从CNJY_Fund_Data.h5获取了 {len(cnjy_funds)} 只基金数据")
            
            for fund_info in cnjy_funds:
                try:
                    fund_code = fund_info.get('fund_code', '')
                    if not fund_code:
                        continue
                    
                    # 初始化基金数据
                    fund_data = {col: '' for col in self.required_columns}
                    fund_data['基金代码'] = fund_code
                    fund_data['基金名称'] = fund_info.get('fund_name', fund_code)
                    fund_data['基金类型'] = fund_info.get('fund_type', '未知')
                    
                    # 读取价格数据并计算指标
                    price_df = self._read_fund_price_data(fund_code)
                    if price_df is not None:
                        returns = self._calculate_returns(price_df)
                        risk_metrics = self._calculate_risk_metrics(price_df)
                        
                        # 填充收益率数据
                        fund_data['最新净值'] = round(returns['latest_nav'], 4)
                        fund_data['累计净值'] = round(returns['cumulative_nav'], 4)
                        fund_data['日涨跌幅(%)'] = round(returns['daily_return'], 2)
                        fund_data['周涨跌幅(%)'] = round(returns['weekly_return'], 2)
                        fund_data['月涨跌幅(%)'] = round(returns['monthly_return'], 2)
                        fund_data['季涨跌幅(%)'] = round(returns['quarterly_return'], 2)
                        fund_data['年涨跌幅(%)'] = round(returns['yearly_return'], 2)
                        fund_data['成立以来(%)'] = round(returns['since_inception_return'], 2)
                        
                        # 填充风险指标
                        fund_data['夏普比率'] = round(risk_metrics['sharpe_ratio'], 4)
                        fund_data['最大回撤(%)'] = round(risk_metrics['max_drawdown'], 2)
                        fund_data['波动率(%)'] = round(risk_metrics['volatility'], 2)
                        fund_data['贝塔系数'] = round(risk_metrics['beta'], 4)
                        fund_data['R平方'] = round(risk_metrics['r_squared'], 4)
                        fund_data['Alpha'] = round(risk_metrics['alpha'], 4)
                    
                    # 从fund_status获取申购赎回状态
                    if isinstance(fund_status, pd.DataFrame):
                        fund_status_row = fund_status[fund_status['基金代码'] == fund_code]
                        if not fund_status_row.empty:
                            fund_data['申购状态'] = fund_status_row.iloc[0].get('申购状态', '未知')
                            fund_data['赎回状态'] = fund_status_row.iloc[0].get('赎回状态', '未知')
                    
                    # 从FBS排名数据获取额外信息
                    if fbs_ranking:
                        for rank_info in fbs_ranking:
                            if rank_info.get('fund_code') == fund_code:
                                # 填充额外信息
                                break
                    
                    # 添加其他默认信息
                    fund_data['规模(亿元)'] = fund_info.get('scale', 0)
                    fund_data['成立日期'] = fund_info.get('establish_date', '')
                    fund_data['基金经理'] = fund_info.get('manager', '未知')
                    fund_data['基金公司'] = fund_info.get('company', '未知')
                    fund_data['风险等级'] = fund_info.get('risk_level', '未知')
                    
                    self.fund_data.append(fund_data)
                except Exception as e:
                    print(f"处理基金 {fund_code} 数据时出错: {str(e)}")
        
        # 如果CNJY_Fund_Data.h5为空，尝试从All_Fund_Data.h5获取基金代码列表
        if not self.fund_data and os.path.exists(self.all_fund_data_path):
            print("尝试从All_Fund_Data.h5获取基金代码...")
            try:
                with h5py.File(self.all_fund_data_path, 'r') as hf:
                    fund_codes = list(hf.keys())
                    print(f"从All_Fund_Data.h5获取了 {len(fund_codes)} 只基金代码")
                    
                    # 限制处理的基金数量以避免处理时间过长
                    max_funds = 1000
                    fund_codes = fund_codes[:max_funds]
                    
                    for i, fund_code in enumerate(fund_codes, 1):
                        if i % 100 == 0:
                            print(f"处理基金 {i}/{len(fund_codes)}")
                        
                        try:
                            # 初始化基金数据
                            fund_data = {col: '' for col in self.required_columns}
                            fund_data['基金代码'] = fund_code
                            fund_data['基金名称'] = fund_code  # 默认使用代码作为名称
                            
                            # 读取价格数据并计算指标
                            price_df = self._read_fund_price_data(fund_code)
                            if price_df is not None:
                                returns = self._calculate_returns(price_df)
                                risk_metrics = self._calculate_risk_metrics(price_df)
                                
                                # 填充收益率数据
                                fund_data['最新净值'] = round(returns['latest_nav'], 4)
                                fund_data['累计净值'] = round(returns['cumulative_nav'], 4)
                                fund_data['日涨跌幅(%)'] = round(returns['daily_return'], 2)
                                fund_data['周涨跌幅(%)'] = round(returns['weekly_return'], 2)
                                fund_data['月涨跌幅(%)'] = round(returns['monthly_return'], 2)
                                fund_data['季涨跌幅(%)'] = round(returns['quarterly_return'], 2)
                                fund_data['年涨跌幅(%)'] = round(returns['yearly_return'], 2)
                                fund_data['成立以来(%)'] = round(returns['since_inception_return'], 2)
                                
                                # 填充风险指标
                                fund_data['夏普比率'] = round(risk_metrics['sharpe_ratio'], 4)
                                fund_data['最大回撤(%)'] = round(risk_metrics['max_drawdown'], 2)
                                fund_data['波动率(%)'] = round(risk_metrics['volatility'], 2)
                                
                            self.fund_data.append(fund_data)
                        except Exception as e:
                            print(f"处理基金 {fund_code} 数据时出错: {str(e)}")
            except Exception as e:
                print(f"从All_Fund_Data.h5读取数据时出错: {str(e)}")
        
        print(f"总共收集了 {len(self.fund_data)} 只基金的数据")
        return len(self.fund_data) > 0
    
    def generate_excel(self, output_file=None):
        """
        生成Excel报表
        """
        if not self.fund_data:
            print("错误：没有收集到基金数据，无法生成Excel报表")
            return None
        
        # 设置输出文件路径
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d")
            output_file = os.path.join(self.base_dir, f"KQ4_CHN_FND_{timestamp}.xlsx")
        
        print(f"正在生成Excel报表: {output_file}")
        
        try:
            # 创建DataFrame
            df = pd.DataFrame(self.fund_data, columns=self.required_columns)
            
            # 导出到Excel
            with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
                # 写入数据
                df.to_excel(writer, sheet_name='基金数据', index=False)
                
                # 获取工作表
                worksheet = writer.sheets['基金数据']
                
                # 设置列宽
                for col_num, col_name in enumerate(df.columns):
                    # 估算适当的列宽
                    max_length = max(
                        df[col_name].astype(str).map(len).max(),
                        len(col_name)
                    ) + 2
                    # 限制最大宽度
                    max_length = min(max_length, 50)
                    worksheet.set_column(col_num, col_num, max_length)
                
                # 添加格式
                header_format = writer.book.add_format({
                    'bold': True,
                    'text_wrap': True,
                    'valign': 'top',
                    'fg_color': '#D7E4BC',
                    'border': 1})
                
                # 应用表头格式
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                # 添加条件格式
                # 1. 日涨跌幅为负的标红
                if '日涨跌幅(%)' in df.columns:
                    daily_return_col = df.columns.get_loc('日涨跌幅(%)')
                    worksheet.conditional_format(
                        1, daily_return_col, len(df), daily_return_col,
                        {
                            'type': 'cell',
                            'criteria': '<',
                            'value': 0,
                            'format': writer.book.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'}),
                        }
                    )
                
                # 2. 夏普比率大于1的标绿
                if '夏普比率' in df.columns:
                    sharpe_col = df.columns.get_loc('夏普比率')
                    worksheet.conditional_format(
                        1, sharpe_col, len(df), sharpe_col,
                        {
                            'type': 'cell',
                            'criteria': '>',
                            'value': 1,
                            'format': writer.book.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'}),
                        }
                    )
            
            print(f"Excel报表生成成功: {output_file}")
            return output_file
        except Exception as e:
            print(f"生成Excel报表时出错: {str(e)}")
            return None
    
    def run(self, output_file=None):
        """
        运行整个流程
        """
        print("=== 基金Excel报表生成器 ===")
        
        # 收集数据
        if not self.collect_fund_data():
            print("无法收集基金数据，程序退出")
            return False
        
        # 生成Excel报表
        excel_file = self.generate_excel(output_file)
        
        if excel_file:
            print(f"\n基金Excel报表生成完成！")
            print(f"报表文件: {excel_file}")
            print(f"包含基金数量: {len(self.fund_data)}")
            return True
        else:
            print("生成Excel报表失败")
            return False


def main():
    """
    主函数
    """
    # 创建生成器实例
    generator = FundExcelGenerator()
    
    # 处理命令行参数
    if len(sys.argv) > 1:
        output_file = sys.argv[1]
        generator.run(output_file)
    else:
        # 生成默认文件名
        generator.run()


if __name__ == "__main__":
    main()