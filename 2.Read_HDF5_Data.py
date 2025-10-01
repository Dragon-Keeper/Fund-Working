#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
基金HDF5数据读取与分析系统
用于读取、分析和可视化基金历史数据
优化版本：添加索引机制、查询结果缓存、预计算统计指标、向量化操作、交互式图表和数据导出模块
"""

import os
import sys
import h5py
import numpy as np
import pandas as pd
import time
import pickle
import hashlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from functools import lru_cache
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
# from scipy import stats  # 临时注释掉scipy导入

# 检查并安装必要的依赖
def check_and_install_dependencies():
    """检查并安装必要的依赖"""
    required_packages = ['pandas', 'numpy', 'h5py', 'matplotlib', 'plotly']  # 移除scipy
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"缺少必要的依赖包: {', '.join(missing_packages)}")
        print("正在安装...")
        os.system(f"{sys.executable} -m pip install {' '.join(missing_packages)}")
        print("安装完成，请重新运行程序")
        sys.exit(0)
    
    print("所有依赖包已安装")

# 获取HDF5文件路径
def get_hdf5_path():
    """获取HDF5文件路径"""
    # 获取当前脚本所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 构建HDF5文件路径
    hdf5_path = os.path.join(current_dir, "data", "All_Fund_Data.h5")
    return hdf5_path

# 获取缓存文件路径
def get_cache_path():
    """获取缓存文件路径"""
    # 获取当前脚本所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 构建缓存文件路径
    cache_path = os.path.join(current_dir, "cache")
    # 确保缓存目录存在
    if not os.path.exists(cache_path):
        os.makedirs(cache_path)
    return cache_path

# 创建基金代码索引
def create_fund_index(storage_file):
    """创建基金代码索引，加速查询"""
    index_file = os.path.join(get_cache_path(), "fund_index.pkl")
    
    # 检查索引文件是否存在
    if os.path.exists(index_file):
        try:
            with open(index_file, 'rb') as f:
                index_data = pickle.load(f)
            # 检查索引文件是否是最新的
            file_mtime = os.path.getmtime(storage_file)
            if index_data.get('file_mtime') == file_mtime:
                return index_data['index']
        except:
            pass
    
    # 创建新索引
    print("正在创建基金代码索引...")
    start_time = time.time()
    
    try:
        with h5py.File(storage_file, 'r') as hf:
            # 获取所有基金代码
            fund_codes = list(hf.keys())
            
            # 为每个基金代码创建索引信息
            index = {}
            for code in fund_codes:
                group = hf[code]
                record_count = group.attrs.get('record_count', 0)
                first_date = group['date'][0].decode('utf-8') if record_count > 0 else ""
                last_date = group['date'][-1].decode('utf-8') if record_count > 0 else ""
                
                index[code] = {
                    'record_count': record_count,
                    'first_date': first_date,
                    'last_date': last_date
                }
            
            # 保存索引
            index_data = {
                'file_mtime': os.path.getmtime(storage_file),
                'index': index
            }
            
            with open(index_file, 'wb') as f:
                pickle.dump(index_data, f)
            
            elapsed_time = time.time() - start_time
            print(f"索引创建完成，耗时: {elapsed_time:.2f}秒")
            return index
    except Exception as e:
        print(f"创建索引时出错: {e}")
        return {}

# 获取基金代码索引
def get_fund_index(storage_file):
    """获取基金代码索引"""
    return create_fund_index(storage_file)

# 计算查询哈希值，用于缓存
def calculate_query_hash(storage_file, stock_code, query_type, **kwargs):
    """计算查询哈希值，用于缓存"""
    # 构建查询字符串
    query_str = f"{storage_file}_{stock_code}_{query_type}"
    for key, value in sorted(kwargs.items()):
        query_str += f"_{key}_{value}"
    
    # 计算哈希值
    return hashlib.md5(query_str.encode()).hexdigest()

# 读取单个基金的历史数据（向量化版本）
def read_fund_data_vectorized(storage_file, stock_code):
    """读取单个基金的历史数据（向量化版本）"""
    cache_key = calculate_query_hash(storage_file, stock_code, "read_fund_data")
    cache_file = os.path.join(get_cache_path(), f"{cache_key}.pkl")
    
    # 检查缓存是否存在
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'rb') as f:
                data = pickle.load(f)
            return data
        except:
            pass
    
    try:
        with h5py.File(storage_file, 'r') as hf:
            if stock_code in hf:
                group = hf[stock_code]
                # 读取数据并转换为DataFrame
                dates = [d.decode('utf-8') for d in group['date'][()]]
                
                # 创建DataFrame
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
                
                # 计算涨跌幅
                df['change'] = df['close'] - df['prev_close']
                df['change_pct'] = (df['change'] / df['prev_close']) * 100
                
                # 保存到缓存
                with open(cache_file, 'wb') as f:
                    pickle.dump(df, f)
                
                return df
    except Exception as e:
        print(f"读取数据时出错: {e}")
    return None

# 获取基金基本信息（使用索引）
def get_fund_info_indexed(storage_file, stock_code):
    """获取基金基本信息（使用索引）"""
    # 获取索引
    index = get_fund_index(storage_file)
    
    if stock_code in index:
        return {
            'stock_code': stock_code,
            'record_count': index[stock_code]['record_count'],
            'first_date': index[stock_code]['first_date'],
            'last_date': index[stock_code]['last_date']
        }
    else:
        return None

# 获取所有基金代码（使用索引）
def get_all_fund_codes_indexed(storage_file):
    """获取所有基金代码（使用索引）"""
    # 获取索引
    index = get_fund_index(storage_file)
    return list(index.keys())

# 获取基金在指定日期的数据（向量化版本）
def get_fund_data_on_date_vectorized(storage_file, stock_code, date):
    """获取基金在指定日期的数据（向量化版本）"""
    # 将YYYYMMDD格式转换为YYYY-MM-DD格式
    if len(date) == 8 and date.isdigit():
        year = date[:4]
        month = date[4:6]
        day = date[6:8]
        date = f"{year}-{month}-{day}"
    
    # 获取该基金的所有历史数据
    df = read_fund_data_vectorized(storage_file, stock_code)
    if df is None:
        return None
    
    # 转换日期格式
    query_date = pd.to_datetime(date)
    
    # 查找指定日期的数据
    result = df[df['date'] == query_date]
    
    if result.empty:
        return None
    
    # 返回第一条记录
    record = result.iloc[0]
    return {
        'date': record['date'].strftime('%Y-%m-%d'),
        'open': float(record['open']),
        'high': float(record['high']),
        'low': float(record['low']),
        'close': float(record['close']),
        'amount': float(record['amount']),
        'volume': float(record['volume']),
        'prev_close': float(record['prev_close']),
        'change': float(record['change']),
        'change_pct': float(record['change_pct'])
    }

# 预计算常用统计指标
def precompute_statistics(storage_file, stock_code):
    """预计算常用统计指标"""
    cache_key = calculate_query_hash(storage_file, stock_code, "precompute_statistics")
    cache_file = os.path.join(get_cache_path(), f"{cache_key}.pkl")
    
    # 检查缓存是否存在
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'rb') as f:
                stats = pickle.load(f)
            return stats
        except:
            pass
    
    # 获取基金数据
    df = read_fund_data_vectorized(storage_file, stock_code)
    if df is None:
        return None
    
    try:
        # 计算基本统计指标
        stats = {
            'max_nav': float(df['close'].max()),
            'min_nav': float(df['close'].min()),
            'avg_nav': float(df['close'].mean()),
            'std_nav': float(df['close'].std()),
            'median_nav': float(df['close'].median()),
            'max_change_pct': float(df['change_pct'].max()),
            'min_change_pct': float(df['change_pct'].min()),
            'avg_change_pct': float(df['change_pct'].mean()),
            'positive_days': int((df['change_pct'] > 0).sum()),
            'negative_days': int((df['change_pct'] < 0).sum()),
            'total_days': len(df),
            'max_date': df.loc[df['close'].idxmax(), 'date'].strftime('%Y-%m-%d'),
            'min_date': df.loc[df['close'].idxmin(), 'date'].strftime('%Y-%m-%d')
        }
        
        # 计算年化收益率和波动率
        if len(df) > 1:
            # 计算日收益率
            daily_returns = df['close'].pct_change().dropna()
            
            # 年化收益率（假设一年252个交易日）
            annual_return = (1 + daily_returns.mean()) ** 252 - 1
            
            # 年化波动率
            annual_volatility = daily_returns.std() * np.sqrt(252)
            
            # 夏普比率（假设无风险利率为3%）
            risk_free_rate = 0.03
            sharpe_ratio = (annual_return - risk_free_rate) / annual_volatility if annual_volatility != 0 else 0
            
            stats.update({
                'annual_return': float(annual_return),
                'annual_volatility': float(annual_volatility),
                'sharpe_ratio': float(sharpe_ratio)
            })
        
        # 计算移动平均线
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ma10'] = df['close'].rolling(window=10).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()
        df['ma60'] = df['close'].rolling(window=60).mean()
        
        # 计算布林带
        df['bb_middle'] = df['close'].rolling(window=20).mean()
        df['bb_upper'] = df['bb_middle'] + 2 * df['close'].rolling(window=20).std()
        df['bb_lower'] = df['bb_middle'] - 2 * df['close'].rolling(window=20).std()
        
        # 计算RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # 保存到缓存
        with open(cache_file, 'wb') as f:
            pickle.dump(stats, f)
        
        return stats
    except Exception as e:
        print(f"计算统计指标时出错: {e}")
        return None

# 显示基金历史净值数据概览（使用预计算统计指标）
def display_fund_overview_enhanced(storage_file, stock_code):
    """显示基金历史净值数据概览（使用预计算统计指标）"""
    # 获取基金基本信息
    fund_info = get_fund_info_indexed(storage_file, stock_code)
    if not fund_info:
        print(f"基金代码 {stock_code} 不存在或读取失败")
        return False
    
    # 打印基金基本信息
    print(f"\n基金代码: {fund_info['stock_code']}")
    print(f"记录数量: {fund_info['record_count']} 条")
    print(f"数据范围: {fund_info['first_date']} 至 {fund_info['last_date']}")
    
    # 获取预计算的统计指标
    stats = precompute_statistics(storage_file, stock_code)
    if not stats:
        print("无法计算统计指标")
        return False
    
    # 打印统计信息
    print(f"\n净值统计信息:")
    print(f"最高净值: {stats['max_nav']:.4f} ({stats['max_date']})")
    print(f"最低净值: {stats['min_nav']:.4f} ({stats['min_date']})")
    print(f"平均净值: {stats['avg_nav']:.4f}")
    print(f"净值标准差: {stats['std_nav']:.4f}")
    print(f"净值中位数: {stats['median_nav']:.4f}")
    
    print(f"\n涨跌统计信息:")
    print(f"最大单日涨幅: {stats['max_change_pct']:.2f}%")
    print(f"最大单日跌幅: {stats['min_change_pct']:.2f}%")
    print(f"平均单日涨跌幅: {stats['avg_change_pct']:.2f}%")
    print(f"上涨天数: {stats['positive_days']} 天")
    print(f"下跌天数: {stats['negative_days']} 天")
    print(f"上涨天数占比: {stats['positive_days']/stats['total_days']*100:.1f}%")
    
    if 'annual_return' in stats:
        print(f"\n风险收益指标:")
        print(f"年化收益率: {stats['annual_return']*100:.2f}%")
        print(f"年化波动率: {stats['annual_volatility']*100:.2f}%")
        print(f"夏普比率: {stats['sharpe_ratio']:.2f}")
    
    # 获取基金数据
    df = read_fund_data_vectorized(storage_file, stock_code)
    if df is None:
        print("无法读取基金历史数据")
        return False
    
    # 显示最近几条记录
    print(f"\n最近5条净值记录:")
    print(f"{'-'*90}")
    print(f"{'日期':<12} {'开盘价':<10} {'最高价':<10} {'最低价':<10} {'收盘价(净值)':<15} {'涨跌幅':<10} {'成交额':<10}")
    print(f"{'-'*90}")
    
    # 显示最近5条记录
    for _, record in df.tail(5).iterrows():
        print(f"{record['date'].strftime('%Y-%m-%d'):<12} {record['open']:<10.4f} {record['high']:<10.4f} {record['low']:<10.4f} "
              f"{record['close']:<15.4f} {record['change_pct']:<10.2f}% {record['amount']:<10.2f}")
    
    print(f"{'-'*90}")
    return True

# 显示基金在指定日期的数据（使用向量化版本）
def display_fund_data_on_date_enhanced(storage_file, stock_code, date):
    """显示基金在指定日期的数据（使用向量化版本）"""
    # 获取指定日期的数据
    data = get_fund_data_on_date_vectorized(storage_file, stock_code, date)
    if not data:
        print(f"基金代码 {stock_code} 在 {date} 没有数据或读取失败")
        return False
    
    # 打印数据
    print(f"\n基金代码: {stock_code} 在 {date} 的数据:")
    print(f"{'-'*60}")
    print(f"日期: {data['date']}")
    print(f"开盘价: {data['open']:.4f}")
    print(f"最高价: {data['high']:.4f}")
    print(f"最低价: {data['low']:.4f}")
    print(f"收盘价(净值): {data['close']:.4f}")
    print(f"成交额: {data['amount']:.2f}")
    print(f"成交量: {data['volume']:.2f}")
    print(f"前收盘价: {data['prev_close']:.4f}")
    print(f"{'-'*60}")
    
    # 计算涨跌幅
    print(f"涨跌幅: {data['change']:.4f} ({data['change_pct']:.2f}%)")
    return True

# 创建交互式净值走势图
def create_interactive_nav_chart(storage_file, stock_code, chart_type='line'):
    """创建交互式净值走势图"""
    # 获取基金数据
    df = read_fund_data_vectorized(storage_file, stock_code)
    if df is None:
        print("无法读取基金历史数据")
        return None
    
    try:
        # 创建图表
        fig = go.Figure()
        
        if chart_type == 'line':
            # 线图
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['close'],
                mode='lines',
                name='净值',
                line=dict(color='blue', width=2)
            ))
        elif chart_type == 'candlestick':
            # K线图
            fig.add_trace(go.Candlestick(
                x=df['date'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name='K线'
            ))
        elif chart_type == 'ohlc':
            # OHLC图
            fig.add_trace(go.Ohlc(
                x=df['date'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name='OHLC'
            ))
        
        # 添加移动平均线
        if 'ma5' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['ma5'],
                mode='lines',
                name='MA5',
                line=dict(color='orange', width=1)
            ))
        
        if 'ma10' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['ma10'],
                mode='lines',
                name='MA10',
                line=dict(color='green', width=1)
            ))
        
        if 'ma20' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['ma20'],
                mode='lines',
                name='MA20',
                line=dict(color='red', width=1)
            ))
        
        # 添加布林带
        if 'bb_upper' in df.columns and 'bb_lower' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['bb_upper'],
                mode='lines',
                name='布林带上轨',
                line=dict(color='purple', width=1, dash='dash'),
                showlegend=False
            ))
            
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['bb_lower'],
                mode='lines',
                name='布林带下轨',
                line=dict(color='purple', width=1, dash='dash'),
                fill='tonexty',
                fillcolor='rgba(128, 0, 128, 0.1)',
                showlegend=False
            ))
        
        # 设置图表布局
        fig.update_layout(
            title=f'基金 {stock_code} 净值走势图',
            xaxis_title='日期',
            yaxis_title='净值',
            hovermode='x unified',
            template='plotly_white'
        )
        
        return fig
    except Exception as e:
        print(f"创建图表时出错: {e}")
        return None

# 创建交互式统计分析图
def create_interactive_stats_chart(storage_file, stock_code):
    """创建交互式统计分析图"""
    # 获取基金数据
    df = read_fund_data_vectorized(storage_file, stock_code)
    if df is None:
        print("无法读取基金历史数据")
        return None
    
    try:
        # 计算月度收益率
        df_monthly = df.set_index('date')
        df_monthly = df_monthly.resample('ME').last()  # 使用'ME'替代'M'，避免弃用警告
        df_monthly['monthly_return'] = df_monthly['close'].pct_change() * 100
        
        # 创建子图
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('月度收益率分布', '净值分布', '涨跌幅分布', '收益率热力图'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # 月度收益率分布
        fig.add_trace(
            go.Histogram(x=df_monthly['monthly_return'].dropna(), name='月度收益率'),
            row=1, col=1
        )
        
        # 净值分布
        fig.add_trace(
            go.Histogram(x=df['close'], name='净值分布'),
            row=1, col=2
        )
        
        # 涨跌幅分布
        fig.add_trace(
            go.Histogram(x=df['change_pct'], name='涨跌幅分布'),
            row=2, col=1
        )
        
        # 收益率热力图（按年月）
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df_pivot = df.pivot_table(values='change_pct', index='year', columns='month', aggfunc='mean')
        
        for year in df_pivot.index:
            fig.add_trace(
                go.Heatmap(
                    z=[df_pivot.loc[year].values],
                    colorscale='RdYlGn',
                    showscale=False
                ),
                row=2, col=2
            )
        
        # 更新布局
        fig.update_layout(
            title=f'基金 {stock_code} 统计分析',
            height=800,
            template='plotly_white'
        )
        
        return fig
    except Exception as e:
        print(f"创建统计分析图时出错: {e}")
        return None

# 导出数据为CSV文件
def export_to_csv(storage_file, stock_code, output_file=None):
    """导出数据为CSV文件"""
    # 获取基金数据
    df = read_fund_data_vectorized(storage_file, stock_code)
    if df is None:
        print("无法读取基金历史数据")
        return False
    
    try:
        # 设置输出文件路径
        if output_file is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            output_dir = os.path.join(current_dir, "exports")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            output_file = os.path.join(output_dir, f"{stock_code}_data.csv")
        
        # 导出数据
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"数据已导出到: {output_file}")
        return True
    except Exception as e:
        print(f"导出数据时出错: {e}")
        return False

# 导出数据为Excel文件
def export_to_excel(storage_file, stock_code, output_file=None):
    """导出数据为Excel文件"""
    # 获取基金数据
    df = read_fund_data_vectorized(storage_file, stock_code)
    if df is None:
        print("无法读取基金历史数据")
        return False
    
    try:
        # 设置输出文件路径
        if output_file is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            output_dir = os.path.join(current_dir, "exports")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            output_file = os.path.join(output_dir, f"{stock_code}_data.xlsx")
        
        # 创建Excel写入器
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 写入原始数据
            df.to_excel(writer, sheet_name='原始数据', index=False)
            
            # 计算并写入月度数据
            df_monthly = df.set_index('date')
            df_monthly = df_monthly.resample('M').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'amount': 'sum',
                'volume': 'sum'
            })
            df_monthly['monthly_return'] = df_monthly['close'].pct_change() * 100
            df_monthly.reset_index(inplace=True)
            df_monthly.to_excel(writer, sheet_name='月度数据', index=False)
            
            # 计算并写入年度数据
            df_yearly = df.set_index('date')
            df_yearly = df_yearly.resample('Y').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'amount': 'sum',
                'volume': 'sum'
            })
            df_yearly['yearly_return'] = df_yearly['close'].pct_change() * 100
            df_yearly.reset_index(inplace=True)
            df_yearly.to_excel(writer, sheet_name='年度数据', index=False)
            
            # 获取统计指标
            stats = precompute_statistics(storage_file, stock_code)
            if stats:
                # 将统计指标转换为DataFrame
                stats_df = pd.DataFrame(list(stats.items()), columns=['指标', '值'])
                stats_df.to_excel(writer, sheet_name='统计指标', index=False)
        
        print(f"数据已导出到: {output_file}")
        return True
    except Exception as e:
        print(f"导出数据时出错: {e}")
        return False

# 导出图表为图片文件
def export_chart_to_image(fig, stock_code, chart_type, output_file=None):
    """导出图表为图片文件"""
    try:
        # 设置输出文件路径
        if output_file is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            output_dir = os.path.join(current_dir, "exports")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            output_file = os.path.join(output_dir, f"{stock_code}_{chart_type}.png")
        
        # 导出图表
        fig.write_image(output_file)
        print(f"图表已导出到: {output_file}")
        return True
    except Exception as e:
        print(f"导出图表时出错: {e}")
        return False

# 主函数
def main():
    """主函数"""
    # 检查并安装依赖
    check_and_install_dependencies()
    
    # 设置HDF5文件路径
    storage_file = get_hdf5_path()
    
    # 检查文件是否存在
    if not os.path.exists(storage_file):
        print(f"错误: HDF5文件不存在: {storage_file}")
        print("请先运行TDX_To_HDF5.py生成数据文件")
        return
    
    # 获取所有基金代码
    all_fund_codes = get_all_fund_codes_indexed(storage_file)
    if not all_fund_codes:
        print("错误: 无法获取基金代码列表或HDF5文件中没有数据")
        return
    
    print(f"HDF5文件中包含 {len(all_fund_codes)} 只基金的数据")
    
    while True:
        print("\n===== 基金数据查询与分析系统（优化版） =====")
        print("1. 显示指定基金的历史净值数据概览")
        print("2. 显示指定基金在某一天的数据")
        print("3. 显示指定基金的交互式净值走势图")
        print("4. 显示指定基金的统计分析图")
        print("5. 导出指定基金的数据为CSV文件")
        print("6. 导出指定基金的数据为Excel文件")
        print("7. 显示所有基金代码")
        print("0. 退出")
        
        choice = input("请选择操作 (0-7): ").strip()
        
        if choice == '1':
            stock_code = input("请输入基金代码: ").strip()
            display_fund_overview_enhanced(storage_file, stock_code)
            
        elif choice == '2':
            stock_code = input("请输入基金代码: ").strip()
            date = input("请输入查询日期 (格式: YYYYMMDD): ").strip()
            display_fund_data_on_date_enhanced(storage_file, stock_code, date)
            
        elif choice == '3':
            stock_code = input("请输入基金代码: ").strip()
            print("\n选择图表类型:")
            print("1. 线图")
            print("2. K线图")
            print("3. OHLC图")
            
            chart_choice = input("请选择图表类型 (1-3): ").strip()
            if chart_choice == '1':
                chart_type = 'line'
            elif chart_choice == '2':
                chart_type = 'candlestick'
            elif chart_choice == '3':
                chart_type = 'ohlc'
            else:
                print("无效选择，使用默认线图")
                chart_type = 'line'
            
            fig = create_interactive_nav_chart(storage_file, stock_code, chart_type)
            if fig:
                fig.show()
                
                # 询问是否导出图表
                export_choice = input("\n是否导出图表为图片? (y/n): ").strip().lower()
                if export_choice == 'y':
                    export_chart_to_image(fig, stock_code, chart_type)
            
        elif choice == '4':
            stock_code = input("请输入基金代码: ").strip()
            fig = create_interactive_stats_chart(storage_file, stock_code)
            if fig:
                fig.show()
                
                # 询问是否导出图表
                export_choice = input("\n是否导出图表为图片? (y/n): ").strip().lower()
                if export_choice == 'y':
                    export_chart_to_image(fig, stock_code, 'stats')
            
        elif choice == '5':
            stock_code = input("请输入基金代码: ").strip()
            export_to_csv(storage_file, stock_code)
            
        elif choice == '6':
            stock_code = input("请输入基金代码: ").strip()
            export_to_excel(storage_file, stock_code)
            
        elif choice == '7':
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
            
        elif choice == '0':
            print("感谢使用基金数据查询与分析系统，再见!")
            break
        
        else:
            print("无效的选择，请重新输入")
        
        # 询问是否继续
        if choice not in ['7', '0']:
            continue_choice = input("\n是否继续查询? (y/n): ").strip().lower()
            if continue_choice != 'y':
                print("返回主菜单")
                continue

if __name__ == "__main__":
    main()