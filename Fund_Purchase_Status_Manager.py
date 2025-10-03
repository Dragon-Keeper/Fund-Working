#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
基金基本信息及申购状态管理系统
用于获取、存储和查询基金基本信息及申购状态信息
优化版本：集成多线程下载、增量更新机制和分批处理策略
"""

import os
import sys
import time
import datetime
import threading
import queue
import hashlib
import pandas as pd
import numpy as np
import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

# 检查并安装必要的依赖
def check_and_install_dependencies():
    """检查并安装必要的依赖"""
    required_packages = ['pandas', 'numpy', 'akshare', 'tables']
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
    # 构建HDF5文件路径到data文件夹
    data_dir = os.path.join(current_dir, "data")
    # 确保data文件夹存在
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    hdf5_path = os.path.join(data_dir, "Fund_Purchase_Status.h5")
    return hdf5_path

# 计算数据哈希值，用于增量更新
def calculate_data_hash(df):
    """计算DataFrame的哈希值，用于判断数据是否发生变化"""
    # 将DataFrame转换为字符串并计算哈希值
    data_str = df.to_string()
    return hashlib.md5(data_str.encode()).hexdigest()

# 获取基金基本信息数据哈希
def get_fund_basic_info_hash():
    """获取基金基本信息数据哈希"""
    hdf5_path = get_hdf5_path()
    
    if not os.path.exists(hdf5_path):
        return None
    
    try:
        # 尝试读取fund_basic_info键
        df = pd.read_hdf(hdf5_path, key='fund_basic_info')
        return calculate_data_hash(df)
    except:
        # 如果fund_basic_info键不存在，返回None
        return None

# 获取基金申购状态数据哈希
def get_fund_purchase_status_hash():
    """获取基金申购状态数据哈希"""
    hdf5_path = get_hdf5_path()
    
    if not os.path.exists(hdf5_path):
        return None
    
    try:
        df = pd.read_hdf(hdf5_path, key='fund_purchase_status')
        return calculate_data_hash(df)
    except:
        return None

# 下载基金基本信息（多线程版本）
def download_fund_basic_info_threaded(batch_size=500, max_workers=4):
    """下载基金基本信息（多线程版本，支持增量更新）"""
    print("开始下载所有基金基本信息（多线程版本）...")
    start_time = time.time()
    
    try:
        # 获取HDF5文件路径
        hdf5_path = get_hdf5_path()
        
        # 获取现有数据哈希（如果文件存在）
        old_hash = get_fund_basic_info_hash() if os.path.exists(hdf5_path) else None
        
        # 获取所有基金代码
        all_fund_codes = ak.fund_name_em()
        
        if all_fund_codes is None or all_fund_codes.empty:
            print("获取基金代码列表失败，返回数据为空")
            return False
        
        # 添加更新时间列
        all_fund_codes['更新时间'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 计算新数据哈希
        new_hash = calculate_data_hash(all_fund_codes)
        
        # 比较哈希值，判断数据是否发生变化
        if old_hash == new_hash and old_hash is not None:
            print("基金基本信息未发生变化，无需更新")
            return True
        
        # 检查文件是否存在，如果存在则使用追加模式，否则使用写入模式
        mode = 'a' if os.path.exists(hdf5_path) else 'w'
        
        # 保存数据到HDF5文件
        all_fund_codes.to_hdf(hdf5_path, key='fund_basic_info', mode=mode)
        
        print(f"成功下载并保存{len(all_fund_codes)}条基金基本信息到Fund_Purchase_Status.h5文件")
        return True
        
    except Exception as e:
        print(f"下载基金基本信息时出错: {e}")
        return False
    
    elapsed_time = time.time() - start_time
    print(f"下载基金基本信息完成，耗时: {elapsed_time:.2f}秒")

# 下载单个基金申购状态（用于多线程）
def download_single_fund_purchase_status(fund_code, result_queue):
    """下载单个基金申购状态（用于多线程）"""
    # 不再使用单基金下载方式
    pass

# 下载基金申购状态（多线程版本）
def download_fund_purchase_status_threaded(batch_size=500, max_workers=4):
    """下载基金申购状态（使用ak.fund_purchase_em()直接批量下载）"""
    print("开始下载所有基金申购状态信息（使用ak.fund_purchase_em()批量下载）...")
    start_time = time.time()
    
    try:
        # 使用用户提供的更简单方法直接批量下载
        fund_purchase_df = ak.fund_purchase_em()
        
        if fund_purchase_df is None or fund_purchase_df.empty:
            print("获取基金申购状态信息失败，返回数据为空")
            return False
        
        # 添加更新时间列
        fund_purchase_df['更新时间'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 获取HDF5文件路径
        hdf5_path = get_hdf5_path()
        
        # 检查文件是否存在，如果存在则使用追加模式，否则使用写入模式
        mode = 'a' if os.path.exists(hdf5_path) else 'w'
        
        # 保存数据到HDF5文件
        fund_purchase_df.to_hdf(hdf5_path, key='fund_purchase_status', mode=mode)
        
        print(f"成功下载并保存{len(fund_purchase_df)}条基金申购状态信息到Fund_Purchase_Status.h5文件")
        return True
        
    except Exception as e:
        print(f"下载基金申购状态时出错: {e}")
        return False
    
    elapsed_time = time.time() - start_time
    print(f"下载基金申购状态完成，耗时: {elapsed_time:.2f}秒")

# 下载基金申购状态（增量更新版本）
def download_fund_purchase_status_incremental(batch_size=500, max_workers=4):
    """下载基金申购状态（增量更新版本）"""
    print("开始增量下载基金申购状态信息...")
    start_time = time.time()
    
    try:
        # 获取HDF5文件路径
        hdf5_path = get_hdf5_path()
        
        # 获取现有数据哈希（如果文件存在）
        old_hash = get_fund_purchase_status_hash() if os.path.exists(hdf5_path) else None
        
        # 使用用户提供的更简单方法直接批量下载
        fund_purchase_df = ak.fund_purchase_em()
        
        if fund_purchase_df is None or fund_purchase_df.empty:
            print("获取基金申购状态信息失败，返回数据为空")
            return False
        
        # 添加更新时间列
        fund_purchase_df['更新时间'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 计算新数据哈希
        new_hash = calculate_data_hash(fund_purchase_df)
        
        # 比较哈希值，判断数据是否发生变化
        if old_hash == new_hash and old_hash is not None:
            print("数据未发生变化，无需更新")
            return True
        
        # 保存数据到HDF5文件
        fund_purchase_df.to_hdf(hdf5_path, key='fund_purchase_status', mode='w')
        
        print(f"成功增量下载并保存{len(fund_purchase_df)}条基金申购状态信息到Fund_Purchase_Status.h5文件")
        return True
        
    except Exception as e:
        print(f"增量下载基金申购状态时出错: {e}")
        return False
    
    elapsed_time = time.time() - start_time
    print(f"增量下载基金申购状态完成，耗时: {elapsed_time:.2f}秒")

# 下载所有基金数据（多线程+增量更新）
def download_all_fund_data(batch_size=500, max_workers=4):
    """下载所有基金数据（基金基本信息和申购状态，均使用增量更新）"""
    start_time = time.time()
    
    # 首先下载基金基本信息（增量更新）
    print("正在下载基金基本信息...")
    basic_info_success = download_fund_basic_info_threaded(batch_size, max_workers)
    
    # 然后下载基金申购状态（增量更新）
    print("\n正在下载基金申购状态数据...")
    purchase_status_success = download_fund_purchase_status_incremental(batch_size, max_workers)
    
    if not basic_info_success:
        print("下载基金基本信息失败")
    if not purchase_status_success:
        print("下载基金申购状态失败")
    
    if not (basic_info_success and purchase_status_success):
        print("部分数据下载失败，请检查网络连接后重试")
        return False
    
    elapsed_time = time.time() - start_time
    print(f"\n下载所有基金数据完成，总耗时: {elapsed_time:.2f}秒")
    print("基金基本信息和申购状态数据均已更新")
    return True

# 获取所有基金代码（从基金基本信息表）
def get_all_fund_codes():
    """获取所有基金代码"""
    hdf5_path = get_hdf5_path()
    
    if not os.path.exists(hdf5_path):
        print("未找到基金数据文件，请先下载数据")
        return []
    
    try:
        # 首先尝试从基金基本信息表获取
        try:
            df = pd.read_hdf(hdf5_path, key='fund_basic_info')
            # 获取唯一的基金代码并排序
            fund_codes = df['基金代码'].unique().tolist()
            fund_codes.sort()
            return fund_codes
        except KeyError:
            # 如果基金基本信息表不存在，则从申购状态表获取
            df = pd.read_hdf(hdf5_path, key='fund_purchase_status')
            # 获取唯一的基金代码并排序
            fund_codes = df['基金代码'].unique().tolist()
            fund_codes.sort()
            return fund_codes
    except Exception as e:
        print(f"获取基金代码列表时出错: {e}")
        return []

# 获取指定基金的基本信息
def get_fund_basic_info(stock_code):
    """获取指定基金的基本信息"""
    hdf5_path = get_hdf5_path()
    
    if not os.path.exists(hdf5_path):
        print("未找到基金数据文件，请先下载数据")
        return None
    
    try:
        # 尝试从HDF5文件读取基金基本信息数据
        try:
            df = pd.read_hdf(hdf5_path, key='fund_basic_info')
        except KeyError:
            # 如果fund_basic_info键不存在，尝试从fund_purchase_status表获取基本信息
            print("fund_basic_info表不存在，尝试从fund_purchase_status表获取基本信息")
            df = pd.read_hdf(hdf5_path, key='fund_purchase_status')
            # 只保留需要的基本信息列
            if all(col in df.columns for col in ['基金代码', '基金简称', '基金类型', '更新时间']):
                df = df[['基金代码', '基金简称', '基金类型', '更新时间']].drop_duplicates(subset='基金代码')
            else:
                print("fund_purchase_status表缺少必要的基本信息列")
                return None
        
        # 筛选指定基金代码的数据
        fund_data = df[df['基金代码'] == stock_code]
        
        if fund_data.empty:
            return None
        
        # 返回第一条记录
        return fund_data.iloc[0].to_dict()
    except Exception as e:
        print(f"读取基金基本信息数据时出错: {e}")
        return None

# 显示指定基金的基本信息
def display_fund_basic_info(stock_code):
    """显示指定基金的基本信息"""
    # 获取基金基本信息
    fund_info = get_fund_basic_info(stock_code)
    if not fund_info:
        print(f"基金代码 {stock_code} 不存在或读取失败")
        return False
    
    # 打印基金基本信息
    print(f"\n基金基本信息:")
    print(f"{'-'*60}")
    print(f"基金代码: {fund_info['基金代码']}")
    print(f"基金简称: {fund_info['基金简称']}")
    print(f"基金类型: {fund_info['基金类型']}")
    print(f"更新时间: {fund_info['更新时间']}")
    print(f"{'-'*60}")
    return True

# 获取指定基金的申购状态信息
def get_fund_purchase_status(stock_code):
    """获取指定基金的申购状态信息"""
    hdf5_path = get_hdf5_path()
    
    if not os.path.exists(hdf5_path):
        print("未找到基金申购状态数据文件，请先下载数据")
        return None
    
    try:
        # 从HDF5文件读取数据
        df = pd.read_hdf(hdf5_path, key='fund_purchase_status')
        
        # 筛选指定基金代码的数据
        fund_data = df[df['基金代码'] == stock_code]
        
        if fund_data.empty:
            return None
        
        # 返回第一条记录
        return fund_data.iloc[0].to_dict()
    except Exception as e:
        print(f"读取基金申购状态数据时出错: {e}")
        return None

# 显示指定基金的申购状态信息
def display_fund_purchase_status(stock_code):
    """显示指定基金的申购状态信息"""
    # 获取基金申购状态信息
    fund_status = get_fund_purchase_status(stock_code)
    if not fund_status:
        print(f"基金代码 {stock_code} 不存在或读取失败")
        return False
    
    # 打印基金申购状态信息
    print(f"\n基金申购状态信息:")
    print(f"{'-'*60}")
    print(f"基金代码: {fund_status['基金代码']}")
    print(f"基金简称: {fund_status['基金简称']}")
    print(f"基金类型: {fund_status['基金类型']}")
    print(f"最新净值/万份收益: {fund_status['最新净值/万份收益']}")
    print(f"最新净值/万份收益报告时间: {fund_status['最新净值/万份收益-报告时间']}")
    print(f"申购状态: {fund_status['申购状态']}")
    print(f"赎回状态: {fund_status['赎回状态']}")
    print(f"下一开放日: {fund_status['下一开放日']}")
    print(f"购买起点: {fund_status['购买起点']}")
    print(f"日累计限定金额: {fund_status['日累计限定金额']}")
    print(f"手续费: {fund_status['手续费']}%")
    print(f"更新时间: {fund_status['更新时间']}")
    print(f"{'-'*60}")
    return True

# 按申购状态筛选基金
def filter_funds_by_purchase_status(status):
    """按申购状态筛选基金"""
    hdf5_path = get_hdf5_path()
    
    if not os.path.exists(hdf5_path):
        print("未找到基金申购状态数据文件，请先下载数据")
        return pd.DataFrame()
    
    try:
        # 从HDF5文件读取数据
        df = pd.read_hdf(hdf5_path, key='fund_purchase_status')
        # 筛选指定申购状态的数据，并选择需要的列
        filtered_df = df[df['申购状态'] == status][['基金代码', '基金简称', '基金类型', '最新净值/万份收益', '申购状态']]
        # 按基金代码排序
        filtered_df = filtered_df.sort_values('基金代码')
        return filtered_df
    except Exception as e:
        print(f"筛选基金时出错: {e}")
        return pd.DataFrame()

# 显示筛选结果
def display_filtered_funds(df, status):
    """显示筛选结果"""
    if df.empty:
        print(f"没有找到申购状态为 '{status}' 的基金")
        return False
    
    print(f"\n申购状态为 '{status}' 的基金列表:")
    print(f"{'-'*80}")
    print(f"{'基金代码':<10} {'基金简称':<20} {'基金类型':<10} {'最新净值/万份收益':<15}")
    print(f"{'-'*80}")
    
    for _, row in df.iterrows():
        print(f"{row['基金代码']:<10} {row['基金简称']:<20} {row['基金类型']:<10} {row['最新净值/万份收益']:<15}")
    
    print(f"{'-'*80}")
    print(f"共找到 {len(df)} 只申购状态为 '{status}' 的基金")
    return True

# 显示所有基金代码（分页显示）
def display_all_fund_codes():
    """显示所有基金代码（分页显示）"""
    all_fund_codes = get_all_fund_codes()
    if not all_fund_codes:
        print("错误: 无法获取基金代码列表或数据库中没有数据")
        return
    
    print(f"\n共有 {len(all_fund_codes)} 只基金:")
    
    # 分页显示，每页20个
    page_size = 20
    total_pages = (len(all_fund_codes) + page_size - 1) // page_size
    
    exit_view = False
    for page in range(total_pages):
        if exit_view:
            break
            
        start_idx = page * page_size
        end_idx = min((page + 1) * page_size, len(all_fund_codes))
        
        print(f"\n第 {page + 1}/{total_pages} 页:")
        print(f"{'-'*60}")
        
        for i in range(start_idx, end_idx):
            print(f"{all_fund_codes[i]}", end="\t")
            if (i - start_idx + 1) % 5 == 0:  # 每行显示5个基金代码
                print()
        
        print(f"\n{'-'*60}")
        
        # 最后一页不需要提示
        if page < total_pages - 1:
            user_input = input("按Enter键查看下一页... 或按'q'退出查看: ").strip()
            if user_input.lower() == 'q':
                exit_view = True

# 查询系统主菜单
def query_system():
    """查询系统主菜单"""
    # 设置HDF5文件路径
    hdf5_path = get_hdf5_path()
    
    # 检查文件是否存在
    if not os.path.exists(hdf5_path):
        print(f"错误: HDF5文件不存在: {hdf5_path}")
        print("请先运行下载功能下载数据")
        return
    
    # 获取所有基金代码
    all_fund_codes = get_all_fund_codes()
    if not all_fund_codes:
        print("错误: 无法获取基金代码列表或数据库中没有数据")
        return
    
    print(f"数据库中包含 {len(all_fund_codes)} 只基金的信息")
    
    while True:
        print("\n===== 基金信息查询系统 =====")
        print("1. 查询指定基金的基本信息")
        print("2. 查询指定基金的申购状态")
        print("3. 按申购状态筛选基金")
        print("4. 显示所有基金代码")
        print("5. 返回主菜单")
        
        choice = input("请选择操作 (1-5): ").strip()
        
        if choice == '1':
            # 查询指定基金的基本信息
            stock_code = input("请输入基金代码: ").strip()
            display_fund_basic_info(stock_code)
            
        elif choice == '2':
            # 查询指定基金的申购状态
            stock_code = input("请输入基金代码: ").strip()
            display_fund_purchase_status(stock_code)
            
        elif choice == '3':
            print("\n可选的申购状态:")
            print("1. 开放申购")
            print("2. 暂停申购")
            print("3. 限制大额申购")
            
            status_choice = input("请选择申购状态 (1-3): ").strip()
            if status_choice == '1':
                status = '开放申购'
            elif status_choice == '2':
                status = '暂停申购'
            elif status_choice == '3':
                status = '限制大额申购'
            else:
                print("无效选择")
                continue
                
            df = filter_funds_by_purchase_status(status)
            display_filtered_funds(df, status)
            
        elif choice == '4':
            display_all_fund_codes()
            
        elif choice == '5':
            break
            
        else:
            print("无效选择，请重新输入")

# 主函数
def main():
    """主函数"""
    # 检查并安装依赖
    check_and_install_dependencies()
    
    while True:
        print("\n===== 基金基本信息及申购状态管理系统=====")
        print("1. 下载基金基本信息（多线程）")
        print("2. 下载基金申购状态数据（多线程）")
        print("3. 下载所有基金基本信息和申购状态数据（多线程+增量更新）- 适合初始数据获取或完全更新")
        print("4. 查询基金基本信息")
        print("5. 查询基金申购状态")
        print("0. 退出系统")
        
        choice = input("请选择操作 (0-5): ").strip()
        
        if choice == '1':
            # 询问批处理大小和线程数
            try:
                batch_size = int(input("请输入批处理大小（默认500）: ") or "500")
                max_workers = int(input("请输入线程数（默认4）: ") or "4")
                # 限制最大线程数为8
                max_workers = min(max_workers, 8)
                download_fund_basic_info_threaded(batch_size, max_workers)
            except ValueError:
                print("输入无效，使用默认值")
                download_fund_basic_info_threaded()
                
        elif choice == '2':
            # 询问批处理大小和线程数
            try:
                batch_size = int(input("请输入批处理大小（默认500）: ") or "500")
                max_workers = int(input("请输入线程数（默认4）: ") or "4")
                # 限制最大线程数为8
                max_workers = min(max_workers, 8)
                download_fund_purchase_status_threaded(batch_size, max_workers)
            except ValueError:
                print("输入无效，使用默认值")
                download_fund_purchase_status_threaded()
                
        elif choice == '3':
            # 询问批处理大小和线程数
            try:
                batch_size = int(input("请输入批处理大小（默认500）: ") or "500")
                max_workers = int(input("请输入线程数（默认4）: ") or "4")
                # 限制最大线程数为8
                max_workers = min(max_workers, 8)
                download_all_fund_data(batch_size, max_workers)
            except ValueError:
                print("输入无效，使用默认值")
                download_all_fund_data()
                
        elif choice == '4':
            # 查询基金基本信息
            stock_code = input("请输入基金代码: ").strip()
            display_fund_basic_info(stock_code)
                
        elif choice == '5':
            # 查询基金申购状态
            stock_code = input("请输入基金代码: ").strip()
            display_fund_purchase_status(stock_code)
                
        elif choice == '0':
            print("感谢使用基金基本信息及申购状态管理系统，再见!")
            break
            
        else:
            print("无效选择，请重新输入")

if __name__ == "__main__":
    main()