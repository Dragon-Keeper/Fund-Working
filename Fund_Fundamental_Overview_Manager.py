#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
基金基本面概况管理系统
用于从HDF5文件读取基金代码，下载基金基本面信息，并提供查询功能
支持多线程下载和交互菜单

功能特性：
- 从Fund_Purchase_Status.h5读取基金代码
- 使用akshare的fund_overview_em接口下载基金基本面信息
- 支持多线程/单线程下载选择
- 数据存储到Fund_Fundamental_Overview.h5
- 提供基金信息查询功能
"""

import os
import sys
import time
import datetime
import pandas as pd
import akshare as ak
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

# 检查并安装必要的依赖
def check_and_install_dependencies():
    """检查并安装必要的依赖"""
    required_packages = ['pandas', 'akshare', 'tables']
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
def get_hdf5_paths():
    """获取HDF5文件路径
    
    Returns:
        tuple: (source_hdf5_path, target_hdf5_path)
    """
    # 获取当前脚本所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 构建HDF5文件路径到data文件夹
    data_dir = os.path.join(current_dir, "data")
    # 确保data文件夹存在
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    # 返回两个文件路径，保存在data文件夹中
    source_hdf5_path = os.path.join(data_dir, "Fund_Purchase_Status.h5")
    target_hdf5_path = os.path.join(data_dir, "Fund_Fundamental_Overview.h5")
    
    return source_hdf5_path, target_hdf5_path

# 从HDF5文件读取基金代码
def read_fund_codes_from_hdf5(source_hdf5_path):
    """从Fund_Purchase_Status.h5文件读取所有基金代码"""
    try:
        # 检查文件是否存在
        if not os.path.exists(source_hdf5_path):
            print(f"错误：找不到HDF5文件 {source_hdf5_path}")
            return None
        
        # 使用上下文管理器读取文件
        with pd.HDFStore(source_hdf5_path, mode='r') as store:
            # 获取所有键
            keys = store.keys()
            
            # 尝试从fund_purchase_status键读取
            if '/fund_purchase_status' in keys:
                df = store['fund_purchase_status']
                # 尝试不同的可能的基金代码列名
                code_columns = ['基金代码', '代码', 'code']
                for col in code_columns:
                    if col in df.columns:
                        fund_codes = df[col].tolist()
                        print(f"从fund_purchase_status键的{col}列读取到{len(fund_codes)}个基金代码")
                        return fund_codes
            
            # 尝试从fund_basic_info键读取
            if '/fund_basic_info' in keys:
                df = store['fund_basic_info']
                code_columns = ['基金代码', '代码', 'code', '代码/名称', '基金代码/名称']
                for col in code_columns:
                    if col in df.columns:
                        # 提取纯基金代码（去除可能的前后缀）
                        fund_codes = []
                        for code in df[col]:
                            # 提取数字部分
                            num_code = ''.join(filter(str.isdigit, str(code)))
                            if num_code and len(num_code) >= 6:
                                fund_codes.append(num_code[:6])  # 确保只有6位数字
                        print(f"从fund_basic_info键的{col}列读取到{len(fund_codes)}个基金代码")
                        return list(set(fund_codes))  # 去重
        
        # 如果文件存在但没有找到正确的键，尝试直接读取
        try:
            # 尝试直接读取不同的键
            possible_keys = ['fund_purchase_status', 'fund_basic_info']
            for key in possible_keys:
                try:
                    df = pd.read_hdf(source_hdf5_path, key=key)
                    # 查找可能的代码列
                    for col in df.columns:
                        # 检查列内容是否包含基金代码格式的数据
                        sample_data = str(df.iloc[0][col])
                        if any(c.isdigit() for c in sample_data):
                            # 提取数字部分
                            fund_codes = []
                            for code in df[col]:
                                num_code = ''.join(filter(str.isdigit, str(code)))
                                if num_code and len(num_code) >= 6:
                                    fund_codes.append(num_code[:6])
                            if fund_codes:
                                print(f"从{key}键的{col}列读取到{len(fund_codes)}个基金代码")
                                return list(set(fund_codes))
                except:
                    continue
        except:
            pass
        
        print("错误：无法从HDF5文件中找到基金代码列")
        return None
        
    except Exception as e:
        print(f"读取HDF5文件时出错: {e}")
        # 尝试使用备用方法
        try:
            # 直接读取整个文件内容来调试
            print("尝试直接读取文件内容...")
            df = pd.read_hdf(source_hdf5_path)
            print(f"文件包含{len(df)}条记录")
            print(f"列名: {list(df.columns)}")
            # 尝试从任何列中提取可能的基金代码
            all_codes = []
            for col in df.columns:
                for code in df[col]:
                    num_code = ''.join(filter(str.isdigit, str(code)))
                    if num_code and len(num_code) >= 6:
                        all_codes.append(num_code[:6])
            if all_codes:
                unique_codes = list(set(all_codes))
                print(f"从文件中提取到{len(unique_codes)}个可能的基金代码")
                return unique_codes
        except:
            pass
        return None

# 下载单个基金基本面信息
def download_single_fund_overview(fund_code):
    """下载单个基金的基本面概况信息"""
    try:
        # 调用akshare接口获取基金概况
        df = ak.fund_overview_em(symbol=fund_code)
        
        if df is None or df.empty:
            print(f"警告：基金{fund_code}没有返回数据")
            return None
        
        # 提取需要的字段
        required_fields = {
            '基金代码': None,
            '基金简称': None,
            '基金类型': None,
            '成立日期': None,
            '资产规模': None,
            '基金经理人': None,
            '成立来分红': None,
            '数据日期': None
        }
        
        # 映射字段，处理不同可能的列名
        field_mapping = {
            '基金代码': ['基金代码', '代码', 'code'],
            '基金简称': ['基金简称', '简称', '名称', 'fund_name'],
            '基金类型': ['基金类型', '类型', 'type'],
            '成立日期': ['成立日期/规模', '成立日期', 'launch_date', '日期'],
            '资产规模': ['资产规模', '规模', 'asset_size'],
            '基金经理人': ['基金经理人', '基金经理', '经理', 'manager'],
            '成立来分红': ['成立来分红', '分红', 'dividend']
        }
        
        # 提取数据
        result = {'基金代码': fund_code}
        
        for target_field, possible_names in field_mapping.items():
            if target_field == '基金代码':
                continue  # 已经设置
            
            for name in possible_names:
                if name in df.columns:
                    value = str(df.iloc[0][name])
                    
                    # 处理成立日期：去掉后面的规模信息
                    if target_field == '成立日期':
                        if '/' in value:
                            value = value.split('/')[0].strip()
                        result[target_field] = value
                    # 处理资产规模：提取规模数字和单位，保留截止日期作为数据日期
                    elif target_field == '资产规模':
                        # 提取资产规模部分
                        if '（截止至：' in value and '）' in value:
                            # 提取规模部分
                            size_part = value.split('（截止至：')[0].strip()
                            # 提取日期部分
                            date_part = value.split('（截止至：')[1].split('）')[0].strip()
                            result['数据日期'] = date_part
                            result[target_field] = size_part
                        else:
                            result[target_field] = value
                    else:
                        result[target_field] = value
                    break
            else:
                result[target_field] = None
        
        # 从成立日期/规模中提取成立日期（备用方案）
        if '成立日期' not in result or result['成立日期'] is None:
            for col in df.columns:
                if '成立日期' in col or '日期' in col:
                    date_str = str(df.iloc[0][col])
                    # 提取日期部分
                    if '/' in date_str:
                        result['成立日期'] = date_str.split('/')[0].strip()
                    else:
                        result['成立日期'] = date_str.strip()
                    break
        
        # 如果没有提取到数据日期，设置为当前日期
        if '数据日期' not in result or result['数据日期'] is None:
            result['数据日期'] = datetime.datetime.now().strftime('%Y年%m月%d日')
        
        return result
        
    except Exception as e:
        print(f"下载基金{fund_code}信息时出错: {e}")
        return None

# 多线程下载所有基金基本面信息
def download_all_fund_overview(fund_codes, use_multithread=True, max_workers=None):
    """下载所有基金的基本面信息"""
    if not fund_codes:
        print("没有基金代码可下载")
        return None
    
    total_count = len(fund_codes)
    print(f"开始下载{total_count}个基金的基本面信息...")
    
    # 设置线程数
    if use_multithread:
        if max_workers is None:
            # 根据CPU核心数自动设置线程数
            max_workers = max(1, multiprocessing.cpu_count() - 1)
        print(f"使用多线程模式，线程数: {max_workers}")
    else:
        print("使用单线程模式")
    
    all_results = []
    success_count = 0
    fail_count = 0
    start_time = time.time()
    last_progress_time = start_time
    
    try:
        if use_multithread:
            # 使用线程池
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_code = {executor.submit(download_single_fund_overview, code): code 
                                 for code in fund_codes}
                
                # 处理结果
                for i, future in enumerate(as_completed(future_to_code), 1):
                    current_time = time.time()
                    code = future_to_code[future]
                    
                    try:
                        result = future.result()
                        if result:
                            all_results.append(result)
                            success_count += 1
                        else:
                            fail_count += 1
                    except Exception as e:
                        print(f"处理基金{code}结果时出错: {e}")
                        fail_count += 1
                    
                    # 计算下载速度和剩余时间
                    elapsed = current_time - start_time
                    processed_per_sec = i / elapsed if elapsed > 0 else 0
                    remaining = total_count - i
                    est_remaining = remaining / processed_per_sec if processed_per_sec > 0 else 0
                    
                    # 计算成功率
                    success_rate = (success_count / i * 100) if i > 0 else 0
                    
                    # 显示进度（每10个或每1秒更新一次）
                    if i % 10 == 0 or i == total_count or (current_time - last_progress_time >= 1.0):
                        last_progress_time = current_time
                        print(f"进度: {i}/{total_count} ({i/total_count*100:.1f}%), "
                              f"成功: {success_count}, 失败: {fail_count}, "
                              f"成功率: {success_rate:.1f}%, "
                              f"速度: {processed_per_sec:.2f}个/秒, "
                              f"预计剩余: {est_remaining:.0f}秒")
        else:
            # 单线程下载
            for i, code in enumerate(fund_codes, 1):
                current_time = time.time()
                
                try:
                    result = download_single_fund_overview(code)
                    if result:
                        all_results.append(result)
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception as e:
                    print(f"处理基金{code}时出错: {e}")
                    fail_count += 1
                
                # 计算下载速度和剩余时间
                elapsed = current_time - start_time
                processed_per_sec = i / elapsed if elapsed > 0 else 0
                remaining = total_count - i
                est_remaining = remaining / processed_per_sec if processed_per_sec > 0 else 0
                
                # 计算成功率
                success_rate = (success_count / i * 100) if i > 0 else 0
                
                # 显示进度（每10个或每1秒更新一次）
                if i % 10 == 0 or i == total_count or (current_time - last_progress_time >= 1.0):
                    last_progress_time = current_time
                    print(f"进度: {i}/{total_count} ({i/total_count*100:.1f}%), "
                          f"成功: {success_count}, 失败: {fail_count}, "
                          f"成功率: {success_rate:.1f}%, "
                          f"速度: {processed_per_sec:.2f}个/秒, "
                          f"预计剩余: {est_remaining:.0f}秒")
    
    except KeyboardInterrupt:
        print("\n下载被用户中断")
        # 询问用户是否保留已下载的数据
        if all_results:
            keep_data = input("是否保留已下载的数据？(y/n): ")
            if keep_data.lower() != 'y':
                all_results = []
    except Exception as e:
        print(f"\n下载过程中发生错误: {e}")
    
    elapsed_time = time.time() - start_time
    total_processed = success_count + fail_count
    success_rate = (success_count / total_processed * 100) if total_processed > 0 else 0
    
    print(f"\n下载完成，总耗时: {elapsed_time:.2f}秒")
    print(f"处理基金总数: {total_processed}/{total_count}")
    print(f"成功下载: {success_count}个 ({success_rate:.1f}%)")
    print(f"下载失败: {fail_count}个")
    
    if total_processed < total_count:
        print(f"未处理: {total_count - total_processed}个")
    
    return all_results

# 保存数据到HDF5文件
def save_to_hdf5(results, target_hdf5_path):
    """保存下载的基金基本面信息到HDF5文件"""
    if not results:
        print("没有数据可保存")
        return False
    
    try:
        # 转换为DataFrame
        df = pd.DataFrame(results)
        
        # 添加更新时间列
        df['更新时间'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 保存到HDF5文件
        mode = 'a' if os.path.exists(target_hdf5_path) else 'w'
        df.to_hdf(target_hdf5_path, key='fund_fundamental_overview', mode=mode)
        
        print(f"成功保存{len(df)}条基金基本面信息到{target_hdf5_path}")
        return True
    except Exception as e:
        print(f"保存数据到HDF5文件时出错: {e}")
        return False

# 显示所有基金代码
def display_all_fund_codes(source_hdf5_path):
    """显示所有基金代码"""
    fund_codes = read_fund_codes_from_hdf5(source_hdf5_path)
    
    if not fund_codes:
        return
    
    print(f"\n总共找到{len(fund_codes)}个基金代码：")
    # 分批次显示
    batch_size = 20
    for i in range(0, len(fund_codes), batch_size):
        batch = fund_codes[i:i+batch_size]
        print(', '.join(batch))
        
        # 每显示一批就询问是否继续
        if i + batch_size < len(fund_codes):
            choice = input("是否继续显示？(y/n): ")
            if choice.lower() != 'y':
                break

# 查询基金基本面信息
def query_fund_overview(target_hdf5_path):
    """查询基金基本面信息"""
    if not os.path.exists(target_hdf5_path):
        print("错误：找不到基金基本面信息数据库，请先下载数据")
        return
    
    try:
        # 读取数据
        df = pd.read_hdf(target_hdf5_path, key='fund_fundamental_overview')
        
        # 定义要显示的字段，包含数据日期
        display_fields = ['基金代码', '基金简称', '基金类型', '成立日期', '资产规模', '基金经理人', '成立来分红', '数据日期']
        
        # 过滤掉不存在的字段
        available_fields = [field for field in display_fields if field in df.columns]
        
        while True:
            print("\n=== 基金查询 ===")
            print("1. 根据基金代码查询")
            print("2. 显示前N条数据")
            print("3. 返回主菜单")
            
            choice = input("请选择操作(1-3): ")
            
            if choice == '1':
                fund_code = input("请输入基金代码: ")
                # 查找基金
                result = df[df['基金代码'] == fund_code]
                if result.empty:
                    print(f"未找到基金代码{fund_code}")
                else:
                    print("\n查询结果:")
                    for _, row in result.iterrows():
                        for col in available_fields:
                            print(f"{col}: {row.get(col, 'N/A')}")
            
            elif choice == '2':
                try:
                    n = int(input("请输入要显示的记录数: "))
                    print(f"\n前{n}条基金数据:")
                    print(df[available_fields].head(n))
                except ValueError:
                    print("请输入有效的数字")
            
            elif choice == '3':
                break
            
            else:
                print("无效的选择，请重新输入")
    
    except Exception as e:
        print(f"查询基金信息时出错: {e}")

# 主菜单函数
def main_menu():
    """显示主菜单"""
    check_and_install_dependencies()
    source_hdf5_path, target_hdf5_path = get_hdf5_paths()
    
    # 显示欢迎信息
    print("欢迎使用基金基本面概况管理系统")
    print(f"数据源文件: {source_hdf5_path}")
    print(f"数据存储文件: {target_hdf5_path}")
    
    while True:
        print("\n=== 基金基本面概况管理系统 ===")
        print("1. 下载基金规模、成立日期等数据")
        print("2. 显示所有基金代码")
        print("3. 查询基金规模、成立日期等数据")
        print("4. 快速测试（下载前10个基金）")
        print("0. 退出系统")
        
        choice = input("请选择操作(0-4): ")
        
        if choice == '1':
            # 读取基金代码
            fund_codes = read_fund_codes_from_hdf5(source_hdf5_path)
            if not fund_codes:
                print("无法继续，按回车键返回主菜单...")
                input()
                continue
            
            # 显示基金数量
            print(f"总共找到 {len(fund_codes)} 个基金代码")
            
            # 选择下载模式
            mode_choice = input("请选择下载模式: 1. 多线程(默认)  2. 单线程: ")
            use_multithread = mode_choice != '2'
            
            # 再次确认
            confirm = input(f"确认要下载 {len(fund_codes)} 个基金的信息吗？(y/n): ")
            if confirm.lower() != 'y':
                print("取消下载")
                continue
            
            try:
                # 下载数据
                results = download_all_fund_overview(fund_codes, use_multithread=use_multithread)
                
                # 保存数据
                if results:
                    save_to_hdf5(results, target_hdf5_path)
            except KeyboardInterrupt:
                print("\n下载被用户中断")
            except Exception as e:
                print(f"\n操作过程中出错: {e}")
            
            print("操作完成，按回车键返回主菜单...")
            input()
        
        elif choice == '2':
            try:
                display_all_fund_codes(source_hdf5_path)
            except Exception as e:
                print(f"显示基金代码时出错: {e}")
            print("\n按回车键返回主菜单...")
            input()
        
        elif choice == '3':
            try:
                query_fund_overview(target_hdf5_path)
            except Exception as e:
                print(f"查询基金信息时出错: {e}")
                print("按回车键返回主菜单...")
                input()
        
        elif choice == '4':
            # 快速测试功能
            print("\n=== 快速测试 ===")
            print("将下载前10个基金进行功能验证...")
            
            # 读取基金代码并取前10个
            fund_codes = read_fund_codes_from_hdf5(source_hdf5_path)
            if not fund_codes:
                print("无法继续，按回车键返回主菜单...")
                input()
                continue
            
            test_codes = fund_codes[:10]
            print(f"将测试以下基金代码: {', '.join(test_codes)}")
            
            try:
                # 下载测试数据
                results = download_all_fund_overview(test_codes, use_multithread=True)
                
                if results:
                    print(f"\n成功下载 {len(results)} 个基金的测试数据")
                    # 显示前3条数据预览
                    for i, result in enumerate(results[:3]):
                        print(f"\n基金 {i+1}:")
                        for field in ['基金代码', '基金简称', '基金类型', '资产规模', '数据日期']:
                            print(f"  {field}: {result.get(field, 'N/A')}")
                    
                    # 询问是否保存测试数据
                    save_choice = input("\n是否保存测试数据到HDF5文件？(y/n): ")
                    if save_choice.lower() == 'y':
                        save_to_hdf5(results, target_hdf5_path)
            except Exception as e:
                print(f"测试过程中出错: {e}")
            
            print("\n快速测试完成，按回车键返回主菜单...")
            input()
        
        elif choice == '0':
            print("感谢使用基金基本面概况管理系统，再见！")
            break
        
        else:
            print("无效的选择，请重新输入")

def download_all_fund_data():
    """下载所有基金基本面信息（供其他模块调用）"""
    source_hdf5_path, target_hdf5_path = get_hdf5_paths()
    
    # 读取基金代码
    fund_codes = read_fund_codes_from_hdf5(source_hdf5_path)
    if not fund_codes:
        print("无法读取基金代码，下载失败")
        return False
    
    print(f"总共找到 {len(fund_codes)} 个基金代码")
    
    try:
        # 下载数据（默认使用多线程）
        results = download_all_fund_overview(fund_codes, use_multithread=True)
        
        # 保存数据
        if results:
            return save_to_hdf5(results, target_hdf5_path)
        else:
            return False
    except Exception as e:
        print(f"下载过程中出错: {e}")
        return False

# 主函数
if __name__ == "__main__":
    # 检查是否有命令行参数
    if len(sys.argv) > 1 and sys.argv[1] == "--auto":
        # 自动模式：下载所有数据并退出
        print("基金基本面概况管理系统 - 自动模式")
        download_all_fund_data()
    else:
        # 交互式菜单模式
        main_menu()