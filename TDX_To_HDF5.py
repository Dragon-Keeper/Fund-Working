import struct
import os
import csv
import datetime
import glob
import time
import sys
import threading
import queue
import pickle
import hashlib
import psutil
import platform
import argparse  # 添加argparse模块导入
import mmap  # 添加mmap模块导入
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import h5py
import numpy as np
import pandas as pd

# 检查并安装必要的依赖
def check_and_install_dependencies():
    """检查并安装必要的依赖"""
    required_packages = ['pandas', 'numpy', 'h5py', 'psutil']
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

# 获取系统信息
def get_system_info():
    """获取系统信息"""
    system_info = {
        'cpu_count': psutil.cpu_count(logical=True),
        'memory_total': psutil.virtual_memory().total,
        'system': platform.system(),
        'python_version': platform.python_version()
    }
    return system_info

# 动态调整线程数
def calculate_optimal_threads(system_info, max_threads=8, default_threads=1):
    """根据系统资源动态计算最优线程数"""
    # 获取CPU核心数和内存信息
    cpu_count = system_info['cpu_count']
    memory_total = system_info['memory_total']
    
    # 基于CPU核心数计算线程数
    cpu_based_threads = min(cpu_count, max_threads)
    
    # 基于内存计算线程数（假设每个线程需要至少512MB内存）
    memory_based_threads = min(int(memory_total / (512 * 1024 * 1024)), max_threads)
    
    # 取两者中的较小值
    optimal_threads = min(cpu_based_threads, memory_based_threads)
    
    # 确保至少有一个线程
    optimal_threads = max(optimal_threads, default_threads)
    
    return optimal_threads

# 动态调整批量写入大小
def calculate_optimal_batch_size(system_info, file_count, min_batch=50, max_batch=500):
    """根据系统资源和文件数量动态计算最优批量写入大小"""
    # 获取内存信息
    memory_total = system_info['memory_total']
    memory_available = psutil.virtual_memory().available
    
    # 基于可用内存计算批量大小
    # 假设每个记录需要约200字节，每个文件平均有1000条记录
    estimated_record_size = 200
    estimated_records_per_file = 1000
    estimated_file_size = estimated_record_size * estimated_records_per_file
    
    # 计算基于内存的批量大小
    memory_based_batch = min(
        int(memory_available / (estimated_file_size * 2)),  # 保留一半内存用于系统
        max_batch
    )
    
    # 基于文件数量计算批量大小
    if file_count < 100:
        file_based_batch = min(file_count // 2, max_batch)
    else:
        file_based_batch = min(file_count // 10, max_batch)
    
    # 取两者中的较小值，但不小于最小批量
    optimal_batch = max(min(memory_based_batch, file_based_batch), min_batch)
    
    return optimal_batch

# 创建进度和状态保存目录
def get_cache_dir():
    """获取缓存目录路径"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    cache_dir = os.path.join(current_dir, "cache")
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    return cache_dir

# 保存处理状态
def save_processing_state(state, state_file):
    """保存处理状态到文件"""
    try:
        with open(state_file, 'wb') as f:
            pickle.dump(state, f)
        return True
    except Exception as e:
        print(f"保存处理状态时出错: {e}")
        return False

# 加载处理状态
def load_processing_state(state_file):
    """从文件加载处理状态"""
    try:
        if os.path.exists(state_file):
            with open(state_file, 'rb') as f:
                return pickle.load(f)
    except Exception as e:
        print(f"加载处理状态时出错: {e}")
    return None

# 计算文件哈希值
def calculate_file_hash(file_path):
    """计算文件哈希值，用于检查文件是否已更改"""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            # 只读取文件的前1KB和后1KB来计算哈希，提高速度
            chunk_size = 1024
            # 读取前1KB
            chunk = f.read(chunk_size)
            hash_md5.update(chunk)
            
            # 获取文件大小
            file_size = os.path.getsize(file_path)
            
            # 如果文件大于2KB，读取最后1KB
            if file_size > chunk_size * 2:
                f.seek(-chunk_size, os.SEEK_END)
                chunk = f.read(chunk_size)
                hash_md5.update(chunk)
        
        return hash_md5.hexdigest()
    except Exception as e:
        print(f"计算文件哈希值时出错: {e}")
        return None

# 使用内存映射技术解析基金数据
def parse_fund_data_mmap(file_path, min_date=None):
    """使用内存映射技术解析通达信基金历史净值数据文件"""
    try:
        # 获取文件大小
        file_size = os.path.getsize(file_path)
        
        # 检查文件大小是否是32字节的倍数
        if file_size % 32 != 0:
            print(f"警告: 文件 {file_path} 大小不是32字节的倍数，可能已损坏")
            return []
        
        # 计算记录数量
        record_count = file_size // 32
        
        # 使用内存映射读取文件
        with open(file_path, 'rb') as f:
            with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
                data = []
                
                # 流式处理，避免一次性加载所有数据
                for i in range(record_count):
                    # 读取32字节数据
                    offset = i * 32
                    buffer = mm[offset:offset+32]
                    
                    # 对于基金数据，通达信的格式可能与股票不同
                    # 尝试多种可能的解析格式
                    try:
                        # 格式1: IffffffI - 尝试原始格式
                        fields = struct.unpack('IffffffI', buffer)
                        format_used = 1
                    except:
                        try:
                            # 格式2: IfffffII - 可能的基金格式变体
                            fields = struct.unpack('IfffffII', buffer)
                            format_used = 2
                        except:
                            # 如果所有尝试都失败，跳过此记录
                            continue
                    
                    # 整理数据
                    # 将日期整数转换为年月日格式
                    date_int = fields[0]
                    year = date_int // 10000
                    month = (date_int % 10000) // 100
                    day = date_int % 100
                    date_str = f"{year:04d}-{month:02d}-{day:02d}"
                    
                    # 如果有最小日期限制，则进行筛选
                    if min_date and date_str < min_date:
                        continue
                    
                    # 根据使用的格式构建记录
                    if format_used == 1:
                        # 原始格式: IffffffI
                        record = {
                            'date': date_str,
                            'open': round(fields[1], 4),
                            'high': round(fields[2], 4),
                            'low': round(fields[3], 4),
                            'close': round(fields[4], 4),
                            'amount': fields[5] if fields[5] != 0 else None,
                            'volume': fields[6] if fields[6] != 0 else None,
                            'prev_close': round(fields[7], 4) if fields[7] != 0 else None
                        }
                    else:
                        # 格式2: IfffffII
                        record = {
                            'date': date_str,
                            'open': round(fields[1], 4),
                            'high': round(fields[2], 4),
                            'low': round(fields[3], 4),
                            'close': round(fields[4], 4),
                            'amount': fields[5] if fields[5] != 0 else None,
                            'volume': fields[6] if fields[6] != 0 else None,
                            'prev_close': round(fields[7], 4) if fields[7] != 0 else None
                        }
                    
                    data.append(record)
                
                # 按日期排序（可能需要，取决于文件存储顺序）
                data.sort(key=lambda x: x['date'])
                return data
    except Exception as e:
        print(f"解析文件 {file_path} 时出错: {e}")
        return []

# 增强错误恢复机制
def process_single_file_enhanced(args):
    """处理单个文件的函数，增强错误恢复机制"""
    file_path, storage_file, min_date, total_files, start_time, progress_queue, file_index, state_file, processed_files = args
    
    try:
        # 检查文件是否已处理
        file_hash = calculate_file_hash(file_path)
        if file_hash in processed_files:
            return True  # 文件已处理，跳过
        
        # 从文件名中提取股票代码（#号后6位数）
        filename = os.path.basename(file_path)
        if '#' in filename:
            stock_code = filename.split('#')[1].split('.')[0]
        else:
            print(f"\n无法从文件名 {filename} 中提取股票代码，跳过")
            return False
        
        # 解析数据（使用内存映射技术）
        fund_data = parse_fund_data_mmap(file_path, min_date)
        if not fund_data:
            print(f"\n文件 {filename} 中没有有效数据，跳过")
            return False
        
        # 保存到存储（使用流式处理）
        save_to_storage_streaming(fund_data, stock_code, storage_file, file_index, total_files, start_time, progress_queue)
        
        # 更新处理状态
        processed_files[file_hash] = {
            'file_path': file_path,
            'stock_code': stock_code,
            'process_time': time.time(),
            'record_count': len(fund_data)
        }
        
        # 定期保存处理状态
        if len(processed_files) % 10 == 0:  # 每处理10个文件保存一次状态
            # 创建字典的深拷贝，避免多线程并发修改导致的序列化问题
            processed_files_copy = processed_files.copy()
            state = {
                'processed_files': processed_files_copy,
                'total_files': total_files,
                'processed_count': len(processed_files_copy),
                'start_time': start_time
            }
            save_processing_state(state, state_file)
        
        return True
        
    except Exception as e:
        print(f"\n处理文件 {file_path} 时出错: {e}")
        # 记录错误文件
        error_dir = os.path.join(get_cache_dir(), "errors")
        if not os.path.exists(error_dir):
            os.makedirs(error_dir)
        
        error_file = os.path.join(error_dir, f"error_{time.time()}.txt")
        with open(error_file, 'w', encoding='utf-8') as f:
            f.write(f"文件路径: {file_path}\n")
            f.write(f"错误时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"错误信息: {str(e)}\n")
        
        return False

# 流式处理数据写入
def save_to_storage_streaming(data, stock_code, storage_file, current_file, total_files, start_time, progress_queue):
    """使用流式处理将数据保存到存储"""
    # 创建临时文件
    temp_file = f"{storage_file}.tmp_{stock_code}_{int(time.time())}"
    
    try:
        # 确保存储目录存在
        os.makedirs(os.path.dirname(storage_file), exist_ok=True)
        
        # 使用临时文件写入数据
        with h5py.File(temp_file, 'w') as hf_temp:
            # 创建组
            group = hf_temp.create_group(stock_code)
            
            # 将数据转换为numpy数组并存储
            dates = np.array([record['date'] for record in data], dtype='S10')
            opens = np.array([record['open'] for record in data], dtype='float32')
            highs = np.array([record['high'] for record in data], dtype='float32')
            lows = np.array([record['low'] for record in data], dtype='float32')
            closes = np.array([record['close'] for record in data], dtype='float32')
            amounts = np.array([record['amount'] for record in data], dtype='float64')
            volumes = np.array([record['volume'] for record in data], dtype='float64')
            prev_closes = np.array([record['prev_close'] for record in data], dtype='float32')
            
            # 创建数据集
            group.create_dataset('date', data=dates)
            group.create_dataset('open', data=opens)
            group.create_dataset('high', data=highs)
            group.create_dataset('low', data=lows)
            group.create_dataset('close', data=closes)
            group.create_dataset('amount', data=amounts)
            group.create_dataset('volume', data=volumes)
            group.create_dataset('prev_close', data=prev_closes)
            
            # 添加属性
            group.attrs['record_count'] = len(data)
        
        # 使用文件锁确保安全合并，增强锁机制
        lock_file = f"{storage_file}.lock"
        lock_acquired = False
        max_retries = 10  # 最大重试次数
        retry_count = 0
        
        try:
            # 尝试获取文件锁
            while not lock_acquired and retry_count < max_retries:
                try:
                    with open(lock_file, 'x') as f:
                        # 写入当前进程ID，便于调试
                        f.write(str(os.getpid()))
                        lock_acquired = True
                except FileExistsError:
                    retry_count += 1
                    sleep_time = 0.2 * retry_count  # 指数退避策略
                    time.sleep(sleep_time)  # 等待后重试
            
            if not lock_acquired:
                print(f"警告：无法获取文件锁，尝试直接写入 {stock_code}")
                # 即使没有锁也尝试写入，但会增加错误风险
                
            # 使用'a'模式直接打开主文件，避免重复创建
            try:
                with h5py.File(storage_file, 'a', libver='latest', swmr=False) as hf_main:
                    # 如果组已存在，先删除
                    if stock_code in hf_main:
                        del hf_main[stock_code]
                    
                    # 从临时文件复制数据
                    with h5py.File(temp_file, 'r') as hf_temp:
                        if stock_code in hf_temp:
                            hf_main.copy(hf_temp[stock_code], hf_main)
                            hf_main.flush()  # 确保数据写入磁盘
            except Exception as inner_e:
                print(f"合并数据时出错: {inner_e}")
                # 即使出错也继续，尝试下一个文件
        finally:
            # 释放文件锁
            if lock_acquired:
                try:
                    os.remove(lock_file)
                except:
                    pass
        
        # 删除临时文件
        try:
            os.remove(temp_file)
        except:
            pass
        
        # 将进度信息放入队列，添加success参数
        progress_queue.put((current_file, total_files, start_time, True))
        
    except Exception as e:
        print(f"保存数据时出错: {e}")
        # 尝试删除临时文件
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except:
            pass

# 批量写入数据到HDF5文件（优化版本）
def batch_write_to_hdf5_optimized(storage_file, batch_data):
    """优化版本的批量写入数据到HDF5文件"""
    if not batch_data:
        return
    
    # 确保存储目录存在
    os.makedirs(os.path.dirname(storage_file), exist_ok=True)
    
    # 使用文件锁确保文件操作安全，增强锁机制
    lock_file = f"{storage_file}.lock"
    lock_acquired = False
    max_retries = 10  # 最大重试次数
    retry_count = 0
    
    try:
        # 尝试获取文件锁
        while not lock_acquired and retry_count < max_retries:
            try:
                with open(lock_file, 'x') as f:
                    # 写入当前进程ID，便于调试
                    f.write(str(os.getpid()))
                    lock_acquired = True
            except FileExistsError:
                retry_count += 1
                sleep_time = 0.2 * retry_count  # 指数退避策略
                time.sleep(sleep_time)  # 等待后重试
        
        if not lock_acquired:
            print(f"警告：无法获取文件锁，尝试直接批量写入数据")
            # 即使没有锁也尝试写入，但会增加错误风险
        
        # 直接使用'a'模式打开HDF5文件，避免重复创建和打开
        try:
            with h5py.File(storage_file, 'a', libver='latest', swmr=False) as hf:
                for stock_code, records in batch_data.items():
                    # 如果组已存在，先删除
                    if stock_code in hf:
                        del hf[stock_code]
                    
                    # 创建新组
                    group = hf.create_group(stock_code)
                    
                    # 将数据转换为numpy数组并存储
                    dates = np.array([record['date'] for record in records], dtype='S10')
                    opens = np.array([record['open'] for record in records], dtype='float32')
                    highs = np.array([record['high'] for record in records], dtype='float32')
                    lows = np.array([record['low'] for record in records], dtype='float32')
                    closes = np.array([record['close'] for record in records], dtype='float32')
                    amounts = np.array([record['amount'] for record in records], dtype='float64')
                    volumes = np.array([record['volume'] for record in records], dtype='float64')
                    prev_closes = np.array([record['prev_close'] for record in records], dtype='float32')
                    
                    # 创建数据集
                    group.create_dataset('date', data=dates)
                    group.create_dataset('open', data=opens)
                    group.create_dataset('high', data=highs)
                    group.create_dataset('low', data=lows)
                    group.create_dataset('close', data=closes)
                    group.create_dataset('amount', data=amounts)
                    group.create_dataset('volume', data=volumes)
                    group.create_dataset('prev_close', data=prev_closes)
                    
                    # 添加属性
                    group.attrs['record_count'] = len(records)
                    
                hf.flush()  # 确保所有数据写入磁盘
        except Exception as inner_e:
            print(f"批量写入数据时出错: {inner_e}")
            # 尝试使用备选方法
            try:
                # 备选方法：使用不同的文件模式尝试写入
                with h5py.File(storage_file, 'a', libver='latest') as hf:
                    # 简化操作，只写入第一个数据集
                    first_stock = next(iter(batch_data.keys()))
                    if first_stock in hf:
                        del hf[first_stock]
                    group = hf.create_group(first_stock)
                    records = batch_data[first_stock]
                    group.create_dataset('date', data=np.array([record['date'] for record in records], dtype='S10'))
                    print(f"已尝试备选方法写入第一个数据集 {first_stock}")
            except Exception as fallback_e:
                print(f"备选方法也失败: {fallback_e}")
                
    except Exception as e:
        print(f"批量写入数据时出错: {e}")
        
    finally:
        # 释放文件锁
        if lock_acquired:
            try:
                os.remove(lock_file)
            except:
                pass

# 自动批量写入线程函数（优化版本）
def auto_batch_write_optimized(storage_file, batch_size):
    """优化版本的自动批量写入线程函数"""
    global data_cache_queue, batch_write_running
    
    batch_data = {}
    
    while batch_write_running or not data_cache_queue.empty():
        try:
            # 从队列中获取数据
            stock_code, data = data_cache_queue.get(timeout=0.1)
            
            # 添加到批量数据
            if stock_code not in batch_data:
                batch_data[stock_code] = []
            batch_data[stock_code].extend(data)
            
            # 检查是否达到批量大小
            total_records = sum(len(records) for records in batch_data.values())
            if total_records >= batch_size:
                batch_write_to_hdf5_optimized(storage_file, batch_data)
                batch_data = {}
            
            data_cache_queue.task_done()
            
        except queue.Empty:
            # 如果队列为空但有数据，执行批量写入
            if batch_data:
                batch_write_to_hdf5_optimized(storage_file, batch_data)
                batch_data = {}
            continue
        except Exception as e:
            print(f"自动批量写入时出错: {e}")
            continue
    
    # 确保所有剩余数据都被写入
    if batch_data:
        batch_write_to_hdf5_optimized(storage_file, batch_data)

# 更新进度显示的函数（优化版本）
def update_progress_optimized(progress_queue, total_files, start_time):
    """优化版本的更新进度显示函数"""
    processed_count = 0
    error_count = 0
    
    while True:
        try:
            # 从队列获取进度信息
            current_file, total_files, start_time, success = progress_queue.get(timeout=0.1)
            processed_count += 1
            
            if not success:
                error_count += 1
            
            # 计算进度和剩余时间
            progress = (processed_count / total_files) * 100
            elapsed_time = time.time() - start_time
            if processed_count > 0:
                avg_time_per_file = elapsed_time / processed_count
                remaining_files = total_files - processed_count
                remaining_time = avg_time_per_file * remaining_files
            else:
                remaining_time = 0
            
            # 显示进度信息
            sys.stdout.write(f"\r处理进度: {progress:.1f}% ({processed_count}/{total_files}) | 错误: {error_count} | 剩余时间: {remaining_time:.1f}秒")
            sys.stdout.flush()
            
            # 标记任务完成
            progress_queue.task_done()
            
        except queue.Empty:
            # 检查是否所有文件都已处理完成
            if processed_count >= total_files:
                break
            continue

# 处理目录下所有的.day文件（优化版本）
def process_all_day_files_optimized(source_dir, storage_file, min_date=None, num_threads=None):
    """优化版本的处理目录下所有的.day文件"""
    # 检查并安装依赖
    check_and_install_dependencies()
    
    # 获取系统信息
    system_info = get_system_info()
    
    # 动态计算线程数
    if num_threads is None:
        num_threads = calculate_optimal_threads(system_info)
        print(f"根据系统资源自动计算最优线程数: {num_threads}")
    
    # 确保存储目录存在
    os.makedirs(os.path.dirname(storage_file), exist_ok=True)
    
    # 获取所有.day文件
    day_files = glob.glob(os.path.join(source_dir, "*.day"))
    
    if not day_files:
        print(f"在目录 {source_dir} 中未找到.day文件")
        return
    
    # 动态计算批量写入大小
    batch_size = calculate_optimal_batch_size(system_info, len(day_files))
    print(f"根据系统资源和文件数量自动计算最优批量大小: {batch_size}")
    
    print(f"找到 {len(day_files)} 个.day文件，使用 {num_threads} 个线程开始处理...")
    print(f"使用HDF5格式存储数据到: {storage_file}")
    if min_date:
        print(f"只提取 {min_date} 之后的数据")
    
    # 创建状态文件
    state_file = os.path.join(get_cache_dir(), "processing_state.pkl")
    
    # 检查存储文件是否存在
    storage_file_exists = os.path.exists(storage_file)
    
    # 尝试加载之前的处理状态
    state = load_processing_state(state_file)
    if state:
        print(f"检测到之前的处理状态，已处理 {state['processed_count']} 个文件")
        
        # 如果HDF5文件不存在但有处理状态，提供选择
        if not storage_file_exists:
            print(f"警告: 存储文件 '{storage_file}' 不存在，但有之前的处理状态记录")
            choice = input("是否重新开始处理? (y/n，默认为y): ").strip().lower() or "y"
            if choice == "y":
                print("将重新开始处理所有文件")
                processed_files = {}
                start_time = time.time()
            else:
                # 继续使用之前的状态，但创建新的存储文件
                print("将继续使用之前的处理状态，但创建新的存储文件")
                processed_files = state['processed_files']
                start_time = time.time()
        else:
            processed_files = state['processed_files']
            start_time = state['start_time']
    else:
        processed_files = {}
        start_time = time.time()
    
    # 创建进度队列
    progress_queue = queue.Queue()
    
    # 重置批量写入线程控制标志
    global batch_write_running, data_cache_queue
    batch_write_running = True
    data_cache_queue = queue.Queue()
    
    # 启动自动批量写入线程
    batch_thread = threading.Thread(target=auto_batch_write_optimized, args=(storage_file, batch_size))
    batch_thread.daemon = True
    batch_thread.start()
    
    # 准备参数列表，包含文件索引
    args_list = [(file_path, storage_file, min_date, len(day_files), start_time, progress_queue, i+1, state_file, processed_files) 
                 for i, file_path in enumerate(day_files)]
    
    # 启动进度更新线程
    progress_thread = threading.Thread(target=update_progress_optimized, args=(progress_queue, len(day_files), start_time))
    progress_thread.daemon = True
    progress_thread.start()
    
    # 使用线程池处理文件
    processed_count = 0
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # 提交所有任务
        future_to_file = {executor.submit(process_single_file_enhanced, args): args[0] for args in args_list}
        
        # 等待所有任务完成
        for future in as_completed(future_to_file):
            try:
                result = future.result()
                if result:
                    processed_count += 1
            except Exception as e:
                print(f"\n处理文件时发生异常: {e}")
    
    # 等待进度队列处理完成
    progress_queue.join()
    
    # 停止自动批量写入线程
    batch_write_running = False
    batch_thread.join(timeout=5)  # 等待最多5秒
    
    # 确保所有缓存数据都被写入文件
    while not data_cache_queue.empty():
        batch_data = {}
        while not data_cache_queue.empty():
            try:
                stock_code, data = data_cache_queue.get_nowait()
                if stock_code not in batch_data:
                    batch_data[stock_code] = []
                batch_data[stock_code].extend(data)
                data_cache_queue.task_done()
            except queue.Empty:
                break
        
        if batch_data:
            batch_write_to_hdf5_optimized(storage_file, batch_data)
    
    # 保存最终处理状态
    # 创建字典的深拷贝，避免多线程并发修改导致的序列化问题
    processed_files_copy = processed_files.copy()
    final_state = {
        'processed_files': processed_files_copy,
        'total_files': len(day_files),
        'processed_count': len(processed_files_copy),
        'start_time': start_time,
        'end_time': time.time()
    }
    save_processing_state(final_state, state_file)
    
    # 显示最终结果
    print(f"\n处理完成! 共处理了 {processed_count} 个文件")
    total_time = time.time() - start_time
    print(f"总耗时: {total_time:.1f}秒")
    
    # 统计存储的股票数量
    try:
        with h5py.File(storage_file, 'r') as hf:
            stock_count = len(hf.keys())
            print(f"成功存储 {stock_count} 只股票的数据")
    except Exception as e:
        print(f"统计股票数量时出错: {e}")

# 主函数
def main():
    """主函数，协调整个TDX数据转HDF5的过程"""
    # 导入必要的日志模块
    import logging
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("tdx_to_hdf5.log"),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='TDX数据转HDF5格式工具')
    parser.add_argument('--auto', action='store_true', help='自动模式：使用默认参数执行转换，无需用户交互')
    parser.add_argument('--source_dir', type=str, help='源数据目录路径（自动模式下可选）')
    parser.add_argument('--min_date', type=str, help='最小日期，格式为YYYYMMDD（自动模式下可选）')
    parser.add_argument('--max_workers', type=int, help='最大工作线程数（自动模式下可选）')
    args = parser.parse_args()
    
    # 获取系统信息
    system_info = get_system_info()
    logger.info(f"系统信息: {system_info}")
    print(f"===== TDX数据转HDF5格式工具 =====")
    print(f"当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"系统: {system_info}")

    # 默认参数
    default_source_dir = r"D:\SoftWare\TDX\vipdoc\ds\lday"
    default_storage_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "All_Fund_Data.h5")
    
    if args.auto:
        # 自动模式：使用默认参数或命令行参数执行
        try:
            print("\n自动模式：开始数据转换...")
            
            # 使用命令行参数或默认值
            source_dir = args.source_dir or default_source_dir
            storage_file = default_storage_file
            
            # 处理最小日期
            min_date = None
            if args.min_date:
                date_input = args.min_date
                try:
                    if len(date_input) == 8 and date_input.isdigit():
                        year = date_input[:4]
                        month = date_input[4:6]
                        day = date_input[6:8]
                        min_date = f"{year}-{month}-{day}"
                        print(f"将只提取 {min_date} 之后的数据")
                except Exception as e:
                    logger.warning(f"日期处理出错: {e}，将提取所有数据")
            
            # 计算合适的线程数
            if args.max_workers:
                num_threads = args.max_workers
            else:
                # 自动计算线程数
                num_threads = None
            
            logger.info(f"自动模式参数 - 源目录: {source_dir}, 最小日期: {min_date}, 线程数: {num_threads}")
            print(f"源数据目录: {source_dir}")
            print(f"最小日期: {min_date or '全部'}")
            print(f"工作线程数: {'自动计算' if num_threads is None else num_threads}")
            
            # 执行数据处理
            start_time = time.time()
            process_all_day_files_optimized(source_dir, storage_file, min_date, num_threads)
            end_time = time.time()
            
            elapsed_time = end_time - start_time
            logger.info(f"自动模式转换完成，耗时: {elapsed_time:.2f}秒")
            print(f"\n数据转换完成！总耗时: {elapsed_time:.2f}秒")
            
        except Exception as e:
            logger.error(f"自动模式执行失败: {str(e)}")
            print(f"\n错误：在自动模式下执行时发生异常: {str(e)}")
    else:
        # 交互式模式
        # 检查源目录是否存在，如果不存在则提示用户输入
        source_dir = default_source_dir
        storage_file = default_storage_file
        
        while not os.path.isdir(source_dir):
            print(f"警告：源目录 '{source_dir}' 不存在或无法访问")
            user_input = input("请输入通达信基金数据文件(.day)所在目录路径，或输入'q'退出程序: ").strip()
            if user_input.lower() == 'q':
                print("退出程序")
                return
            elif user_input:
                source_dir = user_input
            else:
                print("目录路径不能为空，请重新输入")
        
        # 获取用户输入的最小日期
        min_date = None
        date_input = input("请输入最小日期(格式: YYYYMMDD，直接回车则提取所有数据): ").strip()
        if date_input:
            try:
                # 验证日期格式
                if len(date_input) == 8 and date_input.isdigit():
                    year = date_input[:4]
                    month = date_input[4:6]
                    day = date_input[6:8]
                    min_date = f"{year}-{month}-{day}"
                    print(f"将只提取 {min_date} 之后的数据")
                else:
                    print("日期格式不正确，将提取所有数据")
            except Exception as e:
                print(f"日期处理出错: {e}，将提取所有数据")
        
        # 线程数选择菜单
        print("\n请选择线程数模式：")
        print("1. 单线程")
        print("2. auto自动计算最佳线程数")
        print("3. 退出")
        
        while True:
            choice = input("请输入选择 (1-3): ").strip()
            
            if choice == "1":
                num_threads = 1
                print("将使用单线程处理数据")
                break
            elif choice == "2":
                num_threads = None  # 将自动计算
                print("将根据系统资源自动计算最优线程数")
                break
            elif choice == "3":
                print("退出程序")
                return
            else:
                print("无效的选择，请重新输入")
        
        # 执行数据处理
        try:
            print("\n开始数据转换...")
            start_time = time.time()
            process_all_day_files_optimized(source_dir, storage_file, min_date, num_threads)
            end_time = time.time()
            
            elapsed_time = end_time - start_time
            logger.info(f"转换完成，耗时: {elapsed_time:.2f}秒")
            print(f"\n数据转换完成！总耗时: {elapsed_time:.2f}秒")
        except Exception as e:
            logger.error(f"数据转换失败: {str(e)}")
            print(f"\n错误：数据转换时发生异常: {str(e)}")

    print(f"\n程序结束于: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=====================================")
    logger.info("程序执行结束")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n程序已被用户中断")
    except Exception as e:
        print(f"\n程序运行时发生错误: {str(e)}")
        # 使用简单打印替代logger，避免在导入失败时出错
        print(f"日志记录失败: {str(e)}")