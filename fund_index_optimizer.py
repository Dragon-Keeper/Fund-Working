#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
基金HDF5数据读取与分析系统 - 索引优化模块
用于优化基金代码索引的创建和缓存机制，提升模块加载速度
"""

import os
import sys
import h5py
import time
import pickle
import hashlib
import json
from datetime import datetime

class FundIndexManager:
    """基金索引管理器，负责索引的创建、缓存和更新"""
    
    def __init__(self, hdf5_path, cache_dir=None):
        """初始化索引管理器
        
        Args:
            hdf5_path: HDF5文件路径
            cache_dir: 缓存目录路径，如果为None则使用默认路径
        """
        self.hdf5_path = hdf5_path
        self.cache_dir = cache_dir or os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
        self.index_file = os.path.join(self.cache_dir, "fund_index_v2.pkl")
        self.meta_file = os.path.join(self.cache_dir, "fund_index_meta.json")
        
        # 确保缓存目录存在
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
    
    def _get_hdf5_file_info(self):
        """获取HDF5文件的基本信息，用于判断是否需要重建索引"""
        if not os.path.exists(self.hdf5_path):
            return None
            
        file_stat = os.stat(self.hdf5_path)
        file_size = file_stat.st_size
        file_mtime = file_stat.st_mtime
        
        # 计算文件哈希值（仅前1MB，用于快速判断文件是否变化）
        file_hash = None
        try:
            with open(self.hdf5_path, 'rb') as f:
                sample = f.read(1024 * 1024)  # 读取前1MB
                file_hash = hashlib.md5(sample).hexdigest()
        except:
            pass
            
        return {
            'path': self.hdf5_path,
            'size': file_size,
            'mtime': file_mtime,
            'hash': file_hash,
            'check_time': datetime.now().isoformat()
        }
    
    def _is_index_valid(self):
        """检查索引是否有效（是否存在且最新）"""
        # 检查索引文件是否存在
        if not os.path.exists(self.index_file) or not os.path.exists(self.meta_file):
            return False
            
        # 读取元数据
        try:
            with open(self.meta_file, 'r') as f:
                meta = json.load(f)
                
            # 获取当前HDF5文件信息
            current_info = self._get_hdf5_file_info()
            if not current_info:
                return False
                
            # 比较文件信息
            stored_info = meta.get('file_info', {})
            
            # 如果文件大小、修改时间或哈希值发生变化，则需要重建索引
            if (current_info['size'] != stored_info.get('size') or
                abs(current_info['mtime'] - stored_info.get('mtime', 0)) > 1 or
                current_info['hash'] != stored_info.get('hash')):
                return False
                
            # 检查索引版本
            if meta.get('version') != '2.0':
                return False
                
            return True
        except Exception as e:
            print(f"检查索引有效性时出错: {e}")
            return False
    
    def _create_index(self, force_recreate=False):
        """创建基金代码索引
        
        Args:
            force_recreate: 是否强制重新创建索引
            
        Returns:
            索引数据字典
        """
        # 如果不强制重建且索引有效，则直接加载现有索引
        if not force_recreate and self._is_index_valid():
            print("使用有效的缓存索引，无需重新创建")
            return self._load_index()
        else:
            if force_recreate:
                print("强制重新创建索引...")
            else:
                print("缓存索引无效或不存在，正在创建新的索引...")
            
        print("正在创建基金代码索引...")
        start_time = time.time()
        
        try:
            with h5py.File(self.hdf5_path, 'r') as hf:
                # 获取所有基金代码
                fund_codes = list(hf.keys())
                
                # 为每个基金代码创建索引信息
                index = {}
                for i, code in enumerate(fund_codes):
                    if i % 100 == 0 and i > 0:
                        print(f"已处理 {i}/{len(fund_codes)} 只基金...")
                        
                    group = hf[code]
                    record_count = group.attrs.get('record_count', 0)
                    first_date = group['date'][0].decode('utf-8') if record_count > 0 else ""
                    last_date = group['date'][-1].decode('utf-8') if record_count > 0 else ""
                    
                    index[code] = {
                        'record_count': int(record_count),
                        'first_date': first_date,
                        'last_date': last_date
                    }
                
                # 保存索引和元数据
                index_data = {
                    'version': '2.0',
                    'created_time': datetime.now().isoformat(),
                    'fund_count': len(fund_codes),
                    'index': index
                }
                
                with open(self.index_file, 'wb') as f:
                    pickle.dump(index_data, f)
                
                # 保存元数据
                meta_data = {
                    'version': '2.0',
                    'created_time': datetime.now().isoformat(),
                    'file_info': self._get_hdf5_file_info(),
                    'fund_count': len(fund_codes)
                }
                
                with open(self.meta_file, 'w') as f:
                    json.dump(meta_data, f, indent=2)
                
                elapsed_time = time.time() - start_time
                print(f"索引创建完成，包含 {len(fund_codes)} 只基金，耗时: {elapsed_time:.2f}秒")
                return index
        except Exception as e:
            print(f"创建索引时出错: {e}")
            return {}
    
    def _load_index(self):
        """加载已存在的索引
        
        Returns:
            索引数据字典
        """
        try:
            with open(self.index_file, 'rb') as f:
                index_data = pickle.load(f)
            
            print(f"已加载基金索引，包含 {index_data.get('fund_count', 0)} 只基金")
            return index_data.get('index', {})
        except Exception as e:
            print(f"加载索引时出错: {e}")
            return {}
    
    def get_index(self, force_recreate=False):
        """获取基金代码索引
        
        Args:
            force_recreate: 是否强制重新创建索引
            
        Returns:
            索引数据字典
        """
        return self._create_index(force_recreate)
    
    def get_fund_codes(self):
        """获取所有基金代码列表
        
        Returns:
            基金代码列表
        """
        index = self.get_index()
        return list(index.keys())
    
    def get_fund_info(self, fund_code):
        """获取指定基金的基本信息
        
        Args:
            fund_code: 基金代码
            
        Returns:
            基金信息字典，如果基金不存在则返回None
        """
        index = self.get_index()
        return index.get(fund_code)
    
    def update_index(self):
        """强制更新索引
        
        Returns:
            更新后的索引数据字典
        """
        return self._create_index(force_recreate=True)
    
    def get_index_stats(self):
        """获取索引统计信息
        
        Returns:
            索引统计信息字典
        """
        if not os.path.exists(self.meta_file):
            return {"status": "no_index"}
            
        try:
            with open(self.meta_file, 'r') as f:
                meta = json.load(f)
                
            # 获取当前HDF5文件信息
            current_info = self._get_hdf5_file_info()
            stored_info = meta.get('file_info', {})
            
            # 判断索引是否最新
            is_latest = (
                current_info and
                current_info['size'] == stored_info.get('size') and
                abs(current_info['mtime'] - stored_info.get('mtime', 0)) <= 1 and
                current_info['hash'] == stored_info.get('hash')
            )
            
            return {
                "status": "valid" if is_latest else "outdated",
                "version": meta.get('version'),
                "created_time": meta.get('created_time'),
                "fund_count": meta.get('fund_count'),
                "file_size_mb": round(stored_info.get('size', 0) / (1024*1024), 2),
                "file_mtime": datetime.fromtimestamp(stored_info.get('mtime', 0)).isoformat() if stored_info.get('mtime') else None
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}


# 兼容性函数，用于替换原有代码中的函数
def get_fund_index_manager(hdf5_path, cache_dir=None):
    """获取基金索引管理器实例
    
    Args:
        hdf5_path: HDF5文件路径
        cache_dir: 缓存目录路径，如果为None则使用默认路径
        
    Returns:
        FundIndexManager实例
    """
    return FundIndexManager(hdf5_path, cache_dir)


# 优化后的创建基金代码索引函数
def create_fund_index_optimized(storage_file, force_recreate=False):
    """优化后的创建基金代码索引函数
    
    Args:
        storage_file: HDF5文件路径
        force_recreate: 是否强制重新创建索引
        
    Returns:
        索引数据字典
    """
    manager = FundIndexManager(storage_file)
    return manager.get_index(force_recreate)


# 优化后的获取基金代码索引函数
def get_fund_index_optimized(storage_file):
    """优化后的获取基金代码索引函数
    
    Args:
        storage_file: HDF5文件路径
        
    Returns:
        索引数据字典
    """
    manager = FundIndexManager(storage_file)
    return manager.get_index()


# 优化后的获取所有基金代码函数
def get_all_fund_codes_optimized(storage_file):
    """优化后的获取所有基金代码函数
    
    Args:
        storage_file: HDF5文件路径
        
    Returns:
        基金代码列表
    """
    manager = FundIndexManager(storage_file)
    return manager.get_fund_codes()


# 优化后的获取基金基本信息函数
def get_fund_info_optimized(storage_file, fund_code):
    """优化后的获取基金基本信息函数
    
    Args:
        storage_file: HDF5文件路径
        fund_code: 基金代码
        
    Returns:
        基金信息字典，如果基金不存在则返回None
    """
    manager = FundIndexManager(storage_file)
    fund_info = manager.get_fund_info(fund_code)
    
    if fund_info:
        return {
            'stock_code': fund_code,
            'record_count': fund_info['record_count'],
            'first_date': fund_info['first_date'],
            'last_date': fund_info['last_date']
        }
    else:
        return None


if __name__ == "__main__":
    # 测试代码
    import sys
    from pathlib import Path
    
    # 获取HDF5文件路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    hdf5_path = os.path.join(current_dir, "data", "All_Fund_Data.h5")
    
    if not os.path.exists(hdf5_path):
        print(f"错误: HDF5文件不存在: {hdf5_path}")
        sys.exit(1)
    
    # 创建索引管理器
    manager = FundIndexManager(hdf5_path)
    
    # 显示索引状态
    stats = manager.get_index_stats()
    print(f"索引状态: {stats}")
    
    # 获取索引
    index = manager.get_index()
    print(f"基金数量: {len(index)}")
    
    # 获取前5个基金代码
    fund_codes = list(index.keys())[:5]
    print(f"前5个基金代码: {fund_codes}")
    
    # 获取第一个基金的信息
    if fund_codes:
        fund_info = manager.get_fund_info(fund_codes[0])
        print(f"基金 {fund_codes[0]} 信息: {fund_info}")