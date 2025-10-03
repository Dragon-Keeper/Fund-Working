# -*- coding: utf-8 -*-
"""检查data目录中的HDF5文件"""
import os
import h5py
import pandas as pd
from pathlib import Path

# 获取项目根目录
root_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(root_dir, 'data')

print(f"检查数据目录: {data_dir}")

# 检查data目录是否存在
if os.path.exists(data_dir):
    print("data目录已存在")
    
    # 列出data目录中的所有文件
    files = os.listdir(data_dir)
    if files:
        print(f"data目录中找到{len(files)}个文件:")
        
        # 检查HDF5文件
        hdf5_files = [f for f in files if f.endswith('.h5')]
        print(f"找到{len(hdf5_files)}个HDF5文件:")
        
        for hdf5_file in hdf5_files:
            file_path = os.path.join(data_dir, hdf5_file)
            print(f"\n文件: {hdf5_file}")
            print(f"路径: {file_path}")
            print(f"大小: {os.path.getsize(file_path) / 1024:.2f} KB")
            
            try:
                # 尝试打开HDF5文件查看内容结构
                with h5py.File(file_path, 'r') as f:
                    print(f"文件包含的键: {list(f.keys())}")
                    
                    # 尝试查看基金数量（如果有）
                    if 'fund_count' in f.attrs:
                        print(f"基金数量: {f.attrs['fund_count']}")
                    
                    # 尝试查看前几个基金代码
                    if 'funds' in f:
                        fund_group = f['funds']
                        fund_codes = list(fund_group.keys())[:5]  # 只显示前5个
                        print(f"前{min(5, len(fund_group))}个基金代码: {fund_codes}")
                        
                        # 查看第一个基金的数据结构
                        if fund_codes:
                            first_fund = fund_group[fund_codes[0]]
                            print(f"第一个基金({fund_codes[0]})的属性: {list(first_fund.attrs.keys())[:10]}")
            except Exception as e:
                print(f"读取文件内容时出错: {str(e)}")
    else:
        print("data目录为空")
else:
    print("data目录不存在")

print("\n检查完成")