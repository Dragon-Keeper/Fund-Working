import os
import sys
import time
import logging
from datetime import datetime

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'download.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def download_fund_data_if_needed(force_download=False, verify_only=False):
    """
    参数化的一键下载基金数据函数
    
    Args:
        force_download: 是否强制下载数据，True表示无论文件是否存在都下载
        verify_only: 是否仅验证数据存在性，True表示不下载数据，仅检查文件是否存在
    
    Returns:
        tuple: (success_flag, hdf5_path)
    """
    # 导入必要的模块
    try:
        # 动态导入以避免不必要的依赖加载
        from Fetch_Fund_Data import (
            check_and_install_dependencies,
            get_total_pages,
            batch_fetch_fund_data,
            verify_fund_data,
            store_fund_data_to_hdf5,
            get_hdf5_path
        )
        
        logger.info("===== 基金数据下载功能启动 =====")
        
        # 检查依赖
        if not verify_only:
            check_and_install_dependencies()
        
        # 获取HDF5路径
        hdf5_path = get_hdf5_path()
        logger.info(f"目标HDF5文件路径: {hdf5_path}")
        
        # 检查数据文件是否存在
        file_exists = os.path.exists(hdf5_path)
        
        if verify_only:
            # 仅验证模式
            logger.info(f"验证模式 - 检查文件是否存在: {file_exists}")
            return file_exists, hdf5_path
        
        if file_exists and not force_download:
            # 文件已存在且不强制下载
            logger.info(f"基金数据文件已存在: {hdf5_path}，跳过下载")
            print(f"基金数据文件已存在: {hdf5_path}")
            print("如需重新下载，请使用 --force-download 参数")
            return True, hdf5_path
        
        print(f"\n开始下载基金数据...")
        
        # 获取总页数
        total_pages = get_total_pages()
        if total_pages <= 0:
            logger.error("无法获取总页数，操作取消")
            print("\n错误：无法获取总页数，请检查网络连接或网站是否可访问！")
            return False, None
        
        logger.info(f"将爬取 {total_pages} 页基金数据")
        print(f"\n检测到总页数: {total_pages} 页")
        
        # 批量获取基金数据
        logger.info("开始爬取基金数据...")
        print("开始爬取基金数据，请稍候...")
        start_time = time.time()
        all_fund_data = batch_fetch_fund_data(total_pages)
        
        if not all_fund_data:
            logger.error("未获取到任何基金数据")
            print("\n错误：未获取到任何基金数据！")
            return False, None
        
        # 验证数据
        logger.info("开始验证数据...")
        print("\n开始验证数据...")
        if verify_fund_data(all_fund_data):
            # 存储所有数据
            logger.info(f"开始将数据存储到HDF5文件: {hdf5_path}")
            print(f"\n开始将数据存储到HDF5文件: {hdf5_path}")
            store_fund_data_to_hdf5(all_fund_data, hdf5_path)
            
            total_time = time.time() - start_time
            logger.info("数据存储完成")
            print("\n数据存储完成")
            
            # 显示最终统计信息
            logger.info(f"===== 下载任务完成 =====")
            logger.info(f"总页数: {total_pages}")
            logger.info(f"成功爬取基金数量: {len(all_fund_data)}")
            logger.info(f"数据已存储到: {hdf5_path}")
            logger.info(f"总耗时: {total_time:.2f}秒")
            
            print(f"\n===== 下载任务完成 =====")
            print(f"总页数: {total_pages}")
            print(f"成功爬取基金数量: {len(all_fund_data)}")
            print(f"数据已存储到: {hdf5_path}")
            print(f"总耗时: {total_time:.2f}秒")
            
            return True, hdf5_path
        else:
            logger.error("数据验证失败")
            print("\n错误：数据验证失败，可能需要检查爬取逻辑！")
            return False, None
    
    except Exception as e:
        logger.error(f"下载数据时发生错误: {str(e)}")
        print(f"\n错误：下载过程中发生异常: {str(e)}")
        import traceback
        traceback.print_exc()
        return False, None

def read_fund_abbreviations_from_hdf5(hdf5_path):
    """
    从HDF5文件读取基金简称数据
    
    Args:
        hdf5_path: HDF5文件路径
    
    Returns:
        dict: 基金代码到基金简称的映射
    """
    import h5py
    
    fund_abbreviations = {}
    
    try:
        with h5py.File(hdf5_path, 'r') as hf:
            for fund_code in hf.keys():
                try:
                    if 'fund_name' in hf[fund_code].attrs:
                        fund_name = hf[fund_code].attrs['fund_name']
                        if isinstance(fund_name, bytes):
                            fund_name = fund_name.decode('utf-8')
                        fund_abbreviations[fund_code] = fund_name
                except Exception as e:
                    logger.warning(f"读取基金 {fund_code} 简称时出错: {str(e)}")
                    fund_abbreviations[fund_code] = f"未知基金_{fund_code}"
    except Exception as e:
        logger.error(f"从HDF5文件读取基金简称时出错: {str(e)}")
    
    return fund_abbreviations

def generate_fund_abbreviation_data_file(output_path, data_folder='data'):
    """
    读取data文件夹中所有HDF5数据库的基金简称，并生成用于Excel表头的数据
    
    Args:
        output_path: 输出文件路径
        data_folder: 数据文件夹路径
    """
    import json
    
    # 构建完整的数据文件夹路径
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), data_folder)
    
    # 收集所有HDF5文件
    hdf5_files = []
    if os.path.exists(data_dir):
        for root, _, files in os.walk(data_dir):
            for file in files:
                if file.endswith('.h5'):
                    hdf5_files.append(os.path.join(root, file))
    
    if not hdf5_files:
        logger.warning(f"未在 {data_dir} 中找到HDF5文件")
        # 返回一个空的基金简称字典
        return {}
    
    # 从所有HDF5文件中收集基金简称
    all_abbreviations = {}
    
    for hdf5_path in hdf5_files:
        logger.info(f"从文件 {hdf5_path} 读取基金简称")
        abbreviations = read_fund_abbreviations_from_hdf5(hdf5_path)
        all_abbreviations.update(abbreviations)
    
    logger.info(f"总共读取到 {len(all_abbreviations)} 个基金简称")
    
    # 保存到JSON文件供其他模块使用
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_abbreviations, f, ensure_ascii=False, indent=2)
    
    logger.info(f"基金简称数据已保存到: {output_path}")
    
    return all_abbreviations

if __name__ == "__main__":
    # 直接运行时显示帮助信息
    print("基金数据管理实用工具")
    print("可用功能:")
    print("1. 下载基金数据 (download_fund_data_if_needed)")
    print("2. 从HDF5文件读取基金简称 (read_fund_abbreviations_from_hdf5)")
    print("3. 生成基金简称数据文件 (generate_fund_abbreviation_data_file)")