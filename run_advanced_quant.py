import os
import sys
import argparse
import time
from datetime import datetime

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入高级量化分析器
from advanced_quant_analysis import AdvancedQuantAnalyzer

def run_advanced_quant_analysis(hdf5_path=None, start_date=None, thread_mode='auto', custom_thread_count=None, output_file=None):
    """运行高级量化分析
    
    Args:
        hdf5_path: HDF5数据文件路径
        start_date: 分析起始日期，格式为YYYYMMDD
        thread_mode: 线程模式，可选值为'auto'、'single'或'custom'
        custom_thread_count: 自定义线程数，仅在thread_mode为'custom'时有效
        output_file: 输出Excel文件路径
    """
    print("=== 高级基金量化分析系统 ===")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 创建高级量化分析器实例
        analyzer = AdvancedQuantAnalyzer(hdf5_path=hdf5_path, start_date_str=start_date)
        print(f"分析时间范围: {analyzer.start_date.strftime('%Y-%m-%d')} 至 {analyzer.end_date.strftime('%Y-%m-%d')}")
        
        # 开始计时
        start_time = time.time()
        
        # 执行分析
        success = analyzer.analyze_all_funds(thread_mode=thread_mode, custom_thread_count=custom_thread_count)
        
        # 计算分析耗时
        analysis_time = time.time() - start_time
        
        if success:
            print(f"\n分析完成！总耗时: {analysis_time:.2f} 秒")
            print(f"成功分析基金数量: {len(analyzer.results)}")
            
            # 导出结果到Excel
            excel_file = analyzer.export_to_excel(output_file)
            if excel_file:
                print(f"量化分析结果已导出至: {excel_file}")
                return excel_file
        else:
            print(f"\n量化分析失败！总耗时: {analysis_time:.2f} 秒")
            return None
            
    except Exception as e:
        print(f"运行过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
    
    return None

def main():
    """主函数，用于接收命令行参数并运行分析"""
    parser = argparse.ArgumentParser(description='基金高级量化分析工具')
    
    # 添加命令行参数
    parser.add_argument('--data', '-d', type=str, help='HDF5数据文件路径')
    parser.add_argument('--start-date', '-s', type=str, help='分析起始日期，格式为YYYYMMDD')
    parser.add_argument('--thread-mode', '-t', type=str, choices=['auto', 'single', 'custom'], 
                        default='auto', help='线程模式: auto(自动)、single(单线程)或custom(自定义)')
    parser.add_argument('--thread-count', '-n', type=int, help='自定义线程数，仅在thread-mode为custom时有效')
    parser.add_argument('--output', '-o', type=str, help='输出Excel文件路径')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 验证参数
    if args.thread_mode == 'custom' and not args.thread_count:
        parser.error('使用自定义线程模式时，必须指定线程数(--thread-count)')
    
    # 运行分析
    run_advanced_quant_analysis(
        hdf5_path=args.data,
        start_date=args.start_date,
        thread_mode=args.thread_mode,
        custom_thread_count=args.thread_count,
        output_file=args.output
    )

if __name__ == "__main__":
    main()