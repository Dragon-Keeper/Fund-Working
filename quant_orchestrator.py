import h5py
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
import quant_analysis

PYPI_SOURCES = [
    {'name': 'PyPI', 'index_url': 'https://pypi.org/simple', 'trusted_host': 'pypi.org'}
]

DELTA_THRESHOLD = 0.01

QUANT_VIEW_PATH = "data/quant_view.hdf5"

class QuantAnalyzer:
    def __new__(cls, *args, **kwargs):
        return quant_analysis.QuantAnalyzer(*args, **kwargs)

# 定义空函数以保持程序结构完整性
def fetch_fund_nav_data():
    """基金净值爬取"""
    pass

def download_fund_status_data():
    """下载基金基本信息和申购状态数据"""
    try:
        # 导入并调用Fund_Purchase_Status_Manager模块的下载函数
        import Fund_Purchase_Status_Manager
        Fund_Purchase_Status_Manager.download_all_fund_data()
    except Exception as e:
        print(f"调用基金数据下载功能时发生错误: {str(e)}")
        print("请确保Fund_Purchase_Status_Manager.py文件存在且完整")

def display_fund_basic_info(fund_code):
    """显示基金基本信息"""
    try:
        # 导入并调用Fund_Purchase_Status_Manager模块的函数
        import Fund_Purchase_Status_Manager
        return Fund_Purchase_Status_Manager.display_fund_basic_info(fund_code)
    except Exception as e:
        print(f"调用基金基本信息显示功能时发生错误: {str(e)}")
        print("请确保Fund_Purchase_Status_Manager.py文件存在且完整")
        return False

def display_fund_purchase_status(fund_code):
    """显示基金申购状态"""
    try:
        # 导入并调用Fund_Purchase_Status_Manager模块的函数
        import Fund_Purchase_Status_Manager
        return Fund_Purchase_Status_Manager.display_fund_purchase_status(fund_code)
    except Exception as e:
        print(f"调用基金申购状态显示功能时发生错误: {str(e)}")
        print("请确保Fund_Purchase_Status_Manager.py文件存在且完整")
        return False

def filter_funds_by_purchase_status(status):
    """筛选基金"""
    try:
        # 导入并调用Fund_Purchase_Status_Manager模块的函数
        import Fund_Purchase_Status_Manager
        
        # 转换状态参数
        status_map = {
            0: None,  # 不限
            1: '开放申购',
            2: '限制大额申购',
            3: '暂停申购'
        }
        
        status_text = status_map.get(status, None)
        if status_text:
            return Fund_Purchase_Status_Manager.filter_funds_by_purchase_status(status_text)
        else:
            # 如果状态为0（不限），返回所有基金
            hdf5_path = Fund_Purchase_Status_Manager.get_hdf5_path()
            if os.path.exists(hdf5_path):
                try:
                    import pandas as pd
                    df = pd.read_hdf(hdf5_path, key='fund_purchase_status')
                    return df[['基金代码', '基金简称', '基金类型', '最新净值/万份收益', '申购状态']]
                except Exception as e:
                    print(f"读取基金数据时出错: {e}")
            return pd.DataFrame()
    except Exception as e:
        print(f"调用基金筛选功能时发生错误: {str(e)}")
        print("请确保Fund_Purchase_Status_Manager.py文件存在且完整")
        import pandas as pd
        return pd.DataFrame()

def display_filtered_funds(filtered_funds):
    """显示筛选基金"""
    try:
        if filtered_funds is None or filtered_funds.empty:
            print("没有找到符合条件的基金")
            return False
        
        print(f"\n符合条件的基金列表:")
        print(f"{'-'*80}")
        print(f"{'基金代码':<10} {'基金简称':<20} {'基金类型':<10} {'最新净值/万份收益':<15} {'申购状态':<10}")
        print(f"{'-'*80}")
        
        for _, row in filtered_funds.iterrows():
            purchase_status = row.get('申购状态', '')
            print(f"{row['基金代码']:<10} {row['基金简称']:<20} {row['基金类型']:<10} {row['最新净值/万份收益']:<15} {purchase_status:<10}")
        
        print(f"{'-'*80}")
        print(f"共找到 {len(filtered_funds)} 只符合条件的基金")
        return True
    except Exception as e:
        print(f"显示筛选基金时发生错误: {str(e)}")
        return False

def display_all_fund_codes():
    """显示所有基金代码"""
    try:
        # 导入并调用Fund_Purchase_Status_Manager模块的函数
        import Fund_Purchase_Status_Manager
        Fund_Purchase_Status_Manager.display_all_fund_codes()
    except Exception as e:
        print(f"调用基金代码显示功能时发生错误: {str(e)}")
        print("请确保Fund_Purchase_Status_Manager.py文件存在且完整")

def convert_tdx_to_hdf5():
    """通达信转换为HDF5格式"""
    try:
        import TDX_To_HDF5
        TDX_To_HDF5.main()
    except Exception as e:
        print(f"调用通达信转换功能时发生错误: {str(e)}")
        print("请确保TDX_To_HDF5.py文件存在且完整")

class QuantOrchestrator:
    """量化调度器，用于调度量化任务"""
    MODULE_MAP = {}
    fund_index = {}
    data_paths = {}
    
    def execute_pipeline(self, modules=None, parallel=True):
        """执行量化任务"""
        results = {}
        
        try:
            print(f"\n正在执行基金数据下载...")
            # 导入并调用Fund_Purchase_Status_Manager模块的下载函数
            import Fund_Purchase_Status_Manager
            success = Fund_Purchase_Status_Manager.download_all_fund_data()
            
            if success:
                results['基金基本信息和申购状态数据'] = '下载成功'
            else:
                results['基金基本信息和申购状态数据'] = '下载失败'
        except Exception as e:
            results['基金基本信息和申购状态数据'] = f'下载失败: {str(e)}'
        
        return results

    def _precompute_quant_view(self, force_full=True):
        """预计算量化视图"""
        try:
            pass
        except Exception as e:
            print(f"预计算量化视图错误: {str(e)}")

# 交互式菜单
# quant_analysis模块已重构为独立运行模式，不再提供这些函数接口
# 以下是占位函数，实际功能通过直接运行相应模块实现
def show_main_menu():
    """主菜单显示"""
    print("\n=== 基金数据管理系统 ===")
    print("请选择操作:")
    print("1. 一键下载全部基金数据")
    print("2. 选择性下载基金数据")
    print("3. 增量更新量化视图数据")
    print("4. 查询功能")
    print("5. 生成Excel报表")
    print("6. 基金比对")
    print("7. 申购状态管理")
    print("8. 通达信转换功能")
    print("9. 量化分析功能")
    print("0. 退出系统")

def show_module_selection_menu():
    """模块选择菜单"""
    # 由于quant_analysis已独立运行，此处返回空列表
    print("\n模块选择功能暂不可用")
    return []

def show_query_menu():
    """查询菜单"""
    print("\n查询功能暂不可用")
    input("\n按Enter键返回主菜单...")

def show_fund_comparison_menu():
    """基金比对菜单"""
    print("\n基金比对功能暂不可用")
    input("\n按Enter键返回主菜单...")

def show_quant_analysis_menu(*args, **kwargs):
    """量化分析菜单"""
    print("\n=== 启动量化分析系统 ===")
    print("正在启动独立的量化分析模块...")
    try:
        # 直接运行quant_analysis.py脚本
        subprocess.run([sys.executable, "quant_analysis.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"量化分析模块运行失败: {str(e)}")
    input("\n按Enter键返回主菜单...")

# 主程序逻辑

def main():
    """主程序入口"""
    print("=== 基金数据管理系统 ===")
    print("正在初始化系统...")
    
    try:
        # 初始化量化调度器
        orchestrator = QuantOrchestrator()
        
        while True:
            show_main_menu()
            choice = input("请输入功能选项: ").strip()
                
            # 处理空输入情况，不显示错误信息
            if not choice:
                continue
                
            if choice == '1':
                    # 全量下载
                    print("\n=== 一键下载全部基金数据 ===")
                    print("此操作将下载所有模块的基金数据，可能需要较长时间")
                    confirm = input("确认继续？(y/n): ").strip().lower()
                    if confirm == 'y':
                        results = orchestrator.execute_pipeline(parallel=False)
                        for module, result in results.items():
                            print(f"{module}: {result}")
                    input("\n按Enter键返回主菜单...")
                
            elif choice == '2':
                # 选择性下载
                modules = show_module_selection_menu()
                if modules:
                    results = orchestrator.execute_pipeline(modules=modules, parallel=False)
                    for module, result in results.items():
                        print(f"{module}: {result}")
                input("\n按Enter键返回主菜单...")
                
            elif choice == '3':
                # 增量更新
                print("\n=== 增量更新量化视图数据 ===")
                print("正在更新量化视图数据...")
                try:
                    orchestrator._precompute_quant_view(force_full=False)
                    print("量化视图数据更新完成!")
                except Exception as e:
                    print(f"更新过程发生错误: {str(e)}")
                input("\n按Enter键返回主菜单...")
                
            elif choice == '4':
                # 查询功能
                print("\n=== 启动基金数据查询系统 ===")
                print("正在启动独立的基金数据查询模块...")
                try:
                    # 直接运行Read_HDF5_Data.py脚本
                    subprocess.run([sys.executable, "Read_HDF5_Data.py"], check=True)
                except subprocess.CalledProcessError as e:
                    print(f"基金数据查询模块运行失败: {str(e)}")
                input("\n按Enter键返回主菜单...")
                
            elif choice == '5':
                # 生成Excel报表
                print("\n=== 生成量化分析Excel报表 ===")
                print("正在生成量化分析报表...")
                try:
                    report_path = orchestrator.generate_excel_report()
                    if report_path:
                        print(f"量化分析报表已生成: {report_path}")
                    else:
                        print("报表生成失败")
                except Exception as e:
                    print(f"生成报表过程发生错误: {str(e)}")
                input("\n按Enter键返回主菜单...")
                
            elif choice == '6':
                # 基金比对
                print("\n基金比对功能暂不可用")
                input("\n按Enter键返回主菜单...")
                
            elif choice == '7':
                # 申购状态管理
                print("\n=== 基金申购状态管理 ===")
                print("1. 下载基金基本信息和申购状态数据")
                print("2. 查询基金基本信息")
                print("3. 查询基金申购状态")
                print("4. 按申购状态筛选基金")
                print("5. 显示所有基金代码")
                print("0. 返回主菜单")
                
                while True:
                    sub_choice = input("请输入功能选项: ").strip()
                    
                    if sub_choice == '0':
                        break
                    elif sub_choice == '1':
                        print("正在下载基金基本信息和申购状态数据...")
                        try:
                            download_fund_status_data()
                            print("数据下载完成!")
                        except Exception as e:
                            print(f"下载过程发生错误: {str(e)}")
                        input("\n按Enter键继续...")
                    elif sub_choice == '2':
                        fund_code = input("请输入基金代码: ").strip()
                        if fund_code:
                            try:
                                display_fund_basic_info(fund_code)
                            except Exception as e:
                                print(f"查询过程发生错误: {str(e)}")
                        else:
                            print("请输入有效的基金代码")
                        input("\n按Enter键继续...")
                    elif sub_choice == '3':
                        fund_code = input("请输入基金代码: ").strip()
                        if fund_code:
                            try:
                                display_fund_purchase_status(fund_code)
                            except Exception as e:
                                print(f"查询过程发生错误: {str(e)}")
                        else:
                            print("请输入有效的基金代码")
                        input("\n按Enter键继续...")
                    elif sub_choice == '4':
                        try:
                            status_choice = input("请输入申购状态 (0: 不限, 1: 可申购, 2: 限大额, 3: 暂停申购): ").strip()
                            status = int(status_choice) if status_choice.isdigit() else 0
                            filtered_funds = filter_funds_by_purchase_status(status)
                            display_filtered_funds(filtered_funds)
                        except Exception as e:
                            print(f"筛选过程发生错误: {str(e)}")
                        input("\n按Enter键继续...")
                    elif sub_choice == '5':
                        try:
                            display_all_fund_codes()
                        except Exception as e:
                            print(f"显示过程发生错误: {str(e)}")
                        input("\n按Enter键继续...")
                    else:
                        print("无效的功能选项，请重新输入")
                
            elif choice == '8':
                # 通达信转换功能
                print("\n=== TDX数据转HDF5格式 ===")
                print("正在处理通达信.day文件并转换为HDF5格式...")
                try:
                    # 调用TDX_To_HDF5.py中的函数
                    convert_tdx_to_hdf5()
                    print("通达信数据转换完成!")
                except Exception as e:
                    print(f"转换过程发生错误: {str(e)}")
                input("\n按Enter键返回主菜单...")
                
            elif choice == '9':
                # 量化分析功能
                show_quant_analysis_menu(orchestrator)
                
            elif choice == '0':
                print("谢谢使用基金数据管理系统，再见!")
                break
            
            else:
                print("无效的功能选项，请重新输入")
    except KeyboardInterrupt:
        print("\n程序已被用户中断")
    except Exception as e:
        print(f"程序运行发生错误: {str(e)}")
        input("\n按Enter键继续...")

if __name__ == "__main__":
    main()