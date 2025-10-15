import h5py
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
import quant_analysis

PYPI_SOURCES = [
    {"name": "PyPI", "index_url": "https://pypi.org/simple", "trusted_host": "pypi.org"}
]

DELTA_THRESHOLD = 0.01

QUANT_VIEW_PATH = "data/quant_view.hdf5"


class QuantAnalyzer:
    def __new__(cls, *args, **kwargs):
        return quant_analysis.QuantAnalyzer(*args, **kwargs)


# 基金净值数据爬取函数
def fetch_fund_nav_data():
    """基金净值爬取"""
    try:
        # 导入并调用Fetch_Fund_Data模块的批量下载函数
        import Fetch_Fund_Data

        # 获取总页数
        total_pages = Fetch_Fund_Data.get_total_pages()
        if total_pages <= 0:
            print("无法获取总页数，操作取消")
            return False

        print(f"\n检测到总页数: {total_pages} 页")

        # 批量获取基金数据
        print("开始爬取基金数据，请稍候...")
        all_fund_data = Fetch_Fund_Data.batch_fetch_fund_data(total_pages)

        if not all_fund_data:
            print("未获取到任何基金数据")
            return False

        # 验证数据
        print("开始验证数据...")
        if Fetch_Fund_Data.verify_fund_data(all_fund_data):
            # 存储所有数据
            hdf5_path = Fetch_Fund_Data.get_hdf5_path()
            print(f"\n开始将数据存储到HDF5文件: {hdf5_path}")
            Fetch_Fund_Data.store_fund_data_to_hdf5(all_fund_data, hdf5_path)
            print("数据存储完成")

            # 显示最终统计信息
            print(f"\n===== 任务完成 =====")
            print(f"总页数: {total_pages}")
            print(f"成功爬取基金数量: {len(all_fund_data)}")
            print(f"数据已存储到: {hdf5_path}")
            return True
        else:
            print("数据验证失败")
            return False
    except Exception as e:
        print(f"调用基金净值数据下载功能时发生错误: {str(e)}")
        print("请确保Fetch_Fund_Data.py文件存在且完整")
        return False


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
        status_map = {0: None, 1: "开放申购", 2: "限制大额申购", 3: "暂停申购"}  # 不限

        status_text = status_map.get(status, None)
        if status_text:
            return Fund_Purchase_Status_Manager.filter_funds_by_purchase_status(
                status_text
            )
        else:
            # 如果状态为0（不限），返回所有基金
            hdf5_path = Fund_Purchase_Status_Manager.get_hdf5_path()
            if os.path.exists(hdf5_path):
                try:
                    import pandas as pd

                    df = pd.read_hdf(hdf5_path, key="fund_purchase_status")
                    return df[
                        [
                            "基金代码",
                            "基金简称",
                            "基金类型",
                            "最新净值/万份收益",
                            "申购状态",
                        ]
                    ]
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
        print(
            f"{'基金代码':<10} {'基金简称':<20} {'基金类型':<10} {'最新净值/万份收益':<15} {'申购状态':<10}"
        )
        print(f"{'-'*80}")

        for _, row in filtered_funds.iterrows():
            purchase_status = row.get("申购状态", "")
            print(
                f"{row['基金代码']:<10} {row['基金简称']:<20} {row['基金类型']:<10} {row['最新净值/万份收益']:<15} {purchase_status:<10}"
            )

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

        # 如果没有指定模块，默认全部执行
        if modules is None:
            modules = [1, 2, 3, 4, 5, 6, 7, 8]
            print(f"\n正在执行全部基金数据下载...")
        else:
            print(f"\n正在执行选定的基金数据下载模块...")

        # 下载基金基本信息和申购状态数据
        if 1 in modules:
            try:
                print(f"\n正在下载基金基本信息和申购状态数据...")
                import Fund_Purchase_Status_Manager

                success = Fund_Purchase_Status_Manager.download_all_fund_data()

                if success:
                    results["基金基本信息和申购状态数据"] = "下载成功"
                else:
                    results["基金基本信息和申购状态数据"] = "下载失败"
            except Exception as e:
                results["基金基本信息和申购状态数据"] = f"下载失败: {str(e)}"

        # 下载基金净值数据
        if 2 in modules:
            try:
                print(f"\n正在下载基金净值数据...")
                success = fetch_fund_nav_data()

                if success:
                    results["基金净值数据"] = "下载成功"
                else:
                    results["基金净值数据"] = "下载失败"
            except Exception as e:
                results["基金净值数据"] = f"下载失败: {str(e)}"

        # 下载财经网基金数据
        if 3 in modules:
            try:
                print(f"\n正在下载财经网基金数据...")
                # 使用subprocess调用模块，并传递--auto参数以避免交互式菜单
                subprocess.run([sys.executable, "fetch_cnjy_fund_data.py", "--auto"], check=True)
                results["财经网基金数据"] = "下载成功"
            except Exception as e:
                results["财经网基金数据"] = f"下载失败: {str(e)}"

        # 下载货币基金数据
        if 4 in modules:
            try:
                print(f"\n正在下载货币基金数据...")
                # 使用subprocess调用模块，并传递--auto参数以避免交互式菜单
                subprocess.run([sys.executable, "fetch_currency_fund_data.py", "--auto"], check=True)
                results["货币基金数据"] = "下载成功"
            except Exception as e:
                results["货币基金数据"] = f"下载失败: {str(e)}"

        # 下载场内交易基金排名数据
        if 5 in modules:
            try:
                print(f"\n正在下载场内交易基金排名数据...")
                # 使用subprocess调用模块，并传递--auto参数以避免交互式菜单
                subprocess.run([sys.executable, "fetch_fbs_fund_ranking.py", "--auto"], check=True)
                results["场内交易基金排名数据"] = "下载成功"
            except Exception as e:
                results["场内交易基金排名数据"] = f"下载失败: {str(e)}"

        # 下载货币基金排名数据
        if 6 in modules:
            try:
                print(f"\n正在下载货币基金排名数据...")
                # 使用subprocess调用模块，并传递--auto参数以避免交互式菜单
                subprocess.run([sys.executable, "fetch_hbx_fund_ranking.py", "--auto"], check=True)
                results["货币基金排名数据"] = "下载成功"
            except Exception as e:
                results["货币基金排名数据"] = f"下载失败: {str(e)}"

        # 下载开放基金排名数据
        if 7 in modules:
            try:
                print(f"\n正在下载开放基金排名数据...")
                # 使用subprocess调用模块，并传递--auto参数以避免交互式菜单
                subprocess.run([sys.executable, "fetch_open_fund_ranking.py", "--auto"], check=True)
                results["开放基金排名数据"] = "下载成功"
            except Exception as e:
                results["开放基金排名数据"] = f"下载失败: {str(e)}"

        # 通达信数据转换
        if 8 in modules:
            try:
                print(f"\n正在执行通达信数据转换...")
                # 使用subprocess调用模块，并传递--auto参数以避免交互式菜单
                subprocess.run([sys.executable, "TDX_To_HDF5.py", "--auto"], check=True)
                results["通达信数据转换"] = "转换成功"
            except Exception as e:
                results["通达信数据转换"] = f"转换失败: {str(e)}"

        return results

    def _precompute_quant_view(self, force_full=True):
        """预计算量化视图"""
        try:
            pass
        except Exception as e:
            print(f"预计算量化视图错误: {str(e)}")

    def generate_excel_report(self):
        """生成量化分析Excel报表，整合所有相关基金数据到单个工作表"""
        try:
            # 导入Excel报表生成器模块
            import excel_report_generator
            
            # 创建报表生成器实例
            generator = excel_report_generator.ExcelReportGenerator()
            
            # 调用独立模块的生成报表方法
            report_path = generator.generate_excel_report()
            
            return report_path
        except ImportError as e:
            print(f"导入excel_report_generator模块时出错: {str(e)}")
            return None
        except Exception as e:
            print(f"生成Excel报表时发生错误: {str(e)}")
            return None


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
    print("6. 通达信转换功能")
    print("7. 量化分析功能")
    print("0. 退出系统")


def show_module_selection_menu():
    """模块选择菜单"""
    print("\n=== 选择性下载基金数据 ===")
    print("请选择要下载的模块 (可多选，例如: 1,3,5):")
    print("1. 基金基本信息和申购状态数据 [Fund_Purchase_Status_Manager.py]")
    print("2. 基金净值数据 [Fetch_Fund_Data.py]")
    print("3. 财经网基金数据 [fetch_cnjy_fund_data.py]")
    print("4. 货币基金数据 [fetch_currency_fund_data.py]")
    print("5. 场内交易基金排名数据 [fetch_fbs_fund_ranking.py]")
    print("6. 货币基金排名数据 [fetch_hbx_fund_ranking.py]")
    print("7. 开放基金排名数据 [fetch_open_fund_ranking.py]")
    print("8. 通达信转换功能 [TDX_To_HDF5.py]")

    try:
        # 获取用户输入的模块选择
        selection = input("请输入选择 (0返回主菜单): ").strip()

        if selection == "0":
            return []

        # 解析用户选择的模块
        selected_modules = []
        if selection:
            # 分割用户输入的选择
            choices = selection.split(",")
            for choice in choices:
                choice = choice.strip()
                if choice.isdigit():
                    module_num = int(choice)
                    if 1 <= module_num <= 8:
                        selected_modules.append(module_num)

        return selected_modules
    except Exception as e:
        print(f"模块选择过程中发生错误: {str(e)}")
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
    # 移除按键确认环节


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

            if choice == "1":
                # 全量下载
                print("\n=== 一键下载全部基金数据 ===")
                print("此操作将下载所有模块的基金数据，可能需要较长时间")
                confirm = input("确认继续？(y/n): ").strip().lower()
                if confirm == "y":
                    results = orchestrator.execute_pipeline(parallel=False)
                    for module, result in results.items():
                        print(f"{module}: {result}")
                # 移除按键确认环节

            elif choice == "2":
                # 选择性下载
                while True:
                    modules = show_module_selection_menu()
                    if not modules:
                        break

                    results = orchestrator.execute_pipeline(
                        modules=modules, parallel=False
                    )
                    for module, result in results.items():
                        print(f"{module}: {result}")

                    # 显示下载完成提示
                    print("\n下载任务已完成！")
                    # 继续循环，等待用户输入下一步指令
                    # 用户可以通过输入0返回主菜单
                # 移除按键确认环节

            elif choice == "3":
                # 增量更新
                print("\n=== 增量更新量化视图数据 ===")
                print("正在更新量化视图数据...")
                try:
                    orchestrator._precompute_quant_view(force_full=False)
                    print("量化视图数据更新完成!")
                except Exception as e:
                    print(f"更新过程发生错误: {str(e)}")
                # 移除按键确认环节

            elif choice == "4":
                # 查询功能
                while True:
                    print("\n=== 基金数据查询功能 ===")
                    print("请选择要查询的数据源:")
                    print("1. 财经网基金数据 (fetch_cnjy_fund_data.py)")
                    print("2. 货币基金数据 (fetch_currency_fund_data.py)")
                    print("3. 场内交易基金排名数据 (fetch_fbs_fund_ranking.py)")
                    print("4. 基金净值数据 (Fetch_Fund_Data.py)")
                    print("5. 货币基金排名数据 (fetch_hbx_fund_ranking.py)")
                    print("6. 开放基金排名数据 (fetch_open_fund_ranking.py)")
                    print("7. 综合基金数据 (Read_HDF5_Data.py)")
                    print("8. 基金申购状态管理")
                    print("0. 返回主菜单")
                    
                    sub_choice = input("请输入功能选项: ").strip()
                    
                    if sub_choice == "0":
                        break  # 退出查询功能菜单，返回主菜单
                    elif sub_choice == "8":
                        # 基金申购状态管理子菜单
                        while True:
                            print("\n=== 基金申购状态管理 ===")
                            print("1. 下载基金基本信息和申购状态数据")
                            print("2. 查询基金基本信息")
                            print("3. 查询基金申购状态")
                            print("4. 按申购状态筛选基金")
                            print("5. 显示所有基金代码")
                            print("0. 返回上一级菜单")
                            
                            申购_status_sub_choice = input("请输入功能选项: ").strip()
                            
                            if 申购_status_sub_choice == "0":
                                break
                            elif 申购_status_sub_choice == "1":
                                print("正在下载基金基本信息和申购状态数据...")
                                try:
                                    download_fund_status_data()
                                    print("数据下载完成!")
                                except Exception as e:
                                    print(f"下载过程发生错误: {str(e)}")
                            elif 申购_status_sub_choice == "2":
                                fund_code = input("请输入基金代码: ").strip()
                                if fund_code:
                                    try:
                                        display_fund_basic_info(fund_code)
                                    except Exception as e:
                                        print(f"查询过程发生错误: {str(e)}")
                                else:
                                    print("请输入有效的基金代码")
                            elif 申购_status_sub_choice == "3":
                                fund_code = input("请输入基金代码: ").strip()
                                if fund_code:
                                    try:
                                        display_fund_purchase_status(fund_code)
                                    except Exception as e:
                                        print(f"查询过程发生错误: {str(e)}")
                                else:
                                    print("请输入有效的基金代码")
                            elif 申购_status_sub_choice == "4":
                                try:
                                    status_choice = input(
                                        "请输入申购状态 (0: 不限, 1: 可申购, 2: 限大额, 3: 暂停申购): "
                                    ).strip()
                                    status = (
                                        int(status_choice) if status_choice.isdigit() else 0
                                    )
                                    filtered_funds = filter_funds_by_purchase_status(status)
                                    display_filtered_funds(filtered_funds)
                                except Exception as e:
                                    print(f"筛选过程发生错误: {str(e)}")
                            elif 申购_status_sub_choice == "5":
                                try:
                                    display_all_fund_codes()
                                except Exception as e:
                                    print(f"显示过程发生错误: {str(e)}")
                            else:
                                print("无效的功能选项，请重新输入")
                    elif sub_choice == "1":
                        print("\n=== 财经网基金数据查询 ===")
                        try:
                            import fetch_cnjy_fund_data
                            # 调用完整菜单，而不仅仅是显示基金代码
                            fetch_cnjy_fund_data.main()
                        except Exception as e:
                            print(f"财经网基金数据查询失败: {str(e)}")
                            print("请确保fetch_cnjy_fund_data.py文件存在且完整")
                    elif sub_choice == "2":
                        print("\n=== 货币基金数据查询 ===")
                        try:
                            import fetch_currency_fund_data
                            # 调用完整菜单，而仅仅是显示基金代码
                            fetch_currency_fund_data.main()
                        except Exception as e:
                            print(f"货币基金数据查询失败: {str(e)}")
                            print("请确保fetch_currency_fund_data.py文件存在且完整")
                    elif sub_choice == "3":
                        print("\n=== 场内交易基金排名数据查询 ===")
                        try:
                            import fetch_fbs_fund_ranking
                            # 调用完整菜单，而仅仅是显示基金代码
                            fetch_fbs_fund_ranking.main()
                        except Exception as e:
                            print(f"场内交易基金排名数据查询失败: {str(e)}")
                            print("请确保fetch_fbs_fund_ranking.py文件存在且完整")
                    elif sub_choice == "4":
                        print("\n=== 基金净值数据查询 ===")
                        try:
                            import Fetch_Fund_Data
                            # 调用完整菜单，而仅仅是显示基金代码
                            Fetch_Fund_Data.main()
                        except Exception as e:
                            print(f"基金净值数据查询失败: {str(e)}")
                            print("请确保Fetch_Fund_Data.py文件存在且完整")
                    elif sub_choice == "5":
                        print("\n=== 货币基金排名数据查询 ===")
                        try:
                            import fetch_hbx_fund_ranking
                            # 调用完整菜单，而仅仅是显示基金代码
                            fetch_hbx_fund_ranking.main()
                        except Exception as e:
                            print(f"货币基金数据查询失败: {str(e)}")
                            print("请确保fetch_hbx_fund_ranking.py文件存在且完整")
                    elif sub_choice == "6":
                        print("\n=== 开放基金排名数据查询 ===")
                        try:
                            import fetch_open_fund_ranking
                            # 调用完整菜单，而仅仅是显示基金代码
                            fetch_open_fund_ranking.main()
                        except Exception as e:
                            print(f"开放基金排名数据查询失败: {str(e)}")
                            print("请确保fetch_open_fund_ranking.py文件存在且完整")
                    elif sub_choice == "7":
                        print("\n=== 综合基金数据查询 ===")
                        try:
                            import Read_HDF5_Data
                            # 直接调用Read_HDF5_Data的main函数，使用其完整的菜单系统
                            Read_HDF5_Data.main()
                        except Exception as e:
                            print(f"综合基金数据查询失败: {str(e)}")
                            print("请确保Read_HDF5_Data.py文件存在且完整")
                    else:
                        print("无效的功能选项，请重新输入")

                    # 显示提示信息，确保用户知道可以继续选择其他数据源
                    print("\n(提示：查询完成后将自动返回查询功能菜单)")

            elif choice == "5":
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
                # 移除按键确认环节

            elif choice == "6":
                # 通达信转换功能
                print("\n=== TDX数据转HDF5格式 ===")
                print("正在处理通达信.day文件并转换为HDF5格式...")
                try:
                    # 调用TDX_To_HDF5.py中的函数
                    convert_tdx_to_hdf5()
                    print("通达信数据转换完成!")
                except Exception as e:
                    print(f"转换过程发生错误: {str(e)}")
                # 移除按键确认环节

            elif choice == "7":
                # 量化分析功能
                show_quant_analysis_menu(orchestrator)

            elif choice == "0":
                print("谢谢使用基金数据管理系统，再见!")
                break

            else:
                print("无效的功能选项，请重新输入")
    except KeyboardInterrupt:
        print("\n程序已被用户中断")
    except Exception as e:
        print(f"程序运行发生错误: {str(e)}")
        # 移除按键确认环节


if __name__ == "__main__":
    main()
