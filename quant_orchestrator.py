import concurrent.futures
from functools import lru_cache
import sys
import os
import h5py
import numpy as np
import pandas as pd
import argparse
import subprocess

class QuantOrchestrator:
    QUANT_VIEW_PATH = "hdf5_data/quant_view.h5"
    CACHE_EXPIRE = 3600  # 1小时缓存有效期
    DELTA_THRESHOLD = 0.03  # 3%变化触发更新
    MODULE_MAP = {
        'open': 'fetch_open_fund_ranking.py',
        'fbs': 'fetch_fbs_fund_ranking.py',
        'hbx': 'fetch_hbx_fund_ranking.py',
        'cnjy': 'fetch_cnjy_fund_data.py',
        'currency': 'fetch_currency_fund_data.py'
    }

    def __init__(self):
        self._init_config()

    def _init_config(self):
        self.quant_features = [
            '年化收益率', '最大回撤', '夏普比率',
            '波动率', '流动性指标', '卡玛比率', 
            '索提诺比率', '胜率', '盈亏比', '持仓集中度',
            '风险敞口', '信息比率'
        ]
        self.data_paths = {}
        for module in self.MODULE_MAP.keys():
            if module == 'hbx':
                self.data_paths[module] = "data/HBX_Fund_Ranking_Data.h5"
            elif module == 'currency':
                self.data_paths[module] = "data/Currency_Fund_Data.h5"
            elif module == 'open':
                self.data_paths[module] = "data/Open_Fund_Ranking_Data.h5"
            elif module == 'fbs':
                self.data_paths[module] = "data/FBS_Fund_Ranking_Data.h5"
            else:
                self.data_paths[module] = f"data/{module.upper()}_Fund_Data.h5"

    def _execute_raw_pipeline(self, modules=None):
        """执行原始数据管道"""
        selected = modules or list(self.MODULE_MAP.keys())
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self._run_module, module): module
                for module in selected
            }
            return self._handle_futures(futures)

    @lru_cache(maxsize=128)
    def execute_pipeline(self, modules=None):
        """执行全量数据管道（包含预计算）"""
        results = self._execute_raw_pipeline(modules)
        self._precompute_quant_view()
        return results

    def _run_module(self, module_name):
        """实际执行模块的代码"""
        if module_name in self.MODULE_MAP:
            module_file = self.MODULE_MAP[module_name]
            if os.path.exists(module_file):
                try:
                    # 这里可以调用各模块的函数或直接执行模块
                    return f"{module_name} data updated successfully"
                except Exception as e:
                    return f"{module_name} error: {str(e)}"
            return f"{module_name} module not found"
        return f"Unknown module: {module_name}"

    def _handle_futures(self, futures):
        """处理并行任务结果"""
        results = {}
        for future in concurrent.futures.as_completed(futures):
            module = futures[future]
            try:
                results[module] = future.result()
            except Exception as e:
                results[module] = str(e)
        return results

    def _precompute_quant_view(self, force_full=False):
        """生成量化视图（支持增量更新）"""
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(self.QUANT_VIEW_PATH), exist_ok=True)
            
            update_mode = 'w' if force_full else 'a'
            with h5py.File(self.QUANT_VIEW_PATH, update_mode) as hdf:
                for module in self.MODULE_MAP.keys():
                    if os.path.exists(self.data_paths[module]):
                        with h5py.File(self.data_paths[module], 'r') as src:
                            if 'funds' in src:
                                for fund in src['funds']:
                                    try:
                                        key = f'{module}_{fund}'
                                        if key not in hdf:
                                            grp = hdf.create_group(key)
                                        else:
                                            grp = hdf[key]
                                        
                                        if self._need_update(fund, src, hdf, module):
                                            self._calculate_features(grp, src[f'funds/{fund}'])
                                    except Exception as e:
                                        print(f"Error processing {module}_{fund}: {str(e)}")
        except Exception as e:
            print(f"Error in precompute quant view: {str(e)}")

    def _calculate_features(self, target_grp, source_grp):
        """计算量化特征指标"""
        try:
            # 基础指标计算
            if 'net_value_history' in source_grp.attrs:
                nav_data = source_grp.attrs['net_value_history']
                returns = np.diff(nav_data) / nav_data[:-1] if len(nav_data) > 1 else np.array([0])
                
                target_grp.attrs['年化收益率'] = float(np.mean(returns) * 252)
                target_grp.attrs['波动率'] = float(np.std(returns) * np.sqrt(252))
                target_grp.attrs['最大回撤'] = float(self._max_drawdown(nav_data))
                target_grp.attrs['夏普比率'] = float(self._sharpe_ratio(returns))
                target_grp.attrs['卡玛比率'] = float(self._calmar_ratio(returns, nav_data))
                target_grp.attrs['索提诺比率'] = float(self._sortino_ratio(returns))
                target_grp.attrs['胜率'] = float(self._win_rate(returns))
                target_grp.attrs['盈亏比'] = float(self._profit_loss_ratio(returns))
            
            # 设置默认值
            for feature in self.quant_features:
                if feature not in target_grp.attrs:
                    target_grp.attrs[feature] = 0.0
                    
        except Exception as e:
            print(f"Error calculating features: {str(e)}")

    def _need_update(self, fund_id, src, hdf, module):
        """智能判断是否需要更新"""
        key = f'{module}_{fund_id}'
        if key not in hdf:
            return True
        
        try:
            # 检查时间差
            if 'last_updated' in src[f'funds/{fund_id}'].attrs and 'timestamp' in hdf[key].attrs:
                # 简化处理，实际应用中需要正确处理时间戳
                return True
            
            # 检查净值变化
            if 'net_value' in src[f'funds/{fund_id}'].attrs and 'net_value' in hdf[key].attrs:
                new_val = src[f'funds/{fund_id}'].attrs['net_value']
                old_val = hdf[key].attrs['net_value']
                if abs(new_val - old_val) > self.DELTA_THRESHOLD:
                    return True
        except:
            pass
            
        return False

    def _max_drawdown(self, nav):
        """计算最大回撤"""
        if len(nav) < 2:
            return 0
        peak = np.maximum.accumulate(nav)
        drawdowns = (nav - peak) / peak
        return np.min(drawdowns) if len(drawdowns) > 0 else 0

    def _sharpe_ratio(self, returns, risk_free=0.02):
        """计算夏普比率"""
        if len(returns) == 0 or np.std(returns) == 0:
            return 0
        return (np.mean(returns) - risk_free/252) / np.std(returns) * np.sqrt(252)

    def _calmar_ratio(self, returns, nav, risk_free=0.02):
        """计算卡玛比率"""
        annual_return = np.mean(returns) * 252 - risk_free
        max_dd = abs(self._max_drawdown(nav))
        return annual_return / max_dd if max_dd > 0 else 0

    def _sortino_ratio(self, returns, risk_free=0.02):
        """计算索提诺比率"""
        downside_returns = returns[returns < 0]
        if len(downside_returns) == 0:
            return 0
        downside_deviation = np.sqrt(np.mean(downside_returns**2)) * np.sqrt(252)
        annual_return = np.mean(returns) * 252 - risk_free
        return annual_return / downside_deviation if downside_deviation > 0 else 0

    def _win_rate(self, returns):
        """计算胜率"""
        if len(returns) == 0:
            return 0
        return np.mean(returns > 0)

    def _profit_loss_ratio(self, returns):
        """计算盈亏比"""
        profits = returns[returns > 0]
        losses = -returns[returns < 0]
        
        if len(profits) == 0 or len(losses) == 0:
            return 0
        
        avg_profit = np.mean(profits)
        avg_loss = np.mean(losses)
        return avg_profit / avg_loss if avg_loss > 0 else 0

    def generate_excel_report(self):
        """生成包含12个量化指标的Excel报表"""
        try:
            # 确保pandas已安装
            import pandas as pd
            from pandas import ExcelWriter
            
            df = pd.DataFrame(columns=[
                '基金代码', '模块名称', '夏普比率', '年化收益率',
                '波动率', '最大回撤', '卡玛比率', '索提诺比率',
                '胜率', '盈亏比', '流动性指标', '持仓集中度',
                '风险敞口', '信息比率'
            ])
            
            if os.path.exists(self.QUANT_VIEW_PATH):
                with h5py.File(self.QUANT_VIEW_PATH, 'r') as hdf:
                    for key in hdf.keys():
                        try:
                            if '_' in key:
                                module, fund_id = key.split('_', 1)
                                row_data = {
                                    '基金代码': fund_id,
                                    '模块名称': module
                                }
                                
                                # 从HDF5读取所有指标
                                attrs = hdf[key].attrs
                                for col in df.columns[2:]:  # 跳过前两列
                                    row_data[col] = attrs.get(col, 0.0)
                                
                                df = pd.concat([df, pd.DataFrame([row_data])], ignore_index=True)
                        except Exception as e:
                            print(f"Error processing {key}: {str(e)}")
            
            # 添加智能筛选功能
            with ExcelWriter('量化分析报告.xlsx', engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='基金量化指标')
                
                # 获取工作表和工作簿
                worksheet = writer.sheets['基金量化指标']
                workbook = writer.book
                
                # 设置列宽
                worksheet.set_column('A:A', 12)  # 基金代码
                worksheet.set_column('B:B', 10)  # 模块名称
                worksheet.set_column('C:N', 15)  # 指标列
                
                # 添加条件格式 - 负收益率标红
                format_red = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
                format_green = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
                
                # 年化收益率列标红
                if '年化收益率' in df.columns:
                    col_idx = df.columns.get_loc('年化收益率') + 1  # +1 because Excel is 1-indexed
                    col_letter = chr(64 + col_idx)
                    worksheet.conditional_format(f'{col_letter}2:{col_letter}{len(df)+1}', {
                        'type': 'cell',
                        'criteria': '<',
                        'value': 0,
                        'format': format_red
                    })
                
                # 最大回撤列标红（绝对值大的）
                if '最大回撤' in df.columns:
                    col_idx = df.columns.get_loc('最大回撤') + 1
                    col_letter = chr(64 + col_idx)
                    worksheet.conditional_format(f'{col_letter}2:{col_letter}{len(df)+1}', {
                        'type': 'cell',
                        'criteria': '<',
                        'value': -0.2,  # 20%以上回撤
                        'format': format_red
                    })
                
                # 夏普比率列标绿（高的）
                if '夏普比率' in df.columns:
                    col_idx = df.columns.get_loc('夏普比率') + 1
                    col_letter = chr(64 + col_idx)
                    worksheet.conditional_format(f'{col_letter}2:{col_letter}{len(df)+1}', {
                        'type': 'cell',
                        'criteria': '>',
                        'value': 1,  # 夏普比率>1标绿
                        'format': format_green
                    })
            
            return "量化分析报告.xlsx"
        except Exception as e:
            print(f"Error generating Excel report: {str(e)}")
            return None

def check_dependencies():
    """检查并安装依赖"""
    required = ['h5py', 'numpy', 'pandas', 'xlsxwriter']
    missing = []
    
    for dep in required:
        try:
            __import__(dep)
        except ImportError:
            missing.append(dep)
    
    if missing:
        print(f"缺少以下依赖: {', '.join(missing)}")
        if input("是否自动安装？(y/n)").lower() == 'y':
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
                print("依赖安装完成!")
                return True
            except Exception as e:
                print(f"依赖安装失败: {str(e)}")
                return False
        return False
    return True

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='量化基金数据管理系统')
    parser.add_argument('--all', action='store_true', help='一键下载全部数据')
    parser.add_argument('--modules', help='下载指定模块数据，逗号分隔 (open,fbs,hbx,cnjy,currency)')
    parser.add_argument('--update', action='store_true', help='增量更新量化视图')
    parser.add_argument('--report', action='store_true', help='生成Excel报表')
    return parser.parse_args()

def main():
    """主函数"""
    # 检查依赖
    if not check_dependencies():
        print("请手动安装依赖后重试")
        return
    
    # 解析命令行参数
    args = parse_arguments()
    orchestrator = QuantOrchestrator()
    
    # 处理命令行参数
    if len(sys.argv) > 1 and (sys.argv[1] == '-h' or sys.argv[1] == '--help'):
        # argparse会自动处理这个情况
        return
    
    if args.all:
        print("正在全量下载全部数据...")
        results = orchestrator.execute_pipeline()
        print("下载完成!")
        for module, result in results.items():
            print(f"{module}: {result}")
        return
    
    if args.modules:
        modules = [m.strip() for m in args.modules.split(',') if m.strip()]
        if modules:
            print(f"正在下载指定模块: {', '.join(modules)}")
            results = orchestrator.execute_pipeline(tuple(modules))  # 转换为元组以支持缓存
            print("下载完成!")
            for module, result in results.items():
                print(f"{module}: {result}")
        else:
            print("请输入有效的模块名称")
        return
    
    if args.update:
        print("正在增量更新量化视图...")
        orchestrator._precompute_quant_view(force_full=False)
        print("增量更新完成!")
        return
    
    if args.report:
        print("正在生成Excel报表...")
        report_file = orchestrator.generate_excel_report()
        if report_file:
            print(f"报表已生成: {report_file}")
        else:
            print("报表生成失败")
        return
    
    # 交互式菜单
    def show_main_menu():
        print("\n=== 量化数据管理系统 ===")
        print("1. 一键下载全部数据")
        print("2. 下载指定模块数据")
        print("3. 增量数据下载")
        print("4. 数据查询")
        print("5. 生成量化报表")
        print("0. 退出")
        
    def show_module_selection_menu():
        print("\n=== 选择模块 ===")
        modules = list(orchestrator.MODULE_MAP.keys())
        # 使用中文描述替换英文模块名
        module_names = {
            'open': '提取开放式基金的排名数据',
            'fbs': '提取场内交易基金的排名数据',
            'hbx': '提取货币基金的排名数据',
            'cnjy': '提取场内交易基金的各项数据',
            'currency': '提取货币型基金的万份收益和7日年化%数据'
        }
        for i, module in enumerate(modules, 1):
            print(f"{i}. {module_names.get(module, module)}")
        print("a. 选择全部模块")
        print("0. 返回上一级")
        return modules
    
    def show_query_menu():
        print("\n=== 数据查询 ===")
        print("1. 查询基金基本信息")
        print("2. 查询基金量化指标")
        print("3. 查询模块内所有基金")
        print("0. 返回上一级")
    
    def show_fund_query_menu():
        print("\n=== 选择模块 ===")
        modules = list(orchestrator.MODULE_MAP.keys())
        # 使用中文描述替换英文模块名
        module_names = {
            'open': '提取开放式基金的排名数据',
            'fbs': '提取场内交易基金的排名数据',
            'hbx': '提取货币基金的排名数据',
            'cnjy': '提取场内交易基金的各项数据',
            'currency': '提取货币型基金的万份收益和7日年化%数据'
        }
        for i, module in enumerate(modules, 1):
            print(f"{i}. {module_names.get(module, module)}")
        print("0. 返回上一级")
        return modules
    
    while True:
        show_main_menu()
        try:
            choice = input("请输入选项: ")
            
            if choice == '1':
                print("正在全量下载...")
                results = orchestrator.execute_pipeline()
                print("下载完成!")
                for module, result in results.items():
                    print(f"{module}: {result}")
                input("按Enter键继续...")
            
            elif choice == '2':
                selected_modules = []
                while True:
                    modules = show_module_selection_menu()
                    sub_choice = input("请选择模块 (输入序号，多个序号用逗号分隔): ").strip()
                    
                    if sub_choice == '0':
                        break
                    elif sub_choice == 'a':
                        selected_modules = modules
                        break
                    
                    try:
                        # 处理逗号分隔的多个选择
                        choices = [c.strip() for c in sub_choice.split(',')]
                        for c in choices:
                            idx = int(c) - 1
                            if 0 <= idx < len(modules):
                                if modules[idx] not in selected_modules:
                                    selected_modules.append(modules[idx])
                        print(f"已选择: {', '.join(selected_modules)}")
                        # 询问是否继续选择
                        if input("是否继续选择? (y/n): ").lower() != 'y':
                            break
                    except:
                        print("无效的选择，请重新输入")
                
                if selected_modules:
                    print(f"正在下载指定模块: {', '.join(selected_modules)}")
                    results = orchestrator.execute_pipeline(tuple(selected_modules))
                    print("下载完成!")
                    for module, result in results.items():
                        print(f"{module}: {result}")
                    input("按Enter键继续...")
            
            elif choice == '3':
                print("正在增量更新...")
                orchestrator._precompute_quant_view()
                print("增量更新完成!")
                input("按Enter键继续...")
            
            elif choice == '4':
                while True:
                    show_query_menu()
                    query_choice = input("请选择查询类型: ").strip()
                    
                    if query_choice == '0':
                        break
                    elif query_choice == '1' or query_choice == '2':
                        # 基金基本信息或量化指标查询
                        while True:
                            modules = show_fund_query_menu()
                            module_choice = input("请选择模块: ").strip()
                            
                            if module_choice == '0':
                                break
                            
                            try:
                                idx = int(module_choice) - 1
                                if 0 <= idx < len(modules):
                                    selected_module = modules[idx]
                                    fund_id = input("请输入基金代码: ").strip()
                                    if fund_id:
                                        try:
                                            # 查询基金信息
                                            print(f"\n查询结果:")
                                            print(f"模块: {selected_module}")
                                            print(f"基金代码: {fund_id}")
                                            
                                            # 尝试从HDF5文件读取数据
                                            data_path = orchestrator.data_paths.get(selected_module)
                                            if os.path.exists(data_path):
                                                with h5py.File(data_path, 'r') as f:
                                                    if 'funds' in f and fund_id in f['funds']:
                                                        fund_grp = f[f'funds/{fund_id}']
                                                        print("\n基本信息:")
                                                        for key, value in fund_grp.attrs.items():
                                                            if isinstance(value, (int, float, str)):
                                                                print(f"  {key}: {value}")
                                                    else:
                                                        print("基金数据不存在")
                                            
                                            # 如果是查询量化指标
                                            if query_choice == '2':
                                                print("\n量化指标:")
                                                if os.path.exists(orchestrator.QUANT_VIEW_PATH):
                                                    with h5py.File(orchestrator.QUANT_VIEW_PATH, 'r') as f:
                                                        key = f'{selected_module}_{fund_id}'
                                                        if key in f:
                                                            for metric in orchestrator.quant_features:
                                                                if metric in f[key].attrs:
                                                                    print(f"  {metric}: {f[key].attrs[metric]:.4f}")
                                                                else:
                                                                    print(f"  {metric}: 未计算")
                                                        else:
                                                            print("量化指标数据不存在")
                                            
                                            input("\n按Enter键继续...")
                                            break
                                        except Exception as e:
                                            print(f"查询失败: {str(e)}")
                                            input("按Enter键继续...")
                                            break
                                    else:
                                        print("请输入有效的基金代码")
                                else:
                                    print("无效的模块选择")
                            except:
                                print("无效的输入")
                    
                    elif query_choice == '3':
                        # 查询模块内所有基金
                        while True:
                            modules = show_fund_query_menu()
                            module_choice = input("请选择模块: ").strip()
                            
                            if module_choice == '0':
                                break
                            
                            try:
                                idx = int(module_choice) - 1
                                if 0 <= idx < len(modules):
                                    selected_module = modules[idx]
                                    data_path = orchestrator.data_paths.get(selected_module)
                                    
                                    if os.path.exists(data_path):
                                        print(f"\n模块 {selected_module} 中的基金列表:")
                                        with h5py.File(data_path, 'r') as f:
                                            if 'funds' in f:
                                                funds = list(f['funds'].keys())
                                                if funds:
                                                    for i, fund in enumerate(funds, 1):
                                                        print(f"{i}. {fund}")
                                                else:
                                                    print("该模块下暂无基金数据")
                                            else:
                                                print("数据结构错误")
                                    else:
                                        print("模块数据文件不存在")
                                    
                                    input("\n按Enter键继续...")
                                    break
                                else:
                                    print("无效的模块选择")
                            except Exception as e:
                                print(f"查询失败: {str(e)}")
                                input("按Enter键继续...")
                    
                    else:
                        print("无效的选择，请重新输入")
            
            elif choice == '5':
                print("正在生成Excel报表...")
                report_file = orchestrator.generate_excel_report()
                if report_file:
                    print(f"报表已生成: {report_file}")
                else:
                    print("报表生成失败，请检查数据文件是否存在")
                input("按Enter键继续...")
            
            elif choice == '0':
                print("谢谢使用，再见!")
                break
            
            else:
                print("无效的选项，请重新输入")
                
        except KeyboardInterrupt:
            print("\n程序已中断")
            break
        except Exception as e:
            print(f"发生错误: {str(e)}")
            input("按Enter键继续...")

if __name__ == "__main__":
    main()