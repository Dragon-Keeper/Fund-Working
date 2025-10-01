import concurrent.futures
from functools import lru_cache
import sys
import os
import h5py
import numpy as np
import pandas as pd
import argparse
import subprocess
from datetime import datetime
import time
import json

# 添加对其他模块的导入
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import importlib.util

# 初始化函数和变量
download_fund_status_data = None
display_fund_basic_info = None
display_fund_purchase_status = None
filter_funds_by_purchase_status = None
display_filtered_funds = None
display_all_fund_codes = None

fetch_fund_nav_data = None
get_total_pages = None
store_fund_data_to_hdf5 = None
query_fund_nav = None

convert_tdx_to_hdf5 = None

# 使用importlib.util来导入带数字前缀的模块
def load_module_from_file(module_name, file_path):
    """从文件路径加载模块"""
    try:
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None:
            raise ImportError(f"无法创建模块规范: {file_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        print(f"加载模块 {file_path} 失败: {str(e)}")
        return None

try:
    # 动态导入1.Fund_Purchase_Status_Manager.py
    fund_purchase_file = os.path.join(os.path.dirname(__file__), '1.Fund_Purchase_Status_Manager.py')
    fund_purchase_manager = load_module_from_file('fund_purchase_manager', fund_purchase_file)
    if fund_purchase_manager:
        download_fund_status_data = getattr(fund_purchase_manager, 'download_all_fund_data', None)
        display_fund_basic_info = getattr(fund_purchase_manager, 'display_fund_basic_info', None)
        display_fund_purchase_status = getattr(fund_purchase_manager, 'display_fund_purchase_status', None)
        filter_funds_by_purchase_status = getattr(fund_purchase_manager, 'filter_funds_by_purchase_status', None)
        display_filtered_funds = getattr(fund_purchase_manager, 'display_filtered_funds', None)
        display_all_fund_codes = getattr(fund_purchase_manager, 'display_all_fund_codes', None)
    
    # 动态导入3.Fetch_Fund_Data.py
    fetch_fund_file = os.path.join(os.path.dirname(__file__), '3.Fetch_Fund_Data.py')
    fetch_fund_data = load_module_from_file('fetch_fund_data', fetch_fund_file)
    if fetch_fund_data:
        fetch_fund_nav_data = getattr(fetch_fund_data, 'batch_fetch_fund_data', None)
        get_total_pages = getattr(fetch_fund_data, 'get_total_pages', None)
        store_fund_data_to_hdf5 = getattr(fetch_fund_data, 'store_fund_data_to_hdf5', None)
        query_fund_nav = getattr(fetch_fund_data, 'query_fund_by_code', None)
    
    # 动态导入2.TDX_To_HDF5.py
    tdx_file = os.path.join(os.path.dirname(__file__), '2.TDX_To_HDF5.py')
    tdx_to_hdf5 = load_module_from_file('tdx_to_hdf5', tdx_file)
    if tdx_to_hdf5:
        convert_tdx_to_hdf5 = getattr(tdx_to_hdf5, 'process_all_day_files_optimized', None)
    
    # 检查是否所有模块都成功导入
    all_functions = [download_fund_status_data, display_fund_basic_info, display_fund_purchase_status,
                    filter_funds_by_purchase_status, display_filtered_funds, display_all_fund_codes,
                    fetch_fund_nav_data, get_total_pages, store_fund_data_to_hdf5, query_fund_nav,
                    convert_tdx_to_hdf5]
    
    if all(f is not None for f in all_functions):
        print("所有模块导入成功！")
    else:
        missing_functions = [f.__name__ for f in all_functions if f is None]
        print(f"警告: 部分函数导入失败")

except Exception as e:
    print(f"模块导入过程发生错误: {str(e)}")
    print("警告: 某些功能可能无法使用")

# 项目所需的所有依赖项
ALL_REQUIRED_DEPENDENCIES = [
    'pandas>=1.3.0',
    'numpy>=1.20.0',
    'h5py>=3.0.0',
    'tables>=3.6.0',
    'requests>=2.25.0',
    'beautifulsoup4>=4.9.0',
    'plotly>=5.0.0',
    'openpyxl>=3.0.0',
    'xlsxwriter>=3.0.0',
    'psutil>=5.8.0',
    'akshare>=1.4.0',
    'playwright>=1.20.0'
]

# 安装源配置
PYPI_SOURCES = [
    {
        'name': '阿里源',
        'index_url': 'https://mirrors.aliyun.com/pypi/simple',
        'trusted_host': 'mirrors.aliyun.com'
    },
    {
        'name': '清华源',
        'index_url': 'https://pypi.tuna.tsinghua.edu.cn/simple/',
        'trusted_host': 'pypi.tuna.tsinghua.edu.cn'
    }
]

class DataAccessLayer:
    """统一数据访问层，负责处理数据冗余和统一访问"""
    def __init__(self):
        # 字段映射配置，处理不同模块间的字段差异
        self.field_mappings = {
            # 通用字段映射
            'net_value': {'open': '最新净值', 'fbs': '最新价格', 'hbx': '当前净值', 'cnjy': '最新净值', 'currency': '单位净值'},
            'date': {'open': '日期', 'fbs': '日期', 'hbx': '日期', 'cnjy': '更新日期', 'currency': '日期'},
            'fund_name': {'open': '基金名称', 'fbs': '基金名称', 'hbx': '基金名称', 'cnjy': '基金名称', 'currency': '基金名称'},
            # 模块特定字段
            'yield_7d': {'currency': '7日年化收益率'},
            'yield_10k': {'currency': '万份收益'}
        }
        
        # 主数据源配置，用于解决冲突时的优先选择
        self.primary_source = 'open'  # 默认以开放式基金数据为主
        
        # 数据质量缓存
        self.data_quality_cache = {}
        
    def get_field_name(self, module, common_field):
        """获取特定模块的实际字段名"""
        if common_field in self.field_mappings and module in self.field_mappings[common_field]:
            return self.field_mappings[common_field][module]
        return common_field
    
    def get_common_field(self, module, specific_field):
        """将特定模块的字段名转换为通用字段名"""
        for common_field, mappings in self.field_mappings.items():
            if module in mappings and mappings[module] == specific_field:
                return common_field
        return specific_field
    
    def get_data(self, module, fund_id, field, default=None):
        """统一的数据获取接口"""
        orchestrator = QuantOrchestrator()
        data_path = orchestrator.data_paths.get(module)
        if not os.path.exists(data_path):
            return default
            
        try:
            with h5py.File(data_path, 'r') as f:
                if 'funds' in f and fund_id in f['funds']:
                    fund_grp = f[f'funds/{fund_id}']
                    specific_field = self.get_field_name(module, field)
                    return fund_grp.attrs.get(specific_field, default)
        except Exception:
            pass
        return default
    
    def get_consolidated_data(self, fund_id, field, default=None):
        """获取跨模块的整合数据"""
        orchestrator = QuantOrchestrator()
        data_values = []
        
        for module in orchestrator.MODULE_MAP.keys():
            value = self.get_data(module, fund_id, field)
            if value is not None:
                # 记录数据来源和质量评分
                quality_score = self._assess_data_quality(module, fund_id, field, value)
                data_values.append((value, module, quality_score))
        
        if not data_values:
            return default
        
        # 优先选择主数据源
        for value, module, score in data_values:
            if module == self.primary_source:
                return value
        
        # 否则选择质量评分最高的数据
        data_values.sort(key=lambda x: x[2], reverse=True)
        return data_values[0][0]
    
    def _assess_data_quality(self, module, fund_id, field, value):
        """评估数据质量"""
        cache_key = f'{module}_{fund_id}_{field}'
        
        # 检查缓存
        if cache_key in self.data_quality_cache:
            cached_score, timestamp = self.data_quality_cache[cache_key]
            # 10分钟内的缓存有效
            if time.time() - timestamp < 600:
                return cached_score
        
        # 计算质量分数（简化实现）
        score = 1.0  # 基础分数
        
        # 数据类型检查
        expected_type = self._get_expected_field_type(field)
        if expected_type and isinstance(value, expected_type):
            score += 0.5
        
        # 模块权重（主数据源得分更高）
        if module == self.primary_source:
            score += 1.0
        
        # 记录缓存
        self.data_quality_cache[cache_key] = (score, time.time())
        return score
    
    def _get_expected_field_type(self, field):
        """获取字段的预期数据类型"""
        type_mappings = {
            'net_value': (int, float),
            'date': str,
            'fund_name': str,
            'yield_7d': (int, float),
            'yield_10k': (int, float)
        }
        return type_mappings.get(field)

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
        self.data_access = DataAccessLayer()
        # 初始化数据索引系统
        self.fund_index = {}
        self.build_fund_index()

    def _init_config(self):
        self.quant_features = [
            '年化收益率', '最大回撤', '夏普比率',
            '波动率', '流动比率', 
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

    def build_fund_index(self):
        """构建基金索引，用于快速查找"""
        self.fund_index = {}
        
        for module in self.MODULE_MAP.keys():
            data_path = self.data_paths[module]
            if os.path.exists(data_path):
                try:
                    with h5py.File(data_path, 'r') as f:
                        if 'funds' in f:
                            for fund_id in f['funds'].keys():
                                if fund_id not in self.fund_index:
                                    self.fund_index[fund_id] = []
                                self.fund_index[fund_id].append(module)
                except Exception as e:
                    print(f"构建索引错误 ({module}): {str(e)}")
        
        print(f"已构建基金索引，共包含 {len(self.fund_index)} 个基金代码")

    def _execute_raw_pipeline(self, modules=None, parallel=False):
        """执行原始数据管道，支持并行或串行处理"""
        selected = modules or list(self.MODULE_MAP.keys())
        results = {}
        
        if parallel:
            # 并行处理模式
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {
                    executor.submit(self._run_module, module): module
                    for module in selected
                }
                results = self._handle_futures(futures)
        else:
            # 串行处理模式（逐个下载处理模块数据）
            for module in selected:
                try:
                    results[module] = self._run_module(module)
                except Exception as e:
                    results[module] = str(e)
        
        return results

    @lru_cache(maxsize=128)
    def execute_pipeline(self, modules=None, parallel=False):
        """执行全量数据管道（包含预计算），默认串行处理以避免系统无响应"""
        results = self._execute_raw_pipeline(modules, parallel=parallel)
        self.build_fund_index()
        self._precompute_quant_view()
        return results

    def _run_module(self, module_name):
        """实际执行模块的代码"""
        if module_name in self.MODULE_MAP:
            module_file = self.MODULE_MAP[module_name]
            if os.path.exists(module_file):
                try:
                    # 执行实际的模块代码
                    print(f"  正在处理 {module_name} 模块...")
                    start_time = time.time()
                    
                    # 对不同模块使用不同的执行方式，直接调用下载函数而不是显示菜单
                    if module_name == 'open':
                        # 对于开放式基金，直接调用下载函数
                        import fetch_open_fund_ranking
                        if hasattr(fetch_open_fund_ranking, 'download_all_open_funds'):
                            fetch_open_fund_ranking.download_all_open_funds()
                        else:
                            subprocess.run([sys.executable, module_file], check=True)
                    elif module_name == 'fbs':
                        # 对于fbs基金，直接调用下载函数
                        import fetch_fbs_fund_ranking
                        if hasattr(fetch_fbs_fund_ranking, 'download_all_fbs_funds'):
                            fetch_fbs_fund_ranking.download_all_fbs_funds()
                        else:
                            subprocess.run([sys.executable, module_file], check=True)
                    elif module_name == 'hbx':
                        # 对于hbx基金，直接调用下载函数（注意：hbx模块实际是货币基金）
                        import fetch_hbx_fund_ranking
                        if hasattr(fetch_hbx_fund_ranking, 'download_all_currency_funds'):
                            fetch_hbx_fund_ranking.download_all_currency_funds()
                        else:
                            subprocess.run([sys.executable, module_file], check=True)
                    elif module_name == 'cnjy':
                        # 对于cnjy基金，直接调用下载函数
                        import fetch_cnjy_fund_data
                        if hasattr(fetch_cnjy_fund_data, 'download_all_cnjy_funds'):
                            fetch_cnjy_fund_data.download_all_cnjy_funds()
                        else:
                            subprocess.run([sys.executable, module_file], check=True)
                    elif module_name == 'currency':
                        # 对于currency基金，直接调用下载函数
                        try:
                            import fetch_currency_fund_data
                            if hasattr(fetch_currency_fund_data, 'download_all_currency_funds'):
                                fetch_currency_fund_data.download_all_currency_funds()
                            else:
                                subprocess.run([sys.executable, module_file], check=True)
                        except ImportError:
                            # 如果currency模块导入失败，尝试使用hbx模块
                            import fetch_hbx_fund_ranking
                            if hasattr(fetch_hbx_fund_ranking, 'download_all_currency_funds'):
                                fetch_hbx_fund_ranking.download_all_currency_funds()
                            else:
                                subprocess.run([sys.executable, module_file], check=True)
                    else:
                        # 对于其他模块，使用默认的执行方式
                        subprocess.run([sys.executable, module_file], check=True)
                    
                    elapsed_time = time.time() - start_time
                    return f"{module_name} 模块数据更新成功 (耗时: {elapsed_time:.2f}秒)"
                except Exception as e:
                    return f"{module_name} 模块执行错误: {str(e)}"
            return f"{module_name} 模块文件不存在"
        return f"未知模块: {module_name}"

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
                                        print(f"处理 {module}_{fund} 错误: {str(e)}")
        except Exception as e:
            print(f"预计算量化视图错误: {str(e)}")

    def _calculate_features(self, target_grp, source_grp):
        """计算量化特征指标"""
        try:
            # 基础指标计算
            nav_data = None
            if 'net_value_history' in source_grp.attrs:
                nav_data = source_grp.attrs['net_value_history']
            elif '净值历史' in source_grp.attrs:
                nav_data = source_grp.attrs['净值历史']
                
            if nav_data is not None and len(nav_data) > 1:
                returns = np.diff(nav_data) / nav_data[:-1]
                
                target_grp.attrs['年化收益率'] = float(np.mean(returns) * 252)
                target_grp.attrs['波动率'] = float(np.std(returns) * np.sqrt(252))
                target_grp.attrs['最大回撤'] = float(self._max_drawdown(nav_data))
                target_grp.attrs['夏普比率'] = float(self._sharpe_ratio(returns))
                target_grp.attrs['卡玛比率'] = float(self._calmar_ratio(returns, nav_data))
                target_grp.attrs['索提诺比率'] = float(self._sortino_ratio(returns))
                target_grp.attrs['胜率'] = float(self._win_rate(returns))
                target_grp.attrs['盈亏比'] = float(self._profit_loss_ratio(returns))
            else:
                # 设置默认值
                target_grp.attrs['年化收益率'] = 0.0
                target_grp.attrs['波动率'] = 0.0
                target_grp.attrs['最大回撤'] = 0.0
                target_grp.attrs['夏普比率'] = 0.0
                target_grp.attrs['卡玛比率'] = 0.0
                target_grp.attrs['索提诺比率'] = 0.0
                target_grp.attrs['胜率'] = 0.0
                target_grp.attrs['盈亏比'] = 0.0
            
            # 其他指标默认值
            target_grp.attrs['流动比率'] = 0.0
            target_grp.attrs['持仓集中度'] = 0.0
            target_grp.attrs['风险敞口'] = 0.0
            target_grp.attrs['信息比率'] = 0.0
            
            # 添加时间戳
            target_grp.attrs['timestamp'] = datetime.now().isoformat()
            
        except Exception as e:
            print(f"计算特征指标错误: {str(e)}")

    def _need_update(self, fund_id, src, hdf, module):
        """智能判断是否需要更新"""
        key = f'{module}_{fund_id}'
        if key not in hdf:
            return True
        
        try:
            # 检查时间差（如果目标存在时间戳）
            if 'timestamp' in hdf[key].attrs:
                # 尝试获取源数据的更新时间
                src_timestamp = None
                if 'update_time' in src[f'funds/{fund_id}'].attrs:
                    src_timestamp = src[f'funds/{fund_id}'].attrs['update_time']
                elif 'timestamp' in src[f'funds/{fund_id}'].attrs:
                    src_timestamp = src[f'funds/{fund_id}'].attrs['timestamp']
                
                if src_timestamp:
                    # 这里简化处理，实际应用中需要正确处理时间戳
                    try:
                        hdf_time = datetime.fromisoformat(hdf[key].attrs['timestamp'])
                        src_time = datetime.fromisoformat(src_timestamp) if isinstance(src_timestamp, str) else src_timestamp
                        if src_time > hdf_time:
                            return True
                    except:
                        # 时间解析错误，默认更新
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
                '基金代码', '模块名称', '基金名称', '最新净值',
                '年化收益率', '最大回撤', '夏普比率',
                '波动率', '卡玛比率', '索提诺比率',
                '胜率', '盈亏比', '流动比率', '持仓集中度',
                '风险敞口', '信息比率', '数据来源'
            ])
            
            if os.path.exists(self.QUANT_VIEW_PATH):
                with h5py.File(self.QUANT_VIEW_PATH, 'r') as hdf:
                    for key in hdf.keys():
                        try:
                            if '_' in key:
                                module, fund_id = key.split('_', 1)
                                # 获取基金名称（从原始数据源）
                                fund_name = self.data_access.get_data(module, fund_id, 'fund_name', '未知名称')
                                net_value = self.data_access.get_data(module, fund_id, 'net_value', 0.0)
                                
                                row_data = {
                                    '基金代码': fund_id,
                                    '模块名称': module,
                                    '基金名称': fund_name,
                                    '最新净值': net_value,
                                    '数据来源': module
                                }
                                
                                # 从HDF5读取所有指标
                                attrs = hdf[key].attrs
                                for col in df.columns[4:-1]:  # 跳过前四列和最后一列
                                    row_data[col] = attrs.get(col, 0.0)
                                
                                df = pd.concat([df, pd.DataFrame([row_data])], ignore_index=True)
                        except Exception as e:
                            print(f"处理 {key} 错误: {str(e)}")
            
            # 添加智能筛选功能
            with ExcelWriter('量化分析报告.xlsx', engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False, sheet_name='基金量化指标')
                
                # 获取工作表和工作簿
                worksheet = writer.sheets['基金量化指标']
                workbook = writer.book
                
                # 设置列宽
                worksheet.set_column('A:A', 12)  # 基金代码
                worksheet.set_column('B:B', 10)  # 模块名称
                worksheet.set_column('C:C', 20)  # 基金名称
                worksheet.set_column('D:R', 15)  # 其他列
                
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
                
                # 最大回撤列标红
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
            print(f"生成Excel报表错误: {str(e)}")
            return None

    def compare_fund_data(self, fund_id):
        """比较同一基金在不同模块中的数据差异"""
        if fund_id not in self.fund_index:
            return f"基金 {fund_id} 不在索引中"
        
        results = {}
        modules = self.fund_index[fund_id]
        
        # 比较关键字段
        key_fields = ['net_value', 'date', 'fund_name']
        
        for field in key_fields:
            field_data = {}
            values = []
            
            for module in modules:
                value = self.data_access.get_data(module, fund_id, field)
                if value is not None:
                    field_name = self.data_access.get_field_name(module, field)
                    field_data[module] = (field_name, value)
                    values.append(value)
            
            # 检查是否存在差异
            has_diff = len(set(str(v) for v in values)) > 1 if values else False
            results[field] = {
                'data': field_data,
                'has_difference': has_diff
            }
        
        return {
            'fund_id': fund_id,
            'modules': modules,
            'comparisons': results
        }

def set_pypi_source(source_index):
    """设置PyPI源"""
    if 0 <= source_index < len(PYPI_SOURCES):
        source = PYPI_SOURCES[source_index]
        return {
            'index_url': source['index_url'],
            'trusted_host': source['trusted_host']
        }
    return None

# 保持原有函数不变：test_pypi_source, check_dependencies, parse_arguments

def test_pypi_source(source_config):
    """测试PyPI源是否可用"""
    try:
        import requests
        # 简单测试源是否可访问
        response = requests.get(source_config['index_url'], timeout=5)
        return response.status_code == 200
    except Exception:
        return False

# 修复beautifulsoup4安装失败问题
def check_dependencies():
    """检查项目依赖是否完备，不完备则尝试安装"""
    try:
        # 创建一个配置文件来记录依赖安装状态
        config_dir = os.path.join(os.path.dirname(__file__), '.config')
        config_file = os.path.join(config_dir, 'deps_installed.json')
        os.makedirs(config_dir, exist_ok=True)
        
        # 加载已安装的依赖配置
        installed_deps = set()
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    installed_deps = set(json.load(f))
        except Exception as e:
            print(f"加载依赖配置失败: {str(e)}")
        
        # 首先测试各个源的可用性
        available_sources = []
        for i, source in enumerate(PYPI_SOURCES):
            if test_pypi_source(source):
                available_sources.append(i)
        
        # 如果没有可用源，显示错误
        if not available_sources:
            print("错误: 无法连接到任何PyPI源，请检查网络连接")
            return False
        
        # 选择第一个可用的源
        source_config = set_pypi_source(available_sources[0])
        print(f"使用PyPI源: {PYPI_SOURCES[available_sources[0]]['name']}")
        
        # 检查并安装依赖
        missing_deps = []
        for dep in ALL_REQUIRED_DEPENDENCIES:
            # 如果依赖已经在已安装列表中，跳过检查
            if dep in installed_deps:
                print(f"跳过依赖检查: {dep} (已在配置文件中标记为已安装)")
                continue
                
            try:
                # 尝试导入包（不考虑版本）
                dep_name = dep.split('>=')[0].split('==')[0]
                __import__(dep_name)
                print(f"依赖验证成功: {dep}")
                # 验证成功，添加到已安装列表
                installed_deps.add(dep)
            except ImportError:
                missing_deps.append(dep)
        
        if missing_deps:
            print(f"发现 {len(missing_deps)} 个缺失的依赖项，开始安装...")
            
            # 分批安装
            regular_deps = [d for d in missing_deps if 'playwright' not in d]
            playwright_dep = next((d for d in missing_deps if 'playwright' in d), None)
            
            # 特别处理beautifulsoup4的安装
            beautifulsoup4_dep = next((d for d in regular_deps if 'beautifulsoup4' in d), None)
            if beautifulsoup4_dep:
                # 从regular_deps中移除，单独处理
                regular_deps.remove(beautifulsoup4_dep)
                print(f"单独安装: {beautifulsoup4_dep}")
                # 尝试使用多个源安装beautifulsoup4
                bs4_installed = False
                for source_idx in available_sources:
                    try:
                        source_config = set_pypi_source(source_idx)
                        print(f"尝试使用源: {PYPI_SOURCES[source_idx]['name']}")
                        cmd = [
                            sys.executable, '-m', 'pip', 'install',
                            '--index-url', source_config['index_url'],
                            '--trusted-host', source_config['trusted_host'],
                            beautifulsoup4_dep
                            # 移除 --upgrade --force-reinstall 参数，避免不必要的重装
                        ]
                        subprocess.run(cmd, check=True)
                        print(f"{beautifulsoup4_dep} 安装成功!")
                        # 验证安装是否成功
                        try:
                            __import__('bs4')  # beautifulsoup4的实际导入名称是bs4
                            print(f"验证 {beautifulsoup4_dep} 安装成功!")
                            bs4_installed = True
                            # 从missing_deps中移除已成功安装的beautifulsoup4
                            if beautifulsoup4_dep in missing_deps:
                                missing_deps.remove(beautifulsoup4_dep)
                            # 添加到已安装列表
                            installed_deps.add(beautifulsoup4_dep)
                            break  # 安装成功就退出循环
                        except ImportError:
                            print(f"验证 {beautifulsoup4_dep} 安装失败!")
                    except Exception as e:
                        print(f"在源 {PYPI_SOURCES[source_idx]['name']} 安装 {beautifulsoup4_dep} 失败: {str(e)}")
                
                if not bs4_installed:
                    # 尝试不使用源直接安装
                    try:
                        print("尝试不使用镜像源安装...")
                        cmd = [
                            sys.executable, '-m', 'pip', 'install',
                            beautifulsoup4_dep,
                            '--upgrade', '--force-reinstall'
                        ]
                        subprocess.run(cmd, check=True)
                        print(f"{beautifulsoup4_dep} 安装成功!")
                        bs4_installed = True
                    except Exception as e:
                        print(f"直接安装 {beautifulsoup4_dep} 也失败: {str(e)}")
                        print(f"警告: {beautifulsoup4_dep} 安装失败，某些功能可能无法使用")
            
            # 安装剩余的常规依赖
            if regular_deps:
                try:
                    # 使用pip批量安装
                    cmd = [
                        sys.executable, '-m', 'pip', 'install',
                        '--index-url', source_config['index_url'],
                        '--trusted-host', source_config['trusted_host']
                    ] + regular_deps
                    
                    print(f"安装依赖: {', '.join(regular_deps)}")
                    result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    print("常规依赖安装成功!")
                except subprocess.CalledProcessError as e:
                    print(f"依赖安装失败: {e.stderr.decode('utf-8', errors='ignore')}")
                    # 尝试使用下一个源
                    if len(available_sources) > 1:
                        print(f"尝试使用备用源: {PYPI_SOURCES[available_sources[1]]['name']}")
                        source_config = set_pypi_source(available_sources[1])
                        try:
                            cmd = [
                                sys.executable, '-m', 'pip', 'install',
                                '--index-url', source_config['index_url'],
                                '--trusted-host', source_config['trusted_host']
                            ] + regular_deps
                            subprocess.run(cmd, check=True)
                            print("备用源安装成功!")
                        except Exception as e2:
                            print(f"备用源安装也失败: {str(e2)}")
                            return False
                    else:
                        return False
            
            # 安装playwright并设置
            if playwright_dep:
                try:
                    # 安装playwright包
                    cmd = [
                        sys.executable, '-m', 'pip', 'install',
                        '--index-url', source_config['index_url'],
                        '--trusted-host', source_config['trusted_host'],
                        playwright_dep
                    ]
                    subprocess.run(cmd, check=True)
                    print("playwright包安装成功")
                    
                    # 安装playwright浏览器
                    print("正在安装playwright浏览器...")
                    cmd = [sys.executable, '-m', 'playwright', 'install']
                    subprocess.run(cmd, check=True)
                    print("playwright浏览器安装成功")
                except Exception as e:
                    print(f"playwright安装失败: {str(e)}")
                    # 这个不是致命错误，可以继续运行程序
                    print("警告: playwright安装失败，某些功能可能无法使用")
        
        # 检查所有依赖是否已安装
        missing_after_install = []
        for dep in missing_deps:
            try:
                dep_name = dep.split('>=')[0].split('==')[0]
                __import__(dep_name)
            except ImportError:
                missing_after_install.append(dep)
        
        if missing_after_install:
            print(f"警告: 以下依赖安装失败，某些功能可能无法使用: {', '.join(missing_after_install)}")
        else:
            # 保存已安装依赖的配置
            try:
                with open(config_file, 'w', encoding='utf-8') as f:
                    json.dump(list(installed_deps), f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"保存依赖配置失败: {str(e)}")
                
            print("所有依赖项安装成功！")
            return True
    except Exception as e:
        print(f"依赖检查过程中发生错误: {str(e)}")
        return False

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='基金数据管理系统')
    parser.add_argument('--all', action='store_true', help='下载全部数据')
    parser.add_argument('--modules', type=str, help='指定要下载的模块，用逗号分隔')
    parser.add_argument('--update', action='store_true', help='增量更新数据')
    parser.add_argument('--report', action='store_true', help='生成量化分析报表')
    return parser.parse_args()

# 修改main函数以支持新功能
def main():
    """主函数"""
    # 首先检查项目依赖是否完备
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
    
    # 交互式菜单 - 如果没有提供命令行参数
    print("\n[系统提示] 没有提供命令行参数，进入交互式菜单...")
    
    # 交互式菜单 - 改进版（如果需要保留交互式菜单功能）
    def show_main_menu():
        print("\n=== 基金数据管理系统 ===")
        print("功能模块介绍:")
        print("1. 数据获取 - 一键下载全部基金数据")
        print("2. 数据获取 - 选择性下载指定模块数据")
        print("3. 数据更新 - 增量更新量化视图数据")
        print("4. 数据查询 - 查询基金详细信息和指标")
        print("5. 数据报表 - 生成量化分析Excel报表")
        print("6. 数据管理 - 基金数据跨模块比对")
        print("7. 基金申购 - 基金申购状态管理")
        print("8. 通达信转换 - TDX数据转HDF5格式")
        print("0. 退出系统")
        
    def show_module_selection_menu():
        print("\n=== 选择数据获取模块 ===")
        modules = list(orchestrator.MODULE_MAP.keys())
        # 使用中文描述替换英文模块名
        module_names = {
            'open': '开放式基金排名数据',
            'fbs': '场内交易基金排名数据',
            'hbx': '货币基金排名数据',
            'cnjy': '场内交易基金详细数据',
            'currency': '货币基金万份收益和7日年化数据',
            'nav': '基金净值详细数据'
        }
        for i, module in enumerate(modules, 1):
            print(f"{i}. {module} - {module_names.get(module, module)}")
        # 添加净值爬取作为额外选项
        print(f"{len(modules)+1}. nav - {module_names.get('nav', '基金净值详细数据')}")
        print("a. 选择全部模块")
        print("0. 返回上一级")
        return modules
    
    def show_query_menu():
        print("\n=== 数据查询中心 ===")
        print("1. 查询基金基本信息 - 查看基金名称、净值、日期等基础数据")
        print("2. 查询基金量化指标 - 查看12项专业量化评估指标")
        print("3. 查询模块内所有基金 - 浏览特定模块下的全部基金列表")
        print("4. 按条件筛选基金 - 基于指标阈值筛选优质基金")
        print("0. 返回上一级")
    
    def show_fund_query_menu():
        print("\n=== 选择数据模块 ===")
        modules = list(orchestrator.MODULE_MAP.keys())
        # 使用中文描述替换英文模块名
        module_names = {
            'open': '开放式基金排名数据',
            'fbs': '场内交易基金排名数据',
            'hbx': '货币基金排名数据',
            'cnjy': '场内交易基金详细数据',
            'currency': '货币基金万份收益和7日年化数据'
        }
        for i, module in enumerate(modules, 1):
            print(f"{i}. {module} - {module_names.get(module, module)}")
        print("0. 返回上一级")
        return modules
    
    def show_fund_comparison_menu():
        print("\n=== 基金数据比对 ===")
        print("此功能用于比对同一基金在不同数据源中的数据差异，帮助识别数据质量问题")
        
    while True:
        show_main_menu()
        try:
            choice = input("请输入功能选项: ").strip()
            
            # 验证输入是否为有效的菜单选项
            valid_choices = ['0', '1', '2', '3', '4', '5', '6', '7', '8']
            if choice not in valid_choices:
                print("无效的功能选项，请重新输入")
                continue
            
            if choice == '1':
                print("正在全量下载所有基金数据...")
                print("采用串行下载模式以避免系统无响应...")
                start_time = time.time()
                results = orchestrator.execute_pipeline(parallel=False)  # 使用串行处理
                elapsed_time = time.time() - start_time
                print(f"全量数据下载完成! 总耗时: {elapsed_time:.2f}秒")
                for module, result in results.items():
                    print(f"{module}: {result}")
                input("\n按Enter键返回主菜单...")
            
            elif choice == '2':
                selected_modules = []
                include_nav = False
                while True:
                    modules = show_module_selection_menu()
                    sub_choice = input("请选择要下载的模块 (输入序号，多个序号用逗号分隔): ").strip()
                    
                    if sub_choice == '0':
                        break
                    elif sub_choice == 'a':
                        selected_modules = modules
                        include_nav = True  # 选择全部时包含净值爬取
                        break
                    
                    try:
                        # 处理逗号分隔的多个选择
                        choices = [c.strip() for c in sub_choice.split(',')]
                        for c in choices:
                            idx = int(c) - 1
                            if 0 <= idx < len(modules):
                                if modules[idx] not in selected_modules:
                                    selected_modules.append(modules[idx])
                            elif idx == len(modules):  # 净值爬取选项
                                include_nav = True
                        print(f"已选择: {', '.join(selected_modules)}{', nav' if include_nav else ''}")
                        # 询问是否继续选择其他模块? (y/n):
                        if input("是否继续选择其他模块? (y/n): ").lower() != 'y':
                            break
                    except:
                        print("无效的选择，请重新输入数字序号")
                
                if selected_modules or include_nav:
                    results = {}
                    if selected_modules:
                            print(f"正在下载指定模块: {', '.join(selected_modules)}")
                            # 添加进度反馈
                            print("开始下载数据，请稍候...")
                            start_time = time.time()
                            results = orchestrator.execute_pipeline(tuple(selected_modules), parallel=False)  # 使用串行处理
                            elapsed_time = time.time() - start_time
                            print(f"指定模块数据下载完成! 耗时: {elapsed_time:.2f}秒")
                            for module, result in results.items():
                                print(f"{module}: {result}")
                    
                    if include_nav:
                        print("\n正在爬取基金净值详细数据...")
                        try:
                            # 添加进度反馈
                            start_time = time.time()
                            fetch_fund_nav_data()  # 调用净值爬取函数
                            elapsed_time = time.time() - start_time
                            print(f"基金净值数据爬取完成! 耗时: {elapsed_time:.2f}秒")
                            results['nav'] = "基金净值数据爬取成功"
                        except Exception as e:
                            print(f"爬取过程发生错误: {str(e)}")
                            results['nav'] = f"基金净值数据爬取失败: {str(e)}"
                    input("\n按Enter键返回主菜单...")
            
            elif choice == '3':
                print("正在增量更新量化视图数据...")
                orchestrator._precompute_quant_view()
                print("量化视图增量更新完成!")
                input("\n按Enter键返回主菜单...")
            
            elif choice == '4':
                while True:
                    show_query_menu()
                    query_choice = input("请输入查询功能选项: ").strip()
                    
                    if query_choice == '0':
                        break
                    elif query_choice == '1' or query_choice == '2':
                        # 查询基金基本信息或量化指标
                        while True:
                            modules = show_fund_query_menu()
                            module_choice = input("请选择要查询的数据源模块: ").strip()
                            
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
                                            print(f"\n=== 查询结果 ===")
                                            print(f"数据源模块: {selected_module}")
                                            print(f"基金代码: {fund_id}")
                                            
                                            # 尝试从HDF5文件读取数据
                                            data_path = orchestrator.data_paths.get(selected_module)
                                            if os.path.exists(data_path):
                                                with h5py.File(data_path, 'r') as f:
                                                    if 'funds' in f and fund_id in f['funds']:
                                                        fund_grp = f[f'funds/{fund_id}']
                                                        print("\n基本信息:")
                                                        # 使用数据访问层格式化显示
                                                        for key, value in fund_grp.attrs.items():
                                                            if isinstance(value, (int, float, str)):
                                                                # 转换为通用字段名
                                                                common_key = orchestrator.data_access.get_common_field(selected_module, key)
                                                                print(f"  {common_key} ({key}): {value}")
                                                    else:
                                                        print("该基金数据在当前模块中不存在")
                                            else:
                                                print(f"模块数据文件 {data_path} 不存在，请先下载数据")
                                            
                                            # 如果是查询量化指标
                                            if query_choice == '2':
                                                print("\n量化指标:")
                                                if os.path.exists(orchestrator.QUANT_VIEW_PATH):
                                                    with h5py.File(orchestrator.QUANT_VIEW_PATH, 'r') as f:
                                                        key = f'{selected_module}_{fund_id}'
                                                        if key in f:
                                                            # 按照重要性排序显示指标
                                                            important_metrics = ['夏普比率', '年化收益率', '最大回撤', '卡玛比率']
                                                            other_metrics = [m for m in orchestrator.quant_features if m not in important_metrics]
                                                        
                                                            print("  核心指标:")
                                                            for metric in important_metrics:
                                                                if metric in f[key].attrs:
                                                                    print(f"    {metric}: {f[key].attrs[metric]:.4f}")
                                                                else:
                                                                    print(f"    {metric}: 未计算")
                                                        
                                                            print("  其他指标:")
                                                            for metric in other_metrics:
                                                                if metric in f[key].attrs:
                                                                    print(f"    {metric}: {f[key].attrs[metric]:.4f}")
                                                                else:
                                                                    print(f"    {metric}: 未计算")
                                                        else:
                                                            print("该基金的量化指标数据不存在，请先运行数据更新")
                                            
                                            input("\n按Enter键继续...")
                                            break
                                        except Exception as e:
                                            print(f"查询过程发生错误: {str(e)}")
                                            input("\n按Enter键继续...")
                                            break
                                    else:
                                        print("请输入有效的基金代码")
                                else:
                                    print("无效的模块选择序号")
                            except:
                                print("无效的输入，请输入数字序号")
                    
                    elif query_choice == '3':
                        # 查询模块内所有基金
                        while True:
                            modules = show_fund_query_menu()
                            module_choice = input("请选择要查询的数据源模块: ").strip()
                            
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
                                                    # 按基金代码排序显示
                                                    funds.sort()
                                                    total_count = len(funds)
                                                    batch_size = 20  # 每页显示20个
                                                    page = 1
                                                    
                                                    while True:
                                                        start_idx = (page - 1) * batch_size
                                                        end_idx = min(start_idx + batch_size, total_count)
                                                        
                                                        print(f"\n第 {page} 页 (共 {total_count} 个基金):")
                                                        for i in range(start_idx, end_idx):
                                                            print(f"{i+1}. {funds[i]}")
                                                        
                                                        if end_idx < total_count:
                                                            action = input("输入 'n' 查看下一页，其他键返回: ").strip().lower()
                                                            if action != 'n':
                                                                break
                                                            page += 1
                                                        else:
                                                            input("已显示全部基金，按Enter键返回: ")
                                                            break
                                                else:
                                                    print("该模块下暂无基金数据")
                                            else:
                                                print("数据结构错误，找不到funds组")
                                    else:
                                        print(f"模块数据文件 {data_path} 不存在，请先下载数据")
                                    
                                    input("\n按Enter键返回...")
                                    break
                                else:
                                    print("无效的模块选择序号")
                            except Exception as e:
                                print(f"查询过程发生错误: {str(e)}")
                                input("\n按Enter键返回...")
                    
                    elif query_choice == '4':
                        # 按条件筛选基金
                        print("\n基金筛选功能尚未实现，敬请期待！")
                        input("\n按Enter键返回...")
                    
                    else:
                        print("无效的选择，请重新输入功能选项")
            
            elif choice == '5':
                print("正在生成Excel量化分析报表...")
                report_file = orchestrator.generate_excel_report()
                if report_file:
                    print(f"报表已成功生成: {report_file}")
                    print("报表包含所有下载基金的12项量化指标，并已应用智能条件格式")
                    print("  - 年化收益率为负的单元格已标红")
                    print("  - 最大回撤超过20%的单元格已标红")
                    print("  - 夏普比率大于1的单元格已标绿")
                else:
                    print("报表生成失败，请检查数据文件是否存在")
                input("\n按Enter键返回主菜单...")
            
            elif choice == '6':
                show_fund_comparison_menu()
                fund_id = input("请输入要比对的基金代码: ").strip()
                
                if fund_id:
                    comparison_result = orchestrator.compare_fund_data(fund_id)
                    
                    if isinstance(comparison_result, str):
                        print(f"比对失败: {comparison_result}")
                    else:
                        print(f"\n=== 基金 {fund_id} 数据比对结果 ===")
                        print(f"该基金存在于以下数据源: {', '.join(comparison_result['modules'])}")
                        
                        for field, info in comparison_result['comparisons'].items():
                            print(f"\n{field} 比对:")
                            
                            if info['has_difference']:
                                print("  发现数据差异!")
                            else:
                                print("  各数据源数据一致")
                            
                            for module, (field_name, value) in info['data'].items():
                                print(f"  {module}: {field_name} = {value}")
                    
                    input("\n按Enter键返回主菜单...")
                else:
                    print("请输入有效的基金代码")
                    input("\n按Enter键继续...")
            
            elif choice == '7':
                # 基金申购状态管理
                def show_purchase_status_menu():
                    print("\n=== 基金申购状态管理 ===")
                    print("1. 下载基金基本信息和申购状态数据")
                    print("2. 查询单只基金基本信息")
                    print("3. 查询单只基金申购状态")
                    print("4. 按申购状态筛选基金")
                    print("5. 查看所有基金代码")
                    print("0. 返回上一级")
                
                while True:
                    show_purchase_status_menu()
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
            

                
            elif choice == '0':
                print("谢谢使用基金数据管理系统，再见!")
                break
            
            else:
                print("无效的功能选项，请重新输入")
                
        except KeyboardInterrupt:
            print("\n程序已被用户中断")
            break
        except Exception as e:
            print(f"程序运行发生错误: {str(e)}")
            input("\n按Enter键继续...")

if __name__ == "__main__":
    main()