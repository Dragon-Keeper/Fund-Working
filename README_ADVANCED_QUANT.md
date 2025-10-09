# 基金高级量化分析模块

## 介绍

本模块实现了一个高级量化分析系统，用于对基金数据进行全面的量化分析，并按照指定的格式生成分析结果报表。该模块支持多种量化指标计算，包括收益率分析、风险评估、波动率计算等，同时提供多线程处理能力，提高大数据量分析的效率。

## 功能特性

- 全面的量化指标计算，包括：
  - 时间维度分析：年化收益率、上涨季度/月份/星期比例
  - 风险评估：最大回撤率、第二大回撤率
  - 波动性分析：不同时间周期的涨跌幅标准差
  - 风险调整收益：夏普率、卡玛率、OLS离散系数
  - 多时间段对比：全时期、近3年、近1年的指标对比
  - 趋势分析：不同周期涨跌幅计算

- 高效处理能力：
  - 支持自动、单线程和自定义线程模式
  - 智能线程数分配，优化多核心CPU利用率

- 美观的Excel报表输出：
  - 带时间戳的报表文件名
  - 自动调整列宽
  - 条件格式高亮显示关键指标

## 文件结构

- `advanced_quant_analysis.py`：核心量化分析模块，包含所有分析算法
- `run_advanced_quant.py`：命令行接口，用于运行分析任务
- `reports/`：存放生成的Excel报表（自动创建）

## 安装要求

本模块依赖以下Python库：

```
pandas>=1.0.0
numpy>=1.18.0
h5py>=2.10.0
scipy>=1.4.0
xlsxwriter>=1.3.0
```

确保这些库已安装：

```bash
pip install -r requirements.txt
```

## 使用方法

### 命令行方式

您可以通过命令行参数运行量化分析：

```bash
python run_advanced_quant.py [参数]
```

支持的参数：

- `--data` 或 `-d`：HDF5数据文件路径（可选，默认为`data/All_Fund_Data.h5`）
- `--start-date` 或 `-s`：分析起始日期，格式为YYYYMMDD（可选，默认为最近一年）
- `--thread-mode` 或 `-t`：线程模式，可选值为`auto`、`single`或`custom`（默认为`auto`）
- `--thread-count` 或 `-n`：自定义线程数，仅在thread-mode为custom时有效
- `--output` 或 `-o`：输出Excel文件路径（可选，默认为`reports/全面的基金量化分析报表_时间戳.xlsx`）

示例：

```bash
# 使用自动线程模式运行分析
python run_advanced_quant.py

# 指定数据文件和起始日期
python run_advanced_quant.py -d ./data/Custom_Fund_Data.h5 -s 20200101

# 使用4个线程运行分析
python run_advanced_quant.py -t custom -n 4

# 指定输出文件路径
python run_advanced_quant.py -o ./my_analysis_report.xlsx
```

### 编程方式

您也可以在Python代码中直接使用：

```python
from advanced_quant_analysis import AdvancedQuantAnalyzer

# 创建分析器实例
analyzer = AdvancedQuantAnalyzer(
    hdf5_path='./data/Fund_Data.h5',
    start_date_str='20200101'
)

# 运行分析（使用自动线程模式）
analyzer.analyze_all_funds(thread_mode='auto')

# 导出结果
output_file = analyzer.export_to_excel('./analysis_result.xlsx')
print(f'分析结果已保存至: {output_file}')
```

## 输出文件格式

生成的Excel报表包含以下关键信息：

- **基本信息**：基金代码、基金简称、期初日期、期末日期、规模、年数
- **全时期分析指标**：上涨季度/月份/星期比例、波动率、收益率、风险指标
- **近3年分析指标**：与全时期相同的指标集，但仅针对最近3年数据
- **近1年分析指标**：与全时期相同的指标集，但仅针对最近1年数据
- **周期涨跌幅**：不同时间段的涨跌幅，如近1周、近1月、近3月等
- **年份涨跌幅**：2024年、2025年的涨跌幅
- **其他指标**：月涨跌幅最大异常、封闭类型、状态等

Excel报表还应用了条件格式，方便快速识别重要信息：
- 年化收益率为负的单元格显示为红色
- 夏普率大于1的单元格显示为绿色
- 最大回撤率大于20%的单元格显示为红色

## 注意事项

1. 请确保您的HDF5数据文件格式正确，包含必要的字段（date, open, high, low, close, amount, volume, prev_close）
2. 对于部分需要较长时间序列的指标（如季度分析），至少需要有2个季度的数据才能计算
3. 如遇内存不足问题，建议使用单线程模式或增加系统内存
4. 报表中部分信息（如封闭类型、类别等）需根据实际数据源进行填充

## 更新记录

- v1.0：初始版本，支持所有核心量化指标计算和Excel报表生成

## 联系方式

如有问题或建议，请联系开发者。