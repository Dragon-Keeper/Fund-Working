# KQ4_CHN_FND Excel报表生成器

## 项目介绍

本工具是一个Python程序，用于逆向开发生成KQ4_CHN_FND_20250929.xlsm格式的Excel报表。程序通过从多个HDF5数据库文件中读取基金数据，计算各种投资指标，并生成结构化的Excel报表。

## 功能特点

- **多数据源整合**：从多个HDF5数据库文件中读取基金基本信息、价格数据和评级数据
- **全面的指标计算**：
  - 收益率指标：日、周、月、季、年涨跌幅以及成立以来涨跌幅
  - 风险指标：夏普比率、最大回撤、波动率、贝塔系数、R平方和Alpha
- **Excel格式化**：自动设置列宽、表头样式和条件格式
- **灵活的输出选项**：支持指定输出文件路径

## 文件说明

- `generate_kq4_fund_excel.py`：主程序文件，包含基金数据收集和Excel报表生成功能

## 依赖库

程序需要安装以下Python库：

- pandas
- numpy
- h5py
- openpyxl (用于Excel输出)

可以使用以下命令安装依赖：

```bash
pip install pandas numpy h5py openpyxl
```

## 数据来源

程序从以下HDF5文件中读取数据（假设这些文件位于`data`子目录中）：

1. `CNJY_Fund_Data.h5` - 基金基本信息
2. `Fund_Purchase_Status.h5` - 基金申购赎回状态
3. `FBS_Fund_Ranking_Data.h5` - 基金排名数据
4. `All_Fund_Data.h5` - 基金历史价格数据

如果某些数据源不存在，程序会尝试从其他可用源获取数据，确保尽可能多地收集基金信息。

## 使用方法

### 命令行使用

```bash
# 使用默认文件名生成Excel报表
python generate_kq4_fund_excel.py

# 指定输出文件路径
python generate_kq4_fund_excel.py "d:\WorkRoom\Fund\Fund-Working\KQ4_CHN_FND_20250929.xlsx"
```

### 编程方式使用

```python
from generate_kq4_fund_excel import FundExcelGenerator

# 创建生成器实例
generator = FundExcelGenerator()

# 运行并生成报表
success = generator.run("d:\WorkRoom\Fund\Fund-Working\KQ4_CHN_FND_20250929.xlsx")

if success:
    print("Excel报表生成成功！")
else:
    print("Excel报表生成失败。")
```

## 输出文件格式

生成的Excel文件包含以下内容：

- 工作表名称：基金数据
- 列包括：基金代码、基金名称、基金类型、最新净值、累计净值、日涨跌幅(%)、周涨跌幅(%)、月涨跌幅(%)、季涨跌幅(%)、年涨跌幅(%)、成立以来(%)、规模(亿元)、成立日期、基金经理、基金公司、申购状态、赎回状态、近一年收益率(%)、近二年收益率(%)、近三年收益率(%)、风险等级、夏普比率、最大回撤(%)、波动率(%)、贝塔系数、R平方、Alpha

## 条件格式

- 日涨跌幅(%)：负值显示为红色
- 夏普比率：大于1的值显示为绿色

## 注意事项

1. 请确保HDF5数据文件存在且格式正确
2. 如果某些数据字段缺失，对应的单元格将显示为空或默认值
3. 对于大量基金数据，生成过程可能需要较长时间
4. 程序使用简化的风险指标计算方法，实际投资决策应使用更专业的工具

## 错误处理

程序包含基本的错误处理机制：

- 如果数据文件不存在，会显示警告信息并尝试从其他源获取数据
- 处理单个基金数据时出错不会影响整个程序运行
- 所有错误和异常都会被捕获并打印，确保程序不会意外中断

## 维护说明

- 如需添加新的数据字段，请修改`required_columns`列表
- 如需更改Excel格式化，请修改`generate_excel`方法中的相关代码
- 如需更新风险指标计算方法，请修改`_calculate_risk_metrics`方法

## 版本历史

- v1.0.0：初始版本，实现基本的基金数据收集和Excel报表生成功能

---

## 联系信息

如有任何问题或建议，请联系作者。