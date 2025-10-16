# 基金基本面概况管理系统

## 功能概述

基金基本面概况管理系统是一个用于获取和管理基金基本面信息的工具，能够从HDF5文件读取基金代码，使用akshare接口下载基金规模、成立日期等基本面信息，并提供查询功能。系统支持多线程下载，提高数据获取效率。

## 主要功能

1. **基金数据下载**：从HDF5文件读取基金代码，批量下载基金基本面信息
2. **多线程支持**：自动根据CPU核心数选择最佳线程数，大幅提高下载速度
3. **数据存储**：将下载的基金基本面信息保存到HDF5文件中
4. **查询功能**：支持按基金代码查询和显示前N条数据
5. **快速测试**：提供快速测试功能，下载少量基金数据验证系统功能
6. **进度显示**：实时显示下载进度、成功率、速度和预计剩余时间

## 数据字段说明

系统下载并存储以下基金基本面信息字段：

| 字段名 | 说明 |
|-------|------|
| 基金代码 | 基金的唯一标识符 |
| 基金简称 | 基金的中文简称 |
| 基金类型 | 基金的类型分类 |
| 成立日期 | 基金的成立日期（格式：YYYY年MM月DD日） |
| 资产规模 | 基金的资产规模（不含日期后缀） |
| 基金经理人 | 基金的管理人员信息 |
| 成立来分红 | 基金自成立以来的分红情况 |
| 数据日期 | 资产规模数据的截止日期 |
| 更新时间 | 数据下载的时间戳 |

## 使用方法

### 命令行运行

直接运行脚本进入交互式菜单：

```bash
python Fund_Fundamental_Overview_Manager.py
```

### 菜单操作

在主菜单中，您可以选择以下操作：

1. **下载基金规模、成立日期等数据**：读取所有基金代码并下载基本面信息
2. **显示所有基金代码**：显示数据源文件中的所有基金代码
3. **查询基金规模、成立日期等数据**：查询已下载的基金基本面信息
4. **快速测试（下载前10个基金）**：下载少量基金数据进行功能验证
0. **退出系统**：退出程序

### 作为模块导入

该脚本可以作为模块导入到其他Python程序中使用：

```python
import Fund_Fundamental_Overview_Manager

# 获取HDF5文件路径
source_hdf5_path, target_hdf5_path = Fund_Fundamental_Overview_Manager.get_hdf5_paths()

# 读取基金代码
fund_codes = Fund_Fundamental_Overview_Manager.read_fund_codes_from_hdf5(source_hdf5_path)

# 下载基金基本面信息
results = Fund_Fundamental_Overview_Manager.download_all_fund_overview(fund_codes)

# 保存数据
Fund_Fundamental_Overview_Manager.save_to_hdf5(results, target_hdf5_path)

# 查询基金信息
Fund_Fundamental_Overview_Manager.query_fund_overview(target_hdf5_path)
```

### 非交互式模式

脚本支持通过命令行参数进行非交互式操作：

```bash
# 自动下载所有基金数据并保存
python Fund_Fundamental_Overview_Manager.py --auto
```

## 数据存储

- **数据源文件**：`data/Fund_Purchase_Status.h5`（从该文件读取基金代码）
- **数据存储文件**：`data/Fund_Fundamental_Overview.h5`（下载的数据保存在此文件）

## 依赖要求

- Python 3.6+
- pandas
- akshare
- tables (用于HDF5文件操作)

## 安装依赖

```bash
pip install pandas akshare tables
```

脚本会自动检查并提示安装缺失的依赖包。

## 注意事项

1. 请确保数据源文件`Fund_Purchase_Status.h5`存在且包含有效的基金代码
2. 下载大量基金数据时，请注意网络连接稳定性
3. 系统会自动处理数据格式，确保数据一致性
4. 数据默认存储在`data`文件夹中，系统会自动创建该文件夹

## 性能优化

1. **多线程下载**：自动根据CPU核心数优化线程数，提高下载效率
2. **进度跟踪**：实时显示下载进度和预计剩余时间
3. **错误处理**：完善的错误捕获和处理机制，确保程序稳定运行