# Excel 报表生成器

## 功能概述

Excel 报表生成器是一个专为基金量化分析数据设计的报表自动化工具，能够从HDF5数据文件中提取基金数据，进行处理后生成格式规范、功能完善的Excel报表。该工具通过columns_to_delete_final_excel列表在写入Excel文件前删除不需要显示的列，确保报表的简洁性和专业性。

## 功能特性

- **灵活的数据提取**：支持从All_Fund_Data.h5等HDF5文件中高效读取基金数据
- **数据预处理**：自动处理日期格式、数据类型转换等操作
- **列管理**：通过columns_to_delete_final_excel列表在写入Excel前删除不需要显示的列
- **自定义列顺序**：支持根据columns_config.json配置文件自定义Excel报表中的列顺序
- **条件格式**：自动为负收益等关键指标添加红色标识，便于快速识别风险
- **自动筛选**：为表头添加筛选功能，支持多条件组合筛选
- **排序功能**：支持按任意指标排序，快速找到最优基金

## 技术实现

### 数据处理流程

1. **数据读取**：从HDF5文件中读取基金数据
2. **数据预处理**：处理日期格式、数据类型等
3. **列处理**：
   - 在配置保存前删除指定列（如'成交额'、'成交量'、'前收盘价'）
   - 在写入Excel前通过columns_to_delete_final_excel列表删除不需要显示的列
4. **应用配置**：根据columns_config.json应用列顺序配置
5. **Excel生成**：创建ExcelWriter对象，写入处理后的数据并应用格式

### 关键代码片段

```python
# 写入Excel前删除不需要显示的列
columns_to_delete_final_excel = ["基金名称", "成交额", "成交量", "前收盘价"]
for col in columns_to_delete_final_excel:
    if col in all_fund_df.columns:
        all_fund_df = all_fund_df.drop(columns=[col])

# 创建ExcelWriter对象并写入数据
excel_file_name = f"基金量化分析报表_{timestamp}.xlsx"
excel_file_path = os.path.join("reports", excel_file_name)
with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
    all_fund_df.to_excel(writer, sheet_name="基金数据", index=False)
    # 应用条件格式和其他设置
```

## 使用方法

### 前提条件

确保已安装必要的Python库：
```bash
pip install pandas h5py xlsxwriter openpyxl
```

### 启动程序

直接运行excel_report_generator.py文件：
```bash
python excel_report_generator.py
```

### 配置文件

程序会读取data/columns_config.json文件来确定Excel报表的列顺序：
- columns_order：定义Excel报表中列的显示顺序
- 程序会在配置保存前删除'成交额'、'成交量'、'前收盘价'等不需要的列

### 自定义不需要显示的列

要添加或删除不需要在Excel报表中显示的列，可以修改代码中的columns_to_delete_final_excel列表：
```python
# 修改前\ columns_to_delete_final_excel = ["基金名称", "成交额", "成交量", "前收盘价"]

# 修改后 (示例)
columns_to_delete_final_excel = ["基金名称", "成交额", "成交量", "前收盘价", "其他不需要的列"]
```

## 输出文件

生成的Excel报表文件保存在reports目录下，文件名格式为：
```
基金量化分析报表_YYYYMMDD_HHMMSS.xlsx
```

## 注意事项

1. **数据文件**：确保data目录下存在All_Fund_Data.h5文件
2. **报表目录**：首次运行时，程序会自动创建reports目录
3. **配置文件**：如果data/columns_config.json文件不存在，程序会创建一个默认配置
4. **内存要求**：处理大量基金数据时，建议系统至少有4GB内存

## 系统要求

- **Python版本**：Python 3.6或更高版本
- **依赖库**：
  - pandas：数据处理
  - numpy：数值计算
  - h5py：HDF5文件操作
  - xlsxwriter：Excel文件创建和格式化
  - openpyxl：Excel文件读取

## 常见问题与解决方案

### 1. 无法找到HDF5文件
- 确保All_Fund_Data.h5文件位于data目录下
- 检查文件路径和权限

### 2. 内存不足错误
- 减少同时处理的基金数量
- 增加系统内存

### 3. Excel文件格式问题
- 确保安装了最新版本的xlsxwriter和openpyxl库
- 尝试使用Excel 2016或更高版本打开报表文件