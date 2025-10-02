import os
print('检查数据文件是否存在:', os.path.exists('data/Fund_Purchase_Status.h5'))
try:
    import Fund_Purchase_Status_Manager
    # 获取所有基金代码的前10个
    all_codes = Fund_Purchase_Status_Manager.get_all_fund_codes()[:10]
    print(f'获取到{len(all_codes)}个基金代码的前10个')
    
    # 如果有基金代码，尝试查询第一个基金的基本信息
    if all_codes:
        print(f'\n尝试查询基金代码 {all_codes[0]} 的基本信息:')
        Fund_Purchase_Status_Manager.display_fund_basic_info(all_codes[0])
        
        print(f'\n尝试查询基金代码 {all_codes[0]} 的申购状态:')
        Fund_Purchase_Status_Manager.display_fund_purchase_status(all_codes[0])
except Exception as e:
    print('错误:', e)