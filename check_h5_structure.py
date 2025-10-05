import h5py
import os

try:
    # 打开All_Fund_Data.h5文件
    file_path = os.path.join('data', 'All_Fund_Data.h5')
    print(f"尝试打开文件: {file_path}")
    
    with h5py.File(file_path, 'r') as f:
        print("文件已成功打开，文件结构:")
        
        # 定义一个函数来打印文件结构
        def print_structure(name, obj):
            print(f"{name} -> {type(obj)}")
            
        # 遍历文件结构
        f.visititems(print_structure)
        
        # 检查是否有funds组
        if 'funds' in f:
            funds = f['funds']
            print(f"\nfunds组包含 {len(funds)} 个项目")
            
            # 查看第一个基金的数据结构示例
            if len(funds) > 0:
                first_fund_key = list(funds.keys())[0]
                first_fund = funds[first_fund_key]
                print(f"\n第一个基金 '{first_fund_key}' 的属性:")
                for attr_name, attr_value in first_fund.attrs.items():
                    print(f"  {attr_name}: {type(attr_value)} = {attr_value}")
                
                # 检查是否有数据组
                print(f"\n第一个基金 '{first_fund_key}' 的子组和数据集:")
                for item_name, item in first_fund.items():
                    print(f"  {item_name} -> {type(item)}")
                    
                    # 如果是数据集，显示其形状
                    if isinstance(item, h5py.Dataset):
                        print(f"    形状: {item.shape}")
                        print(f"    数据类型: {item.dtype}")
                        
                        # 显示前几个数据点
                        if len(item) > 0:
                            print(f"    前5个数据点示例:")
                            for i in range(min(5, len(item))):
                                print(f"      {item[i]}")

            # 检查是否有任何基金包含交易数据字段
            print("\n检查交易数据字段:")
            has_date_field = False
            has_price_field = False
            
            for fund_code in funds:
                fund_group = funds[fund_code]
                if 'date' in fund_group.attrs:
                    has_date_field = True
                if any(field in fund_group.attrs for field in ['open', 'high', 'low', 'close']):
                    has_price_field = True
                
                # 如果找到需要的字段，可以提前退出
                if has_date_field and has_price_field:
                    print(f"在基金 '{fund_code}' 中找到交易数据字段")
                    break
            
            if not has_date_field:
                print("未找到date字段")
            if not has_price_field:
                print("未找到价格相关字段(open/high/low/close)")
            
finally:
    print("检查完成")