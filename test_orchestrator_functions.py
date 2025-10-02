import os
import subprocess
import sys

print('=== 测试quant_orchestrator.py中的函数 ===')

# 测试download_fund_status_data函数
print('\n测试download_fund_status_data函数:')
try:
    subprocess.run([sys.executable, '-c', 'import quant_orchestrator; quant_orchestrator.download_fund_status_data()'], check=True, capture_output=True, text=True)
    print('download_fund_status_data函数测试成功')
except Exception as e:
    print(f'download_fund_status_data函数测试失败: {e}')

# 测试display_fund_basic_info函数
print('\n测试display_fund_basic_info函数:')
try:
    subprocess.run([sys.executable, '-c', 'import quant_orchestrator; quant_orchestrator.display_fund_basic_info("000001")'], check=True, capture_output=True, text=True)
    print('display_fund_basic_info函数测试成功')
except Exception as e:
    print(f'display_fund_basic_info函数测试失败: {e}')

# 测试display_fund_purchase_status函数
print('\n测试display_fund_purchase_status函数:')
try:
    subprocess.run([sys.executable, '-c', 'import quant_orchestrator; quant_orchestrator.display_fund_purchase_status("000001")'], check=True, capture_output=True, text=True)
    print('display_fund_purchase_status函数测试成功')
except Exception as e:
    print(f'display_fund_purchase_status函数测试失败: {e}')

# 测试filter_funds_by_purchase_status和display_filtered_funds函数
print('\n测试filter_funds_by_purchase_status和display_filtered_funds函数:')
try:
    subprocess.run([sys.executable, '-c', 'import quant_orchestrator; funds = quant_orchestrator.filter_funds_by_purchase_status(1); quant_orchestrator.display_filtered_funds(funds)'], check=True, capture_output=True, text=True)
    print('filter_funds_by_purchase_status和display_filtered_funds函数测试成功')
except Exception as e:
    print(f'filter_funds_by_purchase_status和display_filtered_funds函数测试失败: {e}')

# 测试display_all_fund_codes函数
print('\n测试display_all_fund_codes函数:')
try:
    subprocess.run([sys.executable, '-c', 'import quant_orchestrator; quant_orchestrator.display_all_fund_codes()'], check=True, capture_output=True, text=True)
    print('display_all_fund_codes函数测试成功')
except Exception as e:
    print(f'display_all_fund_codes函数测试失败: {e}')

print('\n=== 所有测试完成 ===')