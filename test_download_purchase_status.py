import os
print('检查数据文件是否存在:', os.path.exists('data/Fund_Purchase_Status.h5'))
try:
    import Fund_Purchase_Status_Manager
    print('开始尝试下载基金申购状态数据...')
    success = Fund_Purchase_Status_Manager.download_fund_purchase_status_incremental()
    print('下载结果:', '成功' if success else '失败')
except Exception as e:
    print('错误:', e)