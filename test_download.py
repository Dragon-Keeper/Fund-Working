import os
print('data目录存在:', os.path.exists('data'))
try:
    import Fund_Purchase_Status_Manager
    print('开始尝试下载测试数据...')
    success = Fund_Purchase_Status_Manager.download_fund_basic_info_threaded(batch_size=10, max_workers=1)
    print('下载结果:', '成功' if success else '失败')
except Exception as e:
    print('错误:', e)