from collections import namedtuple
import pandas as pd

def convert_to_mb(bytes):
    return bytes/1048576

def convert_to_gb(bytes):
    return bytes/1073741824

files = [
    # 'logs/amz_category_bestsellers.log',
    # 'logs/amz_category_hot_new_releases.log',
    # 'logs/amz_category_movers_and_shakers.log',
    # 'logs/amz_category_most_wished_for.log',
    # 'logs/amz_category_urls_bestsellers.log',
    # 'logs/amz_category_urls_hot_new_releases.log',
    # 'logs/amz_category_urls_movers_and_shakers.log',
    # 'logs/amz_category_urls_most_wished_for.log',
    'logs/amz_product.log', 
    'logs/amzProductsLC_jewelry.log', 
    'logs/amzProductsLC_office.log',
    'logs/amz_lc_luggage.log',
    'logs/amz_lc_kitchen.log'
]
total_mbs = 0
total_gb = 0
Log = namedtuple('Log', ['file', 'run_count', 'item_count', 'data_transferred_mb', 'data_transferred_gb'])
logs = []
for file in files:
    print(file)
    run_count = 0
    response_bytes = 0
    request_bytes = 0
    item_count = 0
    with open(file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if 'downloader/response_bytes' in line:
                run_count += 1
                bytes = int(line.split(': ')[1].split(',')[0])
                response_bytes += bytes
            elif 'downloader/request_bytes' in line:
                bytes = int(line.split(': ')[1].split(',')[0])
                request_bytes += bytes
            elif 'item_scraped_count' in line:
                item_count += int(line.split(': ')[1].split(',')[0])
    
    log = Log(file.split('/')[-1], run_count, item_count, convert_to_mb(response_bytes)+convert_to_mb(request_bytes), convert_to_gb(response_bytes)+convert_to_gb(request_bytes))
    print(log)
    logs.append(log)
    total_mbs += log.data_transferred_mb
    total_gb += log.data_transferred_gb
    print("\n")

print("\n")
print(f'Total MBs: {total_mbs}')
print(f'Total GBs: {total_gb}')
logs_df = pd.DataFrame(logs)
# logs_df.to_csv('logs.csv', index=False)

logs_df