"""
    done : get the urls of active categories and sub_categories
        delete the records from [staging].[stg_amz__best_seller_products] for the categories we are going to scrap
    done: scrap these categories and ingest in stage table
        run SP to transform data and ingest it to processed schema
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
# import from webdriver_manager (using underscore)
from webdriver_manager.chrome import ChromeDriverManager 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
import time
from datetime import datetime as dt
from collections import namedtuple
from common.util import SQLServerExpress, find_element
import pandas as pd
import traceback
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--load_type", default='Incremental', help="Incremental | FullRefresh")

# Parse arguments
args = parser.parse_args()
print(args.load_type)

db = SQLServerExpress(estd_conn=True)
if args.load_type == 'FullRefresh':
    query = """
        select category, url from processed.amz__product_category WHERE IsActive = 1
        union 
        select concat(category, '||', SubCategory) as category, url from processed.amz__product_subcategory WHERE IsActive = 1
    """
elif args.load_type == 'Incremental':
    query = """
        SELECT category, url FROM processed.amz__product_category 
        WHERE IsActive = 1 AND cast(LastRefreshedTimestamp as date) < cast(getdate() as date)
        UNION
        SELECT concat(category, '||', SubCategory) as category, url FROM processed.amz__product_subcategory 
        WHERE IsActive = 1 AND cast(LastRefreshedTimestamp as date) < cast(getdate() as date)
    """
    
scrap_urls = db.fetch_all(query)
db.close()
if len(scrap_urls) == 0:
    exit('ALREADY REFRESHED!!')

options = webdriver.ChromeOptions()
options.add_argument("--headless")
driver = webdriver.Chrome(options=options, service=Service(ChromeDriverManager().install()))

product_details = namedtuple("BestSellers", ['asin', 'category', 'sub_category', 'product_name', 'rank', 'product_url', 'load_timestamp'])

for url in scrap_urls:
    category = url[0]
    best_sellers = list()
    # if 'car' in category.lower(): continue
    print('Sracping for category:',category)
    try:
        driver.get(url[1])
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(10) # let the whole page load
        items = wait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@id, "gridItemRoot")]')))
    except Exception as e:
        tb = traceback.TracebackException.from_exception(e)
        print(f'\nFailed to fetch data for {category}\nError: {tb.exc_type_str}\n')
        continue
    load_time = dt.now()
    for i,item in enumerate(items):
        # print(i)
        rnk = find_element(item, By.XPATH, f'//*[@id="p13n-asin-index-{i}"]/div/div[1]/div[1]/span').text
        asin = find_element(item, By.CLASS_NAME, 'p13n-sc-uncoverable-faceout').get_attribute("id")
        product_url = find_element(item, By.XPATH, f'//*[@id="{asin}"]/div/div/a').get_attribute("href")
        name = find_element(item, By.XPATH, f'//*[@id="{asin}"]/div/div/a/span/div').text
        best_sellers.append(
            product_details(
                asin=asin,
                category=category.split('||')[0],
                sub_category= '||'.join(category.split('||')[1:]),
                product_name=name,
                rank=rnk,
                product_url=product_url,
                load_timestamp=load_time.strftime('%Y-%m-%d %H:%M:%S')
            )    
        )
    df = pd.DataFrame(best_sellers)
    # df["load_timestamp"] = dt.now().strftime('%Y-%m-%d %H:%M:%S')
    insert_query = '''
        INSERT INTO staging.stg_amz__best_sellers (ASIN,Category,SubCategory,ProductName,Rank,ProductUrl,LoadTimestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    '''
    db.connect()
    db.execute_many_query(insert_query, df.values.tolist())
    db.close()
    # break
driver.quit()
# print(len(BestSellers))
# for i in BestSellers:
#     print(i)

# df.values.tolist()

db.connect()
db.execute_query("exec dbo.sp_update_master_tables")
db.execute_query("exec dbo.sp_process_best_seller")
db.close()