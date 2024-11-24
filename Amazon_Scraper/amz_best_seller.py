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

db = SQLServerExpress(estd_conn=True)
query = """
    select category, url from processed.amz__product_category WHERE IsActive = 1
    union 
    select id as category, url from processed.amz__product_subcategory WHERE IsActive = 1
"""

scrap_urls = db.fetch_all(query)
db.close()

options = webdriver.ChromeOptions()
options.add_argument("--headless")
driver = webdriver.Chrome(options=options, service=Service(ChromeDriverManager().install()))

productDetails = namedtuple("BestSellers", ['asin', 'product_name', 'category', 'rank', 'product_url', 'load_timestamp'])
BestSellers = list()

for url in scrap_urls:
    category = url[0]
    print('Sracping for category:',category)
    driver.get(url[1])
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(10) # let the whole page load
    items = wait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@id, "gridItemRoot")]')))
    load_time = dt.now()
    for i,item in enumerate(items):
        # print(i)
        rnk = find_element(item, By.XPATH, f'//*[@id="p13n-asin-index-{i}"]/div/div[1]/div[1]/span').text
        asin = find_element(item, By.CLASS_NAME, 'p13n-sc-uncoverable-faceout').get_attribute("id")
        product_url = find_element(item, By.XPATH, f'//*[@id="{asin}"]/div/div/a').get_attribute("href")
        name = find_element(item, By.XPATH, f'//*[@id="{asin}"]/div/div/a/span/div').text
        BestSellers.append(
            productDetails(
                asin=asin,
                product_name=name,
                category=category,
                rank=rnk,
                product_url=product_url,
                load_timestamp=load_time.strftime('%Y-%m-%d %H:%M:%S')
            )    
        )
    # break
driver.quit()
print(len(BestSellers))
# for i in BestSellers:
#     print(i)

df = pd.DataFrame(BestSellers)
insert_query = '''
    INSERT INTO staging.stg_amz__best_sellers (ASIN,ProductName,CategoryName,Rank,ProductUrl,LoadTimestamp)
    VALUES (?, ?, ?, ?, ?, ?)
'''

db.connect()
db.execute_many_query(insert_query, df.values.tolist())
db.close()

# df.values.tolist()