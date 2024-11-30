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
parser.add_argument("--load_type", default='FullRefresh', help="Incremental | FullRefresh")

# Parse arguments
args = parser.parse_args()
print(args.load_type)

def extract_and_load_bs(driver, db_instance, category, url):
    product_details = namedtuple("BestSellers", ['asin', 'category', 'sub_category', 'product_name', 'rank', 'product_url', 'load_timestamp'])
    best_sellers = list()
    print('Sracping for category:', category)
    try:
        driver.get(url)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(10) # let the whole page load
        items = wait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@id, "gridItemRoot")]')))
    except Exception as e:
        tb = traceback.TracebackException.from_exception(e)
        print(f'\nFailed to fetch data for {category}\nError: {tb.exc_type_str}\n')
        return False
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
    db_instance.connect()
    db_instance.execute_many_query(insert_query, df.values.tolist())
    db_instance.close()
    # break

def get_next_page(driver, url):
    try:
        driver.get(url)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(10) # let the whole page load
        next_page = find_element(driver, By.CSS_SELECTOR, 'li.a-last a').get_attribute("href")
    except Exception as e:
        tb = traceback.TracebackException.from_exception(e)
        print(f'\nFailed to fetch next page url for {category}\nError: {tb.exc_type_str}\n')
        return False
    return next_page    

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


for category, url in scrap_urls:
    resp = extract_and_load_bs(driver, db, category, url)
    if url_next := get_next_page(driver, url):
        print("Scraping Next Page...")
        resp = extract_and_load_bs(driver, db, category, url_next)
    else:
        print("Next button not found or no next page not available.")

driver.quit()

db.connect()
db.execute_query("exec dbo.sp_update_master_tables")
db.execute_query("exec dbo.sp_process_best_seller")
db.close()