from selenium import webdriver
from selenium.webdriver.chrome.service import Service
# import from webdriver_manager (using underscore)
from webdriver_manager.chrome import ChromeDriverManager 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from common.util import SQLServerExpress, printArgs, safe_get_element_value



db = SQLServerExpress(estd_conn=True)
bs_products = db.fetch_all("""
                            with cte as (
                                select *, ROW_NUMBER() over(partition by asin order by id desc) r_num from processed.amz__best_sellers where isLatest = 1
                            ) SELECT * FROM cte
                            WHERE R_NUM = 1 and ASIN not in (select distinct asin from staging.STG_amz__product_details)""")

bs_cols = db.fetch_all("select * from information_schema.columns where table_name = 'amz__best_sellers'")
# p_cols = db.fetch_all("select * from information_schema.columns where table_name = 'stg_amz__product_details'")
db.close()


options = webdriver.ChromeOptions()
options.add_argument('--headless')
driver = webdriver.Chrome(options=options, service=Service(ChromeDriverManager().install()))
products = list()

for rec in bs_products:
    data = {col[3]:rec[i] for i,col in enumerate(bs_cols)}
    # print(data)
    asin = data['ASIN']
    product_name = data['ProductName']
    product_url = data['ProductUrl']
    print(product_url)
    driver.get(product_url)
    
    rating = safe_get_element_value(driver, By.XPATH, '//*[@id="averageCustomerReviews"]/span[1]/span[1]/span/a/span', mode='text')
    reviews_count = safe_get_element_value(driver, By.XPATH, '//*[@id="averageCustomerReviews"]/span[3]/a/span', mode='text')
    reviews_url = safe_get_element_value(driver, By.XPATH, '//*[contains(@class, "FocalReviews")]/div/div/div[4]/div[2]/a', mode='attribute', attribute='href')
    seller_name = safe_get_element_value(driver, By.XPATH, '//*[@id="bylineInfo_feature_div"]/div[1]/a', mode='text')
    seller_url = safe_get_element_value(driver, By.XPATH, '//*[@id="bylineInfo_feature_div"]/div[1]/a', mode='attribute', attribute='href')
    sales_last_month = safe_get_element_value(driver, By.XPATH, '//*[@id="social-proofing-faceout-title-tk_bought"]/span', mode='text')
    sell_price = safe_get_element_value(driver, By.XPATH, '//*[contains(@id, "corePriceDisplay")]/div[1]/span[3]/span[2]/span[2]', mode='text')
    sell_mrp = safe_get_element_value(driver, By.XPATH, '//*[contains(@id, "corePriceDisplay")]/div[2]/span/span[1]/span[2]/span/span[2]', mode='text')
    
    xpath_bs_category = '//*[@id="productDetails_detailBullets_sections1"]/tbody/tr'
    pd = driver.find_elements(By.XPATH, xpath_bs_category)
    for i, item in enumerate(pd):
        # print(item.text, '\n')
        # if "best seller" in item.text.lower():
        #     category = item.find_element(By.XPATH, f'{xpath_bs_category}[{i+1}]/td/span/span[2]/a').text
        #     category_url = item.find_element(By.XPATH, f'{xpath_bs_category}[{i+1}]/td/span/span[2]/a').get_attribute("href")
        # elif 'date first available' in item.text.lower():
        #     listing_date = item.find_element(By.XPATH, f'{xpath_bs_category}[{i+1}]/td').text
        category = safe_get_element_value(item, By.XPATH, f'{xpath_bs_category}[{i+1}]/td/span/span[2]/a', mode='text') if "best seller" in item.text.lower() else None
        category_url = safe_get_element_value(item, By.XPATH, f'{xpath_bs_category}[{i+1}]/td/span/span[2]/a', mode='attribute', attribute='href') if "best seller" in item.text.lower() else None
        listing_date = safe_get_element_value(item, By.XPATH, f'{xpath_bs_category}[{i+1}]/td', mode='text') if 'date first available' in item.text.lower() else None
    # tmp = [asin, product_name, seller_name, sales_last_month, rating, reviews_count, sell_mrp, sell_price, category, listing_date, product_url, reviews_url, seller_url, category_url]
    # products.append(dict(zip(p_cols,tmp)))
    print(category)
    products.append([asin, product_name, seller_name, sales_last_month, rating, reviews_count, sell_mrp, sell_price, category, listing_date, product_url, reviews_url, seller_url, category_url])
    break
    
insert_query = '''
        INSERT INTO staging.stg_amz__product_details (asin,productName,sellerName,lastMonthSale,rating,reviewsCount,sellMrp,sellPrice,lowestCategory,launchDate,productUrl,reviewsUrl,sellerStoreUrl,categoryUrl)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
print(products)
db.connect()
db.execute_many_query(insert_query, products)
db.close()