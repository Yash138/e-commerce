"""Types of Pages to Scrap on Amazon

    1. Top 100 Products Page
    2. Product Page
    3. Product Review Page
    4. Keyword serach page
    5. Seller Details/Store Page

"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
# import from webdriver_manager (using underscore)
from webdriver_manager.chrome import ChromeDriverManager 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC

from collections import namedtuple
from common.util import printArgs



options = webdriver.ChromeOptions()
options.add_argument('--headless')
# create a driver object using driver_path as a parameter
driver = webdriver.Chrome(options = options, service = Service(ChromeDriverManager().install()))
# assign your website to scrape
web = "https://www.amazon.in/gp/bestsellers/home-improvement/ref=zg_bs_nav_home-improvement_0"
web = "https://www.amazon.in/gp/bestsellers/electronics/ref=zg_bs_nav_electronics_0"
web = "https://www.amazon.in/gp/bestsellers/home-improvement/4286644031/ref=zg_bs_nav_home-improvement_1"
category = "home-improvement"
driver.get(web)
items = wait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@id, "gridItemRoot")]')))

productDetails = namedtuple("Top100ProductDetails", ['asin', 'product_name', 'product_price', 'rating', 'number_of_ratings', 'rank', 'category', 'product_url', 'image_url', 'reviews_url'])
Top100ProductDetails = list()
for i,item in enumerate(items):
    # rnk, name, num_ratings, price = item.find_element(By.XPATH, f'.//*[@id="p13n-asin-index-{i}"]/div').text.split("\n")
    rnk = item.find_element(By.XPATH, f'//*[@id="p13n-asin-index-{i}"]/div/div[1]/div[1]/span').text
    asin = item.find_element(By.CLASS_NAME, 'p13n-sc-uncoverable-faceout').get_attribute("id")
    img_url = item.find_element(By.XPATH, f'//*[@id="{asin}"]/a/div/img').get_attribute("src")
    product_url = item.find_element(By.XPATH, f'//*[@id="{asin}"]/div/div/a').get_attribute("href")
    name = item.find_element(By.XPATH, f'//*[@id="{asin}"]/div/div/a/span/div').text
    price = item.find_element(By.XPATH, f'//*[@id="{asin}"]/div/div/div[2]/div/div/a/div/span/span').text
    num_ratings = item.find_element(By.XPATH, f'//*[@id="{asin}"]/div/div/div[1]/div/a/span').text
    rating_obj = item.find_element(By.XPATH, f'//*[@id="{asin}"]/div/div/div[1]/div/a')
    rating = rating_obj.get_attribute("title")
    reviews_url = rating_obj.get_attribute("href")
    
    pd = productDetails(
        rank=rnk,
        product_name=name,
        number_of_ratings=num_ratings,
        product_price=price,
        asin=asin,
        image_url=img_url,
        product_url=product_url,
        category=category,
        rating=rating,
        reviews_url=reviews_url
    )
    Top100ProductDetails.append(pd)    
driver.quit()

for i in Top100ProductDetails:
    print(i)
# print(Top100ProductDetails)

