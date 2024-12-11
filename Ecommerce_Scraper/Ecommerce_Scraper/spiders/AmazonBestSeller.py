import scrapy
from datetime import datetime as dt
from Ecommerce_Scraper.items import AmazonBestSellerItem
from Ecommerce_Scraper.utility import getCategoryUrls #,MongoDBHandler
# from Ecommerce_Scraper.settings import MONGO_DATABASE, MONGO_URI

class AmazonBestSellerSpider(scrapy.Spider):
    name = "AmazonBestSeller"
    allowed_domains = ["www.amazon.in"]
    # start_urls = ["https://www.amazon.in/gp/bestsellers/home-improvement"]
    collection_name = 'stg_amz__best_sellers'

    def __init__(self):
        self.category = None
        self.sub_category = None
    
    def set_category(self,category, sub_category):
        self.category = category
        self.sub_category = sub_category
        
    def start_requests(self):
        # mongo_handler = MongoDBHandler(MONGO_URI, MONGO_DATABASE)
        # results_category = mongo_handler.find(
        #     "amz__product_category", 
        #     {'IsActive':True},
        #     {'Category':1, 'SubCategory':{'$literal':''}, 'Url':1, '_id':0}
        # )
        # results_subCategory = mongo_handler.find(
        #     "amz__product_subcategory", 
        #     {'IsActive':True},
        #     {'Category':1, 'SubCategory':1, 'Url':1, '_id':0}
        # )
        # # print(list(results_category),list(results_subCategory))
        # results = list(results_category)+list(results_subCategory)
        # self.category_urls_dict = {i['Url']:i for i in results}
        self.category_urls_dict = {i['Url']:i for i in getCategoryUrls()}
        self.start_urls = list(self.category_urls_dict.keys())
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        category = self.category_urls_dict[response.url.split('/ref')[0]]['Category']           # split the URL from ref - will work in case of Page 2 url
        subCategory = self.category_urls_dict[response.url.split('/ref')[0]]['SubCategory']     # split the URL from ref - will work in case of Page 2 url
        for i,product in enumerate(response.xpath('//div[contains(@id, "gridItemRoot")]')):
            item = AmazonBestSellerItem()
            item['asin'] = product.xpath(f'//*[@id="p13n-asin-index-{i}"]/div/@data-asin').get()
            item['rank'] = product.xpath(f'//*[@data-asin="{item['asin'] }"]/div[1]/div[1]/span/text()').get()
            item['productName'] = product.xpath(f'//*[@id="{item['asin']}"]/div/div/a/span/div/text()').get()
            item['productUrl'] = f'https://www.amazon.in/dp/{item['asin'] }'
            item['category'] = category
            item['subCategory'] = subCategory
            item['loadTimestamp'] = dt.now()
            yield item
        
        next_page = response.css("li.a-last > a::attr(href)").get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(url=next_page_url, callback=self.parse)
            