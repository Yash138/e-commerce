import scrapy
from datetime import datetime as dt
from Ecommerce_Scraper.items import AmazonBestSellerMongoItem, AmazonBestSellerItem
from Ecommerce_Scraper.utility import getCategoryUrls


# class AmazonBestSellerSpider(scrapy.Spider):
#     name = "AmazonBestSeller"
#     allowed_domains = ["www.amazon.in"]
#     collection_name = 'stg_amz__best_sellers'
        
#     def start_requests(self):
#         self.category_urls_dict = {i['Url']:i for i in getCategoryUrls()}
#         self.start_urls = list(self.category_urls_dict.keys())
#         for url in self.start_urls:
#             yield scrapy.Request(url, callback=self.parse)

#     def parse(self, response):
#         category = self.category_urls_dict[response.url.split('/ref')[0]]['Category']           # split the URL from ref - will work in case of Page 2 url
#         subCategory = self.category_urls_dict[response.url.split('/ref')[0]]['SubCategory']     # split the URL from ref - will work in case of Page 2 url
#         for i,product in enumerate(response.xpath('//div[contains(@id, "gridItemRoot")]')):
#             item = AmazonBestSellerMongoItem()
#             item['asin'] = product.xpath(f'//*[@id="p13n-asin-index-{i}"]/div/@data-asin').get()
#             item['rank'] = int(product.xpath(f'//*[@data-asin="{item['asin'] }"]/div[1]/div[1]/span/text()').get().replace('#',''))
#             item['productName'] = product.xpath(f'//*[@id="{item['asin']}"]/div/div/a/span/div/text()').get()
#             item['productUrl'] = response.urljoin(product.xpath(f'//*[@id="{item['asin']}"]/div/div/a/@href').get()).split('/ref')[0]
#             item['category'] = category
#             item['subCategory'] = subCategory
#             item['loadTimestamp'] = dt.now()
#             yield item
        
#         next_page = response.css("li.a-last > a::attr(href)").get()
#         if next_page:
#             next_page_url = response.urljoin(next_page)
#             yield scrapy.Request(url=next_page_url, callback=self.parse)


class AmazonBestSellerSpider(scrapy.Spider):
    name = "AmazonBestSeller"
    allowed_domains = ["www.amazon.in"]
    table_name = 'staging.stg_amz__best_sellers'
    custom_settings = {
        "ITEM_PIPELINES" : {
            "Ecommerce_Scraper.pipelines.AmazonBSStagingPipeline": 100
        }
    }
        
    def start_requests(self):
        self.category_urls_dict = {i['url']:i for i in getCategoryUrls(db='postgres')}
        self.start_urls = list(self.category_urls_dict.keys())
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        category = self.category_urls_dict[response.url.split('/ref')[0]]['category']           # split the URL from ref - will work in case of Page 2 url
        subCategory = self.category_urls_dict[response.url.split('/ref')[0]]['sub_category']     # split the URL from ref - will work in case of Page 2 url
        for i,product in enumerate(response.xpath('//div[contains(@id, "gridItemRoot")]')):
            item = AmazonBestSellerItem()
            item['asin'] = product.xpath(f'//*[@id="p13n-asin-index-{i}"]/div/@data-asin').get()
            item['rank'] = int(product.xpath(f'//*[@data-asin="{item['asin'] }"]/div[1]/div[1]/span/text()').get().replace('#',''))
            item['product_name'] = product.xpath(f'//*[@id="{item['asin']}"]/div/div/a/span/div/text()').get()
            item['product_url'] = response.urljoin(product.xpath(f'//*[@id="{item['asin']}"]/div/div/a/@href').get()).split('/ref')[0]
            item['category'] = category
            item['sub_category'] = subCategory
            item['load_timestamp'] = dt.now()
            yield item
        
        next_page = response.css("li.a-last > a::attr(href)").get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(url=next_page_url, callback=self.parse)
            