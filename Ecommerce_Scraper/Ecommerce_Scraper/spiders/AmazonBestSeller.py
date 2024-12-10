import scrapy
from Ecommerce_Scraper.items import AmazonBestSellerItem

class AmazonBestSellerSpider(scrapy.Spider):
    name = "AmazonBestSeller"
    allowed_domains = ["www.amazon.in"]
    start_urls = ["https://www.amazon.in/gp/bestsellers/home-improvement"]
    collection_name = 'stg_amz__best_sellers'

    def parse(self, response):
        category = 'Home Improvement'
        subCategory = ''
        for i,product in enumerate(response.xpath('//div[contains(@id, "gridItemRoot")]')):
            item = AmazonBestSellerItem()
            item['asin'] = product.xpath(f'//*[@id="p13n-asin-index-{i}"]/div/@data-asin').get()
            item['rank'] = product.xpath(f'//*[@data-asin="{item['asin'] }"]/div[1]/div[1]/span/text()').get()
            item['productName'] = product.xpath(f'//*[@id="{item['asin']}"]/div/div/a/span/div/text()').get()
            item['productUrl'] = f'https://www.amazon.in/dp/{item['asin'] }'
            item['category'] = category
            item['subCategory'] = subCategory
            yield item
        
        next_page = response.css("li.a-last > a::attr(href)").get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(url=next_page_url, callback=self.parse)
            