import scrapy
from Amazon_Scraper.items import AmazonCategoryItem
from scrapy.http import Request
from datetime import datetime as dt


class AmzcategorySpider(scrapy.Spider):
    name = "AmzCategory"
    table_name = 'staging.stg_amz__product_rankings'
    allowed_domains = ["www.amazon.in"]
    BASE_URL = "https://www.amazon.in/gp/{category}"
    
    custom_settings = {
        'ITEM_PIPELINES': {
            'Amazon_Scraper.pipelines.postgres_pipeline.AmzProductRankingStgSyncPipeline': 300,
        },
        "DOWNLOADER_MIDDLEWARES" : {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
        }
    }

    def __init__(self, list_type="bestsellers", batch_size=1, logfile=None, *args, **kwargs):
        self.list_type = list_type  # Accept list_type parameter
        self.batch_size = int(batch_size)
        self.logfile = logfile
        self.visited_url = set()
        
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Attach Scrapy settings to the spider."""
        spider = cls(*args, **kwargs)
        spider.settings = crawler.settings  # ✅ Assign settings properly
        spider.crawler = crawler

        # since __init__ is called before from_crawler, we can validate the list_type here
        if spider.list_type not in spider.settings['CATEGORIES']:
            raise ValueError(f"Invalid list_type: {spider.list_type}. Must be one of {list(spider.settings['CATEGORIES'].keys())}")
        return spider

    def start_requests(self):
        with open(f"./.urls_to_scrap/category_ranking_urls_{self.list_type}.txt", "r") as f:
            urls = set(f.read().splitlines())
        for url in urls:
            yield scrapy.Request(
                url, 
                callback=self.parse, 
                meta={"list_type": self.list_type})
    
    def parse(self, response):
        list_type = response.meta.get("list_type")
        url = response.url.split("/ref")[0]
        if url.split("/")[-2] == self.settings.get('CATEGORIES')[list_type]:
            category = url.split("/")[-1]
            subCategory = None
        else:
            category = url.split("/")[-2]
            subCategory = url.split("/")[-1]
        for i,product in enumerate(response.xpath('//div[contains(@id, "gridItemRoot")]')):
            item = AmazonCategoryItem()
            item['list_type'] = list_type
            item['category'] = category
            item['sub_category'] = subCategory
            item['asin'] = product.xpath(f'//*[@id="p13n-asin-index-{i}"]/div/@data-asin').get()
            item['rank'] = product.xpath(f'''
                //*[@data-asin="{item["asin"]}"]/div[1]/div[1]/span/text() 
                | //*[@data-asin="{item["asin"]}"]/div[1]/div[1]/div[1]/span/text()
            ''').get()
            item['product_url'] = response.urljoin(product.xpath(f'''//div[@id="{item['asin']}"]/a/@href''').get())
            if list_type == "movers_and_shakers":
                item['sales_rank'] = response.xpath(f'''//div[@data-asin="{item['asin']}"]/div[1]/span/text()''').get()
            yield item
        
        next_page = response.css("li.a-last > a::attr(href)").get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(
                url=next_page_url, 
                callback=self.parse,
                meta= {
                    "list_type":response.meta['list_type']
                }
            )