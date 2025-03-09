import scrapy
from scrapy import signals
import json

class AmzcategoryurlsSpider(scrapy.Spider):
    name = "AmzCategoryUrls"
    allowed_domains = ["www.amazon.in"]
    BASE_URL = "https://www.amazon.in/gp/{category}"
    custom_settings = {
        "DEPTH_LIMIT": 1
    }
    
    def __init__(self, list_type="bestsellers", *args, **kwargs):
        self.list_type = list_type  # Accept list_type parameter
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
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def start_requests(self):
        url = self.BASE_URL.format(category=self.settings["CATEGORIES"][self.list_type])
        yield scrapy.Request(url, callback=self.parse, meta={"list_type": self.list_type})

    def parse(self, response):
        list_type = response.meta["list_type"]
        current_depth = response.meta.get("depth", 0)
        url = response.url.split("/ref")[0]
        if url.split("/")[-1] != self.settings.get('CATEGORIES')[list_type] and url not in self.visited_url:
            self.visited_url.add(url)
        else:
            pass
        
        self.log(f"Current Depth: {current_depth}")
        # list_type = response.meta["list_type"]
        for category in response.xpath("//div[@role='treeitem']"):
            category_name = category.xpath("./a/text()").get()
            # self.log(f"Category: {category_name}")
            category_url = response.urljoin(category.xpath("./a/@href").get())
            if category_name not in self.settings.get('EXCLUDE_CATEGORIES'):
                self.visited_url.add(category_url)
                yield scrapy.Request(
                    url=category_url,
                    callback=self.parse,
                    meta={
                        "list_type":list_type
                    }
                ) 
            else:
                self.log(f"Excluded Category: {category_name}:{category_url}")
    
    def spider_closed(self, reason):
        """Save visited URLs when spider closes."""
        self.log(f"Spider closed: {reason}")
        file_path = f"./.data/visited_urls_{self.list_type}.txt"
        with open(file_path, "w") as file:
            file.writelines(f"{item}\n" for item in self.visited_url)
        print(f"Saved {len(self.visited_url)} visited URLs to {file_path}")