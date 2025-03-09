import scrapy
from Amazon_Scraper.items import AmazonCategoryItem
from scrapy.http import Request
from datetime import datetime as dt


class AmzcategorySpider(scrapy.Spider):
    name = "AmzCategory"
    allowed_domains = ["www.amazon.in"]
    BASE_URL = "https://www.amazon.in/gp/{category}"
    custom_settings = {
        "DEPTH_LIMIT": 3
    }
    
    def __init__(self, list_type="bestsellers", *args, **kwargs):
        self.list_type = list_type  # Accept list_type parameter
        self.visited_url = set()
        # if self.list_type not in self.settings['CATEGORIES']:
        #     raise ValueError(f"Invalid list_type: {self.list_type}. Must be one of {list(self.settings['CATEGORIES'].keys())}")
        
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
        url = self.BASE_URL.format(category=self.settings["CATEGORIES"][self.list_type])
        yield scrapy.Request(url, callback=self.parse, meta={"list_type": self.list_type})
    
    def parse(self, response):
        '''
        start_url sends the category page to parse
        parse function will extract the allowed category and sub-category urls and send them to parse_item
        parse_item will extract the product details and ingest them into the database
        '''
        # current_depth = response.meta.get("depth", 0)
        current_depth = response.meta.get("depth", 0)
        visited_categories = response.meta.get("visited_url", set())
        url = response.url.split("ref")[0]
        if (id := url.split("/")[-1]) not in visited_categories:
            visited_categories.add(id)
        else:
            return
        self.log(f"Current Depth: {current_depth}")
        list_type = response.meta["list_type"]
        for category in response.xpath("//div[@role='treeitem']"):
            category_name = category.xpath("./a/text()").get()
            category_url = response.urljoin(category.xpath("./a/@href").get())
            if category_name not in self.settings.get('EXCLUDE_CATEGORIES'):
                self.visited_url.add(category_url)
                yield Request(
                    url=category_url,
                    callback=self.parse,
                    meta={
                        "list_type":list_type
                    }
                ) 
            else:
                self.log(f"Excluded Category: {category_name}:{category_url}")
        if url.split("/")[-1] != self.settings.get('CATEGORIES')[list_type]:
            yield Request(
                url=response.url,
                callback=self.parse_item,
                meta={
                    "list_type":list_type,
                    "depth":current_depth
                },
                dont_filter=True
            )
    
    def parse_item(self, response):
        self.log(f"{response.meta =}")
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
            item['rank'] = int(
                product.xpath(f'''
                               //*[@data-asin="{item['asin']}"]/div[1]/div[1]/span/text() 
                               | //*[@data-asin="{item['asin']}"]/div[1]/div[1]/div[1]/span/text()
                            ''').get().replace('#',''))
            item['product_url'] = response.urljoin(product.xpath(f'//div[@id="{item['asin']}"]/a/@href').get()).split('/ref')[0]
            item['load_timestamp'] = dt.now()
            if list_type == "movers_and_shakers":
                item['sales_rank'] = response.xpath(f'//div[@data-asin="{item['asin']}"]/div[1]/span/text()').get()
            yield item
        
        next_page = response.css("li.a-last > a::attr(href)").get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(
                url=next_page_url, 
                callback=self.parse_item,
                meta= {
                    "list_type":response.meta['list_type'],
                    "depth":response.meta["depth"]
                }
            )