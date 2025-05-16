import scrapy
from Amazon_Scraper.helpers.postgres_handler import PostgresDBHandler
from scrapy.utils.defer import deferred_from_coro

class AmzLCPageCountSpider(scrapy.Spider):
    name = "AmzLCPageCount"
    allowed_domains = ["www.amazon.in"]
    # start_urls = ["https://www.amazon.in/s?i=kitchen&rh=n%3A27385462031&s=popularity-rank&fs=true&ref=lp_27385462031_sar"]
    custom_settings = {
        "DOWNLOADER_MIDDLEWARES" : {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
        },
        "DOWNLOAD_DELAY" : 5,
        "RANDOMIZE_DOWNLOAD_DELAY" : True  # Add random delays to mimic human behavior
    }

    def __init__(self, postgres_handler):
        self.postgres_handler = postgres_handler

    @classmethod
    def from_crawler(cls, crawler):
        """
        Access settings from Scrapy's configuration.
        """
        postgres_handler = PostgresDBHandler(
                crawler.settings.get('POSTGRES_HOST'),
                crawler.settings.get('POSTGRES_DATABASE'),
                crawler.settings.get('POSTGRES_USERNAME'),
                crawler.settings.get('POSTGRES_PASSWORD'),
                crawler.settings.get('POSTGRES_PORT')
            )
        spider = cls(postgres_handler)
        spider.crawler = crawler  # ✅ Attach the crawler instance
        return spider
        # return cls(postgres_handler)
    
    def start_requests(self):
        self.postgres_handler.connect()
        for row in self.postgres_handler.stream_read(
            query="""
                select 
                    substring(category_url from 'bestsellers/(\w+)') as category, 
                    substring(lowest_category_products_url FROM 'node=(\d+)') AS node_value,
                    round(avg(avg_rating),2) as avg_rating,
                    round(avg(total_sales_qty)) as avg_sales_qty
                from curated.vw_best_selling_lowest_categories 
                group by category_url, lowest_category_products_url""",
            batch_size=500
        ):
            # url_components = row['lowest_category_bs_url'].split("/")
            try:
                # gp_index = url_components.index('gp')
                # category = url_components[gp_index+2]
                # subcategory = url_components[gp_index+3]
                category = row['category']
                subcategory = row['node_value']
                avg_rating = row['avg_rating']
                avg_sales_qty = row['avg_sales_qty']
                if category and subcategory:    
                    url = f"https://www.amazon.in/s?i={category}&rh=n%3A{subcategory}&s=popularity-rank"
                    yield scrapy.Request(
                        url=url, 
                        callback=self.parse,
                        meta={"category": category,"subcategory": subcategory, "avg_rating": avg_rating, "avg_sales_qty": avg_sales_qty
                    })
                else:
                    self.log(f"Category and Subcategory not found in URL: {url['lowest_category_bs_url']}")
            except Exception as e:
                self.log(f"Error in: {url['lowest_category_bs_url']}\n{e}")
                # self.log(f"gp not found in URL: {url['lowest_category_bs_url']}")    
            
    def parse(self, response):        
        # Extracting the total number of pages
        total_pages = response.xpath('//span[contains(@class, "pagination-strip")]/ul/child::span[not(child::*)]/text()').get()
        if total_pages:
            total_pages = int(total_pages)
            # self.log(f"Total number of pages: {total_pages}")
            yield {
                "category": response.meta['category'],
                "subcategory": response.meta['subcategory'],
                "avg_rating": response.meta['avg_rating'],
                "avg_sales_qty": response.meta['avg_sales_qty'],
                "total_pages": total_pages,
                "url": response.url
            }
        else:
            self.log(f"Total number of pages not found for : {response.url}")
