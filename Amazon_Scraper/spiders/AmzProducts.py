import scrapy
import scrapy.signals
from Amazon_Scraper.items import AmazonProductItem
from Amazon_Scraper.helpers.db_postgres_handler import PostgresDBHandler
from datetime import datetime as dt
import math

class AmzproductsSpider(scrapy.Spider):
    name = "AmzProducts"
    allowed_domains = ["www.amazon.in"]
    stg_table_name = 'staging.stg_amz__product'
    none_counter = 0
    max_none_counter = 5
    initial_delay = 2
    delay = initial_delay
    none_response_timestamps = []
    time_window = 60  # 1 minute in seconds
    
    custom_settings = {
        "ITEM_PIPELINES" : {
            "Amazon_Scraper.pipelines.products_pipeline.AmzProductsPipeline": 300,
        },
        "DOWNLOADER_MIDDLEWARES" : {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
        },
        "AUTOTHROTTLE_ENABLED" : False,
        "RANDOMIZE_DOWNLOAD_DELAY" : True,
        "CONCURRENT_REQUESTS" : 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN" : 4,
        "DOWNLOAD_DELAY" : delay,
    }

    handle_httpstatus_list = [404]

    def __init__(self, postgres_handler, batch_size = 1, **kwargs):
        self.postgres_handler = postgres_handler
        self.batch_size = int(batch_size)
        self.failed_urls = list()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
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
        spider = cls(postgres_handler, *args, **kwargs)
        spider.crawler = crawler  # Attach the crawler instance
        
        crawler.signals.connect(spider.spider_closed, signal=scrapy.signals.spider_closed)
        return spider
    
    def start_requests(self):
        self.postgres_handler.connect()
        for i,row in enumerate(self.postgres_handler.stream_read(
            query="""select * from staging.stg_amz__product_url_feeder """,
            batch_size=self.batch_size
        )):
            self.log(f"Processing batch {i+1}")
            try:
                asin = row['asin']
                product_url = row['product_url']
                yield scrapy.Request(
                    url=product_url, 
                    callback=self.parse,
                    meta={"asin": asin})
            except Exception as e:
                self.log(f"Error for: {row}\n{e}")
    
    
    def parse(self, response):
        if response.status == 404:
                url = response.url
                self.failed_urls.append({'asin': response.meta.get('asin'), 'product_url': url, 'status_code': response.status, 'error': 'Page not found'})
                self.logger.warning(f"404 error for: {url}")
                return  # Do not process further

        item = AmazonProductItem()
        item['asin'] = response.meta.get('asin')
        item['category'] = response.xpath('//*[contains(@id,"wayfinding-breadcrumbs")]/ul/li[1]/span/a/text()').get()
        item['lowest_category'] = response.xpath('//*[contains(@id,"wayfinding-breadcrumbs")]/ul/li[last()]/span/a/text()').get()
        item['product_name'] = response.xpath('//*[@id="productTitle"]/text()').get()
        item['seller_id'] = response.xpath(
                '//div[contains(@tabular-attribute-name, "Sold by")]//a/@href |'
                '//td[@class="alm-mod-sfsb-column"]/span/a/@href'
            ).get() or ''
        item['seller_name'] = response.xpath('//div[contains(@tabular-attribute-name, "Sold by")]//a/text()').get()
        item['brand_name'] = response.xpath(
                '//*[@id="bylineInfo_feature_div"]/div[1]/a/text() | '
                '//*[@id="bylineInfo_feature_div"]/div[1]/span/a/text()'
            ).get()        
        item['last_month_sale'] = response.xpath('//*[@id="social-proofing-faceout-title-tk_bought"]/span/text()').get() or '0'
        item['rating'] = response.xpath('//*[@id="averageCustomerReviews"]/span[1]/span[1]/span[1]/a/span/text()').get()
        item['reviews_count'] = response.xpath('//*[@id="averageCustomerReviews"]/span[3]/a/span/text()').get() or '0'
        item['sell_price'] = response.xpath(
                '//*[contains(@id, "corePriceDisplay")]/div[1]/span[3]/span[2]/span[2]/text() | '
                '//*[contains(@id, "corePrice")]/div/div/span[1]/span[1]/text() | '
                '//*[contains(@id, "corePrice")]/div/div/span[1]/span[2]/span[2]/text() | '
                '//*[contains(@class, "apexPriceToPay")]/span[1]/text()'
            ).get()
        item['sell_mrp'] = response.xpath('''//span[contains(text(), "M.R.P.")]/span/span[1]/text()''').get()
        item['launch_date'] = response.xpath(
                '//th[contains(text(), "Date First Available")]/../td/text() | '
                '//span[contains(text(), "Date First Available")]/../span[2]/text()'
            ).get()
        item['is_fba'] = response.xpath('//div[contains(@tabular-attribute-name, "Ships from")]/div/span[contains(@class, "buybox")]/text()').get()
        item['is_variant_available'] = response.xpath('//div[@data-totalvariationcount]/@data-totalvariationcount').get() or '0'
        item['product_url'] = response.url
        item['brand_store_url'] = response.urljoin(response.xpath(
                '//*[@id="bylineInfo_feature_div"]/div[1]/a/@href | '
                '//*[@id="bylineInfo_feature_div"]/div[1]/span/a/@href'
            ).get())
        item['scrape_date'] = dt.now()  # remove this and have the columns value as default in the staging table
        is_unavailable = response.xpath(
                "//text()[normalize-space()='Currently unavailable.'] | "
                "//text()[normalize-space()='Temporarily out of stock.']"
            ).get()
        is_add_to_cart = response.xpath("//*[contains(text(), 'Add to Cart')]/text()").get()
        item['is_oos'] = True if is_unavailable or not is_add_to_cart else False
        
        # handle empty response and increase delay
        if item['product_name'] is None:
            current_time = dt.now()
            self.failed_urls.append({'asin': item['asin'], 'product_url': item['product_url'], 'status_code': response.status, 'error': 'Page not loading properly'})
            
            # Add current timestamp and remove timestamps older than 1 minute
            self.none_response_timestamps.append(current_time)
            self.none_response_timestamps = [ts for ts in self.none_response_timestamps 
                                          if (current_time - ts).total_seconds() <= self.time_window]
            
            self.none_counter = len(self.none_response_timestamps)
            self.logger.warning(f"None responses in last minute: {self.none_counter}")
            
            if self.none_counter >= self.max_none_counter:
                # increase delay exponentially
                self.delay = round(math.sqrt(self.delay)+1, 4)**2
            else:
                # increase delay linearly
                self.delay += 1
                
            self.crawler.engine.downloader.slots['www.amazon.in'].delay = self.delay
            self.logger.warning(f"Download delay: {self.delay}")
            self.logger.warning(self.crawler.engine.downloader.slots)
            return
        else:
            # Check if we have any recent none responses
            current_time = dt.now()
            self.none_response_timestamps = [ts for ts in self.none_response_timestamps 
                                          if (current_time - ts).total_seconds() <= self.time_window]
            
            if not self.none_response_timestamps:
                # If no none responses in the last minute, gradually decrease delay
                self.delay = max(self.initial_delay, round(math.sqrt(self.delay)-1, 4)**2)
                self.crawler.engine.downloader.slots['www.amazon.in'].delay = self.delay
                self.logger.warning("No none responses in last minute, decreasing delay")
            else:
                self.none_counter = len(self.none_response_timestamps)
                self.logger.warning(f"Still have {self.none_counter} none responses in last minute")
                
        yield item

    def spider_closed(self, spider):
        self.postgres_handler.bulk_upsert('staging.stg_amz__product_error_urls', self.failed_urls, ["asin"])
        self.postgres_handler.close()