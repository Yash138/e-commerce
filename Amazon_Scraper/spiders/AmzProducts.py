import scrapy
import scrapy.signals
from Amazon_Scraper.items import AmazonProductItem
from Amazon_Scraper.helpers.db_postgres_handler import PostgresDBHandler
from Amazon_Scraper.helpers.delay_handler import DelayHandler
from Amazon_Scraper.helpers.utils import setup_logger  # Import setup_logger
from Amazon_Scraper.helpers.constants import LOG_DIR  # Import LOG_DIR
from datetime import datetime as dt
import math

class AmzproductsSpider(scrapy.Spider, DelayHandler):
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
        "AUTOTHROTTLE_ENABLED" : True,
        "RANDOMIZE_DOWNLOAD_DELAY" : True,
        "CONCURRENT_REQUESTS" : 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN" : 4,
        "DOWNLOAD_DELAY" : delay,
    }

    handle_httpstatus_list = [404]

    def __init__(self, postgres_handler, batch_size = 1, crawler=None, **kwargs):
        """
        Initialize the spider with Postgres connection details and batch size.

        :param postgres_handler: An instance of PostgresDBHandler
        :param batch_size: The number of records to read from Postgres in each batch
        :param **kwargs: Additional keyword arguments
        """
        
        self.postgres_handler = postgres_handler
        self.batch_size = int(batch_size)
        self.failed_urls = list()
        # log_file = f"{LOG_DIR}/{self.name}.log"  # Define log file path
        # self.custom_logger = setup_logger(self.name, log_file)  # Initialize custom logger
        # self.custom_logger.info("Logger initialized for AmzProducts spider")  # Log initialization message
        DelayHandler.__init__(
            self, 
            initial_delay=crawler.settings.get('DOWNLOAD_DELAY'), 
            max_none_counter=self.max_none_counter, 
            time_window=self.time_window, 
            log=self.log,  # Pass custom logger to DelayHandler
            crawler=crawler  # Will be set in from_crawler
        )

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
        spider = cls(postgres_handler, *args, crawler=crawler, **kwargs)
        spider.crawler = crawler  # Attach the crawler instance
        
        crawler.signals.connect(spider.spider_closed, signal=scrapy.signals.spider_closed)
        return spider
    
    def start_requests(self):
        """
        This function reads the product urls from the staging table in batches of 'batch_size' and
        creates a scrapy request for each product url. The asin is passed as meta data to the
        request. If there is an error while creating the request, the error message is logged.
        """
        self.postgres_handler.connect()
        for i,row in enumerate(self.postgres_handler.stream_read(
            query="""select * from staging.stg_amz__product_url_feeder """,
            batch_size=self.batch_size
        )):
            self.log(f"Asin Num: {i+1}", 20)
            try:
                asin = row['asin']
                product_url = row['product_url']
                yield scrapy.Request(
                    url=product_url, 
                    callback=self.parse,
                    dont_filter=True,
                    meta={"asin": asin})
            except Exception as e:
                self.log(f"Error for: {row}\n{e}", 40)
        
        if self.pipeline.items:
            self.log("Batch Cleanup before requeueing!!", 20)
            self.pipeline.upsert_batch(self.stg_table_name, self)
            self.pipeline._process_batch_cleanup(self.stg_table_name, self)
            # Check if there are still URLs to scrape
            remaining_urls = self.postgres_handler.read(
                query="SELECT COUNT(*) as count FROM staging.stg_amz__product_url_feeder"
            )[0]['count']
            
            if remaining_urls > 0:
                # If URLs remain, call start_requests again
                self.log(f"Remaining URLs: {remaining_urls}", 20)
                yield from self.start_requests()

    def parse(self, response):
        """
        Parses the Amazon product response to extract product details such as ASIN, category,
        product name, seller information, pricing, and availability. Handles 404 errors by
        logging and appending to a failed URLs list. Adjusts download delay based on the
        frequency of empty responses to manage scraping efficiency.

        :param response: The HTTP response object from a Scrapy request.
        :type response: scrapy.http.Response
        :return: Yields an AmazonProductItem populated with the extracted product data.
        :rtype: Generator
        """

        if response.status == 404:
                url = response.url
                asin = response.meta.get('asin')
                if asin not in [url['asin'] for url in self.failed_urls]:
                    self.failed_urls.append({'asin': asin, 'product_url': url, 'status_code': response.status, 'error': 'Page not found'})
                self.log(f"404 error for: {url}", 30)
                return  # Do not process further

        item = AmazonProductItem()
        item['asin'] = response.meta.get('asin')
        item['category'] = response.xpath(
            '//*[contains(@id,"wayfinding-breadcrumbs")]/ul/li[1]/span/a/@href | '
            '//*[contains(text(),"Best Sellers Rank")]/following-sibling::td/span/span[1]/a/@href'
        ).get() or ''
        item['lowest_category'] = response.xpath(
            '//*[contains(@id,"wayfinding-breadcrumbs")]/ul/li[last()]/span/a/@href | '
            '//*[contains(text(),"Best Sellers Rank")]/following-sibling::td/span/span[last()]/a/@href'
        ).get() or ''
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
        item['rating'] = response.xpath('//*[@id="averageCustomerReviews"]/span[1]/span[1]/span[1]/a/span/text()').get() or ''
        item['reviews_count'] = response.xpath('//*[@id="averageCustomerReviews"]/span[3]/a/span/text()').get() or '0'
        item['sell_price'] = response.xpath(
                '//*[contains(text(),"Price:")]/following-sibling::*//*[contains(text(), "₹")]/text() | '
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
        item['spider_name'] = self.name
        # handle empty response and increase delay
        if item['product_name'] is None:
            if self.handle_none_response(self.failed_urls, item, response):
                return
        else:
            self.handle_successful_response(response)
            
        yield item

    def spider_closed(self, spider):
        if self.failed_urls:
            self.postgres_handler.bulk_upsert('staging.stg_amz__product_error_urls', self.failed_urls, ["asin"])
        self.postgres_handler.close()