import scrapy
from Amazon_Scraper.helpers.postgres_handler import PostgresDBHandler
from Amazon_Scraper.items import AmazonProductItem
from datetime import datetime as dt
from Amazon_Scraper.helpers.delay_handler import DelayHandler
from Amazon_Scraper.helpers.utils import setup_logger  # Import setup_logger
from Amazon_Scraper.helpers.constants import LOG_DIR  # Import LOG_DIR

# if running the pipeline same day for same category, then :
    # 1. if refreshed_pages_upto = depth_limit, then skip the category-lowest_category
    # 2. if refreshed_pages_upto < depth_limit, then skip the pages already processed and start from the next page
    # 3. fethc lc urls based on refresh date & depth_limit

class AmzproductslcSpider(scrapy.Spider, DelayHandler):
    name = "AmzProductsLC"
    allowed_domains = ["www.amazon.in"]
    stg_table_name = 'staging.stg_amz__product'
    handle_httpstatus_list = [503]
    custom_settings = {
        "DEPTH_LIMIT": 10,
        "ITEM_PIPELINES" : {
            "Amazon_Scraper.pipelines.products_pipeline.AmzProductsPipeline": 300,
        },
        "DOWNLOADER_MIDDLEWARES" : {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': None,
            'Amazon_Scraper.middlewares.HeaderRotationMiddleware': 500,
            'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 700,
        },
        "DOWNLOAD_DELAY" : 8,
        "RANDOMIZE_DOWNLOAD_DELAY" : True,
        "CONCURRENT_REQUESTS" : 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN" : 16,
        "CONCURRENT_REQUESTS_PER_IP" : 4,
        "COOKIES_ENABLED" : True,
        "RETRY_TIMES": 1,
        "RETRY_DELAY" : 15,
    }

    def __init__(self, postgres_handler, batch_size = 1, category = None, lowest_category = None, retry_count = 3, logfile=None, crawler=None, **kwargs):
        """
        Initialize the spider with Postgres connection details and batch size.

        :param postgres_handler: An instance of PostgresDBHandler
        :param batch_size: The number of records to read from Postgres in each batch
        :param retry_count: Maximum number of retry attempts for failed URLs (default: 3)
        :param **kwargs: Additional keyword arguments
        """
        if category is not None:
            self.category = category
            self.lowest_category = lowest_category
        else:
            raise ValueError("Usage: scrapy crawl AmzProductsLC -a category=<category> [-a lowest_category=<lowest_category>] -a batch_size=<batch_size> -a retry_count=<retry_count> -s DEPTH_LIMIT=<depth_limit> [--logfile=<logfile>]")
        self.postgres_handler = postgres_handler
        self.batch_size = int(batch_size)
        self.retry_count = int(retry_count)
        self.failed_urls = list()
        self.update_category = list()
        self.processed_asins = set()  # Set to track processed ASINs
        self.is_retry_scheduler = False  # Track if we're in retry scheduler mode
        self.logfile = logfile
        DelayHandler.__init__(
            self,
            initial_delay=crawler.settings.get('DOWNLOAD_DELAY'),  # Set initial delay
            max_none_counter=2,  # Set max none counter
            time_window=60,  # Set time window in seconds
            log=self.log,
            crawler=crawler  # Will be set in from_crawler
        )
        
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """
        Access settings from Scrapy's configuration.
        """
        postgres_handler = PostgresDBHandler(
                crawler.settings['POSTGRES_HOST'],
                crawler.settings['POSTGRES_DATABASE'],
                crawler.settings['POSTGRES_USERNAME'],
                crawler.settings['POSTGRES_PASSWORD'],
                crawler.settings['POSTGRES_PORT']
            )   
        spider = cls(postgres_handler, *args, crawler=crawler, **kwargs)
        spider.crawler = crawler  # Attach the crawler instance
        spider.settings = crawler.settings
        
        crawler.signals.connect(spider.spider_closed, signal=scrapy.signals.spider_closed)
        return spider
    
    def start_requests(self):
        if self.category in self.settings['EXCLUDE_CATEGORIES_IDS']:
            raise ValueError(f"Invalid category: {self.category}. Must NOT be one of {self.settings['EXCLUDE_CATEGORIES_IDS']}")
        self.postgres_handler.connect()
        if not self.is_retry_scheduler:
            # First process pending categories from the database
            category_cond = f"category = '{self.category}' "
            lc_cond = f" and lowest_category = '{self.lowest_category}'" if self.lowest_category else ""
            cond = category_cond + lc_cond + f" and (scraping_mandatory is True or refreshed_pages_upto < {self.settings.get('DEPTH_LIMIT')})"
            query = f"select * from transformed.vw_amz__pending_category_refresh where {cond}"
            self.log(f"Query: {query}", 20)
            for row in self.postgres_handler.stream_read(
                query=query,
                batch_size=self.batch_size
            ):
                # # if there are no rows, raise an error
                # if not row:
                #     raise ValueError("Category and lowest_category already processed!!!")
                if row['scraping_mandatory']:
                    current_page = 0
                    url = f"https://www.amazon.in/s?i={row['category']}&rh=n%3A{row['lowest_category']}&s=popularity-rank&fs=true"
                else:
                    current_page = row['refreshed_pages_upto']
                    url = f"https://www.amazon.in/s?i={row['category']}&rh=n%3A{row['lowest_category']}&s=popularity-rank&fs=true&page={current_page}&qid={round(dt.timestamp(dt.now()))}&ref=sr_pg_{current_page-1}"
                    #  "https://www.amazon.in/s?i=computers&rh=n%3A1375325031&s=popularity-rank&fs=true&page=8&qid=1746819223&xpid=hFmZFOPx371or&ref=sr_pg_7"
                yield scrapy.Request(
                    url=url, 
                    callback=self.parse, 
                    meta={
                        "category": row['category'], 
                        "lowest_category": row['lowest_category'],
                        "total_pages": row['total_pages'],
                        "refreshed_pages_upto": current_page,
                        "page": current_page,
                        "initial_run": True,
                        "scraping_mandatory": row['scraping_mandatory']
                    })
                # self.is_retry_scheduler = True
                # yield from self.start_requests()
        # Then process failed URLs if any exist

    def parse(self, response):
        # Handle 503 response
        category = response.meta['category']
        lowest_category = response.meta['lowest_category']
        if response.status == 503:
            self.log(f"503 error for URL: {response.url}", 30)
            self.handle_none_response(self.failed_urls, {}, response)
            # Find the URL in failed_urls list and update its retry count
            for failed_url in self.failed_urls:
                if failed_url["category"] == category and failed_url["lowest_category"] == lowest_category:
                    failed_url['status_code'] = response.status
                    failed_url['total_pages'] = response.meta.get('total_pages', 0)
                    failed_url['refreshed_pages_upto'] = response.meta.get('page', 0) - 1 if not response.meta.get('initial_run', False) else response.meta.get('page', 0)
                    failed_url['retry_count'] = failed_url.get('retry_count', 0) + 1
                    if failed_url['retry_count'] >= self.retry_count:
                        self.log(f"Maximum retries ({self.retry_count}) reached for URL: {response.url}", 40)
                        return
                    else:
                        self.log(f"Retrying URL: {failed_url}", 20)
                        yield scrapy.Request(
                            url=f"https://www.amazon.in/s?i={failed_url['category']}&rh=n%3A{failed_url['lowest_category']}&s=popularity-rank&fs=true&page={failed_url['refreshed_pages_upto']}",
                            callback=self.parse,
                            meta={
                                "category": failed_url['category'],
                                "lowest_category": failed_url['lowest_category'],
                                "total_pages": failed_url['total_pages'],
                                "refreshed_pages_upto": failed_url['refreshed_pages_upto'],
                                "page": failed_url['refreshed_pages_upto'],
                                "initial_run": False
                            },
                            dont_filter=True  # Allow retrying the same URL
                        )
                    break
            else:
                # If URL is not in failed_urls, add it
                failed_url_data = {
                    'status_code': response.status,
                    'category': category,
                    'lowest_category': lowest_category,
                    'error': 'Service Unavailable (503)',
                    'retry_count': 1,
                    'total_pages': response.meta.get('total_pages', 0),
                    'refreshed_pages_upto': response.meta.get('page', 0) - 1 if not response.meta.get('initial_run', False) else response.meta.get('page', 0)
                }
                self.failed_urls.append(failed_url_data)
                self.log(f"Retrying URL: {failed_url_data}", 20)
                yield scrapy.Request(
                    url=f"https://www.amazon.in/s?i={failed_url_data['category']}&rh=n%3A{failed_url_data['lowest_category']}&s=popularity-rank&fs=true&page={failed_url_data['refreshed_pages_upto']}",
                    callback=self.parse,
                    meta={
                        "category": failed_url_data['category'],
                        "lowest_category": failed_url_data['lowest_category'],
                        "total_pages": failed_url_data['total_pages'],
                        "refreshed_pages_upto": failed_url_data['refreshed_pages_upto'],
                        "page": failed_url_data['refreshed_pages_upto'],
                        "initial_run": False
                    },
                    dont_filter=True  # Allow retrying the same URL
                )
            return
        self.handle_successful_response(response)
        self.log(f"Processing category: {category}, lowest_category: {lowest_category}", 20)
        
        # Get current page number from meta or default to 0
        current_page = response.meta.get('page', 0) if response.meta.get('page', 0) != 0 else 1
        product_count = 0 #response.meta.get('product_count', 0)
        skipped_asins = 0
        if response.meta.get('initial_run', True) or int(response.meta['total_pages']) == 0:
            self.total_pages = int(response.xpath('//span[contains(@class, "pagination-strip")]/ul/child::*[last()-1]//text()').get() or '1')
        else:
            self.total_pages = int(response.meta['total_pages'])
        # self.total_pages = int(response.xpath('//span[contains(@class, "pagination-strip")]/ul/child::*[last()-1]//text()').get() or '1')
        # if refreshed_pages_upto <= current_page, then skip the pages already processed and start from the next page
        if current_page <= response.meta['refreshed_pages_upto'] and current_page != 0:
            self.log(f"Skipping already processed pages for category: {category}, lowest_category: {lowest_category}, current_page: {current_page}, already_refreshed_pages_upto: {response.meta['refreshed_pages_upto']}", 20)
        else:
            # Extract product details
            for product in response.xpath('//div[@role="listitem"]'):
                item = AmazonProductItem()
                asin = product.xpath('@data-asin').get()
                if asin in self.processed_asins:
                    self.log(f"Skipping already processed ASIN: {asin}", 20)
                    skipped_asins += 1
                    product_count += 1
                    continue  # Skip if ASIN is already processed

                self.processed_asins.add(asin)  # Add ASIN to the set
                item['category'] = category
                item['lowest_category'] = lowest_category
                item['asin'] = asin
                if not product.xpath('.//*[contains(text(), "Sponsored")]/text()').get():
                    item['product_name'] = product.xpath('.//div[@data-cy="title-recipe"]/a/h2/span/text()').get()
                    item['product_url'] = response.urljoin(product.xpath('.//div[@data-cy="title-recipe"]/a/@href').get()).split('/ref')[0]
                    item['rating'] = product.xpath('.//div[@data-cy="reviews-block"]//*[@data-cy="reviews-ratings-slot"]/span/text()').get() or ''
                    item['reviews_count'] = product.xpath('.//div[@data-cy="reviews-block"]//*[contains(@aria-label," ratings")]/span/text()').get() or '0'
                    item['last_month_sale'] = product.xpath('.//div[@data-cy="reviews-block"]/div[2]/span/text()').get() or '0'
                    item['sell_mrp'] = product.xpath('.//div[@data-cy="price-recipe"]//span[contains(text(),"M.R.P")]/text()').get()
                    item['sell_price'] = product.xpath('.//div[@data-cy="price-recipe"]//span[@class="a-price"]/span/text()').get()
                    item['scrape_date'] = dt.now()
                    item['seller_id'] = ''
                    item['seller_name'] = None
                    item['brand_name'] = None
                    item['is_fba'] = None
                    item['is_oos'] = None
                    item['is_variant_available'] = None
                    item['launch_date'] = None
                    item['brand_store_url'] = None
                    item['spider_name'] = self.name
                    product_count += 1
                    self.log(f"Processing ASIN: {asin} | Product Name: {item['product_name']}", 20)
                    yield item
            for x in self.update_category:
                if x["category"] == category and x["lowest_category"] == lowest_category:
                    x["refreshed_pages_upto"] = current_page
                    x["last_refresh_timestamp"] = dt.now()
                    break
            else:
                self.update_category.append(
                    {"category": category, "lowest_category": lowest_category, "total_pages": self.total_pages, "refreshed_pages_upto": current_page, "last_refresh_timestamp": dt.now()} 
                )
        # Check for the next page
        next_page = response.xpath('//*[contains(@class, "pagination-strip")]//*[contains(text(), "Next")]/@href').get()
        # first page is considered at depth_limit = 0
        if next_page and current_page < int(self.settings['DEPTH_LIMIT']) and current_page < self.total_pages:
            yield scrapy.Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={
                    'category': category,
                    'lowest_category': lowest_category,
                    'total_pages': self.total_pages,
                    'refreshed_pages_upto': current_page,
                    'page': current_page + 1,
                    'initial_run': False,
                    'product_count': product_count if product_count >= response.meta.get('product_count', 0) else response.meta.get('product_count', 0)
                }
            )
        else:
            flg_empty_page = False
            if product_count == 0 and skipped_asins == 0: # this will happen when the LC is not having any products on the first page
                self.log(f"No Products found for category: {category} | Lowest Category: {lowest_category} | Current Page: {current_page}", 20)
                flg_empty_page = True
            # Update refreshed_pages_upto and last_refresh_timestamp when the last page is reached
            for i, x in enumerate(self.update_category):
                if x["category"] == category and x["lowest_category"] == lowest_category:
                    if flg_empty_page:
                        x["refreshed_pages_upto"] = max(0, current_page - 1)  # Ensure it doesn't go below 0
                    else:
                        x["refreshed_pages_upto"] = current_page
                    x["last_refresh_timestamp"] = dt.now(),
                    x["products_per_page"] = product_count,
                    x["total_pages"] = self.total_pages if not flg_empty_page else self.total_pages - 1
                    if self.update_category_stats(x):
                        self.update_category.pop(i)
                    break
            for i,failed_url in enumerate(self.failed_urls):
                if failed_url["category"] == category and failed_url["lowest_category"] == lowest_category:
                    self.failed_urls.pop(i)
                    break
            
    
    def update_category_stats(self, category_stats):
        self.log(f"Category: {category_stats['category']} | Lowest Category: {category_stats['lowest_category']} | Restricted to Depth: {self.settings['DEPTH_LIMIT']} | Current Page: {category_stats['refreshed_pages_upto']} | Total Pages: {self.total_pages}", 20)
        try:
            self.postgres_handler.connect()
            self.log(f"Updating category stats for {category_stats['category']} | {category_stats['lowest_category']}", 20)
            self.postgres_handler.update(
                table="transformed.amz__category_refresh_controller", 
                data={
                    "refreshed_pages_upto": category_stats['refreshed_pages_upto'],
                    "total_pages": category_stats['total_pages'],
                    "last_refresh_timestamp": category_stats['last_refresh_timestamp'],
                    "products_per_page": category_stats['products_per_page']
                },
                conditions={
                    "category": category_stats['category'],
                    "lowest_category": category_stats['lowest_category']
                }
            )
            return True
        except Exception as e:
            self.log(f"Error updating category stats for {category_stats['category']} | {category_stats['lowest_category']}: {e}", 40)
            return False

    def spider_closed(self, spider):
        self.log(f"Category stats {self.update_category}", 20)
        self.log(f"Failed URLs: {self.failed_urls}", 20)
        self.postgres_handler.connect()
        # instead of bulk_upsert, we need to update these values in the table one at a time
        for row in self.update_category:
            try:
                self.postgres_handler.update(
                    table="transformed.amz__category_refresh_controller", 
                    data={
                        "refreshed_pages_upto": row["refreshed_pages_upto"],
                        "total_pages": row["total_pages"],
                        "last_refresh_timestamp": row["last_refresh_timestamp"],
                        "products_per_page": row.get("products_per_page", 0)
                    },
                    conditions={
                        "category": row["category"],
                        "lowest_category": row["lowest_category"]
                    }
                )
            except Exception as e:
                self.log(f"Error updating category stats for {row['category']} | {row['lowest_category']}: {e}", 40)
        self.postgres_handler.close()