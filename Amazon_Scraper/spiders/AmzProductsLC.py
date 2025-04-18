import scrapy
from Amazon_Scraper.helpers.db_postgres_handler import PostgresDBHandler
from Amazon_Scraper.items import AmazonProductItem
from datetime import datetime as dt

class AmzproductslcSpider(scrapy.Spider):
    name = "AmzProductsLC"
    allowed_domains = ["www.amazon.in"]
    stg_table_name = 'staging.stg_amz__product'
    custom_settings = {
        "DEPTH_LIMIT": 10,
        "ITEM_PIPELINES" : {
            # "Amazon_Scraper.pipelines.products_pipeline.AmzProductsPipeline": 300,
        },
        "DOWNLOADER_MIDDLEWARES" : {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
        }
    }

    def __init__(self, postgres_handler, batch_size = 1, category = None, lowest_category = None, **kwargs):
        """
        Initialize the spider with Postgres connection details and batch size.

        :param postgres_handler: An instance of PostgresDBHandler
        :param batch_size: The number of records to read from Postgres in each batch
        :param **kwargs: Additional keyword arguments
        """
        # if category is not None:
        #     self.category = category
        #     self.lowest_category = lowest_category
        # else:
        #     raise ValueError("Category must be provided")
        self.postgres_handler = postgres_handler
        self.batch_size = int(batch_size)
        self.failed_urls = list()
        self.update_category = list()
        
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
        # self.postgres_handler.connect()
        # cond = f"category = '{self.category}' and lowest_category = '{self.lowest_category}'" if self.lowest_category else f"category = '{self.category}'"
        # rows = self.postgres_handler.stream_read(
        #     query=f"""select * from transformed.vw_amz__pending_category_refresh where {cond}""",
        #     batch_size=self.batch_size
        # )
        # # if there are no rows, raise an error
        # if not rows:
        #     raise ValueError("Category and lowest_category already processed!!!")
        # for row in rows:
        #     url = f"https://www.amazon.in/s?i={row['category']}&rh=n%3A{row['lowest_category']}&s=popularity-rank&fs=true"
        #     yield scrapy.Request(
        #         url=url, 
        #         callback=self.parse, 
        #         meta={
        #             "category": row['category'], 
        #             "lowest_category": row['lowest_category']
        #         })
        items = [
            {
                "category": "automotive",
                "lowest_category": "5258384031"
            },
            {
                "category": "kitchen",
                "lowest_category": "3591273031"
            }
        ]
        for item in items:
            yield scrapy.Request(
                url=f"https://www.amazon.in/s?i={item['category']}&rh=n%3A{item['lowest_category']}&s=popularity-rank&fs=true&ref=lp_{item['lowest_category']}_sar", 
                callback=self.parse, 
                meta={
                    "category": item['category'], 
                    "lowest_category": item['lowest_category']
                })

    def parse(self, response):
        item = AmazonProductItem()
        category = response.meta['category']
        lowest_category = response.meta['lowest_category']
        
        # Get current page number from meta or default to 1
        current_page = response.meta.get('page', 1)
        if current_page == 1:
            self.total_pages = response.xpath('//span[contains(@class, "pagination-strip")]/ul/child::span[not(child::*)]/text()').get() or '1'
            self.total_pages = int(self.total_pages)
            self.update_category.append(
                {"category": category, "lowest_category": lowest_category, "total_pages": self.total_pages, "refreshed_pages_upto": 1, "last_refresh_timestamp": dt.now()} 
            )
        else:
            self.total_pages = response.meta['total_pages']
            for x in self.update_category:
                if x["category"] == category and x["lowest_category"] == lowest_category:
                    x["refreshed_pages_upto"] = current_page
                    x["last_refresh_timestamp"] = dt.now()
                    break

        for product in response.xpath('//div[@role="listitem"]'):
            item['category'] = category
            item['lowest_category'] = lowest_category
            item['asin'] = product.xpath('@data-asin').get()
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
                yield item

        # Check for the next page
        next_page = response.xpath('//*[contains(@class, "pagination-strip")]//*[contains(text(), "Next")]/@href').get()
        if next_page and current_page <= self.total_pages:
            yield scrapy.Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={
                    'category': category,
                    'lowest_category': lowest_category,
                    'total_pages': self.total_pages,
                    'page': current_page + 1  # Increment page counter for next request
                }
            )
        else:
            # Update refreshed_pages_upto and last_refresh_timestamp when the last page is reached
            for x in self.update_category:
                if x["category"] == category and x["lowest_category"] == lowest_category:
                    x["refreshed_pages_upto"] = current_page
                    x["last_refresh_timestamp"] = dt.now()
                    break

    def spider_closed(self, spider):
        try:
            self.logger.info(f"Category stats {self.update_category}")
            self.postgres_handler.connect()
            # instead of bulk_upsert, we need to update these values in the table one at a time
            for row in self.update_category:
                self.postgres_handler.update(
                    table="transformed.amz__category_refresh_controller", 
                    data={
                        "refreshed_pages_upto": row["refreshed_pages_upto"],
                        "last_refresh_timestamp": row["last_refresh_timestamp"]
                    },
                    conditions={
                        "category": row["category"],
                        "lowest_category": row["lowest_category"]
                    }
                )
        except Exception as e:
            self.logger.error(f"Error updating category stats: {e}")
        self.postgres_handler.close()