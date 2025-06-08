from Amazon_Scraper.helpers.postgres_handler import PostgresDBHandler
from Amazon_Scraper.helpers.postgres_handler_async import AsyncPostgresDBHandler
from twisted.internet import defer, reactor
from typing import Dict, Any
from Amazon_Scraper.helpers.utils import safe_strip, safe_replace, safe_split

class AmzProductRankingStgSyncPipeline:
    def __init__(self, postgres_handler):
        """
        Initialize the pipeline with MongoDB connection details.
        """
        self.postgres_handler = postgres_handler
        self.items = list()

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
        return cls(postgres_handler)

    def _clean_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and transform item fields."""
        cleaned_item = {k: safe_strip(v) for k, v in item.items()}
        # convert to int if not None
        cleaned_item['rank'] = safe_replace(cleaned_item['rank'], '#', '')
        if cleaned_item['rank']:
            cleaned_item['rank'] = int(cleaned_item['rank'])
        # safe split product_url
        cleaned_item['product_url'] = safe_split(cleaned_item['product_url'], '/ref')[0]
        return cleaned_item

    def open_spider(self, spider):
        """
        Called when the spider is opened.
        """
        self.batch_size = spider.batch_size
        self.list_type = spider.list_type
        self.postgres_handler.connect()

    def close_spider(self, spider):
        """
        Called when the spider is closed.
        """
        if self.items:
            self.upsert_batch(spider.table_name)
        self.postgres_handler.execute(f'''call transformed.sp_amz__process_product_rankings('{self.list_type}');''')
        self.postgres_handler.close()

    def process_item(self, item, spider):
        """
        Process each item and insert it into the MongoDB collection.
        """
        # clean the item
        cleaned_item = self._clean_item(item)
        self.items.append(dict(cleaned_item))
        if len(self.items) >= self.batch_size:
            self.upsert_batch(spider.table_name)
        return item
    
    def upsert_batch(self, table_name):
        try:
            self.postgres_handler.bulk_upsert(
                table_name, 
                self.items, 
                conflict_columns=['list_type', 'category', 'sub_category', 'asin'], 
                update_columns=None)
            self.items = list()
        except Exception as e:
            print(f"Error while bulk upsert: {e}")


class AmzProductRankingStgAsyncPipeline:
    def __init__(self, postgres_handler):
        """
        Initialize the pipeline with Postgres connection details.
        """
        self.postgres_handler = postgres_handler
        self.items = list()

    @classmethod
    def from_crawler(cls, crawler):
        """
        Access settings from Scrapy's configuration.
        """
        postgres_handler = AsyncPostgresDBHandler(
                crawler.settings.get('POSTGRES_HOST'),
                crawler.settings.get('POSTGRES_DATABASE'),
                crawler.settings.get('POSTGRES_USERNAME'),
                crawler.settings.get('POSTGRES_PASSWORD'),
                crawler.settings.get('POSTGRES_PORT')
            )
        return cls(postgres_handler)

    @defer.inlineCallbacks
    def open_spider(self, spider):
        """
        Called when the spider is opened.
        """
        self.batch_size = spider.batch_size
        # await self.postgres_handler.connect()
        yield defer.ensureDeferred(self.postgres_handler.connect())
        # defer.ensureDeferred(self.postgres_handler.connect())
        # table_name = getattr(spider, 'table_name')
        # # clean the stage table
        # self.postgres_handler.delete(table_name, '1=1')

    async def process_item(self, item, spider):
        """
        Process each item and insert it into the Postgres Database.
        """
        self.items.append(dict(item))
        if len(self.items) >= self.batch_size:
            await self.upsert_batch(spider.table_name)
        # await self.postgres_handler.insert(spider.table_name, dict(item))
        return item
    
    async def upsert_batch(self, table_name):
        await self.postgres_handler.bulk_upsert(
            table_name, 
            self.items, 
            conflict_columns=['category', 'sub_category', 'asin'], 
            update_columns=None)
        self.items = list()

    @defer.inlineCallbacks
    def close_spider(self, spider):
        """
        Called when the spider is closed.
        """
        # self.postgres_handler.execute('call curated.sp_process_best_seller();', type='procedure')
        # self.postgres_handler.execute('call curated.sp_update_master_tables();', type='procedure')
        # await self.postgres_handler.close()
        yield defer.ensureDeferred(self.postgres_handler.close())
        # defer.ensureDeferred(self.postgres_handler.close())