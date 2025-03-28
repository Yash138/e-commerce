from Amazon_Scraper.helpers.db_postgres_handler import PostgresDBHandler
from Amazon_Scraper.helpers.utils import extract_numeric_part, safe_strip

from datetime import datetime as dt
import re
from typing import List, Dict, Any

class AmzProductsPipeline:
    # Class constants for SQL queries
    REFRESH_FEEDER_SP = 'call staging.sp_amz__refresh_product_url_feeder();'
    UPDATE_PRODUCT_DATA_SP = 'call transformed.sp_amz__scd2_update_product_data();'
    DELETE_FEEDER_QUERY = 'delete from staging.stg_amz__product_url_feeder where asin in (select asin from {});'
    DELETE_ERROR_URLS_QUERY = 'delete from staging.stg_amz__product_error_urls where asin in (select asin from {});'
    
    def __init__(self, postgres_handler):
        """
        Initialize the pipeline with MongoDB connection details.
        """
        self.postgres_handler = postgres_handler
        self.items: List[Dict[str, Any]] = list()

    @classmethod
    def from_crawler(cls, crawler):
        """
        Access settings from Scrapy's configuration.
        """
        pipeline = cls(
            PostgresDBHandler(
                crawler.settings.get('POSTGRES_HOST'),
                crawler.settings.get('POSTGRES_DATABASE'),
                crawler.settings.get('POSTGRES_USERNAME'),
                crawler.settings.get('POSTGRES_PASSWORD'),
                crawler.settings.get('POSTGRES_PORT')
            ))
        return pipeline

    def _clean_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and transform item fields."""
        cleaned_item = {k: safe_strip(v) for k, v in item.items()}
        
        # Extract seller ID
        seller_match = re.search(r"seller=([A-Z0-9]+)", cleaned_item['seller_id'])
        cleaned_item['seller_id'] = seller_match.group(1) if seller_match else None
        
        # Convert numeric fields
        cleaned_item['last_month_sale'] = round(extract_numeric_part(cleaned_item['last_month_sale']))
        cleaned_item['rating'] = extract_numeric_part(cleaned_item['rating'])
        cleaned_item['reviews_count'] = round(extract_numeric_part(cleaned_item['reviews_count']))
        cleaned_item['sell_mrp'] = extract_numeric_part(cleaned_item['sell_mrp'])
        cleaned_item['sell_price'] = extract_numeric_part(cleaned_item['sell_price'])
        
        # Handle date and boolean fields
        if cleaned_item['launch_date']:
            cleaned_item['launch_date'] = dt.strptime(cleaned_item['launch_date'], "%d %B %Y")
        
        cleaned_item['is_fba'] = cleaned_item['is_fba'] == 'Amazon'
        cleaned_item['is_variant_available'] = (
            cleaned_item['is_variant_available'].isdigit() and 
            int(cleaned_item['is_variant_available']) > 0
        )
        
        return cleaned_item

    def _process_batch_cleanup(self, table_name: str, spider) -> None:
        """Execute cleanup operations after batch processing."""
        self.postgres_handler.execute(self.UPDATE_PRODUCT_DATA_SP)
        self.postgres_handler.execute(self.DELETE_FEEDER_QUERY.format(table_name))
        self.postgres_handler.execute(self.DELETE_ERROR_URLS_QUERY.format(table_name))
        self.postgres_handler.execute(f'truncate table {table_name};')
        if spider.failed_urls:
            self.postgres_handler.execute(f'''
                delete from staging.stg_amz__product_url_feeder 
                where asin in (
                    '{"','".join([url["asin"] for url in spider.failed_urls if url['status_code'] == 404])}'
                )
            ''')

    def open_spider(self, spider):
        """
        Called when the spider is opened.
        """
        spider.pipeline = self  # ✅ This ensures the spider has access to the pipeline
        self.batch_size = spider.batch_size
        self.postgres_handler.connect()
        self.postgres_handler.execute(f'truncate table {spider.stg_table_name};')
        self.postgres_handler.execute(self.REFRESH_FEEDER_SP)

    def close_spider(self, spider, msg=None):
        """
        Called when the spider is closed.
        """
        if self.items:
            self.upsert_batch(spider.stg_table_name)
        self._process_batch_cleanup(spider.stg_table_name, spider)
        self.postgres_handler.close()
        if msg: self.log(f"Closing Spider: {msg}")

    def process_item(self, item, spider):
        """
        Process each item and insert it into the MongoDB collection.
        """
        cleaned_item = self._clean_item(item)
        self.items.append(dict(cleaned_item))
        
        if len(self.items) >= self.batch_size:
            self.upsert_batch(spider.stg_table_name)
            self._process_batch_cleanup(spider.stg_table_name, spider)
            
        return item

    def upsert_batch(self, table_name):
        try:
            self.postgres_handler.bulk_upsert(
                table_name, 
                self.items, 
                conflict_columns=['asin'], 
                update_columns=None)
            self.items = list()
        except Exception as e:
            print(f"Error while bulk upsert: {e}")