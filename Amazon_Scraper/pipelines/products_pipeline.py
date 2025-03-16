from Amazon_Scraper.helpers.db_postgres_handler import PostgresDBHandler
from Amazon_Scraper.helpers.utils import extract_numeric_part, safe_strip
from datetime import datetime as dt

class AmzProductsPipeline:
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

    def open_spider(self, spider):
        """
        Called when the spider is opened.
        """
        self.batch_size = spider.batch_size
        self.postgres_handler.connect()
        self.postgres_handler.execute(f'truncate table {spider.stg_table_name};')
        self.postgres_handler.execute('call staging.sp_amz___refresh_product_url_feeder();')

    def close_spider(self, spider):
        """
        Called when the spider is closed.
        """
        if self.items:
            self.upsert_batch(spider.stg_table_name)
        self.postgres_handler.execute(f'''call transformed.sp_amz__scd2_update_product_data();''')
        self.postgres_handler.close()

    def process_item(self, item, spider):
        """
        Process each item and insert it into the MongoDB collection.
        """
        # clean the item
        item = {k:safe_strip(v) for k,v in item.items()}
        item['seller_id'] = None if not item['seller_id'] else item['seller_id'][item['seller_id'].index('seller=')+7 : item['seller_id'].index('asin=')-1]
        item['last_month_sale'] = round(extract_numeric_part(item['last_month_sale']))
        item['rating'] = extract_numeric_part(item['rating'])
        item['reviews_count'] = round(extract_numeric_part(item['reviews_count']))
        item['sell_mrp'] = extract_numeric_part(item['sell_mrp'])
        item['sell_price'] = extract_numeric_part(item['sell_price'])
        if item['launch_date']:
            item['launch_date'] = dt.strptime(item['launch_date'], "%d %B %Y")
        item['is_fba'] = True if item['is_fba'] == 'Amazon' else False
        if item['is_variant_available'].isdigit() and int(item['is_variant_available']) > 0:
            item['is_variant_available'] = True
        else:
            item['is_variant_available'] = False
        
        self.items.append(dict(item))
        if len(self.items) >= self.batch_size:
            self.upsert_batch(spider.stg_table_name)
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