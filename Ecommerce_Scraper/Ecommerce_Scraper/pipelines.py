# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from Ecommerce_Scraper.utility import MongoDBHandler, PostgresDBHandler, add_suffixes, remove_suffixes
import pandas as pd

class EcommerceScraperPipeline:
    def process_item(self, item, spider):
        return item

class AmazonBSStagingMongoPipeline:
    def __init__(self, mongo_uri, mongo_db):
        """
        Initialize the pipeline with MongoDB connection details.
        """
        self.mongo_handler = MongoDBHandler(mongo_uri, mongo_db)

    @classmethod
    def from_crawler(cls, crawler):
        """
        Access settings from Scrapy's configuration.
        """
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE")
        )

    def open_spider(self, spider):
        """
        Called when the spider is opened.
        """
        self.mongo_handler.connect()
        collection_name = getattr(spider, 'collection_name')
        # clean the stage table
        self.mongo_handler.delete_many(collection_name, {})

    def close_spider(self, spider):
        """
        Called when the spider is closed.
        """
        trf_pipeline = AmazonBSTransformationPipeline(self.mongo_handler)
        trf_pipeline.transform_data()
        self.mongo_handler.close()

    def process_item(self, item, spider):
        """
        Process each item and insert it into the MongoDB collection.
        """
        collection_name = getattr(spider, 'collection_name')
        self.mongo_handler.insert_one(collection_name, dict(item))
        return item


class AmazonBSStagingPipeline:
    def __init__(self, postgres_handler):
        """
        Initialize the pipeline with MongoDB connection details.
        """
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
        return cls(postgres_handler)

    def open_spider(self, spider):
        """
        Called when the spider is opened.
        """
        self.postgres_handler.connect()
        table_name = getattr(spider, 'table_name')
        # clean the stage table
        self.postgres_handler.delete(table_name, '1=1')

    def close_spider(self, spider):
        """
        Called when the spider is closed.
        """
        self.postgres_handler.execute('call processed.sp_process_best_seller();', type='procedure')
        self.postgres_handler.execute('call processed.sp_update_master_tables();', type='procedure')
        self.postgres_handler.close()

    def process_item(self, item, spider):
        """
        Process each item and insert it into the MongoDB collection.
        """
        table_name = getattr(spider, 'table_name')
        self.postgres_handler.insert(table_name, dict(item))
        return item

class AmazonBSTransformationPipeline:
    def __init__(self, mongo_handler):
        """
        Initialize the pipeline with MongoDB connection details.
        """
        self.mongo_handler = mongo_handler
        
    def transform_data(self):
        # pull data from stage table & final table
        # self.mongo_handler.connect()
        suffixes = ['_trf', '_stg']
        # Fetch data from MongoDB collections
        df_trf = pd.DataFrame(list(self.mongo_handler.find('trf_amz__best_sellers', {'isLatest': True}))) \
                    .pipe(add_suffixes, suffix=suffixes[0])
        df_stg = pd.DataFrame(list(self.mongo_handler.find('stg_amz__best_sellers', {}))) \
                    .assign(isLatest=True) \
                    .pipe(add_suffixes, suffix=suffixes[1])

        if not df_trf.empty:
            # Perform a full outer join on 'category', 'subcategory', and 'asin'
            df_full = pd.merge(df_stg, df_trf, how='outer', 
                            left_on=['category_stg', 'subCategory_stg', 'asin_stg'], 
                            right_on=['category_trf', 'subCategory_trf', 'asin_trf'])

            # Filter rows where df_trf.asin is null (new records in df_stage but not in df_trf)
            df_insert1 = df_full[df_full['asin_trf'].isna()][list(df_stg.columns)]

            # # Filter rows where asin is not null but ranks differ between stage and trf
            df_inter = df_full[
                (df_full['asin_trf'].notna()) 
                & (df_full['asin_stg'].notna()) 
                & (df_full['rank_stg'] != df_full['rank_trf'])
                ]

            # Mark isActive as False in df_trf rows where rank differs
            df_update = df_inter.copy()
            df_update['isLatest_trf'] = False
            df_update = df_update[list(df_trf.columns)]

            # Insert rows from df_stage with differing ranks
            df_insert2 = df_inter[list(df_stg.columns)]
            
            # union all dataframes for upsert
            df_upsert = pd.concat([
                df_insert1.pipe(remove_suffixes, suffixes[0],suffixes[1]),
                df_insert2.pipe(remove_suffixes, suffixes[0],suffixes[1]),
                df_update.pipe(remove_suffixes, suffixes[0],suffixes[1]),
            ])
            
        else: 
            df_upsert = df_stg.pipe(remove_suffixes, suffixes[0],suffixes[1]).copy()
        
        if not df_upsert.empty:
            self.mongo_handler.bulk_upsert(
                'trf_amz__best_sellers', 
                df_upsert, 
                filter_cols=['_id', 'asin', 'category', 'subCategory', 'rank'],
                upsert=True
            )
        else:
            print("NO DATA TO UPSERT")
        
        # -- old product no more in top 100 - set it inactive
        df_trf = pd.DataFrame(list(self.mongo_handler.find('trf_amz__best_sellers', {'isLatest': True})))
        df_trf['d_rank'] = df_trf.groupby(['category','subCategory','rank'])['loadTimestamp'].rank(method='dense', ascending=True)
        df_upsert = df_trf[df_trf['d_rank']>1][['_id', 'isLatest']].assign(isLatest=False)
        if not df_upsert.empty:
            self.mongo_handler.bulk_upsert(
                'trf_amz__best_sellers', 
                df_upsert, 
                filter_cols=['_id'],
                upsert=True
            )
        else:
            print("NO DATA TO UPSERT")
            

class AmazonProductScraperMongoPipeline:
    def __init__(self, mongo_uri, mongo_db):
        """
        Initialize the pipeline with MongoDB connection details.
        """
        self.mongo_handler = MongoDBHandler(mongo_uri, mongo_db)

    @classmethod
    def from_crawler(cls, crawler):
        """
        Access settings from Scrapy's configuration.
        """
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE")
        )
    
    def open_spider(self, spider):
        """
        Called when the spider is opened.
        """
        self.mongo_handler.connect()
    
    def close_spider(self, spider):
        """
        Called when the spider is closed.
        """
        self.mongo_handler.close()

    def process_item(self, item, spider):
        """
        Process each item and insert it into the MongoDB collection.
        """
        collection_name = getattr(spider, 'collection_name')
        self.mongo_handler.insert_one(collection_name, dict(item))
        return item
    
    
class AmazonProductStagePipeline:
    def __init__(self, postgres_handler):
        """
        Initialize the pipeline with MongoDB connection details.
        """
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
        return cls(postgres_handler)
    
    def open_spider(self, spider):
        """
        Called when the spider is opened.
        """
        self.postgres_handler.connect()
    
    def close_spider(self, spider):
        """
        Called when the spider is closed.
        """
        self.postgres_handler.close()

    def process_item(self, item, spider):
        """
        Process each item and insert it into the MongoDB collection.
        """
        table_name = getattr(spider, 'table_name')
        self.postgres_handler.insert(table_name, dict(item))
        return item
