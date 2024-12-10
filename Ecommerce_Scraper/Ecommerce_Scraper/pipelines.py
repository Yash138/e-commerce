# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from Ecommerce_Scraper.utility import MongoDBHandler


class EcommerceScraperPipeline:
    def process_item(self, item, spider):
        return item

class MongoDBPipeline:
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
