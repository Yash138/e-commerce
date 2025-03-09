# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AmazonCategoryItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    list_type = scrapy.Field()
    category = scrapy.Field()
    sub_category = scrapy.Field()
    asin = scrapy.Field()
    rank = scrapy.Field()
    sales_rank = scrapy.Field()
    load_timestamp = scrapy.Field()
    product_url = scrapy.Field()
