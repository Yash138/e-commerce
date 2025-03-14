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

class AmazonProductItem(scrapy.Item):
    asin = scrapy.Field()
    category = scrapy.Field()
    lowest_category = scrapy.Field()
    product_name = scrapy.Field()
    seller_id = scrapy.Field()
    seller_name = scrapy.Field()
    brand_name = scrapy.Field()
    last_month_sale = scrapy.Field()
    rating = scrapy.Field()
    reviews_count = scrapy.Field()
    sell_mrp = scrapy.Field()
    sell_price = scrapy.Field()
    launch_date = scrapy.Field()
    is_fba = scrapy.Field()
    is_variant_available = scrapy.Field()
    product_url = scrapy.Field()
    brand_store_url = scrapy.Field()
    scrape_date = scrapy.Field()