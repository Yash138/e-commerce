# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AmazonBestSellerMongoItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    asin = scrapy.Field()
    rank = scrapy.Field()
    productName = scrapy.Field()
    category = scrapy.Field()
    subCategory = scrapy.Field()
    productUrl = scrapy.Field()
    loadTimestamp = scrapy.Field()
    
class AmazonProductMongoItem(scrapy.Item):
    asin = scrapy.Field()
    productName = scrapy.Field()
    lowestCategory = scrapy.Field()
    lastMonthSale = scrapy.Field()
    rating = scrapy.Field()
    reviewsCount = scrapy.Field()
    sellMrp = scrapy.Field()
    sellPrice = scrapy.Field()
    sellerName = scrapy.Field()
    launchDate = scrapy.Field()
    productUrl = scrapy.Field()
    reviewsUrl = scrapy.Field()
    sellerStoreUrl = scrapy.Field()
    lowestCategoryUrl = scrapy.Field()
    loadTimestamp = scrapy.Field()

class AmazonBestSellerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    asin = scrapy.Field()
    rank = scrapy.Field()
    product_name = scrapy.Field()
    category = scrapy.Field()
    sub_category = scrapy.Field()
    product_url = scrapy.Field()
    load_timestamp = scrapy.Field()
    
class AmazonProductItem(scrapy.Item):
    asin = scrapy.Field()
    product_name = scrapy.Field()
    lowest_category = scrapy.Field()
    last_month_sale = scrapy.Field()
    rating = scrapy.Field()
    reviews_count = scrapy.Field()
    sell_mrp = scrapy.Field()
    sell_price = scrapy.Field()
    seller_name = scrapy.Field()
    launch_date = scrapy.Field()
    product_url = scrapy.Field()
    reviews_url = scrapy.Field()
    seller_store_url = scrapy.Field()
    lowest_category_bs_url = scrapy.Field()
    lowest_category_products_url = scrapy.Field()
    load_timestamp = scrapy.Field()
