# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AmazonBestSellerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    asin = scrapy.Field()
    rank = scrapy.Field()
    productName = scrapy.Field()
    category = scrapy.Field()
    subCategory = scrapy.Field()
    productUrl = scrapy.Field()
    loadTimestamp = scrapy.Field()
    
class AmazonProductItem(scrapy.Item):
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


