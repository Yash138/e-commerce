import scrapy
from Ecommerce_Scraper.items import AmazonProductMongoItem, AmazonProductItem
from Ecommerce_Scraper.utility import getUrlToScrap
from datetime import datetime as dt

# class AmazonProductSpider(scrapy.Spider):
#     name = "AmazonProduct"
#     allowed_domains = ["www.amazon.in"]
#     collection_name = 'stg_amz__product_details'
#     # start_urls = ["https://www.amazon.in/dp/B091V8HK8Z"]

#     def start_requests(self):
#         for url in getUrlToScrap():
#             yield scrapy.Request(url['productUrl'], callback=self.parse)
    
#     def parse(self, response):
#         item = AmazonProductMongoItem()
#         # item['asin'] = response.xpath(
#         #         '//span[contains(text(), "ASIN")]/../span[2]/text() | '
#         #         '//th[contains(text(), "ASIN")]/../td/text() | '
#         #         '//div[contains(@id, "titleblock") or contains(@id, "bylineInfo")]/@data-csa-c-asin)'
#         #     ).get()
#         item['asin'] = response.url.split('/dp/')[-1]
#         item['productName'] = response.xpath('//*[@id="productTitle"]/text()').get()
#         item['productUrl'] = response.url
#         item['rating'] = response.xpath('//*[@id="averageCustomerReviews"]/span[1]/span[1]/span[1]/a/span/text()').get()
#         item['reviewsCount'] = response.xpath('//*[@id="averageCustomerReviews"]/span[3]/a/span/text()').get()
#         item['sellerName'] = response.xpath(
#                 '//*[@id="bylineInfo_feature_div"]/div[1]/a/text() | '
#                 '//*[@id="bylineInfo_feature_div"]/div[1]/span/a/text()'
#             ).get()
#         item['reviewsUrl'] = response.urljoin(response.xpath('//*[contains(@class, "FocalReviews")]/div/div/div[4]/div[2]/a/@href').get())
#         item['sellerStoreUrl'] = response.urljoin(response.xpath(
#                 '//*[@id="bylineInfo_feature_div"]/div[1]/a/@href | '
#                 '//*[@id="bylineInfo_feature_div"]/div[1]/span/a/@href'
#             ).get())
#         item['lastMonthSale'] = response.xpath('//*[@id="social-proofing-faceout-title-tk_bought"]/span/text()').get()
#         item['sellPrice'] = response.xpath(
#                 '//*[contains(@id, "corePriceDisplay")]/div[1]/span[3]/span[2]/span[2]/text() | '
#                 '//*[contains(@id, "corePrice")]/div/div/span[1]/span[1]/text() | '
#                 '//*[contains(@id, "corePrice")]/div/div/span[1]/span[2]/span[2]/text()'
#             ).get()
#         item['sellMrp'] = response.xpath('//span[contains(text(), "M.R.P.")]/text()').get()
#         item['lowestCategory'] = response.xpath(
#                 '//th[contains(text(), "Best Sellers Rank")]/following-sibling::td/span/span[2]/a/text() | '
#                 '//span[contains(text(), "Best Sellers Rank")]/../ul/li/span/a/text()'
#             ).get()
#         item['lowestCategoryUrl'] = response.urljoin(response.xpath(
#                 '//th[contains(text(), "Best Sellers Rank")]/following-sibling::td/span/span[2]/a/@href | '
#                 '//span[contains(text(), "Best Sellers Rank")]/../ul/li/span/a/@href'
#             ).get())
#         item['launchDate'] = response.xpath(
#                 '//th[contains(text(), "Date First Available")]/../td/text() | '
#                 '//span[contains(text(), "Date First Available")]/../span[2]/text()'
#             ).get()
        
#         yield item


class AmazonProductSpider(scrapy.Spider):
    name = "AmazonProduct"
    allowed_domains = ["www.amazon.in"]
    stg_table_name = 'staging.stg_amz__product_details'
    trf_table_name = 'transformed.trf_amz__product_details'
    custom_settings = {
        "ITEM_PIPELINES" : {
            "Ecommerce_Scraper.pipelines.AmazonProductStagePipeline": 200,
            "Ecommerce_Scraper.pipelines.AmazonProductTransformPipeline": 300
        },
        "DOWNLOADER_MIDDLEWARES" : {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
        }
    }
    # start_urls = ["https://www.amazon.in/dp/B091V8HK8Z"]

    def start_requests(self):
        for url in getUrlToScrap('postgres'):
            yield scrapy.Request(url['product_url'], callback=self.parse)     # , meta={'dont_redirect':True, "handle_httpstatus_list": [301]}
    
    def parse(self, response):
        item = AmazonProductItem()
        item['asin'] = response.url.split('/dp/')[-1]
        item['product_name'] = response.xpath('//*[@id="productTitle"]/text()').get()
        item['product_url'] = response.url
        item['rating'] = response.xpath('//*[@id="averageCustomerReviews"]/span[1]/span[1]/span[1]/a/span/text()').get()
        item['reviews_count'] = response.xpath('//*[@id="averageCustomerReviews"]/span[3]/a/span/text()').get()
        item['seller_name'] = response.xpath(
                '//*[@id="bylineInfo_feature_div"]/div[1]/a/text() | '
                '//*[@id="bylineInfo_feature_div"]/div[1]/span/a/text()'
            ).get()
        item['reviews_url'] = response.urljoin(response.xpath('//*[contains(@class, "FocalReviews")]/div/div/div[4]/div[2]/a/@href').get())
        item['seller_store_url'] = response.urljoin(response.xpath(
                '//*[@id="bylineInfo_feature_div"]/div[1]/a/@href | '
                '//*[@id="bylineInfo_feature_div"]/div[1]/span/a/@href'
            ).get())
        item['last_month_sale'] = response.xpath('//*[@id="social-proofing-faceout-title-tk_bought"]/span/text()').get()
        item['sell_price'] = response.xpath(
                '//*[contains(@id, "corePriceDisplay")]/div[1]/span[3]/span[2]/span[2]/text() | '
                '//*[contains(@id, "corePrice")]/div/div/span[1]/span[1]/text() | '
                '//*[contains(@id, "corePrice")]/div/div/span[1]/span[2]/span[2]/text()'
            ).get()
        item['sell_mrp'] = response.xpath('//span[contains(text(), "M.R.P.")]/text()').get()
        item['lowest_category'] = response.xpath(
                '//th[contains(text(), "Best Sellers Rank")]/following-sibling::td/span/span[2]/a/text() | '
                '//span[contains(text(), "Best Sellers Rank")]/../ul/li/span/a/text()'
            ).get()
        item['lowest_category_bs_url'] = response.urljoin(response.xpath(
                '//th[contains(text(), "Best Sellers Rank")]/following-sibling::td/span/span[2]/a/@href | '
                '//span[contains(text(), "Best Sellers Rank")]/../ul/li/span/a/@href'
            ).get())
        item['lowest_category_products_url'] = response.urljoin(response.xpath('//*[contains(@id, "wayfinding-breadcrumbs")]/ul/li[last()]/span/a/@href').get())
        item['launch_date'] = response.xpath(
                '//th[contains(text(), "Date First Available")]/../td/text() | '
                '//span[contains(text(), "Date First Available")]/../span[2]/text()'
            ).get()
        item['load_timestamp'] = dt.now()
        yield item
