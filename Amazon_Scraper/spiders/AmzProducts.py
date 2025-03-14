import scrapy
from Amazon_Scraper.items import AmazonProductItem
from datetime import datetime as dt


class AmzproductsSpider(scrapy.Spider):
    name = "AmzProducts"
    allowed_domains = ["www.amazon.in"]
    stg_table_name = 'staging.stg_amz__product'
    # trf_table_name = 'transformed.trf_amz__product_details'
    custom_settings = {
        "ITEM_PIPELINES" : {
            "Amazon_Scraper.pipelines.products_pipeline.AmzProductsPipeline": 300,
        },
        "DOWNLOADER_MIDDLEWARES" : {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
        }
    }
    start_urls = ["https://www.amazon.in/XMART-INDIA-Drilling-Rectangular-Multipurpose/dp/B0DJYDR2ZN",
                  "https://www.amazon.in/JPS-Stainless-Bathroom-Wall-Mounted-Accessories/dp/B0D51RF652"]

    def __init__(self, batch_size = 1, **kwargs):
        self.batch_size = int(batch_size) 

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Attach Scrapy settings to the spider."""
        spider = cls(*args, **kwargs)
        return spider
    
    # def start_requests(self):
    #     for url in getUrlToScrap('postgres'):
    #         yield scrapy.Request(url['product_url'], callback=self.parse)     # , meta={'dont_redirect':True, "handle_httpstatus_list": [301]}
    
    def parse(self, response):
        item = AmazonProductItem()
        item['asin'] = response.url.split('/dp/')[-1]
        item['category'] = response.xpath('//*[contains(@id,"wayfinding-breadcrumbs")]/ul/li[1]/span/a/text()').get()
        item['lowest_category'] = response.xpath('//*[contains(@id,"wayfinding-breadcrumbs")]/ul/li[last()]/span/a/text()').get()
        item['product_name'] = response.xpath('//*[@id="productTitle"]/text()').get()
        item['seller_id'] = response.xpath('//div[contains(@tabular-attribute-name, "Sold by")]//a/@href').get()
        item['seller_name'] = response.xpath('//div[contains(@tabular-attribute-name, "Sold by")]//a/text()').get()
        item['brand_name'] = response.xpath(
                '//*[@id="bylineInfo_feature_div"]/div[1]/a/text() | '
                '//*[@id="bylineInfo_feature_div"]/div[1]/span/a/text()'
            ).get()        
        item['last_month_sale'] = response.xpath('//*[@id="social-proofing-faceout-title-tk_bought"]/span/text()').get()
        item['rating'] = response.xpath('//*[@id="averageCustomerReviews"]/span[1]/span[1]/span[1]/a/span/text()').get()
        item['reviews_count'] = response.xpath('//*[@id="averageCustomerReviews"]/span[3]/a/span/text()').get()
        item['sell_price'] = response.xpath(
                '//*[contains(@id, "corePriceDisplay")]/div[1]/span[3]/span[2]/span[2]/text() | '
                '//*[contains(@id, "corePrice")]/div/div/span[1]/span[1]/text() | '
                '//*[contains(@id, "corePrice")]/div/div/span[1]/span[2]/span[2]/text()'
            ).get()
        item['sell_mrp'] = response.xpath('//span[contains(text(), "M.R.P.")]/text()').get()
        item['launch_date'] = response.xpath(
                '//th[contains(text(), "Date First Available")]/../td/text() | '
                '//span[contains(text(), "Date First Available")]/../span[2]/text()'
            ).get()
        item['is_fba'] = response.xpath('//div[contains(@tabular-attribute-name, "Ships from")]/div/span[contains(@class, "buybox")]/text()').get()
        item['is_variant_available'] = response.xpath('//div[@data-totalvariationcount]/@data-totalvariationcount').get()
        item['product_url'] = response.url
        item['brand_store_url'] = response.urljoin(response.xpath(
                '//*[@id="bylineInfo_feature_div"]/div[1]/a/@href | '
                '//*[@id="bylineInfo_feature_div"]/div[1]/span/a/@href'
            ).get())
        item['scrape_date'] = dt.now()
        yield item
