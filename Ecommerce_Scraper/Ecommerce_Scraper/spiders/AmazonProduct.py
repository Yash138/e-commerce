import scrapy


class AmazonProductSpider(scrapy.Spider):
    name = "AmazonProduct"
    allowed_domains = ["www.amazon.in"]
    start_urls = ["https://www.amazon.in/Command-Small-Picture-Hanging-Strip/dp/B00016ZLDY/ref=zg_bs_g_7124091031_d_sccl_7/261-2959224-6540156?psc=1"]

    def parse(self, response):
        pass
