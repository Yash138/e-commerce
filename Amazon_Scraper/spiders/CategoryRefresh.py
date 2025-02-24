import scrapy
from scrapy.http import Request


class CategoryrefreshSpider(scrapy.Spider):
    name = "CategoryRefresh"
    allowed_domains = ["www.amazon.in"]
    start_urls = ["https://www.amazon.in/gp/bestsellers/"]

    def parse(self, response):
        for bs in response.xpath('//div[contains(@id, "CardInstance")]/div/ul/li/div/a'):
            yield Request(
                    url=response.urljoin(bs.xpath("@href").get()),
                    callback=self.parse_item,
                    meta={"bs_category": bs.xpath("text()").get()}
                )
        
    def parse_item(self, response):
        bs_category = response.meta.get("bs_category")
        for category in response.xpath("//div[@role='treeitem']/a"):
            yield {
                'bs_category': bs_category,
                "category": category.xpath("text()").get(),
                "url": response.urljoin(category.xpath("@href").get()),
            }