import scrapy
from scrapy.http import Request
import json


class CategoryrefreshSpider(scrapy.Spider):
    name = "CategoryRefresh"
    allowed_domains = ["www.amazon.in"]
    start_urls = ["https://www.amazon.in/gp/bestsellers"]
    
    def __init__(self, *args, **kwargs):
        self.items = dict()
        self.visited_url = set()
        
    def parse(self, response):
        # for bs in response.xpath('//div[contains(@id, "CardInstance")]/div/ul/li/div/a'):
        #     url = response.urljoin(bs.xpath("@href").get())
        #     bs_category = url.split("/")[-1]
        #     yield Request(
        #             url=url,
        #             callback=self.parse_item,
        #             meta={"bs_category": bs_category}
        #         )
        bs_category = response.url.split("/")[-1]
        yield Request(
                url=response.url,
                callback=self.parse_item,
                meta={"bs_category": bs_category}
            )
        
    def parse_item(self, response):
        flg = 0
        bs_category = response.meta.get("bs_category")
        self.items.setdefault(bs_category, {})
        for category in response.xpath("//div[@role='treeitem']"):
            category_url = response.urljoin(category.xpath("./a/@href").get())
            category_name = category.xpath("./a/text()").get()
            category_id = category_url.split("/")[-1]
            parent_category_id = category_url.split("/")[-2]
            if category_url not in self.visited_url:
                flg = 1
                self.visited_url.add(category_url)
                parent_category_id = parent_category_id if parent_category_id != bs_category else category_id
                self.items[bs_category].setdefault(parent_category_id, [])
                self.items[bs_category][parent_category_id].append({
                    'category_id': category_id,
                    "category_name": category_name})
                yield Request(
                    url=category_url,
                    callback=self.parse_item,
                    meta={"bs_category":bs_category}
                )    
            elif not category_name:
                # we will not get the category_name and category_url:
                # either it is the category one-level-up or
                # we are at lowest level of category
                pass
        self.log(f"###################Visited################### : {response.url}")
    def closed(self, reason):
        """Save categories to JSON when spider finishes"""
        with open("categories_mapping.json", "w", encoding="utf-8") as f:
            json.dump(self.items, f, indent=4)
        self.log("Saved all categories in categories.json")
    
    # def parse_item(self, response):
    #     bs_category = response.meta.get("bs_category")
    #     for category in response.xpath("//div[@role='treeitem']/a"):
    #         yield {
    #             'bs_category': bs_category,
    #             "category": category.xpath("text()").get(),
    #             "url": response.urljoin(category.xpath("@href").get()),
    #         }