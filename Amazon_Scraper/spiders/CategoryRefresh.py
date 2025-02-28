import scrapy
from scrapy.http import Request
import json
from Amazon_Scraper.helpers.utils import find_key_path, extract_paths

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
                meta={
                    "bs_category": bs_category,
                    "referer_url": response.url
                }
            )
        
    def parse_item(self, response):
        # flg = 0
        bs_category = response.meta.get("bs_category")
        referer_url = response.meta.get("referer_url")
        main_category_id = response.url.split("/")[-2]
        parent_category_id = response.url.split("/")[-1]
        self.items.setdefault(bs_category, {})
        for category in response.xpath("//div[@role='treeitem']"):
            category_url = response.urljoin(category.xpath("./a/@href").get())
            category_name = category.xpath("./a/text()").get()
            category_id = category_url.split("/")[-1]
            # main_category_id = category_url.split("/")[-2]
            if category_url not in self.visited_url:
                # flg = 1
                self.visited_url.add(category_url)
                if main_category_id == 'gp':
                    self.log("main_category_id is gp")
                    # main_category_id = main_category_id if main_category_id != bs_category else category_id
                    main_category_id = category_id
                    self.items[bs_category].setdefault(main_category_id, {})
                    self.items[bs_category][main_category_id] = {
                        'category_id': category_id,
                        "category_name": category_name
                    }
                else:
                    key = ''
                    for i in find_key_path(self.items, parent_category_id):
                        key += '[' + str(i) + ']' if isinstance(i, int) else '[\'' + i + '\']'
                    eval(f'self.items{key}')[str(category_id)] = {
                        'category_id': category_id,
                        "category_name": category_name
                    }
                yield Request(
                    url=category_url,
                    callback=self.parse_item,
                    meta={
                        "bs_category":bs_category,
                        "referer_url": response.url
                    }
                )    
            elif not category_name:
                # we will not get the category_name and category_url:
                # either it is the category one-level-up or
                # we are at lowest level of category
                pass
        # self.log(f"###################Visited################### : {response.url}")
    def closed(self, reason):
        """Save categories to JSON when spider finishes"""
        with open("categories_mapping.json", "w", encoding="utf-8") as f:
            json.dump(self.items, f, indent=4)