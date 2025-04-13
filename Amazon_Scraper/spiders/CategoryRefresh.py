import scrapy
from scrapy.http import Request
from scrapy.exceptions import CloseSpider
import json
import os
from Amazon_Scraper.helpers.utils import find_key_path, extract_paths

class CategoryrefreshSpider(scrapy.Spider):
    name = "CategoryRefresh"
    allowed_domains = ["www.amazon.in"]
    start_urls = ["https://www.amazon.in/gp/bestsellers"]
    # start_urls = ["https://www.amazon.in/gp/bestsellers/kitchen/1380441031"]
    custom_settings = {
        "ITEM_PIPELINES":{}
    }
    
    def __init__(self, output_file=None, *args, **kwargs):
        self.items = dict()
        self.visited_url = set()
        # stop the spider if the output file already exists or if the file is not provided
        if output_file and os.path.exists(output_file):
            raise CloseSpider(f"Output file {output_file} already exists. Spider will not run.")
        if not output_file:
            raise CloseSpider("No output file provided. Spider will not run.")

        self.output_file = output_file
        self.logger.info(f"Output file: {self.output_file}")
        
    def parse(self, response):
        # update the bs_category and category_name as per start_url
        bs_category = "bestsellers"
        category_name = "Any Department"
        yield Request(
                url=response.url,
                callback=self.parse_item,
                meta={
                    "bs_category": bs_category,
                    "category_name": category_name
                }
            )
        
    def parse_item(self, response):
        bs_category = response.meta.get("bs_category")
        category_name = response.meta.get("category_name")
        parent_category_id = response.url.split("/")[-1]
        self.items.setdefault(bs_category, {"category_id":parent_category_id, "category_name":category_name})
        
        # avoid adding category if parent is changed
        current_category = response.xpath('//*[contains(text(),"Any Department")]/../following-sibling::div[not(@role)]/div[@role="treeitem"]/a/@href').get()
        if current_category:
            current_category = current_category.split('/ref')[0].split('/')[-1]
            if current_category != response.url.split("/")[-2]:
                tmp = list()
                for i in find_key_path(self.items, parent_category_id):
                    tmp.append(str(i))
                eval(f"""self.items[\"{'"]["'.join(tmp[:-1])}\"]""").pop(tmp[-1])
                return
        
        for category in response.xpath(f'''
            //div[@role="treeitem"]/span[contains(text(),"{category_name}")]/../following-sibling::div[@role="group"]/div[@role="treeitem"]
        '''):
            category_url = response.urljoin(category.xpath("./a/@href").get()).split("/ref")[0]
            category_name = category.xpath("./a/text()").get()
            category_id = category_url.split("/")[-1]
            if category_url not in self.visited_url:
                self.visited_url.add(category_url)
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
                        "category_name": category_name
                    }
                )    
    def closed(self, reason):
        """Save categories to JSON when spider finishes"""
        with open(self.output_file, "w", encoding="utf-8") as f:
            json.dump(self.items, f, indent=4)