from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from Ecommerce_Scraper.spiders.AmazonBestSeller import AmazonBestSellerSpider
from Ecommerce_Scraper.spiders.AmazonProduct import AmazonProductSpider

def run_spiders():
    process = CrawlerProcess(get_project_settings())

    # First spider - Best Sellers
    process.crawl(AmazonBestSellerSpider)

    # Second spider - Product Details
    process.crawl(AmazonProductSpider)

    # Start the process
    process.start()

if __name__ == "__main__":
    run_spiders()
