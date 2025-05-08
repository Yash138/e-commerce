# Scrapy settings for Amazon_Scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "Web Scraper"
SPIDER_MODULES = ["Amazon_Scraper.spiders"]
NEWSPIDER_MODULE = "Amazon_Scraper.spiders"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "Amazon_Scraper (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "Amazon_Scraper.middlewares.AmazonScraperSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    # 'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': 400,
    'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
}

RETRY_ENABLED = True
RETRY_TIMES = 3  # Number of retries for a failed request
RETRY_DELAY = 2  # Initial delay of 2 seconds
RETRY_HTTP_CODES = [500, 502, 503, 504, 408]  # Retry on server-side issues

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
EXTENSIONS = {
    "scrapy.extensions.telnet.TelnetConsole": None,
    "Amazon_Scraper.extensions.custom_logger.SpiderLoggerExtension": 500,
}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
#    "Amazon_Scraper.pipelines.postgres_pipeline.AmzProductRankingStgAsyncPipeline": 300,
    "Amazon_Scraper.pipelines.postgres_pipeline.AmzProductRankingStgSyncPipeline": 300,
    "Amazon_Scraper.pipelines.products_pipeline.AmzProductsPipeline": 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 30
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.5
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# PostgreSQL DB Connection Details
POSTGRES_HOST = 'localhost'
POSTGRES_DATABASE = 'ecommerce'
POSTGRES_USERNAME = 'postgres'
POSTGRES_PASSWORD = 'hsV6.sfi2'
POSTGRES_PORT = '5432'

CATEGORIES = {
        "bestsellers": "bestsellers",
        "movers_and_shakers": "movers-and-shakers",
        "most_wished_for": "most-wished-for",
        "hot_new_releases": "new-releases"
    }
EXCLUDE_CATEGORIES = [
    'Amazon Launchpad','Amazon Renewed','Apps & Games', 'Books', 'Clothing & Accessories', 
    'Gift Cards','Kindle Store','Movies & TV Shows','Music','Musical Instruments', 'Software', 
    'Toys & Games','Video Games','Watches']

EXCLUDE_CATEGORIES_IDS = [
    'boost','amazon-renewed','mobile-apps', 'books', 'apparel', 
    'gift-cards','digital-text','dvd','music','musical-instruments', 'software', 
    'toys','videogames','watches']

# Logger Settings
import os

# Directory for log files
LOG_DIR = os.path.join(os.getcwd(), "logs")

# Maximum size of a log file in bytes (e.g., 10 MB)
LOG_ROTATING_MAX_BYTES = 10 * 1024 * 1024

# Number of backup log files to keep
LOG_ROTATING_BACKUP_COUNT = 5

# Date and time formats
DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FORMAT = '%(asctime)s [%(levelname)s] [%(name)s:%(lineno)d]: %(message)s'

# Directory for temporary files
TEMP_DIR = os.path.join(os.getcwd(), "temp")

# Path for error logs
ERROR_LOG_FILE = os.path.join(LOG_DIR, "error.log")