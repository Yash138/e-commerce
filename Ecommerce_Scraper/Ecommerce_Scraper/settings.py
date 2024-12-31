# Scrapy settings for Ecommerce_Scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "Ecommerce_Scraper"

SPIDER_MODULES = ["Ecommerce_Scraper.spiders"]
NEWSPIDER_MODULE = "Ecommerce_Scraper.spiders"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "Ecommerce_Scraper (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 0.5   # Delay of 3 seconds between requests
# RANDOMIZE_DOWNLOAD_DELAY = True  # Add random delays to mimic human behavior
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

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
#    "Ecommerce_Scraper.middlewares.EcommerceScraperSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    # "Ecommerce_Scraper.middlewares.EcommerceScraperDownloaderMiddleware": 543,
#     'Ecommerce_Scraper.middlewares.ExponentialBackoffRetryMiddleware': 550,
# }

RETRY_ENABLED = True
RETRY_TIMES = 3  # Number of retries for a failed request
RETRY_DELAY = 0.25  # Initial delay of 2 seconds
RETRY_HTTP_CODES = [500, 502, 503, 504, 408]  # Retry on server-side issues

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   # "Ecommerce_Scraper.pipelines.AmazonBSStagingMongoPipeline": 300,
   # "Ecommerce_Scraper.pipelines.AmazonProductScraperMongoPipeline": 300,
   
   # "Ecommerce_Scraper.pipelines.AmazonBSStagingPipeline": 300,
   "Ecommerce_Scraper.pipelines.AmazonProductStagePipeline": 300,
   "Ecommerce_Scraper.pipelines.AmazonProductTransformPipeline": 400,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
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

# MongoDB Connection Details
MONGO_URI = "mongodb://localhost:27017"
MONGO_DATABASE = "ecommerce"

# PostgreSQL DB Connection Details
POSTGRES_HOST = 'localhost'
POSTGRES_DATABASE = 'ecommerce'
POSTGRES_USERNAME = 'postgres'
POSTGRES_PASSWORD = 'hsV6.sfi2'
POSTGRES_PORT = '5432'

BEST_SELLER_LOAD_TYPE = 'FULLREFRESH'  # possible values: FULLREFRESH | INCREMENTAL
PRODUCT_LOAD_TYPE = 'INCREMENTAL'  # possible values: FULLREFRESH | INCREMENTAL