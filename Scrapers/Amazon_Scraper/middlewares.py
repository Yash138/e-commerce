# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from datetime import datetime
# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class AmazonScraperSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class AmazonScraperDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)

import random

class ProxyMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self.username = settings.get('PROXY_USER')
        self.password = settings.get('PROXY_PASSWORD')
        self.url = settings.get('PROXY_URL')
        self.ports = settings.get('PROXY_PORTS')

    def process_request(self, request, spider):
        host = f'https://{self.username}:{self.password}@{self.url}:{random.choice(self.ports)}'
        request.meta['proxy'] = host


class RandomizedProxyMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        # full list of your sticky sessions
        self.username = settings.get('PROXY_USER')
        self.password = settings.get('PROXY_PASSWORD')
        self.url = settings.get('PROXY_URL')
        self.ports = settings.get('PROXY_PORTS')
        self.sessions = [f'https://{self.username}:{self.password}@{self.url}:{port}' for port in self.ports]

        # pool will hold the next “cycle” of sessions in random order
        self._reset_pool()

    def _reset_pool(self):
        # copy & shuffle to get a new random order each cycle
        self.pool = self.sessions.copy()
        random.shuffle(self.pool)

    def process_request(self, request, spider):
        # refill & reshuffle when we’ve used up the current pool
        if not self.pool:
            self._reset_pool()

        # pop off one session endpoint
        proxy = self.pool.pop()

        # optionally, with a small probability, pick a completely random one:
        # if random.random() < 0.1:
        #     proxy = random.choice(self.sessions)

        request.meta['proxy'] = proxy


class HeaderRotationMiddleware:
    def __init__(self, group1, group2):
        self.group1 = group1
        self.group2 = group2

    @classmethod
    def from_crawler(cls, crawler):
        group1 = crawler.settings.get('REAL_BROWSER_HEADERS_GROUP_1')
        group2 = crawler.settings.get('REAL_BROWSER_HEADERS_GROUP_2')
        return cls(group1, group2)

    def process_request(self, request, spider):
        # Pick a random header template
        day = datetime.now().weekday()  # Monday=0, Sunday=6
        headers = random.choice(self.group1 if day % 2 != 0 else self.group2)
        # headers = random.choice(self.group1 + self.group2)
        
        selected_headers = headers.copy()
        # Apply each header from the template to the request
        # Ensure User-Agent is NOT set here (handled by scrapy_user_agents)
        # And Cookies are handled by CookiesMiddleware.
        for header_name, header_value in selected_headers.items():
            if header_name.lower() not in ['user-agent', 'cookie']:
                # Using setdefault to only add if the header isn't already present.
                # If you want to force these headers to overwrite any existing ones,
                # use: request.headers[header_name] = header_value
                request.headers.setdefault(header_name, header_value)
        # Log a summary of the applied headers
        # applied = {
        #     k.decode(): b", ".join(v).decode() if isinstance(v, list) else v.decode()
        #     for k, v in request.headers.items()
        # }
        # spider.log(f"[HeaderMiddleware] Applied headers: {applied}")