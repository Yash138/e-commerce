import logging
import os
from logging.handlers import RotatingFileHandler
from scrapy import signals

class SpiderLoggerExtension:
    def __init__(self, max_bytes, backup_count, log_dir, datetime_format, log_format, log_level):
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.log_dir = log_dir
        self.datetime_format = datetime_format
        self.log_format = log_format
        self.log_level = log_level
        self.handler = None

    @classmethod
    def from_crawler(cls, crawler):
        max_bytes = crawler.settings['LOG_ROTATING_MAX_BYTES']
        backup_count = crawler.settings['LOG_ROTATING_BACKUP_COUNT']
        log_dir = crawler.settings['LOG_DIR']
        datetime_format = crawler.settings['DATETIME_FORMAT']
        log_format = crawler.settings['LOG_FORMAT']
        log_level = crawler.settings.get("LOG_LEVEL", "INFO")
        ext = cls(max_bytes, backup_count, log_dir, datetime_format, log_format, log_level)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        # crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_opened(self, spider):
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        log_file = getattr(spider, 'logfile', f"{spider.name}.log")
        self.handler = RotatingFileHandler(
            log_file, 
            maxBytes=self.max_bytes, 
            backupCount=self.backup_count, 
            encoding="utf-8"
        )
        formatter = logging.Formatter(self.log_format, datefmt=self.datetime_format)
        self.handler.setFormatter(formatter)
        self.handler.setLevel(getattr(logging, self.log_level))
        logging.getLogger().addHandler(self.handler)
        self.handler.doRollover()  # <-- Rotate on every spider run!
        spider.logger.info(f"Logging to {log_file} with per-run rotation.")

    # def spider_closed(self, spider):
    #     if self.handler:
    #         logging.getLogger().removeHandler(self.handler)
    #         self.handler.close()
