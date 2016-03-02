# Scrapy settings for twly_crawler project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#
import os
import sys
from os.path import dirname


# add python path for crawler_lib
_PROJECT_PATH = dirname(dirname(dirname(__file__)))
sys.path.append(os.path.join(_PROJECT_PATH, 'crawler'))

BOT_NAME = 'bills_crawler'

SPIDER_MODULES = ['bills_crawler.spiders']
NEWSPIDER_MODULE = 'bills_crawler.spiders'
#COOKIES_ENABLED = True
LOG_FILE = 'log.txt'
# for develop
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
ITEM_PIPELINES = {
    'bills_crawler.pipelines.DuplicatesPipeline': 300,
}

FEED_EXPORTERS = {
    'json': 'crawler_lib.misc.UnicodeJsonItemExporter',
}
