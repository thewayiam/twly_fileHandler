# -*- coding: utf-8 -*-
from scrapy.exceptions import DropItem

class DuplicatesPipeline(object):

    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if item[u'系統號'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item[u'系統號'])
        else:
            self.ids_seen.add(item[u'系統號'])
            return item
