# -*- coding: utf-8 -*-
import os
import re
import codecs
import subprocess
from urlparse import urljoin
import requests
from bs4 import BeautifulSoup
import scrapy


class Spider(scrapy.Spider):
    name = "lci"
    allowed_domains = ["lci.ly.gov.tw/"]
    start_urls = ['http://lci.ly.gov.tw/LyLCEW/lcivAgendarecMore.action']
    download_delay = 1

    def parse(self, response):
        for tr in response.xpath('//tr[re:test(@id, "searchResult_\d+")]'):
            pdf_onclick = tr.xpath('.//input[@value="PDF"]/@onclick').extract()[0]
            pdf_path = pdf_onclick.lstrip("window.open('").rstrip("')")
            item = {
                "category": re.sub('\s', '', tr.xpath('td[2]/text()').extract()[0]),
                "name": re.sub('\s', '', tr.xpath('td[3]/span[1]/text()').extract()[0]),
                "dates": re.sub('\s', '', tr.xpath('td[4]/text()').extract()[0]).split(','),
                "links": {
                    "pdf": urljoin(response.url, pdf_path),
                    "html": 'http://lci.ly.gov.tw/LyLCEW/html/%s' % re.sub('pdf$', 'htm', re.sub('/pdf', '', pdf_path))
                }
            }
            yield item
            op = 'meeting_minutes/%s.txt' % item['name']
            if not os.path.exists(op):
                yield scrapy.Request(item['links']['html'], meta={'op': op}, callback=self.html2txt, dont_filter=True)

    def html2txt(self, response):
        r = requests.get(response.url)
        r.encoding = 'big5'
        soup = BeautifulSoup(r.text, 'lxml')
        with codecs.open('t.txt', 'w', encoding='utf-8') as f:
            f.write(soup.get_text())
#       cmd = u"sed -n '/議事錄/,/allans=0/p' t.txt | tr '傅萁' '傅崐萁' | grep . > %s" % response.request.meta['op']
        cmd = u"sed -n '/議事錄/,$p' t.txt | grep . > %s" % response.request.meta['op']
        subprocess.call(cmd, shell=True)
        ret = subprocess.check_output("rm t.txt && wc -l %s" % response.request.meta['op'], shell=True)
        if int(ret.split()[0]) < 3: # transform haven't finished
            subprocess.call("rm %s" % response.request.meta['op'], shell=True)
