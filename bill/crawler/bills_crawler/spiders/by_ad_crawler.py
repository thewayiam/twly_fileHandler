# -*- coding: utf-8 -*-
from time import sleep
from random import randint
import re
import urllib
import urllib2
from urlparse import urljoin
import scrapy
from scrapy.http import Request, FormRequest
from scrapy.selector import Selector

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pyvirtualdisplay import Display


def first_or_list(key, data):
    data = [x.strip() for x in data if x.strip().strip(';')]
    if key in [u'主提案', u'連署提案', u'主題', u'類別']:
        return data
    return data[0].strip() if len(data) == 1 else data

class Spider(scrapy.Spider):
    name = "lis_by_ad"
    allowed_domains = ["lis.ly.gov.tw"]
    start_urls = [
        "http://lis.ly.gov.tw/lylgmeetc/lgmeetkm_lgmem",
    ]
    download_delay = 1

    def __init__(self, ad=None, *args, **kwargs):
        super(Spider, self).__init__(*args, **kwargs)
        self.display = Display(visible=0, size=(800, 600))
        self.display.start()
        self.driver = webdriver.Chrome("/var/chromedriver/chromedriver")
        self.ad = ad

    def spider_closed(self, spider):
        self.display.close()

    def parse(self, response):
        yield FormRequest.from_response(
            response,
            formdata={
                '_20_8_T': str(self.ad).zfill(2),
                'INFO': response.xpath('//input[@name="INFO"]/@value').extract_first()
            },
            callback=self.parse_max_per_page
        )

    def parse_max_per_page(self, response):
        href = response.xpath('//select[@onchange="instback(this)"]/option[re:test(text(), "^\d+$")]/@value').extract()
        yield Request(urljoin(response.url, href[-1]), callback=self.parse_law_bill_list, dont_filter=True)

    def parse_law_bill_list(self, response):
        self.driver.get(response.url)
        while (True):
            try:
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "block30"))
                )
            except:
                continue
            sleep(randint(1, 2))
            nodes = Selector(text=self.driver.page_source).xpath('//a[@class="link02"]')
            for node in nodes[1::2]:
                href = node.xpath('@href').extract_first()
                yield Request(urljoin(response.url, href), callback=self.parse_law_bill, dont_filter=True)
            try:
                next_page = self.driver.find_element_by_xpath('//input[@name="_IMG_次頁"]')
                next_page.click()
            except:
                break
        self.driver.close()

    def parse_law_bill(self, response):
        trs = response.xpath('//tr[@class="rectr"]')
        item = {tr.xpath('td[1]/nobr/text()').extract_first(): first_or_list(tr.xpath('td[1]/nobr/text()').extract_first(), tr.xpath('td[2]//text()').extract()) for tr in trs}
        item.pop(u"關係文書", None) # this one not proper info, parse below
        has_motions = response.xpath(u'//img[@src="/lylegis/images/ref4.png"]/parent::a/@href').extract_first()
        bill_ref_pdf = response.xpath(u'//img[@src="/lylgmeet/img/view.png"]/parent::a/@href').extract_first()
        bill_ref_doc = response.xpath(u'//img[@src="/lylgmeet/img/doc_icon.png"]/parent::a/@href').extract_first()
        if bill_ref_pdf:
            bill_ref = urljoin(response.url, '/lgcgi/lgmeetimage?%s' % bill_ref_pdf.split('^')[-1])
        elif bill_ref_doc:
            bill_ref = urljoin(response.url, bill_ref_doc)
        else:
            bill_ref = ''
        item['links'] = {
            u'關係文書': bill_ref,
            u'審議進度': urljoin(response.url, has_motions) if has_motions else None
        }
        if has_motions:
            yield Request(item['links'][u'審議進度'], callback=self.parse_law_bill_motions, dont_filter=True, meta={'item': item})
        else:
            item['motions'] = []
            yield item

    def parse_law_bill_motions(self, response):
        item = response.request.meta['item']
        motions = []
        for node in response.xpath('//tr[@class="onetr0"]/parent::table'):
            motion = {}
            for tr in node.xpath('.//tr[@class="onetr1"]'):
                motion[tr.xpath('td[1]/text()').extract_first()] = first_or_list(tr.xpath('td[2]//text()').extract())
            motion.pop(u"影像", None)
            motions.append(motion)
        item['motions'] = motions
        yield item
