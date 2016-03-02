# -*- coding: utf-8 -*-
import re
import urllib
from urlparse import urljoin
import scrapy
from scrapy.http import Request, FormRequest
from scrapy.selector import Selector


def first_or_list(data):
    data = [x.strip() for x in data if x.strip()]
    return data[0].strip() if len(data) == 1 else data

class Spider(scrapy.Spider):
    name = "lis_by_ad"
    allowed_domains = ["lis.ly.gov.tw"]
    start_urls = [
        "http://lis.ly.gov.tw/lylgmeetc/lgmeetkm_lgmem",
    ]
    download_delay = 0.5

    def __init__(self, ad=None, *args, **kwargs):
        super(Spider, self).__init__(*args, **kwargs)
        self.ad = ad

    def parse(self, response):
        yield FormRequest.from_response(
            response,
            formdata={
                '_20_8_T': str(self.ad).zfill(2),
                'INFO': response.xpath('//input[@name="INFO"]/@value').extract_first()
            },
            callback=self.parse_ad
        )

    def parse_ad(self, response):
        length = response.xpath('//td/text()').re(u'委員提案 (\d+) 筆')[0]
        href = response.xpath('//a[@class="linkpage"]/@href').extract_first()
        pages = int(length) / 10
        for page in range(pages+1):
            link = re.sub('01000001', '01%s' % hex(page)[2:].zfill(6).upper(), href)
            yield Request(urljoin(response.url, link), callback=self.parse_law_bill_list, dont_filter=True)

    def parse_law_bill_list(self, response):
        nodes = response.xpath('//a[@class="link02"]')
        for node in nodes[1::2]:
            href = node.xpath('@href').extract_first()
            yield Request(urljoin(response.url, href), callback=self.parse_law_bill, dont_filter=True)

    def parse_law_bill(self, response):
        trs = response.xpath('//tr[@class="rectr"]')
        item = {tr.xpath('td[1]/nobr/text()').extract_first(): first_or_list(tr.xpath('td[2]//text()').extract()) for tr in trs}
        item.pop(u"關係文書", None)
        has_motions = response.xpath(u'//img[@src="/lylegis/images/ref4.png"]/parent::a/@href').extract_first()
        links = {
            u'關係文書': urljoin(response.url, response.xpath(u'//img[@src="/lylegis/images/view.png"]/parent::a/@href').extract_first()),
            u'審議進度': urljoin(response.url, has_motions) if has_motions else None
        }
        item['links'] = links
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
