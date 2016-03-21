# -*- coding: utf-8 -*-
import re
import urllib
import urllib2
from urlparse import urljoin
import scrapy
from scrapy.http import Request, FormRequest
from scrapy.selector import Selector


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
        self.ad = ad

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
        yield Request(urljoin(response.url, href[-1]), callback=self.parse_law_bill_list, dont_filter=True, meta={'page': 1})

    def parse_law_bill_list(self, response):
        nodes = response.xpath('//a[@class="link02"]')
        for node in nodes[1::2]:
            href = node.xpath('@href').extract_first()
            yield Request(urljoin(response.url, href), callback=self.parse_law_bill, dont_filter=True)
        page = response.request.meta['page']
        href = response.xpath('//a[@class="linkpage" and re:test(normalize-space(text()), "%d")]/@href' % (page+1)).extract_first()
        if href:
            yield Request(urljoin(response.url, href), callback=self.parse_law_bill_list, dont_filter=True, meta={'page': (page+1)})

    def parse_law_bill(self, response):
        trs = response.xpath('//tr[@class="rectr"]')
        item = {tr.xpath('td[1]/nobr/text()').extract_first(): first_or_list(tr.xpath('td[1]/nobr/text()').extract_first(), tr.xpath('td[2]//text()').extract()) for tr in trs}
        item.pop(u"關係文書", None) # this one not proper info, parse below
        has_motions = response.xpath(u'//img[@src="/lylegis/images/ref4.png"]/parent::a/@href').extract_first()
        item['links'] = {
            u'關係文書': urljoin(response.url, '/lgcgi/lgmeetimage?%s' % response.xpath(u'//img[@src="/lylgmeet/img/view.png"]/parent::a/@href').extract_first().split('^')[-1]),
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
