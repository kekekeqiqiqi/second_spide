# -*- coding: utf-8 -*-
import scrapy
from scrapy import Selector
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class SecondScrapSpider(CrawlSpider):
    name = 'second_scrap'
    allowed_domains = ['wxapp-union.com']
    start_urls = ['http://www.wxapp-union.com/portal.php?mod=list&catid=2&page=1']

    rules = (
        Rule(LinkExtractor(allow=r'mod=list&catid=2&page=(.*?)'),follow=True),
        Rule(LinkExtractor(allow=r'article-(.*?)-1.html'),callback="parse_item",follow=False)

    )

    def parse_item(self, response):

        print("*"*40)
        # print(response)
        # Selector(response).xpath('//div[@id="resultListning"]').extract()
        title=response.xpath("//h1[@class='ph']/text()").get()
        print(title)
        print("*"*40)
