# -*- coding: utf-8 -*-
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
import json
from yelpCrawl.optutil import OptUtil
# from scrapy.spiders import CrawlSpider, Rule
from scrapy_redis.spiders import RedisCrawlSpider
from yelpCrawl.items import YelpspiderItem


class YelpSpider(RedisCrawlSpider):
    name = 'yelpspider'
    redis_key = "yelpspider:start_urls"

    def __init__(self, *args, **kwargs):
        # Dynamically define the allowed domains list.
        domain = kwargs.pop('domain', '')
        self.allowed_domains = filter(None, domain.split(','))
        super(YelpSpider, self).__init__(*args, **kwargs)
    # 第一级匹配规则，按搜索条件列表
    list_page = LinkExtractor(allow=r"https://www.yelp.com/search\?find_desc=.+")
    # 第二级分页列表，纽约货款公司
    page_link = LinkExtractor(allow=r"https://www.yelp.com/search\?find_desc=.+[&]?[&find_loc=.+]?")
    # 详情页
    # https://www.yelp.com/biz/block-financial-resources-manhattan-2?osq=Mortgage+Company
    # https://www.yelp.com/biz/allegiance-financial-group-astoria?osq=Home+Insurance
    detail = LinkExtractor(allow=r"https://www.yelp.com/biz/\w+-.*?osq=.*")
    rules = (
        Rule(list_page, follow=True),
        Rule(page_link, follow=True),
        Rule(detail, callback='parse_item_detail', follow=False)
    )

    def parse_item_detail(self, response):
        item = YelpspiderItem()
        # item['logo'] = response.meta['_logo']
        item['page_url'] = response.url
        # 公司名
        item['company'] = self.get_company(response)
        item['address'] = self.get_address(response)
        # category
        item['category'] = self.get_category(response)
        # 手机号
        item['phone'] = self.get_phone(response)
        # 公司链接
        item['websiteurl'] = self.get_websiteurl(response)
        # 公司图片,只取一张
        item['img_url'] = self.get_img_url(response)
        # 描述
        item['content'] = self.get_content(response)
        # bussiness_content
        item['business_content'] = self.get_bussiness_content(response)
        # 经纬度
        item['latitude'] = self.get_latitude(response)
        item['longitude'] = self.get_longitude(response)
        yield item

    def get_company(self, response):
        company = response.xpath('//div[@class="top-shelf"]//h1/text()').extract()
        if company:
            return ' '.join(company)
        else:
            return ''

    def get_address(self, response):
        address = response.xpath('//div[@class="top-shelf"]//div[@class="mapbox"]//address/text()').extract()
        if address:
            return address[0].strip()
        else:
            return ''

    def get_category(self, response):
        category = response.xpath('//div[@class="price-category"]/span[@class="category-str-list"]/a/text()').extract()
        if category:
            return category[0].strip()
        else:
            return ''

    def get_phone(self, response):
        phone = response.xpath('//div[@class="top-shelf"]//span[@class="biz-phone"]/text()').extract()
        if phone:
            return phone[0].strip()
        else:
            return ''

    def get_websiteurl(self, response):
        websiteurl = response.xpath('//div[@class="top-shelf"]//span/a[contains(@href, "biz_redir")]/@href').extract()
        if websiteurl:
            websiteurl = websiteurl[0]
            return OptUtil.urlDecoder(websiteurl[websiteurl.index("=") + 1:websiteurl.index("&")])
        else:
             return ''

    def get_img_url(self, response):
        img_url = response.xpath('//a[contains(@href, "/biz_photos")]/img/@src').extract()
        if img_url:
            return img_url[0]
        else:
            return ''

    def get_content(self, response):
        content = response.xpath('//div[contains(@class, "island")]/div[@class="from-biz-owner-content"]/p[position()<=3]').extract()
        if content:
            return ''.join(content).replace("<p>", "").replace("</p>", "").replace("\n", "").replace("\xa0","").replace("<br>", "").strip()
        else:
            return ''

    def get_bussiness_content(self, response):
        business_content = response.xpath('//div[contains(@class, "island")]/div[@class="from-biz-owner-content"]/p[position()>3]').extract()
        if business_content:
            return ''.join(business_content).replace("<p>", "").replace("</p>", "").replace("\n","").replace("\xa0", "").replace("<br>", "").strip()
        else:
            return ''

    def get_location(self, response):
        data_map_state = response.xpath('//div[@class="mapbox-map"]/div/@data-map-state').extract()
        json_result = json.loads(data_map_state[0]) if data_map_state else {}
        markers = json_result.get('markers')[1] if json_result and json_result.get('markers') and len(
            json_result.get('markers')) >= 2 else {}
        if markers and markers.get('location'):
            return markers.get('location')
        else:
            return {}

    def get_latitude(self, response):
        location = self.get_location(response)
        if location and location.get('latitude'):
            return location.get('latitude')
        else:
            return ''

    def get_longitude(self, response):
        location = self.get_location(response)
        if location and location.get('longitude'):
            return location.get('longitude')
        else:
            return ''