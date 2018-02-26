# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class pierceCountyItem(scrapy.Item):
    parcel = scrapy.Field()
    site_address = scrapy.Field()
