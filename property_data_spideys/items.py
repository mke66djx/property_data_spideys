# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class PierceCountyDescriptionItem(scrapy.Item):
    owner_name = scrapy.Field()
    parcel = scrapy.Field()
    site_address = scrapy.Field()
    mailing_address = scrapy.Field()
    tax_year_1 = scrapy.Field()
    tax_year_2 = scrapy.Field()
    tax_year_3 = scrapy.Field()
    tax_year_1_assessed = scrapy.Field()
    tax_year_2_assessed = scrapy.Field()
    tax_year_3_assessed = scrapy.Field()
    current_balance_due = scrapy.Field()
    square_footage = scrapy.Field()
    acres = scrapy.Field()
    electric = scrapy.Field()
    sewer = scrapy.Field()
    water = scrapy.Field()
    property_type = scrapy.Field()
    occupancy = scrapy.Field()
    building_square_footage = scrapy.Field()
    lot_square_footage = scrapy.Field()
    lot_acres = scrapy.Field()
    attached_garage_footage = scrapy.Field()
    year_built = scrapy.Field()
    adj_year_built = scrapy.Field()
    stories = scrapy.Field()
    bedrooms = scrapy.Field()
    baths = scrapy.Field()
    siding_type = scrapy.Field()
    units = scrapy.Field()
    sale_price = scrapy.Field()
    sale_date = scrapy.Field()


class DuvalCountyDescriptionItem(scrapy.Item):
    parcel = scrapy.Field()
    owner_name = scrapy.Field()
    site_address = scrapy.Field()
    mailing_address = scrapy.Field()
    property_type = scrapy.Field()
    occupancy = scrapy.Field()
    building_square_footage = scrapy.Field()
    attached_garage_footage = scrapy.Field()
    year_built = scrapy.Field()
    adj_year_built = scrapy.Field()
    stories = scrapy.Field()
    baths = scrapy.Field()
    bedrooms = scrapy.Field()
    siding_type = scrapy.Field()
    units = scrapy.Field()
    sale1_price = scrapy.Field()
    sale1_date = scrapy.Field()
    sale2_price = scrapy.Field()
    sale2_date = scrapy.Field()
    tax_year_1 = scrapy.Field()
    tax_year_2 = scrapy.Field()
    tax_year_3 = scrapy.Field()
    tax_year_1_assessed = scrapy.Field()
    tax_year_2_assessed = scrapy.Field()
    tax_year_3_assessed = scrapy.Field()
    lot_square_footage = scrapy.Field()
    lot_acres = scrapy.Field()
    electric = scrapy.Field()
    sewer = scrapy.Field()
    units = scrapy.Field()
    water = scrapy.Field()


