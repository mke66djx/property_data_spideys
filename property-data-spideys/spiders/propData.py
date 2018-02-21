import scrapy
from items.py import PropertydatascraperItem
from scrapy.spiders import CSVFeedSpider
import re
#Sample Pin: 6805000260

class pierceCountyScraper(CSVFeedSpider):
    name = "pierce_county_spider"
    start_urls = [ "file:///home/edit/GruntJS/propertyDataScraper/ParcelsLists/parcels.csv"]

    def parse_row(self,response,row):
        pin = row['parcel']
        return scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/summary.cfm?parcel='+pin, callback=self.parse_pin)
        #summary_request = scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/land.cfm?parcel='+pin, callback=self.parse_pin)
        #lands_request = scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/land.cfm?parcel='+pin, callback=self.parse_pin)
        #buildings_request = scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/buildings.cfm?parcel='+pin, callback=self.parse_pin)
        #taxdata_request = scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/taxvalue.cfm?parcel='+pin, callback=self.parse_pin)
        #sales_request = scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/sales.cfm?parcel='+pin, callback=self.parse_pin)

    # def extractItem(self, response, suffix, inner_part=''):
    #     ext = response.xpath('//*[@id="ContentPlaceHolder1_{}"]{}/text()'.format(suffix, inner_part))
    #     if len(ext) == 1:
    #         return ext[0].extract()
    #     else:
    #         return None


    def parse_pin(self, response):
        print("Response:", response.url)
        item = PropertydatascraperItem()
        text=response.xpath('//*[@id="customContent"]/table/tbody/tr[1]/td/table[2]/tbody/tr/td[1]/table/tbody/tr[2]/td[1]/strong/a/text()').extract_first()
        #text =response.xpath('//*[@id="customContent"]/table/tbody/tr[1]/td/table[2]/tbody/tr/td[1]/table/tbody/tr[3]/td[2]').extract()
        print("Text",text)
        #item['site_address'] = self.extractItem(response, 'PropertyInfo_propertyAddress')
        # print(item['site_address'])
        yield item
