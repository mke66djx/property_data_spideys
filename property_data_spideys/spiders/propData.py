import scrapy
from property_data_spideys.items import pierceCountyItem
from scrapy.spiders import CSVFeedSpider
import re
#Sample Pin: 6805000260

class pierceCountyScraper(CSVFeedSpider):
    name = "pierce_county_spider"
    start_urls = [ "file:///home/edit/GruntJS/propertyDataScraper/ParcelsLists/parcels.csv"]

    def parse_row(self,response,row):
        pin = row['parcel']

        #General summary page, will be used for owner info
        request = scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/summary.cfm?parcel='+pin, callback=self.parse_summary)
        request.meta['item'] = pierceCountyItem()
        request.meta['pin'] = pin
        return [request]


    def check_data(self, xpath_return):
        if len(xpath_return) == 1:
            return xpath_return[0]
        else:
            return None

    #Chain data extraction and consolidate into one item
    def parse_summary(self, response):

        parcel = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[1]/table/tr[2]/td[2]/text()').extract())
        site_address = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[1]/table/tr[3]/td[2]/text()').extract())
        mailing_address = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[2]/table/tr[2]/td[2]/text()').extract())\

        item = response.meta['item']
        pin = response.meta['pin']

        item['parcel'] = parcel
        item['site_address'] = site_address
        item['mailing_address'] = mailing_address



        #Tax page will be used for property tax paid/owed and tax assesment
        return [scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/taxvalue.cfm?parcel='+pin, callback=self.parse_taxes,meta={'item': item,'pin':pin})]


    def parse_taxes(self, response):

        tax_year_1 = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[2]/td[1]/text()').extract())
        tax_year_2 = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[3]/td[1]/text()').extract())
        tax_year_3 = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[4]/td[1]/text()').extract())

        tax_year_1_assessed = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[2]/td[4]/text()').extract())
        tax_year_2_assessed = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[3]/td[4]/text()').extract())
        tax_year_3_assessed = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[4]/td[4]/text()').extract())

        current_balance_due = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td[1]/table[1]/tr[4]/td/table/tr[1]/td/table/tr/td[1]/strong/text()').extract())

        item = response.meta['item']
        pin = response.meta['pin']

        item['tax_year_1'] = tax_year_1
        item['tax_year_2'] = tax_year_2
        item['tax_year_3'] = tax_year_3
        item['tax_year_1_assessed'] = tax_year_1_assessed
        item['tax_year_2_assessed'] = tax_year_2_assessed
        item['tax_year_3_assessed'] = tax_year_3_assessed

        item['current_balance_due'] = current_balance_due
        print("Printing#########")
        #Land page will give us lot size/square footage and utility types installed or info on the driveway(paved unpaved)(optional)
        return [scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/land.cfm?parcel='+pin, callback=self.parse_land,meta={'item': item,'pin':pin})]


    def parse_land(self, response):
        print("Printing######### __LAND")
        square_footage = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[1]/td[2]/table/tr[2]/td[2]/text()').extract())
        acres = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[1]/td[2]/table/tr[3]/td[2]/text()').extract())

        electric = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[2]/td[2]/table/tr[2]/td[2]/text()').extract())
        sewer = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[2]/td[2]/table/tr[3]/td[2]/text()').extract())
        water = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[2]/td[2]/table/tr[4]/td[2]/text()').extract())

        item = response.meta['item']
        pin = response.meta['pin']

        item['lot_square_footage'] = square_footage
        item['lot_acres'] = acres

        item['electric'] = electric
        item['sewer'] = sewer
        item['water'] = water


        #Sales page provides sales records if any sales since '99
        return [scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/buildings.cfm?parcel='+pin, callback=self.parse_building,meta={'item': item,'pin':pin})]


    def parse_building(self, response):
        print("Printing#########__building")

        property_type = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[1]/tr[1]/td[2]/text()').extract())
        occupancy = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[1]/tr[5]/td[2]/text()').extract())

        square_footage = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[1]/tr[1]/td[4]/text()').extract())
        attached_garage_footage = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[1]/tr[3]/td[4]/text()').extract())

        year_built = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[2]/text()').extract())
        adj_year_built = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[3]/text()').extract())
        stories = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[5]/text()').extract())
        bedrooms = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[6]/text()').extract())
        baths = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[7]/text()').extract())
        siding_type = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[8]/text()').extract())
        units = self.check_data(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[12]/text()').extract())

        item = response.meta['item']
        pin = response.meta['pin']

        print("Printing#########", property_type,occupancy,square_footage)

        item['property_type'] = property_type
        item['occupancy'] = occupancy
        item['building_square_footage'] = square_footage
        item['attached_garage_footage'] = attached_garage_footage
        item['year_built'] = year_built
        item['adj_year_built'] = adj_year_built
        item['stories'] = stories
        item['bedrooms'] = bedrooms
        item['baths'] = baths
        item['siding_type'] = siding_type
        item['units'] = units

        #Sales page provides sales records if any sales since '99
        return [scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/sales.cfm?parcel='+pin, callback=self.parse_sales,meta={'item': item,'pin':pin})]


    def parse_land(self, response):
        print("Printing#########__land")
        item = response.meta['item']

        sale1_price = 'NA'
        sale1_date = 'NA'
        sale2_price = 'NA'
        sale2_date = 'NA'

        item['sale1_price'] = sale1_price
        item['sale1_date'] = sale1_date
        item['sale2_price'] = sale2_price
        item['sale2_date'] = sale2_date

        #Sales page provides sales records if any sales since '99
        return [item]
