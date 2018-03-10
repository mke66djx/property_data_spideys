import scrapy
from scrapy import signals
from property_data_spideys.items import PierceCountyDescriptionItem
from scrapy.spiders import CSVFeedSpider
from scrapy.xlib.pydispatch import dispatcher
from property_data_spideys import pipelines

class pierceCountyScraper(CSVFeedSpider):
    name = "pierce_county_spider"
    start_urls = [ "file:///C:/Users/ebeluli/Desktop/property_data_spideys/ParcelsLists/parcels.csv"]
    #start_urls = [ "file:///home/edit/GruntJS/propertyDataScraper/ParcelsLists/parcels.csv"]
    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        #This will later be passed in as argument by spider caller
        self.pipelineDB_Action = "FullTableUpdate"
        if(self.pipelineDB_Action == "FullTableUpdate"):
            self.pipeline = set([pipelines.PierceFullPipeline])
        elif(self.pipelineDB_Action == "RowUpdate"):
            self.pipeline = set([pipelines.PierceRowPipeline])
        else:
            self.pipeline = None

    def spider_closed(self, spider):
        pass

    def parse_row(self,response,row):
        pin = row['parcel']

        #General summary page, will be used for owner info
        request = scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/summary.cfm?parcel='+pin, callback=self.parse_summary)
        request.meta['item'] = PierceCountyDescriptionItem()
        request.meta['pin'] = pin
        return [request]

    def check_path(self, xpath_return):
        if len(xpath_return) == 1:
            return xpath_return[0]
        else:
            return None

    #Chain data extraction and consolidate into one item
    def parse_summary(self, response):

        parcel = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[1]/table/tr[2]/td[2]/text()').extract())
        site_address = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[1]/table/tr[3]/td[2]/text()').extract())
        mailing_address = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[2]/table/tr[2]/td[2]/text()').extract())\

        item = response.meta['item']
        pin = response.meta['pin']

        item['parcel'] = parcel
        item['owner_name'] = 'NA'
        item['site_address'] = site_address
        item['mailing_address'] = mailing_address
        item['county'] = "Pierce"

        #Tax page will be used for property tax paid/owed and tax assesment
        return [scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/taxvalue.cfm?parcel='+pin, callback=self.parse_taxes,meta={'item': item,'pin':pin})]


    def parse_taxes(self, response):

        tax_year_1 = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[2]/td[1]/text()').extract())
        tax_year_2 = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[3]/td[1]/text()').extract())
        tax_year_3 = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[4]/td[1]/text()').extract())

        tax_year_1_assessed = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[2]/td[4]/text()').extract())
        tax_year_2_assessed = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[3]/td[4]/text()').extract())
        tax_year_3_assessed = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[4]/td[4]/text()').extract())

        current_balance_due = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td[1]/table[1]/tr[4]/td/table/tr[1]/td/table/tr/td[1]/strong/text()').extract())

        item = response.meta['item']
        pin = response.meta['pin']

        item['tax_year_1'] = tax_year_1
        item['tax_year_2'] = tax_year_2
        item['tax_year_3'] = tax_year_3
        item['tax_year_1_assessed'] = tax_year_1_assessed
        item['tax_year_2_assessed'] = tax_year_2_assessed
        item['tax_year_3_assessed'] = tax_year_3_assessed

        item['current_balance_due'] = current_balance_due

        #Land page will give us lot size/square footage and utility types installed or info on the driveway(paved unpaved)(optional)
        return [scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/land.cfm?parcel='+pin, callback=self.parse_land,meta={'item': item,'pin':pin})]


    def parse_land(self, response):
        lot_square_footage = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[1]/td[2]/table/tr[2]/td[2]/text()').extract())
        acres = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[1]/td[2]/table/tr[3]/td[2]/text()').extract())

        electric = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[2]/td[2]/table/tr[2]/td[2]/text()').extract())
        sewer = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[2]/td[2]/table/tr[3]/td[2]/text()').extract())
        water = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[2]/td[2]/table/tr[4]/td[2]/text()').extract())

        item = response.meta['item']
        pin = response.meta['pin']

        item['lot_square_footage'] = lot_square_footage
        item['lot_acres'] = acres

        item['electric'] = electric
        item['sewer'] = sewer
        item['water'] = water


        #Sales page provides sales records if any sales since '99
        return [scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/buildings.cfm?parcel='+pin, callback=self.parse_building,meta={'item': item,'pin':pin})]


    def parse_building(self, response):
        property_type = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[1]/tr[1]/td[2]/text()').extract())
        occupancy = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[1]/tr[5]/td[2]/text()').extract())

        square_footage = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[1]/tr[1]/td[4]/text()').extract())
        attached_garage_footage = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[1]/tr[3]/td[4]/text()').extract())

        year_built = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[2]/text()').extract())
        adj_year_built = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[3]/text()').extract())
        stories = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[5]/text()').extract())
        bedrooms = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[6]/text()').extract())
        baths = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[7]/text()').extract())
        siding_type = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[8]/text()').extract())
        units = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[12]/text()').extract())

        item = response.meta['item']
        pin = response.meta['pin']

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


    def parse_sales(self, response):
        item = response.meta['item']

        sale1_price = 'NA'
        sale1_date = 'NA'

        item['sale_price'] = sale1_price
        item['sale_date'] = sale1_date

        #Sales page provides sales records if any sales since '99
        return [item]

#
# class duvalCountyScraper(CSVFeedSpider):
#     name = "duval_county_spider"
#     start_urls = [ "file:///C:/Users/ebeluli/Desktop/property_data_spideys/ParcelsLists/duval_parcels.csv"]
#     #start_urls = [ "file:///home/edit/GruntJS/propertyDataScraper/ParcelsLists/parcels.csv"]
#
#     def parse_row(self,response,row):
#         pin = row['parcel']
#         #General summary page, will be used for owner info
#         request = scrapy.Request('http://apps.coj.net/PAO_PropertySearch/Basic/Detail.aspx?RE='+pin, callback=self.parse_summary)
#         request.meta['item'] = countyDataItem()
#         request.meta['pin'] = pin
#         return [request]
#
#     def check_path(self, xpath_return):
#         if len(xpath_return) == 1:
#             return xpath_return[0]
#         else:
#             return None
#
#     #Chain data extraction and consolidate into one item
#     def parse_summary(self, response):
#
#         parcel = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblRealEstateNumber"]/text()').extract())
#         owner_name = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterOwnerInformation_ctl00_lblOwnerName"]/text()').extract())
#         mailing_address_street = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterOwnerInformation_ctl00_lblMailingAddressLine1"]/text()').extract())
#         mailing_address_cityzip = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterOwnerInformation_ctl00_lblMailingAddressLine3"]/text()').extract())
#
#         site_address_street = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblPrimarySiteAddressLine1"]/text()').extract())
#         site_address_cityzip = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblPrimarySiteAddressLine2"]/text()').extract())
#
#         property_use = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblPropertyUse"]/text()').extract())
#         year_built = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_lblYearBuilt"]/text()').extract())
#         stories = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingAttributes"]/tr[2]/td[2]/text()').extract())
#         bedrooms = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingAttributes"]/tr[3]/td[2]/text()').extract())
#         bathrooms = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingAttributes"]/tr[4]/td[2]/text()').extract())
#         units = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingAttributes"]/tr[5]/td[2]/text()').extract())
#         total_heated_sqaure_footage = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingArea"]/tr[7]/td[3]/text()').extract())
#
#         heating_type = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingElements"]/tr[9]/td[3]/text()').extract())
#         ac_type = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingElements"]/tr[10]/td[3]/text()').extract())
#
#         tax_market_value_year1 = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblJustMarketValueCertified"]/text()').extract())
#         tax_market_value_year2 = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblJustMarketValueInProgress"]/text()').extract())
#
#         sale1_date = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblHeaderInProgress"]/text()').extract())
#         sale2_date = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblHeaderCertified"]/text()').extract())
#
#         sale1_price = self.check_path(response.xpath('//*[@id="ctl00_cphBody_gridSalesHistory"]/tr[2]/td[3]/text()').extract())
#         sale2_price = self.check_path(response.xpath('//*[@id="ctl00_cphBody_gridSalesHistory"]/tr[3]/td[3]/text()').extract())
#
#         total_square_footage = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblTotalArea1"]/text()').extract())
#
#         item = response.meta['item']
#
#         item['parcel'] = parcel
#         item['owner_name'] = owner_name
#         item['county'] = 'Duval'
#         item['site_address'] = site_address_street + site_address_cityzip
#         item['mailing_address'] = mailing_address_street + mailing_address_cityzip
#         item['property_type'] = property_use
#         item['occupancy'] = 'NA'
#         item['building_square_footage'] = total_heated_sqaure_footage
#         item['attached_garage_footage'] = 'NA'
#         item['year_built'] = year_built
#         item['adj_year_built'] = 'NA'
#         item['stories'] = stories
#         item['bedrooms'] = bedrooms
#         item['baths'] = bathrooms
#         item['siding_type'] = 'NA'
#         item['units'] = units
#         item['sale1_price'] = sale1_price
#         item['sale1_date'] = sale1_date
#         item['sale2_price'] = sale2_price
#         item['sale2_date'] = sale2_date
#         item['tax_year_1'] = tax_market_value_year1
#         item['tax_year_2'] = tax_market_value_year2
#         item['tax_year_3'] = 'NA'
#         item['tax_year_1_assessed'] = tax_market_value_year1
#         item['tax_year_2_assessed'] = tax_market_value_year2
#         item['tax_year_3_assessed'] = 'NA'
#         item['lot_square_footage'] = total_square_footage
#         item['lot_acres'] = 'NA'
#         item['electric'] = 'NA'
#         item['sewer'] = 'NA'
#         item['water'] = 'NA'
#
#         #Need to get taxes owed
#         #http://fl-duval-taxcollector.publicaccessnow.com/propertytaxsearch/accountdetail.aspx?p=019089-0000
#
#         #Tax page will be used for property tax paid/owed and tax assesment
#         yield item

