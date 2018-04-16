import scrapy
import json
from scrapy import signals
from property_data_spideys.items import PierceCountyDescriptionItem,DuvalCountyDescriptionItem,CookCountyDescriptionItem
from property_data_spideys.items import MaricopaCountyDescriptionItem
from scrapy.spiders import CSVFeedSpider
from scrapy.xlib.pydispatch import dispatcher
from property_data_spideys import pipelines
import locale
import os
import os.path
import re

def getStartUrlFilePath(parcel_file):
    relative_file_path = 'ParcelsLists/'+parcel_file
    filepath = os.path.abspath(os.path.join(os.getcwd(), relative_file_path))
    start_url_path = "file://"+filepath
    return start_url_path

def check_path(xpath_return):
    if len(xpath_return) == 1:
        return xpath_return[0]
    else:
        return 'NA'

def checkIfNa(string,type):
    if string == 'NA' or string == 'N/A' or string == '**' or string == 'n/a' or string == None:
        if type == 'str':
            return 'NA'
        if type == 'num':
            return 0

    else:
        return string

#Scraper for Pierce County- includes property char,taxes & owner info
class PierceCountyScraper(CSVFeedSpider):
    name = "pierce_county_spider"
    start_urls = [getStartUrlFilePath("pierce_parcels.csv")]
    custom_settings = {'ITEM_PIPELINES': {'property_data_spideys.pipelines.PierceFullPipeline': 400},'DOWNLOAD_DELAY':.0025,'CONCURRENT_REQUESTS_PER_DOMAIN': 25,'CONCURRENT_REQUESTS_PER_IP': 25}

    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        pass

    def parse_row(self,response,row):
        pin = row['parcel']

        #General summary page, will be used for owner info
        request = scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/summary.cfm?parcel='+pin, callback=self.parse_summary)
        request.meta['item'] = PierceCountyDescriptionItem()
        request.meta['pin'] = pin
        yield request

    #Chain data extraction and consolidate into one item
    def parse_summary(self, response):

        parcel = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[1]/table/tr[2]/td[2]/text()').extract())
        owner_name = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[2]/table/tr[2]/td[2]/text()').extract())
        site_address = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[1]/table/tr[3]/td[2]/text()').extract())
        mailing_address_street = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[2]/table/tr[3]/td[2]/text()[1]').extract())
        mailing_address_city_zip = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[2]/table/tr[3]/td[2]/text()[2]').extract())

        #-------------------------------Owner Name Splitting---------------------------#
        owner_first_last_list = str(owner_name).split()
        #EVELAND JENNIFER R & HAROLD E
        m = re.search('(\w+)\s(.*)\s&\s(.*)', owner_name)
        owner_2_first = 'NA'
        if "LLC" in owner_name:
            owner_first_name = "LLC"
            owner_last_name = "LLC"
        elif m:
            owner_last_name= m.group(1)
            owner_2_first = m.group(2)
            owner_first_name= m.group(3)
        else:
            if(len(owner_first_last_list)>1):
                owner_last_name = owner_first_last_list[0]
                owner_first_name = owner_first_last_list[1]
            else:
                owner_first_name =  owner_name
                owner_last_name = owner_name

        #----------------------Mail Address Processing Splitting-----------------------#
        mail_streetAddress = str(mailing_address_street).replace('\n', '').replace('\t','').replace('\r','')
        cityStateZipList = str(mailing_address_city_zip).split()
        mail_city = " ".join(cityStateZipList[0:(len(cityStateZipList)-2)])
        mail_state = cityStateZipList[(len(cityStateZipList)-2)]
        mail_zip = cityStateZipList[len(cityStateZipList)-1]
        mail_fullAddress = ",".join([mail_streetAddress,mail_city,mail_state,mail_zip])

        item = response.meta['item']
        pin = response.meta['pin']

        item['parcel'] = parcel
        item['owner_name'] = owner_name
        item['owner_last_name'] = owner_last_name
        item['owner_first_name'] = owner_first_name
        item['owner_2_first'] = owner_2_first
        item['site_address'] = site_address
        item['mailing_address'] = mail_fullAddress
        item['mail_city'] = mail_city
        item['mail_state'] = mail_state
        item['mail_zip'] = mail_zip

        #Tax page will be used for property tax paid/owed and tax assesment
        yield scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/taxvalue.cfm?parcel='+pin, callback=self.parse_taxes,meta={'item': item,'pin':pin})

    def parse_taxes(self, response):
        item = response.meta['item']
        pin = response.meta['pin']

        tax_year_1 = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[2]/td[1]/text()').extract())
        tax_year_2 = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[3]/td[1]/text()').extract())
        tax_year_3 = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[4]/td[1]/text()').extract())
        tax_year_1_assessed = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[2]/td[4]/text()').extract())
        tax_year_2_assessed = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[3]/td[4]/text()').extract())
        tax_year_3_assessed = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[4]/td[4]/text()').extract())
        current_balance_due = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td[1]/table[1]/tr[4]/td/table/tr[1]/td/table/tr/td[1]/strong/text()').extract())
        if current_balance_due == 'NA':
            current_balance_due = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td[1]/table[1]/tr[3]/td/table/tr/td/table/tr/td[1]/strong/text()').extract())

       #------------------------------Format Money Values(Ints/Floats)-----------------------------------#
        tax_year_1_assessed_formatted = float(checkIfNa(str(tax_year_1_assessed).replace(',', ''),'num'))
        tax_year_2_assessed_formatted = float(checkIfNa(str(tax_year_2_assessed).replace(',', ''),'num'))
        tax_year_3_assessed_formatted = float(checkIfNa(str(tax_year_3_assessed).replace(',', ''),'num'))

        exemption = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td[2]/table[1]/tr[2]/td/table/tr[2]/td[2]/text()').extract())

        item['exemption'] = exemption

        item['tax_year_1'] = int(checkIfNa(tax_year_1,'num'))
        item['tax_year_2'] = int(checkIfNa(tax_year_2,'num'))
        item['tax_year_3'] = int(checkIfNa(tax_year_3,'num'))
        item['tax_year_1_assessed'] = tax_year_1_assessed_formatted
        item['tax_year_2_assessed'] = tax_year_2_assessed_formatted
        item['tax_year_3_assessed'] = tax_year_3_assessed_formatted

        item['current_balance_due'] = float(((str(current_balance_due).split()[2]).replace(',', '')))

        #Land page will give us lot size/square footage and utility types installed or info on the driveway(paved unpaved)(optional)
        yield scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/land.cfm?parcel='+pin, callback=self.parse_land,meta={'item': item,'pin':pin})


    def parse_land(self, response):
        lot_square_footage = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[1]/td[2]/table/tr[2]/td[2]/text()').extract())
        acres = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[1]/td[2]/table/tr[3]/td[2]/text()').extract())

        electric = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[2]/td[2]/table/tr[2]/td[2]/text()').extract())
        sewer = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[2]/td[2]/table/tr[3]/td[2]/text()').extract())
        water = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[2]/td[2]/table/tr[4]/td[2]/text()').extract())

        item = response.meta['item']
        pin = response.meta['pin']

        item['lot_square_footage'] = int(checkIfNa(str(lot_square_footage).replace(',', ''),'num'))
        item['lot_acres'] = float(checkIfNa(acres,'num'))

        item['electric'] = electric
        item['sewer'] = sewer
        item['water'] = water


        #Sales page provides sales records if any sales since '99
        yield scrapy.Request('https://epip.co.pierce.wa.us/cfapps/atr/epip/buildings.cfm?parcel='+pin, callback=self.parse_building,meta={'item': item,'pin':pin})


    def parse_building(self, response):

        property_type = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[1]/tr[1]/td[2]/text()').extract())
        occupancy = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[1]/tr[5]/td[2]/text()').extract())
        square_footage = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[1]/tr[1]/td[4]/text()').extract())
        attached_garage_footage = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[1]/tr[3]/td[4]/text()').extract())

        year_built = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[2]/text()').extract())
        adj_year_built = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[3]/text()').extract())
        stories = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[5]/text()').extract())
        bedrooms = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[6]/text()').extract())
        baths = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[7]/text()').extract())
        siding_type = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[8]/text()').extract())
        units = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td/table/tr[2]/td/table[2]/tr/td/table/tr[2]/td/table/tr[2]/td[12]/text()').extract())

        item = response.meta['item']
        pin = response.meta['pin']

        item['property_type'] = property_type
        item['occupancy'] = occupancy
        item['building_square_footage'] = int(checkIfNa(str(square_footage).replace(',', ''),'num'))
        item['attached_garage_footage'] = int(checkIfNa(str(attached_garage_footage).replace(',', ''),'num'))
        item['year_built'] = int(checkIfNa(year_built,'num'))
        item['adj_year_built'] = int(checkIfNa(adj_year_built,'num'))
        item['stories'] = float(checkIfNa(stories,'num'))
        item['bedrooms'] = int(checkIfNa(bedrooms,'num'))
        item['baths'] = float(checkIfNa(baths,'num'))
        item['siding_type'] = siding_type
        item['units'] = int(checkIfNa(units,'num'))

        return [item]

#Scraper for Duval County- includes property char,taxes & owner info
class DuvalCountyScraper(CSVFeedSpider):
    name = "duval_county_spider"
    start_urls = [getStartUrlFilePath("duval_parcels.csv")]
    custom_settings = {'ITEM_PIPELINES': {'property_data_spideys.pipelines.DuvalFullPipeline': 400},'DOWNLOAD_DELAY':.0025,'CONCURRENT_REQUESTS_PER_DOMAIN': 25,'CONCURRENT_REQUESTS_PER_IP': 25}

    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        #This will later be passed in as argument by spider caller

    def spider_closed(self, spider):
        pass

    def parse_row(self,response,row):
        pin = row['parcel']
        #General summary page, will b e used for owner info
        request = scrapy.Request('http://apps.coj.net/PAO_PropertySearch/Basic/Detail.aspx?RE='+pin, callback=self.parse_summary)
        request.meta['item'] = DuvalCountyDescriptionItem()
        request.meta['pin'] = pin
        yield request

    #Chain data extraction and consolidate into one item
    def parse_summary(self, response):

        parcel = check_path(response.xpath('//*[@id="ctl00_cphBody_lblRealEstateNumber"]/text()').extract())
        owner_name = check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterOwnerInformation_ctl00_lblOwnerName"]/text()').extract())
        mailing_address_street = check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterOwnerInformation_ctl00_lblMailingAddressLine1"]/text()').extract())
        mailing_address_cityzip = check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterOwnerInformation_ctl00_lblMailingAddressLine3"]/text()').extract())

        site_address_street = check_path(response.xpath('//*[@id="ctl00_cphBody_lblPrimarySiteAddressLine1"]/text()').extract())
        site_address_cityzip = check_path(response.xpath('//*[@id="ctl00_cphBody_lblPrimarySiteAddressLine2"]/text()').extract())

        property_use = check_path(response.xpath('//*[@id="ctl00_cphBody_lblPropertyUse"]/text()').extract())
        building_type = check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_lblBuildingType"]/text()').extract())

        year_built = check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_lblYearBuilt"]/text()').extract())
        stories = check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingAttributes"]/tr[2]/td[2]/text()').extract())
        bedrooms = check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingAttributes"]/tr[3]/td[2]/text()').extract())
        bathrooms = check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingAttributes"]/tr[4]/td[2]/text()').extract())
        units = check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingAttributes"]/tr[5]/td[2]/text()').extract())
        total_heated_sqaure_footage = check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingArea"]/tr[last()]/td[3]/text()').extract())

        heating_type = check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingElements"]/tr[9]/td[3]/text()').extract())
        ac_type = check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingElements"]/tr[10]/td[3]/text()').extract())

        tax_market_value_year1 = check_path(response.xpath('//*[@id="ctl00_cphBody_lblJustMarketValueCertified"]/text()').extract())
        tax_market_value_year2 = check_path(response.xpath('//*[@id="ctl00_cphBody_lblJustMarketValueInProgress"]/text()').extract())

        sale1_date = check_path(response.xpath('//*[@id="ctl00_cphBody_lblHeaderInProgress"]/text()').extract())
        sale2_date = check_path(response.xpath('//*[@id="ctl00_cphBody_lblHeaderCertified"]/text()').extract())

        sale1_price = check_path(response.xpath('//*[@id="ctl00_cphBody_gridSalesHistory"]/tr[2]/td[3]/text()').extract())
        sale2_price = check_path(response.xpath('//*[@id="ctl00_cphBody_gridSalesHistory"]/tr[3]/td[3]/text()').extract())

        total_square_footage = check_path(response.xpath('//*[@id="ctl00_cphBody_lblTotalArea1"]/text()').extract())

        exemptions_none = check_path(response.xpath('//*[@id="ctl00_cphBody_lblExemptionsCountyNoData"]/li/text()').extract())

        #-----------------------------------------Handle Exemptions-------------------------------------------#
        senior_exemption,homestead_exemption = 0,0
        if exemptions_none == None:
            exemption_1 = check_path(response.xpath('//*[@id="ctl00_cphBody_ul_propExemptionsCounty"]/li[1]/span[1]/text()').extract())
            exemption_2 = check_path(response.xpath('//*[@id="ctl00_cphBody_ul_propExemptionsCounty"]/li[2]/span[1]/text()').extract())
            exemption_3 = check_path(response.xpath('//*[@id="ctl00_cphBody_ul_propExemptionsCounty"]/li[3]/span[1]/text()').extract())
            exemption_4 = check_path(response.xpath('//*[@id="ctl00_cphBody_ul_propExemptionsCounty"]/li[4]/span[1]/text()').extract())
            exemption_5 = check_path(response.xpath('//*[@id="ctl00_cphBody_ul_propExemptionsCounty"]/li[4]/span[1]/text()').extract())

            exemption_list_filtered = []
            for x in exemption_1,exemption_2,exemption_3,exemption_4,exemption_5:
                if x == None:
                    exemption_list_filtered.append('NA')
                else:
                    exemption_list_filtered.append(x)

            expemption_string = '\t'.join(exemption_list_filtered)
            if "Senior" in expemption_string:
                senior_exemption = 1
            if "Homestead" in expemption_string :
                homestead_exemption = 1

        item = response.meta['item']
        item['parcel'] = str(parcel).replace('-', '')
        item['owner_name'] = owner_name
        item['site_address'] = ''.join([str(site_address_street),str(site_address_cityzip)])
        item['mailing_address'] = ''.join([str(mailing_address_street),str(mailing_address_cityzip)])
        item['property_type'] = property_use
        item['building_type'] = building_type
        item['building_square_footage'] = int(checkIfNa(total_heated_sqaure_footage,'num'))
        item['year_built'] = int(checkIfNa(year_built,'num'))
        item['stories'] = float(checkIfNa(stories,'num'))
        item['bedrooms'] = float(checkIfNa(bedrooms,'num'))
        item['baths'] = float(checkIfNa(bathrooms,'num'))
        item['siding_type'] = 'NA'
        item['units'] = float(checkIfNa(units,'num'))
        item['sale1_price'] = sale1_price
        item['sale1_date'] = sale1_date
        item['sale2_price'] = sale2_price
        item['sale2_date'] = sale2_date
        item['tax_year_1_assessed'] = float(checkIfNa((tax_market_value_year1).replace(',', '').replace('$', ''),'num'))
        item['tax_year_2_assessed'] = float(checkIfNa((tax_market_value_year2).replace(',', '').replace('$', ''),'num'))
        item['lot_square_footage'] = int(checkIfNa(total_square_footage,'num'))
        item['lot_acres'] = 'NA'
        item['electric'] = 'NA'
        item['sewer'] = 'NA'
        item['water'] = 'NA'
        item['homestead_exemption'] = homestead_exemption
        item['senior_exemption'] = senior_exemption

        #Need to get taxes owed
        #http://fl-duval-taxcollector.publicaccessnow.com/propertytaxsearch/accountdetail.aspx?p=019089-0000

        yield scrapy.Request('http://fl-duval-taxcollector.publicaccessnow.com/propertytaxsearch/accountdetail.aspx?p='+parcel, callback=self.parse_taxes,meta={'item': item,'parcel':parcel})

    def parse_taxes(self, response):
        item = response.meta['item']
        parcel = response.meta['parcel']

        currentTaxDue,currentDelinquentTax = 0,0
        currentTaxes = check_path(response.xpath('//*[@id="lxT444"]/p/text()').extract())
        if currentTaxes == 'NA':
            currentTaxDue = check_path(response.xpath('//*[@id="lxT444"]/table/tr/td[3]/text()').extract())
            print(response.xpath('//*[@id="lxT444"]/table/tr/td[3]/text()').extract())

        delinquentTaxes = check_path(response.xpath('//*[@id="lxT444"]/p/text()').extract())
        if delinquentTaxes == 'NA':
            print(response.xpath('//*[@id="lxT445"]/table/tr/td[3]/text()').extract())
            currentDelinquentTax = check_path(response.xpath('//*[@id="lxT445"]/table/tr/td[3]/text()').extract())

        print(currentTaxDue,currentDelinquentTax,parcel)


        return item


#Sales Scraper for Duval County- includes parcel sales data
class DuvalCountySalesScraper(CSVFeedSpider):
    name = "duval_sales_spider"
    start_urls = [getStartUrlFilePath("duval_parcels.csv")]

    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        #This will later be passed in as argument by spider caller

    def spider_closed(self, spider):
        pass

    def parse_row(self,response,row):
        pin = row['parcel']
        #General summary page, will b e used for owner info
        request = scrapy.Request('http://apps.coj.net/PAO_PropertySearch/Basic/Detail.aspx?RE='+pin, callback=self.parse_summary)
        request.meta['item'] = DuvalCountyDescriptionItem()
        request.meta['pin'] = pin
        return [request]

    #Chain data extraction and consolidate into one item
    def parse_summary(self, response):

        rows = check_path(response.xpath('count(//*[@id="ctl00_cphBody_gridSalesHistory"]/tr)'))
        print(rows.extract())
        for x in range(0,int(float(rows.extract()))):
            date = check_path(response.xpath('//*[@id="ctl00_cphBody_gridSalesHistory"]/tr['+str(x)+']/td[2]/text()').extract())
            price = check_path(response.xpath('//*[@id="ctl00_cphBody_gridSalesHistory"]/tr['+str(x)+']/td[3]/text()').extract())
            document =check_path(response.xpath('//*[@id="ctl00_cphBody_gridSalesHistory"]/tr['+str(x)+']/td[4]/text()').extract())
            print(date)
            print(price)
            print(document)

        item = response.meta['item']
        # item['parcel'] = str(parcel).replace('-', '')
        # item['owner_name'] = owner_name

        #Need to get taxes owed
        #http://fl-duval-taxcollector.publicaccessnow.com/propertytaxsearch/accountdetail.aspx?p=019089-0000

        yield item

#Scraper for Duval County- includes property char & taxes,owner info,
# document records,unpaid taxes, sold taxes,exemptions
class CookCountyScraper(CSVFeedSpider):
    name = "cook_county_spider"
    #start_urls = [getStartUrlFilePath("cook_parcels.csv")]
    start_urls = [getStartUrlFilePath("cook_parcels.csv")]

    cookieJarIndex1 = 0
    custom_settings = {'ITEM_PIPELINES': {'property_data_spideys.pipelines.CookFullPipeline': 400},'DOWNLOAD_DELAY':.025,'CONCURRENT_REQUESTS_PER_DOMAIN':1,'CONCURRENT_REQUESTS_PER_IP':1}


    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        pass

    def parse_row(self,response,row):
        pin = row['parcel']
        #General summary page, will be used for owner info
        self.cookieJarIndex1 = self.cookieJarIndex1+ 1
        request = scrapy.Request('http://www.cookcountypropertyinfo.com/cookviewerpinresults.aspx?pin='+pin,dont_filter = True,callback=self.parse_summary)
        request.meta['item'] = CookCountyDescriptionItem()
        request.meta['pin'] = pin
        request.meta['cookiejar'] = self.cookieJarIndex1

        return [request]

    def parse_summary(self, response):
        parcel_title = check_path(response.xpath('//*[@id="ContentPlaceHolder1_lblResultTitle"]/text()').extract())
        parcel_stripped = parcel_title.replace("-", "")

        item = response.meta['item']
        pin = response.meta['pin']

        if(parcel_stripped != pin):
            print("Parcels do not match!!!!!!!")
            raise AttributeError
        if(parcel_title == 'NA'):
            print("Parcels NA!!!!!!!",parcel_title)
            raise AttributeError

        #     self.cookieJarIndex1 = self.cookieJarIndex1 + 1
        #     return[scrapy.Request('http://www.cookcountypropertyinfo.com/cookviewerpinresults.aspx?pin='+pin,dont_filter = True,meta={'cookiejar': cookieJarIndex1},callback=self.parse_summary)]

        site_address_street = check_path(response.xpath('//*[@id="ContentPlaceHolder1_PropertyInfo_propertyAddress"]/text()').extract())
        site_address_city = check_path(response.xpath('//*[@id="ContentPlaceHolder1_PropertyInfo_propertyCity"]/text()').extract())
        site_address_zip = check_path(response.xpath('//*[@id="ContentPlaceHolder1_PropertyInfo_propertyZip"]/text()').extract())
        site_township = check_path(response.xpath('//*[@id="ContentPlaceHolder1_PropertyInfo_propertyTownship"]/text()').extract())

        owner_name = check_path(response.xpath('//*[@id="ContentPlaceHolder1_PropertyInfo_propertyMailingName"]/text()').extract())
        mailing_address_street = check_path(response.xpath('//*[@id="ContentPlaceHolder1_PropertyInfo_propertyMailingAddress"]/text()').extract())
        mailing_city_zip_state = check_path(response.xpath('//*[@id="ContentPlaceHolder1_PropertyInfo_propertyMailingCityStateZip"]/text()').extract())

        lot_square_footage = check_path(response.xpath('//*[@id="ContentPlaceHolder1_TaxYearInfo_propertyLotSize"]/text()').extract())
        building_square_footage = check_path(response.xpath('//*[@id="ContentPlaceHolder1_TaxYearInfo_propertyBuildingSize"]/text()').extract())
        #---------------------------------------------------------Tax Info-------------------------------------------------------------------#
        tax_year0 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_TaxBillInfo_rptTaxBill_taxBillYear_0"]/text()').extract())
        tax_year1 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_TaxBillInfo_rptTaxBill_taxBillYear_1"]/text()').extract())
        tax_year2 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_TaxBillInfo_rptTaxBill_taxBillYear_2"]/text()').extract())
        tax_year3 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_TaxBillInfo_rptTaxBill_taxBillYear_3"]/text()').extract())
        tax_year4 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_TaxBillInfo_rptTaxBill_taxBillYear_4"]/text()').extract())

        # Check if last two years are paid in full
        paid_in_full_button1 = check_path(response.xpath('//*[@id="taxpaid'+tax_year0.split(":")[0]+'-button"]/span/text()').extract())
        paid_in_full_button2 = check_path(response.xpath('//*[@id="taxpaid'+tax_year1.split(":")[0]+'-button"]/span/text()').extract())

        tax_not_paid0 = check_path(response.xpath('//*[@id="taxpayonline2'+tax_year0.split(":")[0]+'-button"]/span/text()').extract())
        tax_not_paid1 = check_path(response.xpath('//*[@id="taxpayonline2'+tax_year1.split(":")[0]+'-button"]/span/text()').extract())

        tax0 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_TaxBillInfo_rptTaxBill_taxBillAmount_0"]/text()').extract())
        tax1 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_TaxBillInfo_rptTaxBill_taxBillAmount_1"]/text()').extract())

        #Tax sales on record
        year_0_tax_sale_indicator = check_path(response.xpath('//*[@id="taxsaleredeemed'+tax_year0.split(":")[0]+'-button"]/span/text()').extract())
        year_1_tax_sale_indicator = check_path(response.xpath('//*[@id="taxsaleredeemed'+tax_year1.split(":")[0]+'-button"]/span/text()').extract())
        year_2_tax_sale_indicator = check_path(response.xpath('//*[@id="taxsaleredeemed'+tax_year2.split(":")[0]+'-button"]/span/text()').extract())
        year_3_tax_sale_indicator = check_path(response.xpath('//*[@id="taxsaleredeemed'+tax_year3.split(":")[0]+'-button"]/span/text()').extract())
        year_4_tax_sale_indicator = check_path(response.xpath('//*[@id="taxsaleredeemed'+tax_year4.split(":")[0]+'-button"]/span/text()').extract())


        #--------------------------------------------------------Documents Recorded--------------------------------------------------------------------#
        doc_rec1 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[1]/td/div[1]/div/text()').extract())
        doc_rec2 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[2]/td/div[1]/div/text()').extract())
        doc_rec3 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[3]/td/div[1]/div/text()').extract())
        doc_rec4 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[4]/td/div[1]/div/text()').extract())
        doc_rec5 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[5]/td/div[1]/div/text()').extract())

        doc_rec1_alt = check_path(response.xpath('//*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[1]/td/div[2]/text()').extract())
        doc_rec2_alt = check_path(response.xpath('//*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[2]/td/div[2]/text()').extract())
        doc_rec3_alt = check_path(response.xpath('//*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[3]/td/div[2]/text()').extract())
        doc_rec4_alt = check_path(response.xpath('//*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[4]/td/div[2]/text()').extract())
        doc_rec5_alt = check_path(response.xpath('//*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[5]/td/div[2]/text()').extract())


        if (doc_rec1 == 'NA'):
            doc_rec1 = doc_rec1_alt
        if (doc_rec2 == 'NA'):
            doc_rec2 = doc_rec2_alt
        if (doc_rec3 == 'NA'):
            doc_rec3 = doc_rec3_alt
        if (doc_rec4 == 'NA'):
            doc_rec4 = doc_rec4_alt
        if (doc_rec5 == 'NA'):
            doc_rec5 = doc_rec5_alt


        document1_record_string,document2_record_string,document3_record_string,document4_record_string,document5_record_string,document1_record_date,\
        document2_record_date,document3_record_date,document4_record_date,document5_record_date = 'NA','NA','NA','NA','NA','NA','NA','NA','NA','NA'

        if (len(doc_rec1.split("-")) > 1):
            document1_record_string = doc_rec1.split("-")[1]
            document1_record_date = doc_rec1.split("-")[2]

        if (len(doc_rec2.split("-")) > 1):
            document2_record_string = doc_rec2.split("-")[1]
            document2_record_date = doc_rec2.split("-")[2]

        if (len(doc_rec3.split("-")) > 1):
            document3_record_string = doc_rec3.split("-")[1]
            document3_record_date = doc_rec3.split("-")[2]

        if (len(doc_rec4.split("-")) > 1):
            document4_record_string = doc_rec4.split("-")[1]
            document4_record_date = doc_rec4.split("-")[2]

        if (len(doc_rec5.split("-")) > 1):
            document5_record_string = doc_rec5.split("-")[1]
            document5_record_date = doc_rec5.split("-")[2]

        #Tax Sales Flag
        taxes_sold= 0
        if ("Taxes Sold" in year_0_tax_sale_indicator) or ("Taxes Sold" in year_1_tax_sale_indicator) or ("Taxes Sold" in year_2_tax_sale_indicator) or ("Taxes Sold" in year_3_tax_sale_indicator) or ("Taxes Sold" in year_4_tax_sale_indicator):
            taxes_sold = 1


        #Tax Paid Flag
        if paid_in_full_button1 == "Paid in Full":
            tax_paid_year0 = "Paid_Full"
        elif 'Pay Online' in tax_not_paid0 :
            m = re.search('Pay Online:\s\$(\d+.\d+)', tax_not_paid0)
            if m:
                tax_paid_year0 = m.group(1)
        else:
            tax_paid_year0 = 'NA'

        if paid_in_full_button2 == "Paid in Full":
            tax_paid_year1 = "Paid_Full"
        elif 'Pay Online' in tax_not_paid1 :
            m = re.search('Pay Online:\s\$(\d+.\d+)', tax_not_paid1)
            if m:
                tax_paid_year1 = m.group(1)
        else:
            tax_paid_year1 = 'NA'


    #--------------------Name Checks------------------------#
        owner_2_first = 'NA'
        owner_first_last_list = str(owner_name).split()
        m = re.search('(\w+)\s&\s(\w+)\s(\w+)', owner_name)
        if "LLC" in owner_name:
            owner_first = "LLC"
            owner_last = "LLC"
        elif m:
            owner_first= m.group(1)
            owner_2_first = m.group(2)
            owner_last= m.group(3)
        else:
            if(len(owner_first_last_list)>1):
                owner_first = owner_first_last_list[0]
                owner_last = owner_first_last_list[1]
            else:
                owner_first =  owner_name
                owner_last = owner_name

        #-------------------Mail Address Processing--------------#
        if(mailing_city_zip_state != 'NA'):
            mail_cityStateZipList = str(mailing_city_zip_state).split(",")
            mail_state_zip_list = mail_cityStateZipList[1].split(" ")
            mail_city = mail_cityStateZipList[0]
            mail_state = mail_state_zip_list[1]
            mail_zip = mail_state_zip_list[2]
        else:
            mail_city = 'NA'
            mail_state = 'NA'
            mail_zip = 'NA'


        item['owner_name'] = owner_name
        item['owner_first'] = owner_first
        item['owner_2_first'] = owner_2_first
        item['owner_last'] = owner_last

        item['site_address'] = ",".join([site_address_street,site_address_city,"IL",site_address_zip])
        item['site_address_city'] = site_address_city
        item['site_address_zip'] = site_address_zip
        item['site_address_township'] = site_township

        item['mailing_address'] = ",".join([mailing_address_street,mail_city,mail_state,mail_zip])
        item['mail_city'] = mail_city
        item['mail_state'] = mail_state
        item['mail_zip'] = mail_zip

        item['taxes_sold'] = taxes_sold


        item['tax_paid_year0'] = tax_paid_year0
        item['tax_paid_year0_amount'] = float(checkIfNa(str(tax0).replace(',', '').replace('$',""),'num'))
        item['tax_paid_year1'] = tax_paid_year1
        item['tax_paid_year1_amount'] = float(checkIfNa(str(tax1).replace(',', '').replace('$',""),'num'))

        item['doc1_string'] = document1_record_string
        item['doc1_date'] = document1_record_date
        item['doc2_string'] = document2_record_string
        item['doc2_date'] = document2_record_date
        item['doc3_string'] = document3_record_string
        item['doc3_date'] = document3_record_date
        item['doc4_string'] = document4_record_string
        item['doc4_date'] = document4_record_date
        item['doc5_string'] = document5_record_string
        item['doc5_date'] = document5_record_date


        item['lot_square_footage'] = int(checkIfNa(str(lot_square_footage).replace(',', ''),'num'))
        item['building_square_footage'] = int(checkIfNa(str(building_square_footage).replace(',', ''),'num'))

        return [scrapy.Request('http://www.cookcountyassessor.com/Property.aspx?mode=details&pin='+pin, callback=self.parse_characteristics,meta={'item': item,'pin':pin})]

    def parse_characteristics(self, response):

        parcel = check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropInfoPIN"]/text()').extract())
        current_year_assessed_value  = check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharMktValCurrYear"]/text()').extract())
        prior_year_assessed_value = check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharMktValPrevYear"]/text()').extract())
        property_use = check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharUse"]/text()').extract())
        residence_type = check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharResType"]/text()').extract())
        units = check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharApts"]/text()').extract())
        construction_type = check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharExtConst"]/text()').extract())
        full_bathrooms = check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharFullBaths"]/text()').extract())
        half_bathrooms = check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharHalfBaths"]/text()').extract())
        basement = check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharBasement"]/text()').extract())
        central_air = check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharCentAir"]/text()').extract())
        garage_type = check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharGarage"]/text()').extract())
        age = check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharAge"]/text()').extract())

        home_owner_exemption = check_path(response.xpath('//*[@id="exemptions"]/div[2]/div[1]/span[2]/text()').extract())
        senior_citizen_exemption = check_path(response.xpath('//*[@id="exemptions"]/div[2]/div[2]/span[2]/text()').extract())

        if(str(parcel).replace('-', '')!= response.meta['pin']):
            print("Does not match in second step either!!!!!!!!!!!!!!!")

        item = response.meta['item']
        item['parcel'] = str(parcel).replace('-', '')
        item['current_year_assessed_value'] = float(checkIfNa(str(current_year_assessed_value).replace(',', '').replace('$',""),'num'))
        item['prior_year_assessed_value'] = float(checkIfNa(str(prior_year_assessed_value).replace(',', '').replace('$',""),'num'))
        item['property_use'] = property_use
        item['residence_type'] = residence_type
        item['units'] = int(checkIfNa(units,'num'))
        item['construction_type'] = construction_type
        item['full_bathrooms'] = float(checkIfNa(full_bathrooms,'num'))
        item['half_bathrooms'] = float(checkIfNa(half_bathrooms,'num'))
        item['basement'] = basement
        item['central_air'] = central_air
        item['garage_type'] = garage_type
        item['age'] = int(checkIfNa(age,'num'))

        item['home_owner_exemption'] = home_owner_exemption
        item['senior_citizen_exemption'] = senior_citizen_exemption

        return [item]

#Scraper for Duval County- includes property char & owner info,
class MaricopaSingleParcelAPI(CSVFeedSpider):
    name = "maricopa_county_worker"
    authorization_token = '5ae1363b-28b8-11e8-9917-00155da2c015'

    start_urls = [getStartUrlFilePath("maricopa_parcels.csv")]

    custom_settings = {'ITEM_PIPELINES': {'property_data_spideys.pipelines.MaricopaFullPipeline': 400}}
    #custom_settings = {'ITEM_PIPELINES': {'property_data_spideys.pipelines.MaricopaAddToPipeline': 400}}

    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        pass

    def parse_row(self,response,row):
        pin = row['parcel']
        headerss = {"X-MC-AUTH":"%s" %(MaricopaSingleParcelAPI.authorization_token)}
        request = scrapy.Request('https://api.mcassessor.maricopa.gov/api/parcel/'+pin,headers=headerss,dont_filter = True,callback=self.parse_json)
        request.meta['item'] = MaricopaCountyDescriptionItem()
        request.meta['pin'] = pin
        yield request

    #Chain data extraction and consolidate into one item
    def parse_json(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
        item = response.meta['item']
        pin = response.meta['pin']

        owner_name = jsonresponse["Owner"]["OwnerName"]
        owner_street_address1 = jsonresponse["Owner"]["OwnerMailAddress1"]
        owner_city= jsonresponse["Owner"]["OwnerCity"]
        owner_state = jsonresponse["Owner"]["OwnerState"]
        owner_zip = jsonresponse["Owner"]["OwnerZip"]
        owner_full_address = jsonresponse["Owner"]["FullMailingAddress"]

        full_property_address = jsonresponse["PropertyAddress"]
        property_description = jsonresponse["PEPropertyUseDescription"]
        lot_size = jsonresponse["LotSize"]
        property_type = jsonresponse["PropertyType"]
        rental = jsonresponse["IsRental"]

        value_0 = jsonresponse["Valuations"][0]["FullCashValue"]
        value_1 = jsonresponse["Valuations"][1]["FullCashValue"]
        value_2 = jsonresponse["Valuations"][2]["FullCashValue"]

        last_deed_date = jsonresponse["Owner"]["DeedDate"]
        last_sale_price = jsonresponse["Owner"]["SalePrice"]

        #--------------------------Owner Name Processing-----------------------------#
        if "LLC" in owner_name:
            owner_first = "LLC"
            owner_last = "LLC"
        else:
            owner_last_first_list = str(owner_name).split()
            owner_last = owner_last_first_list[0]
            owner_first = owner_last_first_list[1]

        #------------------------Property Address Processing----------------------------#
        streetCity_StateZipList = str(full_property_address).split('   ')
        site_street = streetCity_StateZipList[0]
        site_zip_city_list = streetCity_StateZipList[1].split()
        site_zip = site_zip_city_list[len(site_zip_city_list)-1]
        site_city = " ".join(site_zip_city_list[0:len(site_zip_city_list)-1])
        #------------------------Build Item Obj-----------------------------------------#
        item['parcel'] = checkIfNa(pin,'str')
        item['owner_first'] = checkIfNa(owner_first,'str')
        item['owner_last'] = checkIfNa(owner_last,'str')
        item['owner_name'] = checkIfNa(owner_name,'str')
        item['owner_full_address'] = checkIfNa(owner_full_address,'str')
        item['owner_street_address1'] = checkIfNa(owner_street_address1,'str')
        item['owner_city'] = checkIfNa(owner_city,'str')
        item['owner_state'] = checkIfNa(owner_state,'str')
        item['owner_zip'] = checkIfNa(owner_zip,'str')
        item['full_property_address'] = checkIfNa(full_property_address,'str')
        item['site_street'] = checkIfNa(site_street,'str')
        item['site_city'] = checkIfNa(site_city,'str')
        item['site_zip'] = checkIfNa(site_zip,'str')
        item['property_description'] = checkIfNa(property_description,'str')
        item['lot_size'] = float(checkIfNa(lot_size,'num'))
        item['property_type'] = checkIfNa(property_type,'str')
        item['rental'] = checkIfNa(rental,'num')
        item['value_0'] = float(checkIfNa(value_0,'num'))
        item['value_1'] = float(checkIfNa(value_1,'num'))
        item['value_2'] = float(checkIfNa(value_2,'num'))
        item['last_deed_date'] = checkIfNa(last_deed_date,'str')
        item['last_sale_price'] = float(checkIfNa(last_sale_price,'num'))

        yield item

