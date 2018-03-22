import scrapy
from scrapy import signals
from property_data_spideys.items import PierceCountyDescriptionItem,DuvalCountyDescriptionItem,CookCountyDescriptionItem
from scrapy.spiders import CSVFeedSpider
from scrapy.xlib.pydispatch import dispatcher
from property_data_spideys import pipelines
import locale

class spiderBaseFunctions():
    pass

class PierceCountyScraper(CSVFeedSpider,spiderBaseFunctions):
    name = "pierce_county_spider"
    start_urls = [ "file:///C:/Users/ebeluli/Desktop/property_data_spideys/ParcelsLists/parcels.csv"]
    #start_urls = [ "file:///home/edit/GruntJS/propertyDataScraper/ParcelsLists/parcels.csv"]

    custom_settings = {'ITEM_PIPELINES': {'property_data_spideys.pipelines.PierceFullPipeline': 400}}

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
        return [request]

    def check_path(self, xpath_return):
        if len(xpath_return) == 1:
            return xpath_return[0]
        else:
            return None

    #Chain data extraction and consolidate into one item
    def parse_summary(self, response):

        parcel = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[1]/table/tr[2]/td[2]/text()').extract())
        owner_name = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[2]/table/tr[2]/td[2]/text()').extract())
        site_address = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[1]/table/tr[3]/td[2]/text()').extract())
        mailing_address_street = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[2]/table/tr[3]/td[2]/text()[1]').extract())
        mailing_address_city_zip = self.check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[2]/table/tr[3]/td[2]/text()[2]').extract())

        #-------------------------------Owner Name Splitting---------------------------#
        owner_name_list = str(owner_name).split()
        owner_last_name = owner_name_list[0]
        owner_first_name = owner_name_list[1]

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
        item['site_address'] = site_address
        item['mailing_address'] = mail_fullAddress
        item['mail_city'] = mail_city
        item['mail_state'] = mail_state
        item['mail_zip'] = mail_zip

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

       #------------------------------Format Money Values-----------------------------------#
        tax_year_1_assessed_formatted = float(str(tax_year_1_assessed).replace(',', ''))
        tax_year_2_assessed_formatted = float(str(tax_year_2_assessed).replace(',', ''))
        tax_year_3_assessed_formatted = float(str(tax_year_3_assessed).replace(',', ''))
        current_balance_formatted = float(((str(current_balance_due).split()[2])).replace(',', ''))

        item = response.meta['item']
        pin = response.meta['pin']

        item['tax_year_1'] = int(tax_year_1)
        item['tax_year_2'] = int(tax_year_2)
        item['tax_year_3'] = int(tax_year_3)
        item['tax_year_1_assessed'] = tax_year_1_assessed_formatted
        item['tax_year_2_assessed'] = tax_year_2_assessed_formatted
        item['tax_year_3_assessed'] = tax_year_3_assessed_formatted

        item['current_balance_due'] = current_balance_formatted

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

        item['lot_square_footage'] = int(str(lot_square_footage).replace(',', ''))
        item['lot_acres'] = float(acres)

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
        item['building_square_footage'] = int(str(square_footage).replace(',', ''))
        item['attached_garage_footage'] = int(str(attached_garage_footage).replace(',', ''))
        item['year_built'] = int(year_built)
        item['adj_year_built'] = int(adj_year_built)
        item['stories'] = float(stories)
        item['bedrooms'] = int(bedrooms)
        item['baths'] = float(baths)
        item['siding_type'] = siding_type
        item['units'] = int(units)

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

class DuvalCountyScraper(CSVFeedSpider):
    name = "duval_county_spider"
    start_urls = [ "file:///C:/Users/ebeluli/Desktop/property_data_spideys/ParcelsLists/duval_parcels.csv"]
    #start_urls = [ "file:///home/edit/GruntJS/propertyDataScraper/ParcelsLists/parcels.csv"]
    custom_settings = {'ITEM_PIPELINES': {'property_data_spideys.pipelines.DuvalFullPipeline': 400}}

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

    def check_path(self, xpath_return):
        if len(xpath_return) == 1:
            return xpath_return[0]
        else:
            return None

    #Chain data extraction and consolidate into one item
    def parse_summary(self, response):

        parcel = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblRealEstateNumber"]/text()').extract())
        owner_name = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterOwnerInformation_ctl00_lblOwnerName"]/text()').extract())
        mailing_address_street = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterOwnerInformation_ctl00_lblMailingAddressLine1"]/text()').extract())
        mailing_address_cityzip = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterOwnerInformation_ctl00_lblMailingAddressLine3"]/text()').extract())

        site_address_street = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblPrimarySiteAddressLine1"]/text()').extract())
        site_address_cityzip = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblPrimarySiteAddressLine2"]/text()').extract())

        property_use = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblPropertyUse"]/text()').extract())
        building_type = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_lblBuildingType"]/text()').extract())

        year_built = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_lblYearBuilt"]/text()').extract())
        stories = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingAttributes"]/tr[2]/td[2]/text()').extract())
        bedrooms = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingAttributes"]/tr[3]/td[2]/text()').extract())
        bathrooms = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingAttributes"]/tr[4]/td[2]/text()').extract())
        units = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingAttributes"]/tr[5]/td[2]/text()').extract())
        total_heated_sqaure_footage = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingArea"]/tr[last()]/td[3]/text()').extract())

        heating_type = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingElements"]/tr[9]/td[3]/text()').extract())
        ac_type = self.check_path(response.xpath('//*[@id="ctl00_cphBody_repeaterBuilding_ctl00_gridBuildingElements"]/tr[10]/td[3]/text()').extract())

        tax_market_value_year1 = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblJustMarketValueCertified"]/text()').extract())
        tax_market_value_year2 = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblJustMarketValueInProgress"]/text()').extract())

        sale1_date = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblHeaderInProgress"]/text()').extract())
        sale2_date = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblHeaderCertified"]/text()').extract())

        sale1_price = self.check_path(response.xpath('//*[@id="ctl00_cphBody_gridSalesHistory"]/tr[2]/td[3]/text()').extract())
        sale2_price = self.check_path(response.xpath('//*[@id="ctl00_cphBody_gridSalesHistory"]/tr[3]/td[3]/text()').extract())

        total_square_footage = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblTotalArea1"]/text()').extract())

        exemptions_none = self.check_path(response.xpath('//*[@id="ctl00_cphBody_lblExemptionsCountyNoData"]/li/text()').extract())

        #-----------------------------------------Handle Exemptions-------------------------------------------#
        senior_exemption,homestead_exemption = 0,0
        if exemptions_none == None:
            exemption_1 = self.check_path(response.xpath('//*[@id="ctl00_cphBody_ul_propExemptionsCounty"]/li[1]/span[1]/text()').extract())
            exemption_2 = self.check_path(response.xpath('//*[@id="ctl00_cphBody_ul_propExemptionsCounty"]/li[2]/span[1]/text()').extract())
            exemption_3 = self.check_path(response.xpath('//*[@id="ctl00_cphBody_ul_propExemptionsCounty"]/li[3]/span[1]/text()').extract())
            exemption_4 = self.check_path(response.xpath('//*[@id="ctl00_cphBody_ul_propExemptionsCounty"]/li[4]/span[1]/text()').extract())
            exemption_5 = self.check_path(response.xpath('//*[@id="ctl00_cphBody_ul_propExemptionsCounty"]/li[4]/span[1]/text()').extract())

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
        item['building_square_footage'] = int(total_heated_sqaure_footage)
        item['year_built'] = int(year_built)
        item['stories'] = float(stories)
        item['bedrooms'] = float(bedrooms)
        item['baths'] = float(bathrooms)
        item['siding_type'] = 'NA'
        item['units'] = float(units)
        item['sale1_price'] = sale1_price
        item['sale1_date'] = sale1_date
        item['sale2_price'] = sale2_price
        item['sale2_date'] = sale2_date
        item['tax_year_1_assessed'] = float((tax_market_value_year1).replace(',', '').replace('$', ''))
        item['tax_year_2_assessed'] = float((tax_market_value_year2).replace(',', '').replace('$', ''))
        item['lot_square_footage'] = int(total_square_footage)
        item['lot_acres'] = 'NA'
        item['electric'] = 'NA'
        item['sewer'] = 'NA'
        item['water'] = 'NA'
        item['homestead_exemption'] = homestead_exemption
        item['senior_exemption'] = senior_exemption

        #Need to get taxes owed
        #http://fl-duval-taxcollector.publicaccessnow.com/propertytaxsearch/accountdetail.aspx?p=019089-0000

        yield item


class CookCountyScraper(CSVFeedSpider):
    name = "cook_county_spider"
    start_urls = [ "file:///C:/Users/ebeluli/Desktop/property_data_spideys/ParcelsLists/cook_parcels.csv"]
    #start_urls = [ "file:///home/edit/GruntJS/propertyDataScraper/ParcelsLists/parcels.csv"]

    custom_settings = {
        'ITEM_PIPELINES': {'property_data_spideys.pipelines.CookFullPipeline': 300}
        }

    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        pass

    def parse_row(self,response,row):
        pin = row['parcel']

        #General summary page, will be used for owner info
        request = scrapy.Request('http://www.cookcountypropertyinfo.com/cookviewerpinresults.aspx?pin='+pin,dont_filter = True,callback=self.parse_summary)
        request.meta['item'] = CookCountyDescriptionItem()
        request.meta['pin'] = pin
        return [request]

    def check_path(self, xpath_return):
        if len(xpath_return) == 1:
            return xpath_return[0]
        else:
            return None

    def parse_summary(self, response):
        site_address_street = self.check_path(response.xpath('//*[@id="ContentPlaceHolder1_PropertyInfo_propertyAddress"]/text()').extract())
        site_address_city = self.check_path(response.xpath('//*[@id="ContentPlaceHolder1_PropertyInfo_propertyCity"]/text()').extract())
        site_address_zip = self.check_path(response.xpath('//*[@id="ContentPlaceHolder1_PropertyInfo_propertyZip"]/text()').extract())
        site_township = self.check_path(response.xpath('//*[@id="ContentPlaceHolder1_PropertyInfo_propertyTownship"]/text()').extract())

        owner_name = self.check_path(response.xpath('//*[@id="ContentPlaceHolder1_PropertyInfo_propertyMailingName"]/text()').extract())
        mailing_address_street = self.check_path(response.xpath('//*[@id="ContentPlaceHolder1_PropertyInfo_propertyMailingAddress"]/text()').extract())
        mailing_city_zip_state = self.check_path(response.xpath('//*[@id="ContentPlaceHolder1_PropertyInfo_propertyMailingCityStateZip"]/text()').extract())

        lot_square_footage = self.check_path(response.xpath('//*[@id="ContentPlaceHolder1_TaxYearInfo_propertyLotSize"]/text()').extract())
        building_square_footage = self.check_path(response.xpath('//*[@id="ContentPlaceHolder1_TaxYearInfo_propertyBuildingSize"]/text()').extract())

        #--------------------Name Checks------------------------#
        owner_first_last_list = str(owner_name).split()

        if "LLC" in owner_name:
            owner_first = "LLC"
            owner_last = "LLC"
        else:
            owner_first_last_list = str(owner_name).split()
            owner_first = owner_first_last_list[0]
            owner_last = owner_first_last_list[1]

        #-------------------Mail Address Processing--------------#
        mail_cityStateZipList = str(mailing_city_zip_state).split(",")
        mail_state_zip_list = mail_cityStateZipList[1].split(" ")
        mail_city = mail_cityStateZipList[0]
        mail_state = mail_state_zip_list[1]
        mail_zip = mail_state_zip_list[2]


        item = response.meta['item']
        pin = response.meta['pin']
        item['parcel'] = pin
        item['owner_name'] = owner_name
        item['owner_first'] = owner_first
        item['owner_last'] = owner_last

        item['site_address'] = ",".join([site_address_street,site_address_city,"IL",site_address_zip])
        item['site_address_city'] = site_address_city
        item['site_address_zip'] = site_address_zip
        item['site_address_township'] = site_township

        item['mailing_address'] = ",".join([mailing_address_street,mail_city,mail_state,mail_zip])
        item['mail_city'] = mail_city
        item['mail_state'] = mail_state
        item['mail_zip'] = mail_zip

        item['lot_square_footage'] = int(str(lot_square_footage).replace(',', ''))
        item['building_square_footage'] = int(str(building_square_footage).replace(',', ''))

        return [scrapy.Request('http://www.cookcountyassessor.com/Property.aspx?mode=details&pin='+pin, callback=self.parse_characteristics,meta={'item': item,'pin':pin})]

    def parse_characteristics(self, response):

        parcel = self.check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropInfoPIN"]/text()').extract())
        current_year_assessed_value  = self.check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharMktValCurrYear"]/text()').extract())
        prior_year_assessed_value = self.check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharMktValPrevYear"]/text()').extract())
        property_use = self.check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharUse"]/text()').extract())
        residence_type = self.check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharResType"]/text()').extract())
        units = self.check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharApts"]/text()').extract())
        construction_type = self.check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharExtConst"]/text()').extract())
        full_bathrooms = self.check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharFullBaths"]/text()').extract())
        half_bathrooms = self.check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharHalfBaths"]/text()').extract())
        basement = self.check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharBasement"]/text()').extract())
        central_air = self.check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharCentAir"]/text()').extract())
        garage_type = self.check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharGarage"]/text()').extract())
        age = self.check_path(response.xpath('//*[@id="ctl00_phArticle_ctlPropertyDetails_lblPropCharAge"]/text()').extract())

        home_owner_exemption = self.check_path(response.xpath('//*[@id="exemptions"]/div[2]/div[1]/span[2]/text()').extract())
        senior_citizen_exemption = self.check_path(response.xpath('//*[@id="exemptions"]/div[2]/div[2]/span[2]/text()').extract())

        item = response.meta['item']
        item['parcel'] = str(parcel).replace('-', '')
        item['current_year_assessed_value'] = float(str(current_year_assessed_value).replace(',', '').replace('$',""))
        item['prior_year_assessed_value'] = float(str(prior_year_assessed_value).replace(',', '').replace('$',""))
        item['property_use'] = property_use
        item['residence_type'] = residence_type
        item['units'] = int(units)
        item['construction_type'] = construction_type
        item['full_bathrooms'] = float(full_bathrooms)
        item['half_bathrooms'] = float(half_bathrooms)
        item['basement'] = basement
        item['central_air'] = central_air
        item['garage_type'] = garage_type
        item['age'] = int(age)

        item['home_owner_exemption'] = home_owner_exemption
        item['senior_citizen_exemption'] = senior_citizen_exemption

        return [item]
