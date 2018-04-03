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

def getStartUrlFilePath(parcel_file):
    relative_file_path = 'ParcelsLists/'+parcel_file
    dirname = os.getcwd()
    filepath = os.path.join(os.getcwd(), relative_file_path)
    start_url_path = "file://"+filepath
    return start_url_path

def check_path(xpath_return):
    if len(xpath_return) == 1:
        return xpath_return[0]
    else:
        return None

class PierceCountyScraper(CSVFeedSpider):
    name = "pierce_county_spider"
    start_urls = [getStartUrlFilePath("parcels.csv")]
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

    #Chain data extraction and consolidate into one item
    def parse_summary(self, response):

        parcel = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[1]/table/tr[2]/td[2]/text()').extract())
        owner_name = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[2]/table/tr[2]/td[2]/text()').extract())
        site_address = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[1]/table/tr[3]/td[2]/text()').extract())
        mailing_address_street = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[2]/table/tr[3]/td[2]/text()[1]').extract())
        mailing_address_city_zip = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[2]/tr/td[2]/table/tr[3]/td[2]/text()[2]').extract())

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

        tax_year_1 = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[2]/td[1]/text()').extract())
        tax_year_2 = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[3]/td[1]/text()').extract())
        tax_year_3 = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[4]/td[1]/text()').extract())
        tax_year_1_assessed = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[2]/td[4]/text()').extract())
        tax_year_2_assessed = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[3]/td[4]/text()').extract())
        tax_year_3_assessed = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr/td/table/tr[2]/td/table/tr[4]/td[4]/text()').extract())
        current_balance_due = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[4]/tr/td[1]/table[1]/tr[4]/td/table/tr[1]/td/table/tr/td[1]/strong/text()').extract())

       #------------------------------Format Money Values(Ints/Floats)-----------------------------------#
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
        lot_square_footage = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[1]/td[2]/table/tr[2]/td[2]/text()').extract())
        acres = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[1]/td[2]/table/tr[3]/td[2]/text()').extract())

        electric = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[2]/td[2]/table/tr[2]/td[2]/text()').extract())
        sewer = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[2]/td[2]/table/tr[3]/td[2]/text()').extract())
        water = check_path(response.xpath('//*[@id="customContent"]/table/tr[1]/td/table[3]/tr[2]/td[2]/table/tr[4]/td[2]/text()').extract())

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
    start_urls = [getStartUrlFilePath("duval_parcels.csv")]
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
    start_urls = [getStartUrlFilePath("cook_parcels.csv")]
    custom_settings = {'ITEM_PIPELINES': {'property_data_spideys.pipelines.CookFullPipeline': 300}}

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

    def parse_summary(self, response):
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

        print(tax_year0,tax_year1,tax_year2,tax_year3,tax_year4)

        #Taxes Paid

        # Check if it was paid
        paid_in_full_amount_year0 = check_path(response.xpath('//*[@id="taxpaid'+tax_year1.split(":")[0]+'-button"]/span/text()').extract())
        paid_in_full_amount_year1 = check_path(response.xpath('//*[@id="taxpaid'+tax_year2.split(":")[0]+'-button"]/span/text()').extract())

        #Tax sales on record
        year_0_tax_sale_indicator = check_path(response.xpath('//*[@id="taxsaleredeemed'+tax_year0.split(":")[0]+'-button"]/span/text()').extract())
        year_1_tax_sale_indicator = check_path(response.xpath('//*[@id="taxsaleredeemed'+tax_year1.split(":")[0]+'-button"]/span/text()').extract())
        year_2_tax_sale_indicator = check_path(response.xpath('//*[@id="taxsaleredeemed'+tax_year2.split(":")[0]+'-button"]/span/text()').extract())
        year_3_tax_sale_indicator = check_path(response.xpath('//*[@id="taxsaleredeemed'+tax_year3.split(":")[0]+'-button"]/span/text()').extract())
        year_4_tax_sale_indicator = check_path(response.xpath('//*[@id="taxsaleredeemed'+tax_year4.split(":")[0]+'-button"]/span/text()').extract())


        #--------------------------------------------------------Documents Recorded--------------------------------------------------------------------#
        doc_rec1 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[1]/td/div[1]/div/text()').extract())
        doc_rec2 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[2]/td/div[1]/div/text()').extract())
        doc_rec3 = check_path(response.xpath('///*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[3]/td/div[1]/div/text()').extract())
        doc_rec4 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[4]/td/div[1]/div/text()').extract())
        doc_rec5 = check_path(response.xpath('//*[@id="ContentPlaceHolder1_success"]/div/div[5]/div[3]/table/tr[5]/td/div[1]/div/text()').extract())


        #Tax Sales Flag
        taxes_sold,foreclosure= 0,0
        foreclosure_date= 'NA'
        sale_indicator_list_filter,doc_list = [],[]


        for x in year_0_tax_sale_indicator,year_1_tax_sale_indicator,year_2_tax_sale_indicator,year_3_tax_sale_indicator,year_4_tax_sale_indicator:
            if x == None:
                sale_indicator_list_filter.append('NA')
            else:
                sale_indicator_list_filter.append(x)

        indicator_string = '\t'.join(sale_indicator_list_filter)

        if "Taxes Sold" in indicator_string:
            taxes_sold = 1

        #Tax Paid Flag
        if paid_in_full_amount_year0 != None:
            tax_paid_year0 = "Paid_Full"
        else:
            tax_paid_year0 = "Not_Paid"

        if paid_in_full_amount_year1 != None:
            tax_paid_year1 = "Paid_Full"
        else:
            tax_paid_year1 = "Not_Paid"

        #Foreclosure Flag
        for x in doc_rec1,doc_rec2,doc_rec3,doc_rec4,doc_rec5:
            if x == None:
                doc_list.append('NA')
            else:
                doc_list.append(x)

        #Check if Lis Pendes Foreclosure
        print(doc_list)
        for x in doc_list:
            print(x)
            x_split = x.split("-")
            print("hellll00")
            print(len(x_split))
            print(x_split)
            if "FORECLOSURE" in x_split[1]:
                print("CHACHNIGGGGGG")
                foreclosure = 1
                foreclosure_date = x_split[1]

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

        item['taxes_sold'] = taxes_sold
        item['tax_paid_year0'] = tax_paid_year0
        item['tax_paid_year1'] = tax_paid_year1
        item['foreclosure'] = foreclosure
        item['foreclosure_date'] = foreclosure_date

        item['lot_square_footage'] = int(str(lot_square_footage).replace(',', ''))
        item['building_square_footage'] = int(str(building_square_footage).replace(',', ''))

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


class MaricopaAPI(CSVFeedSpider):
    name = "maricopa_county_worker"
    authorization_token = '5ae1363b-28b8-11e8-9917-00155da2c015'

    start_urls = [getStartUrlFilePath("maricopa_parcels.csv")]

    custom_settings = {'ITEM_PIPELINES': {'property_data_spideys.pipelines.MaricopaFullPipeline': 400}}

    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)

    def spider_closed(self, spider):
        pass

    def parse_row(self,response,row):
        pin = row['parcel']
        headerss = {"X-MC-AUTH":"%s" %(MaricopaAPI.authorization_token)}
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


        item['owner_name'] = owner_name
        item['owner_full_address'] = owner_full_address
        item['owner_street_address1'] = owner_street_address1
        item['owner_city'] = owner_city
        item['owner_state'] = owner_state
        item['owner_zip'] = owner_zip
        item['full_property_address'] = full_property_address
        item['site_street'] = site_street
        item['site_city'] = site_city
        item['site_zip'] = site_zip
        item['property_description'] = property_description
        item['lot_size'] = lot_size
        item['property_type'] = property_type
        item['rental'] = rental
        item['value_0'] = value_0
        item['value_1'] = value_1
        item['value_2'] = value_2
        item['last_deed_date'] = last_deed_date
        item['last_sale_price'] = last_sale_price

        yield item