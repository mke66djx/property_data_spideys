from property_data_spideys.models import DuvalCountyPropertyData,PierceCountyPropertyData,MaricopaCountyPropertyData,CookCountyPropertyData,CookPropertyDataTemp,MaricopaPropertyDataTemp,PiercePropertyDataTemp,PierceSalesDataTemp,DuvalSalesDataTemp,DuvalPropertyDataTemp,db_connect,create_table
import functools
from sqlalchemy.orm import (mapper,sessionmaker)
from scrapy.exceptions import DropItem
#---------------------------------------------------------------------------------------------------------
#-----------------------------------------Full Table Update Pipelines-------------------------------------
#---------------------------------------------------------------------------------------------------------
#(Non Blocking) - Used to replace/update an entire table
# by first creatinga temp through series of inserts
class PierceFullPipeline(object):
    def __init__(self):
        self.engine = db_connect()
        create_table(self.engine,PiercePropertyDataTemp)
        create_table(self.engine,PierceCountyPropertyData)
        self.Session = sessionmaker(bind=self.engine)

    def open_spider(self, spider):
        spider.myPipeline = self

    def close_spider(self,spider):
        self.upgrade()

    def upgrade(self):
        PierceCountyPropertyData.__table__.drop(self.engine)
        from alembic.config import Config
        from alembic import command
        alembic_cfg = Config('alembic.ini')
        print(alembic_cfg)
        with self.engine.begin() as connection:
            alembic_cfg.attributes['connection'] = connection
            command.upgrade(alembic_cfg, "65c4237f4c27")
            command.downgrade(alembic_cfg, "base")

    def process_item(self,item,spider):
        session = self.Session()

        #Build a row
        propertyDataTemp = PiercePropertyDataTemp()

        propertyDataTemp.parcel = item["parcel"]
        propertyDataTemp.owner_name = item["owner_name"]
        propertyDataTemp.owner_first_name = item["owner_first_name"]
        propertyDataTemp.owner_2_first = item["owner_2_first"]
        propertyDataTemp.owner_last_name = item["owner_last_name"]
        propertyDataTemp.mailing_address = item["mailing_address"]
        propertyDataTemp.mail_city = item["mail_city"]
        propertyDataTemp.mail_state = item["mail_state"]
        propertyDataTemp.mail_zip = item["mail_zip"]
        propertyDataTemp.site_address = item["site_address"]
        propertyDataTemp.property_type = item["property_type"]
        propertyDataTemp.occupancy = item["occupancy"]
        propertyDataTemp.year_built = item["year_built"]
        propertyDataTemp.adj_year_built = item["adj_year_built"]
        propertyDataTemp.units = item["units"]
        propertyDataTemp.bedrooms = item["bedrooms"]
        propertyDataTemp.baths = item["baths"]
        propertyDataTemp.siding_type = item["siding_type"]
        propertyDataTemp.stories = item["stories"]
        propertyDataTemp.building_square_footage = item["building_square_footage"]
        propertyDataTemp.attached_garage_footage = item["attached_garage_footage"]
        propertyDataTemp.lot_square_footage = item["lot_square_footage"]
        propertyDataTemp.lot_acres = item["lot_acres"]
        propertyDataTemp.current_balance_due = item["current_balance_due"]
        propertyDataTemp.tax_year_1 = item["tax_year_1"]
        propertyDataTemp.tax_year_2 = item["tax_year_2"]
        propertyDataTemp.tax_year_3 = item["tax_year_3"]
        propertyDataTemp.tax_year_1_assessed = item["tax_year_1_assessed"]
        propertyDataTemp.tax_year_2_assessed = item["tax_year_2_assessed"]
        propertyDataTemp.tax_year_3_assessed = item["tax_year_3_assessed"]

        propertyDataTemp.exemption = item["exemption"]

        try:
            session.add(propertyDataTemp)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return item

#(Non Blocking) - Used to replace/update an entire table
# by first creatinga temp through series of inserts
class DuvalFullPipeline(object):
    def __init__(self):
        self.engine = db_connect()
        create_table(self.engine,DuvalPropertyDataTemp)
        create_table(self.engine,DuvalCountyPropertyData)
        self.Session = sessionmaker(bind=self.engine)

    def open_spider(self, spider):
        spider.myPipeline = self

    def close_spider(self,spider):
        self.upgrade()

    def upgrade(self):
        DuvalCountyPropertyData.__table__.drop(self.engine)
        from alembic.config import Config
        from alembic import command
        alembic_cfg = Config('alembic.ini')
        with self.engine.begin() as connection:
            alembic_cfg.attributes['connection'] = connection
            command.upgrade(alembic_cfg, "2849d9e93551")
            command.downgrade(alembic_cfg, "base")

    def process_item(self,item,spider):
        session = self.Session()

        #Build a row
        propertyDataTemp = DuvalPropertyDataTemp()

        propertyDataTemp.parcel = item["parcel"]
        propertyDataTemp.mailing_address = item["mailing_address"]
        propertyDataTemp.owner_name = item["owner_name"]
        propertyDataTemp.site_address = item["site_address"]
        propertyDataTemp.property_type = item["property_type"]
        propertyDataTemp.building_type = item["building_type"]
        propertyDataTemp.year_built = item["year_built"]
        propertyDataTemp.building_square_footage = item["building_square_footage"]
        propertyDataTemp.units = item["units"]
        propertyDataTemp.bedrooms = item["bedrooms"]
        propertyDataTemp.baths = item["baths"]
        propertyDataTemp.siding_type = item["siding_type"]
        propertyDataTemp.stories = item["stories"]
        propertyDataTemp.lot_square_footage = item["lot_square_footage"]
        #propertyDataTemp.current_balance_due = item["current_balance_due"]
        propertyDataTemp.tax_year_1_assessed = item["tax_year_1_assessed"]
        propertyDataTemp.tax_year_2_assessed = item["tax_year_2_assessed"]
        propertyDataTemp.homestead_exemption = item["homestead_exemption"]
        propertyDataTemp.senior_exemption = item["senior_exemption"]

        #propertyDataTemp.tax_year_1_assessed = item["sale1_price"]
        #propertyDataTemp.tax_year_2_assessed = item["sale1_date"]


        try:
            session.add(propertyDataTemp)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return item

#(Non Blocking) - Used to replace/update an entire table
# by first creatinga temp through series of inserts
class CookFullPipeline(object):
    def __init__(self):
        self.engine = db_connect()
        create_table(self.engine,CookPropertyDataTemp)
        create_table(self.engine,CookCountyPropertyData)
        self.Session = sessionmaker(bind=self.engine)

    def open_spider(self, spider):
        spider.myPipeline = self

    def close_spider(self,spider):
        self.upgrade()

    def upgrade(self):
        CookCountyPropertyData.__table__.drop(self.engine)
        from alembic.config import Config
        from alembic import command
        alembic_cfg = Config('alembic.ini')
        with self.engine.begin() as connection:
            alembic_cfg.attributes['connection'] = connection
            command.upgrade(alembic_cfg, "110ec6566dca")
            command.downgrade(alembic_cfg, "base")

    def process_item(self,item,spider):

        session = self.Session()
        propertyDataTemp = CookPropertyDataTemp()

        if(item["parcel"] != 'NA'):

            propertyDataTemp.parcel = item["parcel"]
            propertyDataTemp.owner_name = item["owner_name"]
            propertyDataTemp.owner_first = item["owner_first"]
            propertyDataTemp.owner_last = item["owner_last"]
            propertyDataTemp.owner_2_first = item["owner_2_first"]

            propertyDataTemp.site_address = item["site_address"]
            propertyDataTemp.site_address_city = item["site_address_city"]
            propertyDataTemp.site_address_zip = item["site_address_zip"]
            propertyDataTemp.site_address_township = item["site_address_township"]

            propertyDataTemp.mailing_address = item["mailing_address"]
            propertyDataTemp.mail_city = item["mail_city"]
            propertyDataTemp.mail_state = item["mail_state"]
            propertyDataTemp.mail_zip = item["mail_zip"]

            propertyDataTemp.lot_square_footage = item["lot_square_footage"]
            propertyDataTemp.building_square_footage = item["building_square_footage"]
            propertyDataTemp.current_year_assessed_value = item["current_year_assessed_value"]
            propertyDataTemp.prior_year_assessed_value = item["prior_year_assessed_value"]
            propertyDataTemp.property_use = item["property_use"]
            propertyDataTemp.residence_type = item["residence_type"]
            propertyDataTemp.units = item["units"]
            propertyDataTemp.construction_type = item["construction_type"]
            propertyDataTemp.full_bathrooms = item["full_bathrooms"]
            propertyDataTemp.half_bathrooms = item["half_bathrooms"]
            propertyDataTemp.central_air = item["central_air"]
            propertyDataTemp.basement = item["basement"]
            propertyDataTemp.garage_type = item["garage_type"]
            propertyDataTemp.home_owner_exemption = item["home_owner_exemption"]
            propertyDataTemp.senior_citizen_exemption = item["senior_citizen_exemption"]

            propertyDataTemp.taxes_sold = item["taxes_sold"]
            propertyDataTemp.tax_paid_year0 = item["tax_paid_year0"]
            propertyDataTemp.tax_paid_year1 = item["tax_paid_year1"]
            propertyDataTemp.tax_paid_year0_amount = item["tax_paid_year0_amount"]
            propertyDataTemp.tax_paid_year1_amount = item["tax_paid_year1_amount"]


            propertyDataTemp.record1 = item["doc1_string"]
            propertyDataTemp.record2 = item["doc2_string"]
            propertyDataTemp.record3 = item["doc3_string"]
            propertyDataTemp.record4 = item["doc4_string"]
            propertyDataTemp.record5 = item["doc5_string"]
            propertyDataTemp.record1_date = item["doc1_date"]
            propertyDataTemp.record2_date = item["doc2_date"]
            propertyDataTemp.record3_date = item["doc3_date"]
            propertyDataTemp.record4_date = item["doc4_date"]
            propertyDataTemp.record5_date = item["doc5_date"]

            try:
                session.add(propertyDataTemp)
                session.commit()
            except:
                session.rollback()
                raise
            finally:
                session.close()
            return item
        else:
            raise DropItem("Invalid Parcel Found: %s" % item["parcel"])

#(Non Blocking) - Used to replace/update an entire table
# by first creating a temp through series of inserts
class MaricopaFullPipeline(object):
    def __init__(self):
        self.engine = db_connect()
        create_table(self.engine,MaricopaPropertyDataTemp)
        create_table(self.engine,MaricopaCountyPropertyData)
        self.Session = sessionmaker(bind=self.engine)

    def open_spider(self, spider):
        spider.myPipeline = self

    def close_spider(self,spider):
        self.upgrade()

    def upgrade(self):
        MaricopaCountyPropertyData.__table__.drop(self.engine)
        from alembic.config import Config
        from alembic import command
        alembic_cfg = Config('alembic.ini')
        with self.engine.begin() as connection:
            alembic_cfg.attributes['connection'] = connection
            command.upgrade(alembic_cfg, "9dcae9ce3303")
            command.downgrade(alembic_cfg, "base")

    def process_item(self,item,spider):
        session = self.Session()

        propertyDataTemp = MaricopaPropertyDataTemp()

        propertyDataTemp.parcel = item["parcel"]
        propertyDataTemp.owner_name = item["owner_name"]
        propertyDataTemp.owner_first = item["owner_first"]
        propertyDataTemp.owner_last = item["owner_last"]
        propertyDataTemp.owner_full_address = item["owner_full_address"]
        propertyDataTemp.owner_street_address1 = item["owner_street_address1"]
        propertyDataTemp.owner_city = item["owner_city"]
        propertyDataTemp.owner_state = item["owner_state"]
        propertyDataTemp.owner_zip = item["owner_zip"]
        propertyDataTemp.full_property_address = item["full_property_address"]
        propertyDataTemp.site_street = item["site_street"]
        propertyDataTemp.site_city = item["site_city"]
        propertyDataTemp.site_zip = item["site_zip"]
        propertyDataTemp.property_description = item["property_description"]
        propertyDataTemp.lot_size = item["lot_size"]
        propertyDataTemp.property_type = item["property_type"]
        #propertyDataTemp.current_balance_due = item["current_balance_due"]
        propertyDataTemp.rental = item["rental"]
        propertyDataTemp.value_0 = item["value_0"]
        propertyDataTemp.value_1 = item["value_1"]
        propertyDataTemp.value_2 = item["value_2"]

        propertyDataTemp.last_deed_date = item["last_deed_date"]
        propertyDataTemp.last_sale_price = item["last_sale_price"]


        try:
            session.add(propertyDataTemp)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return item

#(Blocking)- Used to add new items to existing Maricopa County table
class MaricopaAddToPipeline(object):
    def __init__(self):
        self.engine = db_connect()
        create_table(self.engine,MaricopaCountyPropertyData)
        self.Session = sessionmaker(bind=self.engine)

    def open_spider(self, spider):
        spider.myPipeline = self

    def close_spider(self,spider):
        pass

    def process_item(self,item,spider):
        session = self.Session()

        propertyData = MaricopaCountyPropertyData()

        propertyData.parcel = item["parcel"]
        propertyData.owner_name = item["owner_name"]
        propertyData.owner_first = item["owner_first"]
        propertyData.owner_last = item["owner_last"]
        propertyData.owner_full_address = item["owner_full_address"]
        propertyData.owner_street_address1 = item["owner_street_address1"]
        propertyData.owner_city = item["owner_city"]
        propertyData.owner_state = item["owner_state"]
        propertyData.owner_zip = item["owner_zip"]
        propertyData.full_property_address = item["full_property_address"]
        propertyData.site_street = item["site_street"]
        propertyData.site_city = item["site_city"]
        propertyData.site_zip = item["site_zip"]
        propertyData.property_description = item["property_description"]
        propertyData.lot_size = item["lot_size"]
        propertyData.property_type = item["property_type"]
        #propertyDataTemp.current_balance_due = item["current_balance_due"]
        propertyData.rental = item["rental"]
        propertyData.value_0 = item["value_0"]
        propertyData.value_1 = item["value_1"]
        propertyData.value_2 = item["value_2"]

        propertyData.last_deed_date = item["last_deed_date"]
        propertyData.last_sale_price = item["last_sale_price"]


        try:
            session.add(propertyData)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return item
#---------------------------------------------------------------------------------------------------------
#-----------------------------------------Row Update Pipelines--------------------------------------------
#---------------------------------------------------------------------------------------------------------
