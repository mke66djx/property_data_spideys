from property_data_spideys.models import CookPropertyDataTemp,CookCountyPropertyData,PiercePropertyDataTemp,PierceSalesDataTemp,DuvalSalesDataTemp,DuvalPropertyDataTemp,db_connect,create_table
import functools
from sqlalchemy.orm import (mapper,sessionmaker)


#---------------------------------------------------------------------------------------------------------
#-----------------------------------------Full Table Update Pipelines-------------------------------------
#---------------------------------------------------------------------------------------------------------

class PierceFullPipeline(object):
    def __init__(self):
        self.engine = db_connect()
        create_table(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def open_spider(self, spider):
        spider.myPipeline = self

    def close_spider(self,spider):
        self.upgrade()

    def upgrade(self):
        from alembic.config import Config
        from alembic import command
        alembic_cfg = Config(r"C:/Users/ebeluli/Desktop/property_data_spideys/alembic.ini")
        with self.engine.begin() as connection:
            alembic_cfg.attributes['connection'] = connection
            command.upgrade(alembic_cfg, "65c4237f4c27")
            command.downgrade(alembic_cfg, "base")

    def process_item(self,item,spider):
        session = self.Session()

        #Build a row
        propertyDataTemp = PiercePropertyDataTemp()
        salesDataTemp = PierceSalesDataTemp()

        propertyDataTemp.parcel = item["parcel"]
        propertyDataTemp.mailing_address = item["mailing_address"]
        propertyDataTemp.owner_name = item["owner_name"]
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
        propertyDataTemp.lot_square_footage = item["lot_square_footage"]
        propertyDataTemp.lot_acres = item["lot_acres"]
        propertyDataTemp.current_balance_due = item["current_balance_due"]
        propertyDataTemp.tax_year_1 = item["tax_year_1"]
        propertyDataTemp.tax_year_2 = item["tax_year_2"]
        propertyDataTemp.tax_year_3 = item["tax_year_3"]
        propertyDataTemp.tax_year_1_assessed = item["tax_year_1_assessed"]
        propertyDataTemp.tax_year_2_assessed = item["tax_year_2_assessed"]
        propertyDataTemp.tax_year_3_assessed = item["tax_year_3_assessed"]

        salesDataTemp.tax_year_1_assessed = item["sale_price"]
        salesDataTemp.tax_year_2_assessed = item["sale_date"]

        try:
            session.add(propertyDataTemp)
            session.add(salesDataTemp)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return item


class DuvalFullPipeline(object):
    def __init__(self):
        self.engine = db_connect()
        create_table(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def open_spider(self, spider):
        spider.myPipeline = self

    def close_spider(self,spider):
        self.upgrade()

    def upgrade(self):
        from alembic.config import Config
        from alembic import command
        alembic_cfg = Config(r"C:/Users/ebeluli/Desktop/property_data_spideys/alembic.ini")
        with self.engine.begin() as connection:
            alembic_cfg.attributes['connection'] = connection
            command.upgrade(alembic_cfg, "2849d9e93551")
            command.downgrade(alembic_cfg, "base")

    def process_item(self,item,spider):
        session = self.Session()

        #Build a row
        propertyDataTemp = DuvalPropertyDataTemp()
        salesDataTemp = DuvalSalesDataTemp()

        propertyDataTemp.parcel = item["parcel"]
        propertyDataTemp.mailing_address = item["mailing_address"]
        propertyDataTemp.owner_name = item["owner_name"]
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
        propertyDataTemp.lot_square_footage = item["lot_square_footage"]
        propertyDataTemp.lot_acres = item["lot_acres"]
        #propertyDataTemp.current_balance_due = item["current_balance_due"]
        propertyDataTemp.tax_year_1 = item["tax_year_1"]
        propertyDataTemp.tax_year_2 = item["tax_year_2"]
        propertyDataTemp.tax_year_3 = item["tax_year_3"]
        propertyDataTemp.tax_year_1_assessed = item["tax_year_1_assessed"]
        propertyDataTemp.tax_year_2_assessed = item["tax_year_2_assessed"]
        propertyDataTemp.tax_year_3_assessed = item["tax_year_3_assessed"]

        salesDataTemp.tax_year_1_assessed = item["sale1_price"]
        salesDataTemp.tax_year_2_assessed = item["sale1_date"]

        try:
            session.add(propertyDataTemp)
            session.add(salesDataTemp)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return item

class CookFullPipeline(object):
    def __init__(self):
        self.engine = db_connect()
        create_table(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def open_spider(self, spider):
        spider.myPipeline = self

    def close_spider(self,spider):
        self.upgrade()

    def upgrade(self):
        from alembic.config import Config
        from alembic import command
        alembic_cfg = Config(r"C:/Users/ebeluli/Desktop/property_data_spideys/alembic.ini")
        with self.engine.begin() as connection:
            alembic_cfg.attributes['connection'] = connection
            command.upgrade(alembic_cfg, "110ec6566dca")
            command.downgrade(alembic_cfg, "base")

    def process_item(self,item,spider):
        session = self.Session()

        #Build a row
        propertyDataTemp = CookPropertyDataTemp()

        propertyDataTemp.parcel = item["parcel"]
        propertyDataTemp.site_address_street = item["site_address_street"]
        propertyDataTemp.site_address_city = item["site_address_city"]
        propertyDataTemp.owner_name = item["owner_name"]
        propertyDataTemp.site_address_zip = item["site_address_zip"]
        propertyDataTemp.mailing_address_city_zip_state = item["mailing_address_city_zip_state"]
        propertyDataTemp.lot_square_footage = item["lot_square_footage"]
        propertyDataTemp.building_square_footage = item["building_square_footage"]
        propertyDataTemp.current_year_assessed_value = item["current_year_assessed_value"]
        propertyDataTemp.tax_year_3_assessed = item["prior_year_assessed_value"]
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


        try:
            session.add(propertyDataTemp)
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

class PierceRowPipeline(object):
    def __init__(self):
        self.engine = db_connect()
        create_table(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def open_spider(self, spider):
        spider.myPipeline = self

    def close_spider(self,spider):
        pass

    def process_item(self,item,spider):

        session = self.Session()

        #Build a row
        propertyDataTemp = PiercePropertyDataTemp()
        salesDataTemp = PierceSalesDataTemp()

        propertyDataTemp.parcel = item["parcel"]
        propertyDataTemp.mailing_address = item["mailing_address"]
        propertyDataTemp.owner_name = item["owner_name"]
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
        propertyDataTemp.lot_square_footage = item["lot_square_footage"]
        propertyDataTemp.lot_acres = item["lot_acres"]
        propertyDataTemp.current_balance_due = item["current_balance_due"]
        propertyDataTemp.tax_year_1 = item["tax_year_1"]
        propertyDataTemp.tax_year_2 = item["tax_year_2"]
        propertyDataTemp.tax_year_3 = item["tax_year_3"]
        propertyDataTemp.tax_year_1_assessed = item["tax_year_1_assessed"]
        propertyDataTemp.tax_year_2_assessed = item["tax_year_2_assessed"]
        propertyDataTemp.tax_year_3_assessed = item["tax_year_3_assessed"]

        salesDataTemp.tax_year_1_assessed = item["sale_price"]
        salesDataTemp.tax_year_2_assessed = item["sale_date"]

        try:
            session.add(propertyDataTemp)
            session.add(salesDataTemp)

            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item

class DuvalRowPipeline(object):
    def __init__(self):
        self.engine = db_connect()
        create_table(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def open_spider(self, spider):
        spider.myPipeline = self

    def close_spider(self,spider):
        pass

    #@check_spider_pipeline
    def process_item(self,item,spider):
        session = self.Session()

        #Build a row
        propertyDataTemp = DuvalPropertyDataTemp()
        salesDataTemp = DuvalSalesDataTemp()

        propertyDataTemp.parcel = item["parcel"]
        propertyDataTemp.mailing_address = item["mailing_address"]
        propertyDataTemp.owner_name = item["owner_name"]
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
        propertyDataTemp.lot_square_footage = item["lot_square_footage"]
        propertyDataTemp.lot_acres = item["lot_acres"]
        propertyDataTemp.current_balance_due = item["current_balance_due"]
        propertyDataTemp.tax_year_1 = item["tax_year_1"]
        propertyDataTemp.tax_year_2 = item["tax_year_2"]
        propertyDataTemp.tax_year_3 = item["tax_year_3"]
        propertyDataTemp.tax_year_1_assessed = item["tax_year_1_assessed"]
        propertyDataTemp.tax_year_2_assessed = item["tax_year_2_assessed"]
        propertyDataTemp.tax_year_3_assessed = item["tax_year_3_assessed"]

        salesDataTemp.tax_year_1_assessed = item["sale_price"]
        salesDataTemp.tax_year_2_assessed = item["sale_date"]

        try:
            session.add(propertyDataTemp)
            session.add(salesDataTemp)

            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item
