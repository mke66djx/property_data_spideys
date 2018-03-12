from property_data_spideys.models import PierceCountyPropertyData,PierceCountySalesData,PiercePropertyDataTemp,PierceSalesDataTemp,DuvalSalesDataTemp,DuvalPropertyDataTemp,db_connect,create_table
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
