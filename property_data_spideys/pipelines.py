from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData
from property_data_spideys.models import PierceCountyPropertyDescriptionData,PierceCountyPropertySalesData,db_connect,create_table,cloneTable
from sqlalchemy import Table
from sqlalchemy.engine import create_engine
from alembic.config import Config
import alembic
from alembic.script import ScriptDirectory
from alembic.runtime.environment import EnvironmentContext
import functools
from alembic import op


#Base Pierce pipeline- performs full table refresh
class PierceFullPipeline(object):
    def __init__(self):
        self.engine = db_connect()
        create_table(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def check_spider_pipeline(process_item_method):
        @functools.wraps(process_item_method)
        def wrapper(self, item, spider):
            if self.__class__ in spider.pipeline:
                return process_item_method(self, item, spider)
            else:
                return item
        return wrapper

    def open_spider(self, spider):
        spider.myPipeline = self
        self.create_temp_tables()

    def close_spider(self,spider):
        self.upgrade()

    def create_temp_tables(self):
        meta = MetaData(bind=self.engine)
        self.propertyTemp = cloneTable('propertyTemp', PierceCountyPropertyDescriptionData.__table__, meta)
        self.salesTemp = cloneTable('salesTemp', PierceCountyPropertySalesData.__table__, meta)
        self.propertyTemp.create()
        self.salesTemp.create()

    def upgrade(self):
        op.drop_table('piercecounty')
        op.drop_table('piercecountysales')
        op.rename_table('propertyTemp', 'piercecounty')
        op.rename_table('salesTemp', 'piercecountysales')
        op.drop_table('salesTemp')
        op.drop_table('propertyTemp')


    @check_spider_pipeline
    def process_item(self,item,spider):

        session = self.Session()

        #Build a row
        # self.propertyTemp = PierceCountyPropertyDescriptionData()
        # self.salesDataTemp = PierceCountyPropertySalesData()

        self.propertyTemp.parcel = item["parcel"]
        self.propertyTemp.mailing_address = item["mailing_address"]
        self.propertyTemp.owner_name = item["owner_name"]
        self.propertyTemp.county = item["county"]
        self.propertyTemp.site_address = item["site_address"]
        self.propertyTemp.property_type = item["property_type"]
        self.propertyTemp.occupancy = item["occupancy"]
        self.propertyTemp.year_built = item["year_built"]
        self.propertyTemp.adj_year_built = item["adj_year_built"]
        self.propertyTemp.units = item["units"]
        self.propertyTemp.bedrooms = item["bedrooms"]
        self.propertyTemp.baths = item["baths"]
        self.propertyTemp.siding_type = item["siding_type"]
        self.propertyTemp.stories = item["stories"]
        self.propertyTemp.lot_square_footage = item["lot_square_footage"]
        self.propertyTemp.lot_acres = item["lot_acres"]
        self.propertyTemp.current_balance_due = item["current_balance_due"]
        self.propertyTemp.tax_year_1 = item["tax_year_1"]
        self.propertyTemp.tax_year_2 = item["tax_year_2"]
        self.propertyTemp.tax_year_3 = item["tax_year_3"]
        self.propertyTemp.tax_year_1_assessed = item["tax_year_1_assessed"]
        self.propertyTemp.tax_year_2_assessed = item["tax_year_2_assessed"]
        self.propertyTemp.tax_year_3_assessed = item["tax_year_3_assessed"]

        self.salesTemp.tax_year_1_assessed = item["sale_price"]
        self.salesTemp.tax_year_2_assessed = item["sale_date"]

        try:
            session.add(self.propertyTemp)
            session.add(self.salesTemp)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
        return item

#Base Pierce pipeline- performs full table refresh
class PierceRowPipeline(object):
    def __init__(self):
        self.engine = db_connect()
        create_table(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def check_spider_pipeline(process_item_method):
        @functools.wraps(process_item_method)
        def wrapper(self, item, spider):
            if self.__class__ in spider.pipeline:
                return process_item_method(self, item, spider)
            else:
                return item
        return wrapper

    def open_spider(self, spider):
        spider.myPipeline = self

    def close_spider(self,spider):
        pass

    @check_spider_pipeline
    def process_item(self,item,spider):

        session = self.Session()

        #Build a row
        self.propertyTemp = PierceCountyPropertyDescriptionData()
        self.salesDataTemp = PierceCountyPropertySalesData()

        self.propertyTemp.parcel = item["parcel"]
        self.propertyTemp.mailing_address = item["mailing_address"]
        self.propertyTemp.owner_name = item["owner_name"]
        self.propertyTemp.county = item["county"]
        self.propertyTemp.site_address = item["site_address"]
        self.propertyTemp.property_type = item["property_type"]
        self.propertyTemp.occupancy = item["occupancy"]
        self.propertyTemp.year_built = item["year_built"]
        self.propertyTemp.adj_year_built = item["adj_year_built"]
        self.propertyTemp.units = item["units"]
        self.propertyTemp.bedrooms = item["bedrooms"]
        self.propertyTemp.baths = item["baths"]
        self.propertyTemp.siding_type = item["siding_type"]
        self.propertyTemp.stories = item["stories"]
        self.propertyTemp.lot_square_footage = item["lot_square_footage"]
        self.propertyTemp.lot_acres = item["lot_acres"]
        self.propertyTemp.current_balance_due = item["current_balance_due"]
        self.propertyTemp.tax_year_1 = item["tax_year_1"]
        self.propertyTemp.tax_year_2 = item["tax_year_2"]
        self.propertyTemp.tax_year_3 = item["tax_year_3"]
        self.propertyTemp.tax_year_1_assessed = item["tax_year_1_assessed"]
        self.propertyTemp.tax_year_2_assessed = item["tax_year_2_assessed"]
        self.propertyTemp.tax_year_3_assessed = item["tax_year_3_assessed"]

        self.salesDataTemp.tax_year_1_assessed = item["sale_price"]
        self.salesDataTemp.tax_year_2_assessed = item["sale_date"]

        try:
            session.add(self.propertyTemp)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item



# class PierceDataPropertyDescriptionPipeline(object):
#     def __init__(self):
#         """
#         Initializes database connection and sessionmaker.
#         Creates tables.
#         """
#         engine = db_connect()
#         create_table(engine)
#         self.Session = sessionmaker(bind=engine)
#
#     def process_item(self,item,spider):
#         """
#         This method is called for every item pipeline component
#         """
#         session = self.Session()
#
#         #Tables
#         salesData = PierceCountyPropertySalesData()
#         salesData.parcel = item["parcel"]
#         sales
#
#
#         try:
#             session.add(salesData)
#             session.commit()
#         except:
#             session.rollback()
#             raise
#         finally:
#             session.close()
#
#         return item