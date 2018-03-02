from sqlalchemy.orm import sessionmaker
from property_data_spideys.models import OwnerInfo,PropertyDescription,Taxes,Sales,db_connect,create_table

class PropertyDataScraperPipeline(object):
    def __init__(self):
        """
        Initializes database connection and sessionmaker.
        Creates deals table.
        """
        engine = db_connect()
        create_table(engine)
        self.Session = sessionmaker(bind=engine)

    def process_item(self,item,spider):
        """
        This method is called for every item pipeline component
        """
        session = self.Session()

        #Tables
        ownerInfo = OwnerInfo()
        propertyDescription = PropertyDescription()
        taxes = Taxes()
        sales = Sales()

        ownerInfo.parcel = propertyDescription.parcel = taxes.parcel = sales.parcel = item["parcel"]
        ownerInfo.mailing_address = item["mailing_address"]

        propertyDescription.site_address = item["site_address"]
        propertyDescription.property_type = item["property_type"]
        propertyDescription.occupancy = item["occupancy"]
        propertyDescription.year_built = item["year_built"]
        propertyDescription.adj_year_built = item["adj_year_built"]
        propertyDescription.units = item["units"]
        propertyDescription.bedrooms = item["bedrooms"]
        propertyDescription.baths = item["baths"]
        propertyDescription.siding_type = item["siding_type"]
        propertyDescription.stories = item["stories"]
        propertyDescription.lot_square_footage = item["lot_square_footage"]
        propertyDescription.lot_acres = item["lot_acres"]

        taxes.current_balance_due = item["current_balance_due"]
        taxes.tax_year_1 = item["tax_year_1"]
        taxes.tax_year_2 = item["tax_year_2"]
        taxes.tax_year_3 = item["tax_year_3"]
        taxes.tax_year_1_assessed = item["tax_year_1_assessed"]
        taxes.tax_year_2_assessed = item["tax_year_2_assessed"]
        taxes.tax_year_3_assessed = item["tax_year_3_assessed"]

        sales.sale1_price = item["sale1_price"]
        sales.sale1_date = item["sale1_date"]
        sales.sale2_date = item["sale2_date"]
        sales.sale2_price = item["sale2_price"]

        try:
            session.add(ownerInfo)
            session.add(propertyDescription)
            session.add(taxes)
            session.add(sales)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item


