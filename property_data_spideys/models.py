from sqlalchemy import create_engine, Column, Table, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Integer, SmallInteger, String, Date, DateTime, Float, Boolean, Text, LargeBinary)
from scrapy.utils.project import get_project_settings

DeclarativeBase = declarative_base()

def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(get_project_settings().get("CONNECTION_STRING"))

def create_table(engine):
    DeclarativeBase.metadata.create_all(engine)

def cloneTable(name,table,metadata):
    cols = [c.copy() for c in table.columns]
    constraints = [c.copy() for c in table.constraints]
    return Table(name, metadata, *(cols + constraints))

class PierceCountyPropertyDescriptionData(DeclarativeBase):
    __tablename__ = "pierceCounty"
    parcel = Column('parcel',String(20),primary_key=True)
    mailing_address = Column('mailing_address', Text())
    owner_name = Column('owner_name', Text())

    site_address = Column('site_address', Text())
    property_type = Column('property_type', Text())
    occupancy = Column('occupancy', Text())
    year_built = Column('year_built', Text())
    adj_year_built = Column('adj_year_built', Text())
    units = Column('units', Text())
    bedrooms = Column('bedrooms', Text())
    baths = Column('baths', Text())
    siding_type = Column('siding_type', Text())
    stories = Column('stories', Text())
    lot_square_footage = Column('lot_square_footage', Text())
    lot_acres = Column('lot_acres', Text())

    current_balance_due = Column('current_balance_due', Text())

    tax_year_1 = Column('tax_year_1', Text())
    tax_year_2 = Column('tax_year_2', Text())
    tax_year_3 = Column('tax_year_3', Text())
    tax_year_1_assessed = Column('tax_year_1_assessed', Text())
    tax_year_2_assessed = Column('tax_year_2_assessed', Text())
    tax_year_3_assessed = Column('tax_year_3_assessed', Text())

class PierceCountyPropertySalesData(DeclarativeBase):
    __tablename__ = "pierceCountySales"
    id = Column(Integer, primary_key=True)
    parcel = Column('parcel', String(20))
    sale_price = Column('sale_price', Text())
    sale_date = Column('sale_date', Text())

