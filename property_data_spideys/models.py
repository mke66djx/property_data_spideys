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

class OwnerInfo(DeclarativeBase):
    __tablename__ = "ownerInfo"
    parcel = Column('parcel',String(20),primary_key=True)
    mailing_address = Column('mailing_address', Text())

class PropertyDescription(DeclarativeBase):
    __tablename__ = "buildingDescription"
    parcel = Column('parcel',String(20),primary_key=True)
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

class Taxes(DeclarativeBase):
    __tablename__ = "taxes"
    parcel = Column('parcel',String(20),primary_key=True)
    current_balance_due = Column('current_balance_due', Text())
    tax_year_1 = Column('tax_year_1', Text())
    tax_year_2 = Column('tax_year_2', Text())
    tax_year_3 = Column('tax_year_3', Text())
    tax_year_1_assessed = Column('tax_year_1_assessed', Text())
    tax_year_2_assessed = Column('tax_year_2_assessed', Text())
    tax_year_3_assessed = Column('tax_year_3_assessed', Text())

class Sales(DeclarativeBase):
    __tablename__ = "sales"
    parcel = Column('parcel',String(20),primary_key=True)
    sale1_price = Column('sale1_price', Text())
    sale1_date = Column('sale1_date', Text())
    sale2_date = Column('sale2_date', Text())
    sale2_price = Column('sale2_price', Text())

