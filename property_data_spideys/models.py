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

class PierceCountyPropertyData(DeclarativeBase):
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

class PierceCountySalesData(DeclarativeBase):
    __tablename__ = "pierceCountySales"
    id = Column(Integer, primary_key=True)
    parcel = Column('parcel', String(20))
    sale_price = Column('sale_price', Text())
    sale_date = Column('sale_date', Text())

class DuvalCountyPropertyData(DeclarativeBase):
    __tablename__ = "duvalCounty"
    parcel = Column('parcel',String(20),primary_key=True)
    mailing_address = Column('mailing_address', Text())
    owner_name = Column('owner_name', Text())

    site_address = Column('site_address', Text())
    property_type = Column('property_type', Text())
    year_built = Column('year_built', Text())
    adj_year_built = Column('adj_year_built', Text())
    units = Column('units', Text())
    bedrooms = Column('bedrooms', Text())
    baths = Column('baths', Text())
    siding_type = Column('siding_type', Text())
    stories = Column('stories', Text())
    building_square_footage = Column('building_square_footage', Text())
    attached_garage_footage = Column('attached_garage_footage', Text())
    lot_square_footage = Column('lot_square_footage', Text())
    lot_acres = Column('lot_acres', Text())

    # current_balance_due = Column('current_balance_due', Text())

    # tax_year_1 = Column('tax_year_1', Text())
    # tax_year_2 = Column('tax_year_2', Text())
    # tax_year_3 = Column('tax_year_3', Text())
    tax_year_1_assessed = Column('tax_year_1_assessed', Text())
    tax_year_2_assessed = Column('tax_year_2_assessed', Text())
    tax_year_3_assessed = Column('tax_year_3_assessed', Text())

class DuvalCountySalesData(DeclarativeBase):
    __tablename__ = "duvalCountySales"
    id = Column(Integer, primary_key=True)
    parcel = Column('parcel', String(20))
    sale_price = Column('sale1_price', Text())
    sale_date = Column('sale1_date', Text())


class CookCountyPropertyData(DeclarativeBase):
    __tablename__ = "cookCounty"
    parcel = Column('parcel',String(20),primary_key=True)
    owner_name = Column('owner_name', Text())
    site_address_street = Column('site_address_street', Text())
    site_address_city = Column('site_address_city', Text())
    site_address_zip = Column('site_address_zip', Text())
    mailing_address_city_zip_state = Column('mailing_address_city_zip_state', Text())
    lot_square_footage = Column('lot_square_footage', Text())
    building_square_footage = Column('building_square_footage', Text())
    current_year_assessed_value = Column('current_year_assessed_value', Text())
    prior_year_assessed_value = Column('prior_year_assessed_value', Text())
    property_use = Column('property_use', Text())
    residence_type = Column('residence_type', Text())
    units = Column('units', Text())
    construction_type = Column('construction_type', Text())
    full_bathrooms = Column('full_bathrooms', Text())
    half_bathrooms = Column('half_bathrooms', Text())
    central_air = Column('central_air', Text())
    basement = Column('basement', Text())
    garage_type = Column('garage_type', Text())
    home_owner_exemption = Column('home_owner_exemption', Text())
    senior_citizen_exemption = Column('senior_citizen_exemption', Text())

#--------------------------------------------------------------------------
#-------The temps below are used when doing full table updates-------------
#--------------------------------------------------------------------------
class PiercePropertyDataTemp(DeclarativeBase):
    __tablename__ = "pierceCountyTemp"
    parcel = Column('parcel',String(20),primary_key=True)
    mailing_address = Column('mailing_address', Text())
    owner_name = Column('owner_name', Text())

    site_address = Column('site_address', Text())
    property_type = Column('property_type', Text())
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

class PierceSalesDataTemp(DeclarativeBase):
    __tablename__ = "pierceCountySalesTemp"
    id = Column(Integer, primary_key=True)
    parcel = Column('parcel', String(20))
    sale_price = Column('sale_price', Text())
    sale_date = Column('sale_date', Text())


class DuvalPropertyDataTemp(DeclarativeBase):
    __tablename__ = "duvalCountyTemp"
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
    building_square_footage = Column('building_square_footage', Text())
    attached_garage_footage = Column('attached_garage_footage', Text())
    lot_square_footage = Column('lot_square_footage', Text())
    lot_acres = Column('lot_acres', Text())

    # current_balance_due = Column('current_balance_due', Text())

    # tax_year_1 = Column('tax_year_1', Text())
    # tax_year_2 = Column('tax_year_2', Text())
    # tax_year_3 = Column('tax_year_3', Text())
    tax_year_1_assessed = Column('tax_year_1_assessed', Text())
    tax_year_2_assessed = Column('tax_year_2_assessed', Text())
    tax_year_3_assessed = Column('tax_year_3_assessed', Text())

class DuvalSalesDataTemp(DeclarativeBase):
    __tablename__ = "duvalCountySalesTemp"
    id = Column(Integer, primary_key=True)
    parcel = Column('parcel', String(20))
    sale_price = Column('sale1_price', Text())
    sale_date = Column('sale1_date', Text())

class CookPropertyDataTemp(DeclarativeBase):
    __tablename__ = "cookCountyTemp"
    parcel = Column('parcel',String(20),primary_key=True)
    owner_name = Column('owner_name', Text())
    site_address_street = Column('site_address_street', Text())
    site_address_city = Column('site_address_city', Text())
    site_address_zip = Column('site_address_zip', Text())
    mailing_address_city_zip_state = Column('mailing_address_city_zip_state', Text())
    lot_square_footage = Column('lot_square_footage', Text())
    building_square_footage = Column('building_square_footage', Text())
    current_year_assessed_value = Column('current_year_assessed_value', Text())
    prior_year_assessed_value = Column('prior_year_assessed_value', Text())
    property_use = Column('property_use', Text())
    residence_type = Column('residence_type', Text())
    units = Column('units', Text())
    construction_type = Column('construction_type', Text())
    full_bathrooms = Column('full_bathrooms', Text())
    half_bathrooms = Column('half_bathrooms', Text())
    central_air = Column('central_air', Text())
    basement = Column('basement', Text())
    garage_type = Column('garage_type', Text())
    home_owner_exemption = Column('home_owner_exemption', Text())
    senior_citizen_exemption = Column('senior_citizen_exemption', Text())


