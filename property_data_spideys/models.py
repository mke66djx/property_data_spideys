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

def create_table(engine,ModelClass):
    #DeclarativeBase.metadata.create_all(engine)
    ModelClass.__table__.create(engine,checkfirst=True)

class PierceCountyPropertyData(DeclarativeBase):
    __tablename__ = "pierceCounty"
    parcel = Column('parcel',String(20),primary_key=True)
    owner_name = Column('owner_name', Text())
    owner_first_name = Column('owner_first_name', Text())
    owner_last_name = Column('owner_last_name', Text())
    mailing_address = Column('mailing_address', Text())
    mail_city = Column('mail_city', Text())
    mail_state = Column('mail_state', Text())
    mail_zip = Column('mail_zip', Text())
    site_address = Column('site_address', Text())
    property_type = Column('property_type', Text())
    occupancy = Column('occupancy', Text())
    year_built = Column('year_built', Integer())
    adj_year_built = Column('adj_year_built', Integer())
    units = Column('units', Integer())
    bedrooms = Column('bedrooms', Integer())
    baths = Column('baths', Float(3))
    siding_type = Column('siding_type', Text())
    stories = Column('stories', Float(3))
    building_square_footage = Column('building_square_footage', Integer())
    attached_garage_footage = Column('attached_garage_footage', Integer())
    lot_square_footage = Column('lot_square_footage', Integer())
    lot_acres = Column('lot_acres', Float(3))

    current_balance_due = Column('current_balance_due', Float(8))

    tax_year_1 = Column('tax_year_1', Integer())
    tax_year_2 = Column('tax_year_2', Integer())
    tax_year_3 = Column('tax_year_3', Integer())
    tax_year_1_assessed = Column('tax_year_1_assessed', Float(8))
    tax_year_2_assessed = Column('tax_year_2_assessed', Float(8))
    tax_year_3_assessed = Column('tax_year_3_assessed', Float(8))

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
    building_type = Column('building_type', Text())
    year_built = Column('year_built', Integer())
    units = Column('units', Float(3))
    bedrooms = Column('bedrooms', Float(3))
    baths = Column('baths', Float(3))
    siding_type = Column('siding_type', Text())
    stories = Column('stories', Float(3))
    building_square_footage = Column('building_square_footage', Text())
    lot_square_footage = Column('lot_square_footage', Text())

    # current_balance_due = Column('current_balance_due', Text())

    tax_year_1_assessed = Column('tax_year_1_assessed', Float(8))
    tax_year_2_assessed = Column('tax_year_2_assessed', Float(8))
    homestead_exemption = Column('homestead_exemption', Integer())
    senior_exemption = Column('senior_exemption', Integer())



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
    owner_first = Column('owner_first', Text())
    owner_2_first = Column('owner_2_first', Text())
    owner_last = Column('owner_last', Text())
    site_address = Column('site_address', Text())
    site_address_city = Column('site_address_city', Text())
    site_address_zip = Column('site_address_zip', Text())
    site_address_township = Column('site_address_township', Text())

    mailing_address= Column('mailing_address', Text())
    mail_city= Column('mailing', Text())
    mail_state= Column('mail_state', Text())
    mail_zip= Column('mail_zip', Text())

    lot_square_footage = Column('lot_square_footage', Integer())
    building_square_footage = Column('building_square_footage',Integer())
    current_year_assessed_value = Column('current_year_assessed_value', Float(8))
    prior_year_assessed_value = Column('prior_year_assessed_value', Float(8))
    property_use = Column('property_use', Text())
    residence_type = Column('residence_type', Text())
    units = Column('units', Integer())
    construction_type = Column('construction_type', Text())
    full_bathrooms = Column('full_bathrooms', Float(3))
    half_bathrooms = Column('half_bathrooms', Float(3))
    central_air = Column('central_air', Text())
    basement = Column('basement', Text())
    garage_type = Column('garage_type', Text())
    home_owner_exemption = Column('home_owner_exemption', Text())
    senior_citizen_exemption = Column('senior_citizen_exemption', Text())

    taxes_sold = Column('taxes_sold', Text())
    tax_paid_year0 = Column('tax_paid_year0', Text())
    tax_paid_year0_amount = Column('tax_paid_year0_amount', Text())
    tax_paid_year1 = Column('tax_paid_year1', Text())
    tax_paid_year1_amount = Column('tax_paid_year1_amount', Text())

    record1 = Column('record1', Text())
    record1_date = Column('record1_date', Text())
    record2 = Column('record2', Text())
    record2_date = Column('record2_date', Text())
    record3 = Column('record3', Text())
    record3_date = Column('record3_date', Text())
    record4 = Column('record4', Text())
    record4_date = Column('record4_date', Text())
    record5 = Column('record5', Text())
    record5_date = Column('record5_date', Text())


class MaricopaCountyPropertyData(DeclarativeBase):
    __tablename__ = "maricopaCounty"
    parcel = Column('parcel',String(20),primary_key=True)
    owner_name = Column('owner_name', Text())
    owner_first = Column('owner_first', Text())
    owner_last = Column('owner_last', Text())

    owner_full_address = Column('owner_full_address', Text())
    owner_street_address1 = Column('owner_street_address1', Text())
    owner_city = Column('owner_city', Text())
    owner_state = Column('owner_state', Text())
    owner_zip= Column('owner_zip', Text())

    full_property_address= Column('full_property_address', Text())
    site_street= Column('site_street', Text())
    site_city= Column('site_city', Text())
    site_zip= Column('site_zip', Text())

    lot_size = Column('lot_size', Float(6))

    property_description= Column('property_description', Text())
    property_type= Column('property_type', Text())

    rental= Column('rental', Text())
    value_0= Column('value_0', Float(8))
    value_1= Column('value_1', Float(8))
    value_2= Column('value_2', Float(8))

    last_deed_date= Column('last_deed_date', Text())
    last_sale_price= Column('last_sale_price', Float(8))


#--------------------------------------------------------------------------
#-------The temps below are used when doing full table updates-------------
#--------------------------------------------------------------------------
class PiercePropertyDataTemp(DeclarativeBase):
    __tablename__ = "pierceCountyTemp"
    parcel = Column('parcel',String(20),primary_key=True)
    owner_name = Column('owner_name', Text())
    owner_first_name = Column('owner_first_name', Text())
    owner_last_name = Column('owner_last_name', Text())
    mailing_address = Column('mailing_address', Text())
    mail_city = Column('mail_city', Text())
    mail_state = Column('mail_state', Text())
    mail_zip = Column('mail_zip', Text())
    site_address = Column('site_address', Text())
    property_type = Column('property_type', Text())
    occupancy = Column('occupancy', Text())
    year_built = Column('year_built', Integer())
    adj_year_built = Column('adj_year_built', Integer())
    units = Column('units', Integer())
    bedrooms = Column('bedrooms', Integer())
    baths = Column('baths', Float(3))
    siding_type = Column('siding_type', Text())
    stories = Column('stories', Float(3))
    building_square_footage = Column('building_square_footage', Integer())
    attached_garage_footage = Column('attached_garage_footage', Integer())
    lot_square_footage = Column('lot_square_footage', Integer())
    lot_acres = Column('lot_acres', Float(3))

    current_balance_due = Column('current_balance_due', Float(8))

    tax_year_1_assessed = Column('tax_year_1_assessed', Float(8))
    tax_year_2_assessed = Column('tax_year_2_assessed', Float(8))
    tax_year_3_assessed = Column('tax_year_3_assessed', Float(8))

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
    building_type = Column('building_type', Text())
    year_built = Column('year_built', Integer())
    units = Column('units', Float(3))
    bedrooms = Column('bedrooms', Float(3))
    baths = Column('baths', Float(3))
    siding_type = Column('siding_type', Text())
    stories = Column('stories', Float(3))
    building_square_footage = Column('building_square_footage', Text())
    lot_square_footage = Column('lot_square_footage', Text())

    # current_balance_due = Column('current_balance_due', Text())

    tax_year_1_assessed = Column('tax_year_1_assessed', Float(8))
    tax_year_2_assessed = Column('tax_year_2_assessed', Float(8))
    homestead_exemption = Column('homestead_exemption', Integer())
    senior_exemption = Column('senior_exemption', Integer())


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
    owner_first = Column('owner_first', Text())
    owner_2_first = Column('owner_2_first', Text())
    owner_last = Column('owner_last', Text())
    site_address = Column('site_address', Text())
    site_address_city = Column('site_address_city', Text())
    site_address_zip = Column('site_address_zip', Text())
    site_address_township = Column('site_address_township', Text())

    mailing_address= Column('mailing_address', Text())
    mail_city= Column('mailing', Text())
    mail_state= Column('mail_state', Text())
    mail_zip= Column('mail_zip', Text())

    lot_square_footage = Column('lot_square_footage', Integer())
    building_square_footage = Column('building_square_footage',Integer())
    current_year_assessed_value = Column('current_year_assessed_value', Float(8))
    prior_year_assessed_value = Column('prior_year_assessed_value', Float(8))
    property_use = Column('property_use', Text())
    residence_type = Column('residence_type', Text())
    units = Column('units', Integer())
    construction_type = Column('construction_type', Text())
    full_bathrooms = Column('full_bathrooms', Float(3))
    half_bathrooms = Column('half_bathrooms', Float(3))
    central_air = Column('central_air', Text())
    basement = Column('basement', Text())
    garage_type = Column('garage_type', Text())
    home_owner_exemption = Column('home_owner_exemption', Text())
    senior_citizen_exemption = Column('senior_citizen_exemption', Text())

    taxes_sold = Column('taxes_sold', Text())
    tax_paid_year0 = Column('tax_paid_year0', Text())
    tax_paid_year0_amount = Column('tax_paid_year0_amount', Float(7))
    tax_paid_year1 = Column('tax_paid_year1', Text())
    tax_paid_year1_amount = Column('tax_paid_year1_amount', Float(7))

    record1 = Column('record1', Text())
    record1_date = Column('record1_date', Text())
    record2 = Column('record2', Text())
    record2_date = Column('record2_date', Text())
    record3 = Column('record3', Text())
    record3_date = Column('record3_date', Text())
    record4 = Column('record4', Text())
    record4_date = Column('record4_date', Text())
    record5 = Column('record5', Text())
    record5_date = Column('record5_date', Text())




class MaricopaPropertyDataTemp(DeclarativeBase):
    __tablename__ = "maricopaCountyTemp"
    parcel = Column('parcel',String(20),primary_key=True)
    owner_name = Column('owner_name', Text())
    owner_first = Column('owner_first', Text())
    owner_last = Column('owner_last', Text())

    owner_full_address = Column('owner_full_address', Text())
    owner_street_address1 = Column('owner_street_address1', Text())
    owner_city = Column('owner_city', Text())
    owner_state = Column('owner_state', Text())
    owner_zip= Column('owner_zip', Text())

    full_property_address= Column('full_property_address', Text())
    site_street= Column('site_street', Text())
    site_city= Column('site_city', Text())
    site_zip= Column('site_zip', Text())

    lot_size = Column('lot_size', Integer())

    property_description= Column('property_description', Text())
    property_type= Column('property_type', Text())

    rental= Column('rental', Text())
    value_0= Column('value_0', Text())
    value_1= Column('value_1', Text())
    value_2= Column('value_2', Text())

    last_deed_date= Column('last_deed_date', Text())
    last_sale_price= Column('last_sale_price', Text())
