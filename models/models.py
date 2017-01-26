from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, \
    Date, Float

Base = declarative_base()
STRLEN = 50


class Category(Base):
    __tablename__ = 'category'
    id = Column(Integer, primary_key=True)
    name = Column(String(STRLEN))
    description = Column(String(STRLEN))
    parent_id = Column(Integer, ForeignKey('category.id'), nullable=True)
    children = relationship(
        'Category',
        backref=backref('parent', remote_side=[id])
    )
    rates = relationship(
        'Rates',
        backref=backref('category')
    )


class Rates(Base):
    __tablename__ = 'rates'
    id = Column(Integer, primary_key=True)
    name = Column(String(STRLEN))
    category_id = Column(Integer, ForeignKey('category.id'))
    source_id = Column(Integer, ForeignKey('source.id'))
    tag = Column(String(STRLEN))
    rates_history = relationship(
        'RatesHistory',
        backref=backref('rates')
    )


class Source(Base):
    __tablename__ = 'source'
    id = Column(Integer, primary_key=True)
    name = Column(String(STRLEN))
    rates = relationship(
        'Rates',
        backref=backref('source')
    )


class RatesHistory(Base):
    __tablename__ = 'rates_history'
    rates_id = Column(Integer, ForeignKey('rates.id'), primary_key=True)
    date = Column(Date, primary_key=True)
    value_double = Column(Float)
    value_char = Column(String(STRLEN))
    tag = Column(String(STRLEN))



