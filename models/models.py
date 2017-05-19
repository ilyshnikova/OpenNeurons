from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import Column, Integer, String, ForeignKey, \
    Date, Float, Sequence

STRLEN = 1000

class Base:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @declared_attr
    def id(cls):
        return Column(Integer, Sequence(cls.__name__.lower() + '_id_seq'), primary_key=True, unique=True)

Base = declarative_base(cls=Base)

class Category(Base):
    id = Column(Integer, Sequence('category_id_seq'), primary_key=True)
    name = Column(String(STRLEN))
    description = Column(String(STRLEN))
    parent_id = Column(Integer, ForeignKey('category.id'), nullable=True)
    category_parent = relationship(
        'Category',
        backref=backref('parent', remote_side=[id])
    )


class Rates(Base):
    id = Column(Integer, Sequence('rates_id_seq'), primary_key=True)
    name = Column(String(STRLEN))
    category_id = Column(Integer, ForeignKey('category.id'))
    source_id = Column(Integer, ForeignKey('source.id'))
    source = relationship(
        'Source',
        backref=backref('source'))
    tag = Column(String(), nullable=True)
    category = relationship(
        'Category',
        backref=backref('category'))


class Source(Base):
    id = Column(Integer, Sequence('source_id_seq'), primary_key=True)
    name = Column(String(STRLEN))


class RatesHistory(Base):
    rates_id = Column(Integer, ForeignKey('rates.id'))
    date = Column(Date, nullable=True)
    float_value = Column(Float, nullable=True)
    string_value = Column(String(), nullable=True)
    tag = Column(String(), nullable=True)
    rates = relationship(
        'Rates',
        backref=backref('rates'))


class Model(Base):
    model_name = Column(String(STRLEN))
    description = Column(String(STRLEN))
    model_type = Column(String(STRLEN))
    model2dataset = relationship(
        'Model2Dataset',
        backref=backref('model')
    )


class DataSet(Base):
    name = Column(String(STRLEN))
    model2dataset = relationship(
        'Model2Dataset',
        backref=backref('dataset')
    )
    datasetcomponent = relationship(
        'DataSetComponent',
        backref=backref('dataset')
    )


class Model2Dataset(Base):
    model_id = Column(Integer, ForeignKey('model.id'))
    dataset_id = Column(Integer, ForeignKey('dataset.id'))


class DataSetComponent(Base):
    dataset_id = Column(Integer, ForeignKey('dataset.id'), primary_key=True)
    component_type = Column(String(STRLEN))
    component_index = Column(Integer)
    component_name = Column(String(STRLEN))
    datasetvalues = relationship(
        'DataSetValues',
        backref=backref('datasetcomponent')
    )


class DataSetValues(Base):
    component_id = Column(Integer, ForeignKey('datasetcomponent.id'), primary_key=True)
    dataset_id = Column(Integer, primary_key=True)
    vector_id = Column(Integer, primary_key=True)
    value = Column(Float)
