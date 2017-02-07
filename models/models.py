from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import Column, Integer, String, ForeignKey, \
    Date, Float, Sequence

STRLEN = 50


class Base:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    @declared_attr
    def id(cls):
        return Column(Integer, Sequence(cls.__name__.lower() + 'id_seq'), primary_key=True)

Base = declarative_base(cls=Base)


class Category(Base):
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
    name = Column(String(STRLEN))
    category_id = Column(Integer, ForeignKey('category.id'))
    source_id = Column(Integer, ForeignKey('source.id'))
    tag = Column(String(STRLEN))
    rates_history = relationship(
        'RatesHistory',
        backref=backref('rates')
    )


class Source(Base):
    name = Column(String(STRLEN))
    rates = relationship(
        'Rates',
        backref=backref('source')
    )


class RatesHistory(Base):
    rates_id = Column(Integer, Sequence('rates_history_id_seq'), ForeignKey('rates.id'), primary_key=True)
    date = Column(Date, primary_key=True)
    value_double = Column(Float)
    value_char = Column(String(STRLEN))


class Model(Base):
    model_name = Column(String(STRLEN))
    description = Column(String(STRLEN))
    model_type = Column(String(STRLEN))
    model2dataset = rates = relationship(
        'Model2Dataset',
        backref=backref('model')
    )


class DataSet(Base):
    name = Column(String(STRLEN))
    model2dataset = relationship(
        'Model2Dataset',
        backref=backref('dataset')
    )
    rates_history = relationship(
        'DataSetComponent',
        backref=backref('dataset')
    )


class Model2Dataset(Base):
    model_id = Column(Integer, ForeignKey('model.id'))
    dataset_id = Column(Integer, ForeignKey('dataset.id'))


class DataSetComponent(Base):
    dataset_id = Column(Integer, Sequence('datasetcomponent_id_seq'), ForeignKey('dataset.id'), primary_key=True)
    component_type = Column(String(STRLEN))
    component_index = Column(Integer)
    component_name = Column(String(STRLEN))
