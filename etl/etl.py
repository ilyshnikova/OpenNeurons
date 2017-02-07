import datetime
import pandas as pd
import lxml

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from zeep import Client
from models.models import Base, Category, Rates, RatesHistory, Source


class ETL:
    def __init__(self, db_name, user='postgres', host='localhost'):
        self.user = user
        self.host = host
        self.db_name = db_name
        self.engine = create_engine(
            'postgresql://{user}@{host}:5432/{db_name}'.format(user=user, host=host, db_name=db_name),
            echo=True
        )
        self.session = sessionmaker(bind=self.engine)()

    def __get_or_create(self, model, kwargs):
        instance = self.session.query(model).filter_by(**kwargs).first()
        if instance:
            return instance
        else:
            instance = model(**kwargs)
            self.session.add(instance)
            return self.session.query(model).filter_by(**kwargs).one()

    def __convert_date(self, inp_date):
        try:
            return datetime.datetime.strptime(inp_date, '%Y%m')
        except:
            return datetime.datetime.strptime(inp_date, '%Y%m%d')

    def write_to_db(self, path, category: Category):
        try:
            rates = []
            rates_history = []
            extension = path.split('.')[-1]
            source = self.__get_or_create(
                Source,
                {'name': path.split('/')[-1]}
            )
            if extension == 'csv':
                data = pd.read_csv(path, encoding="ISO-8859-1", header=2)
                for _, row in data.iterrows():
                    rate = self.__get_or_create(
                        Rates,
                        {'name': row['Issue Name'], 'category_id': category.id,
                         'source_id': source.id, 'tag': 'rb'}
                    )
                    rate_history = RatesHistory(
                        rates_id=rate.id,
                        date=self.__convert_date(str(row['Contract Month'])),
                        value_double=row['Underlying Index'],
                        value_char=str(row['Interest Rate']) + str(row['Underlying Name']),
                        tag='rb'
                    )
                    rates.append(rate)
                    rates_history.append(rate_history)
            elif extension == 'xlsx':
                data = pd.read_excel(path, sheetname=2)
                for _, row in data.iterrows():
                    rate = self.__get_or_create(
                        Rates,
                        {'name': 'kospi_item', 'category_id': category.id,
                         'source_id': source.id, 'tag': 'kospi option'}
                    )
                    rate_history = RatesHistory(
                        rates_id=rate.id,
                        date=row['DATE1'],
                        value_double=row['STRIKE'],
                        value_char='opt' ,
                        tag='kospi option'
                    )
                    rates.append(rate)
                    rates_history.append(rate_history)

            self.session.add_all(rates)
            self.session.add_all(rates_history)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e

    def load_bicurbase(self, start: datetime.datetime, end: datetime.datetime):
        try:
            client = Client('http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL')
            resp = client.service.BiCurBase(start, end)
            category = self.session.query(Category).filter(Category.name == 'CBRF').one()
            source = Source(name='cbr.ru/DailyInfoWebServ/DailyInfo')
            self.session.add(source)
            rate = self.__get_or_create(
                Rates,
                {'name': 'bivalcur', 'category_id': category.id, 'source_id': source.id, 'tag': 'bivalcur'}
            )
            rates_history = []
            header = ['D0', 'VAL']
            for tbl in resp['_value_1'].getchildren()[0].xpath('//BCB'):
                row = [tbl.xpath(col)[0].text for col in header]
                rates_history.append(
                    RatesHistory(
                        rates_id=rate.id,
                        date=datetime.datetime.strptime(row[0].split('T', 1)[0], '%Y-%m-%d'),
                        value_double=row[1],
                        value_char='bicurbase' + str(datetime.datetime.strptime(row[0].split('T', 1)[0], '%Y-%m-%d')),
                        tag='bicurbase'
                    )
                )
            self.session.add_all(rates_history)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e

'''
example of usage

Base.metadata.create_all(self.engine)
etl = ETL(db_name='opentrm')
categories = [
    Category(name='NIKKEI', description='nikkei category'),
    Category(name='KOSPI', description='kospi category'),
    Category(name='CBRF', description='cbrf_data')
]
etl.session.add_all(categories)
etl.session.commit()

etl.load_bicurbase(datetime.datetime(2016, 10, 18, 0, 0, 0, 0), datetime.datetime.today())
bivalcur_rate = etl.session.query(Rates).filter(Rates.name == 'bivalcur').one()
etl.session.query(RatesHistory.date, RatesHistory.value_double).filter(RatesHistory.rates_id == bivalcur_rate.id).all()
etl.write_to_db('rb_e20161027.txt.csv', etl.session.query(Category).filter(Category.name == 'CBRF').one())

'''