import datetime
import pandas as pd
import lxml

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from zeep import Client
from models.models import *


class ETL:
    def __init__(self, db_name, user='postgres', host='localhost'):
        self.user = user
        self.host = host
        self.db_name = db_name
        self.engine = create_engine(
            'postgresql://{user}@{host}:5432/{db_name}'.format(user=user, host=host, db_name=db_name),
            echo=True
        )
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()

    def get_or_create(self, model, kwargs):
        instance = self.session.query(model).filter_by(**kwargs).first()
        if instance:
            return instance
        else:
            instance = model(**kwargs)
            self.session.add(instance)
            return self.session.query(model).filter_by(**kwargs).one()

    def convert_date(self, date):
        try:
            return datetime.datetime.strptime(date, '%Y%m')
        except:
            return datetime.datetime.strptime(date, '%Y%m%d')

    def write_to_db(self, path, category= Category):
        try:
            rates = []
            rates_history = []
            extension = path.split('.')[-1]
            source = self.get_or_create(
                Source,
                {'name': path.split('/')[-1]}
            )
            if extension == 'csv':
                data = pd.read_csv(path, encoding="ISO-8859-1", header=2)
                for _, row in data.iterrows():
                    rate = self.get_or_create(
                        Rates,
                        {'name': row['Issue Name'], 'category_id': category.id,
                         'source_id': source.id, 'tag': str(row['Issue Code'])}
                    )
                    rate_history = RatesHistory(
                        rates_id=rate.id,
                        date=self.convert_date(str(row['Contract Month'])),
                        value_double=row['Interest Rate'],
                        value_char=str(row['Underlying Index']),
                        tag=row['Underlying Name']
                    )
                    rates.append(rate)
                    rates_history.append(rate_history)
            elif extension == 'xlsx':
                data = pd.read_excel(path, sheetname=2)
                for _, row in data.iterrows():
                    rate = self.get_or_create(
                        Rates,
                        {'name': 'kospi_item', 'category_id' :category.id,
                         'source_id': source.id, 'tag': str(row['fRIC'])}
                    )
                    rate_history = RatesHistory(
                        rates_id=rate.id,
                        date=row['DATE1'],
                        value_double=row['STRIKE'],
                        value_char=str(row['cRIC']),
                        tag=row['fRIC']
                    )
                    rates.append(rate)
                    rates_history.append(rate_history)

            self.session.add_all(rates)
            self.session.add_all(rates_history)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e

    def load_bicurbase(self, start= datetime.datetime, end= datetime.datetime):
        try:
            client = Client('http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL')
            resp = client.service.BiCurBase(start, end)
            category = self.session.query(Category).filter(Category.name == 'CBRF').one()
            source = Source(name='cbr.ru/DailyInfoWebServ/DailyInfo')
            self.session.add(source)
            rate = self.get_or_create(
                Rates,
                {'name': 'bivalcur', 'category_id': category.id, 'source_id': source.id, 'tag': 'BiCur'}
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
                        value_char='Nothing to put here',
                        tag='bicurbase'
                    )
                )
            self.session.add_all(rates_history)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e