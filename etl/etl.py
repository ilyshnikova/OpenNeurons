import datetime
import pandas as pd
import lxml

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from zeep import Client
from models.models import Base, Category, Rates, RatesHistory, Source
from manager.reader import DBManager

class ETL:
    def __init__(self, manager: DBManager):
        self.manager = manager

    def __get_or_create(self, model, kwargs):
        instance = self.manager.session.query(model).filter_by(**kwargs).first()
        if instance:
            return instance
        else:
            instance = model(**kwargs)
            self.manager.session.add(instance)
            return self.manager.session.query(model).filter_by(**kwargs).one()

    def __convert_date(self, inp_date):
        try:
            return datetime.datetime.strptime(inp_date, '%Y%m')
        except:
            return datetime.datetime.strptime(inp_date, '%Y%m%d')

    def load_csv(self, path, category: Category):
        try:
            rates = []
            rates_history = []
            source = self.__get_or_create(
                Source,
                {'name': path.split('/')[-1]}
            )
            futures_id = self.manager.session.query(Category.id).filter(Category.name == 'futures').first()
            call_id = self.manager.session.query(Category.id).filter(Category.name == 'call').first()
            put_id = self.manager.session.query(Category.id).filter(Category.name == 'put').first()

            data = pd.read_csv(path, encoding="ISO-8859-1", header=2)

            tag = {None: [futures_id, 'futures'],
                   'CAL': [call_id, 'call'],
                   'PUT': [put_id, 'put']}

            for _, row in data.iterrows():
                rate = self.__get_or_create(
                    Rates,
                    {'name': row['Underlying Name'], 'category_id': tag[row['Put^Call']][0],
                     'source_id': source.id, 'tag': tag[row['Put^Call']][1]}
                )
                rate_history = RatesHistory(
                    rates_id=rate.id,
                    date=self.__convert_date(str(row['Contract Month'])),
                    float_value=row['Strike Price'],
                    string_value='Settlement Price: ' + str(row['Settlement Price']) +
                                 ' Volatility: ' + str(row['Volatility ']) +
                                 ' Underlying index: ' + str(row['Underlying Index']),
                    tag=tag[row['Put^Call']]
                )
                rates.append(rate)
                rates_history.append(rate_history)

        except Exception as e:
            self.manager.session.rollback()
            raise e

    def load_excel(self, path, category: Category):
        try:
            rates = []
            rates_history = []
            source = self.__get_or_create(
                Source,
                {'name': path.split('/')[-1]}
            )
            futures_id = self.manager.session.query(Category.id).filter(Category.name == 'futures').first()
            call_id = self.manager.session.query(Category.id).filter(Category.name == 'call').first()
            put_id = self.manager.session.query(Category.id).filter(Category.name == 'put').first()

            data = pd.read_excel(path, sheetname=2)
            for _, row in data.iterrows():
                cat = [(futures_id, 'futures') if row['fSTL_V'] else
                            (call_id, 'call') if row['cSTL_V'] else (put_id, 'put')]
                rate = self.__get_or_create(
                    Rates,
                    {'name': 'kospi', 'category_id': cat[0],
                     'source_id': source.id, 'tag': cat[1]}
                )
                rate_history = RatesHistory(
                    rates_id=rate.id,
                    date=row['DATE1'],
                    float_value=row['Strike'],
                    string_value=cat[1][0] + 'ASK_V: ' + str(row.get(cat[1][0] + 'ASK_V', None)) +
                                 ' ' + cat[1][0] + 'STL_V: ' + str(row[cat[1][0] + 'STL_V']) +
                                 ' ' + cat[1][0] + 'BID_V: ' + str(row.get(cat[1][0] + 'BID_V', None)),
                    tag=cat[1]

                )
                rates.append(rate)
                rates_history.append(rate_history)

            self.manager.session.add_all(rates)
            self.manager.session.add_all(rates_history)
            self.manager.session.commit()
        except Exception as e:
            self.manager.session.rollback()
            raise e

    def load_bicurbase(self, start: datetime.datetime, end: datetime.datetime):
        try:
            client = Client('http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL')
            resp = client.service.BiCurBase(start, end)
            category = self.manager.session.query(Category).filter(Category.name == 'CBRF').one()
            source = Source(name='cbr.ru/DailyInfoWebServ/DailyInfo')
            self.manager.session.add(source)
            cbr_id = self.manager.session.query(Category.id).filter(Category.name == 'cbr').first()
            rate = self.__get_or_create(
                Rates,
                {'name': 'bivalcur', 'category_id': cbr_id, 'source_id': source.id, 'tag': 'bivalcur'}
            )
            rates_history = []
            header = ['D0', 'VAL']
            for tbl in resp['_value_1'].getchildren()[0].xpath('//BCB'):
                row = [tbl.xpath(col)[0].text for col in header]
                rates_history.append(
                    RatesHistory(
                        rates_id=rate.id,
                        date=datetime.datetime.strptime(row[0].split('T', 1)[0], '%Y-%m-%d'),
                        float_value=row[1],
                        tag='bicurbase'
                    )
                )
            self.manager.session.add_all(rates_history)
            self.manager.session.commit()
        except Exception as e:
            self.manager.session.rollback()
            raise e
