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
            futures_id = self.session.query(Category.id).filter(Category.name == 'futures').first()
            call_id = self.session.query(Category.id).filter(Category.name == 'call').first()
            put_id = self.session.query(Category.id).filter(Category.name == 'put').first()
            if extension == 'csv':
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
                        ask_price=row['Settlement Price'],
                        strike=row['Strike Price'],
                        rates_value=row['Underlying Index'],
                        volatility=row['Volatility '],
                        tag=tag[row['Put^Call']]
                    )
                    rates.append(rate)
                    rates_history.append(rate_history)
            elif extension == 'xlsx':
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
                        ask_price=row.get(cat[1][0] + 'ASK_V', None),
                        strike=row['Strike'],
                        rates_value=row[cat[1][0] + 'STL_V'],
                        volatility=row.get(cat[1][0] + 'BID_V', None),
                        tag=cat[1]

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
            cbr_id = self.session.query(Category.id).filter(Category.name == 'cbr').first()
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
                        strike=row[1],
                        tag='bicurbase'
                    )
                )
            self.session.add_all(rates_history)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
