import datetime
import pandas as pd
import lxml
import numpy as np

from manager.dbmanager  import DBManager
from models.models      import *
from zeep               import Client


class ETL:
    def __init__(self, manager: DBManager):
        self.manager = manager

    def __get_or_create(self, model, **kwargs):
        instance = self.manager.session.query(model).filter_by(**kwargs).first()
        if instance:
            return instance
        else:
            instance = model(**kwargs)
            self.manager.session.add(instance)
            self.manager.session.commit()
            return self.manager.session.query(model).filter_by(**kwargs).one()

    def __convert_date(self, inp_date):
        try:
            return datetime.datetime.strptime(inp_date, '%Y%m')
        except:
            return datetime.datetime.strptime(inp_date, '%Y%m%d')

    def load_supervised_data(self, path, ctg_name):
        try:
            source = self.__get_or_create(
                Source,
                name=path.split('/')[-1]
            )

            category_prnt = self.__get_or_create(
                Category,
                name=ctg_name,
                description='test'
            )

            category_attr = self.__get_or_create(
                Category,
                name=ctg_name + ' Attributes',
                description='Attributes of ' + ctg_name,
                parent_id=category_prnt.id
            )

            category_cls = self.__get_or_create(
                Category,
                name=ctg_name + ' Classes',
                description='Classes of ' + ctg_name,
                parent_id=category_prnt.id
            )

            data = pd.read_csv(path, encoding="ISO-8859-1")
            columns = data.columns.values
            columns[-1] = 'target'
            data.columns = columns

            rates_history = []
            for name in data.columns.values:
                rate_f = self.__get_or_create(
                    Rates,
                    name=name,
                    category_id=category_cls.id if name == 'target' else category_attr.id,
                    source_id=source.id,
                    tag='test')

                for idx in range(data.shape[0]):
                    value = data.get_value(idx, name)
                    rh = RatesHistory(
                        rates_id=rate_f.id,
                        float_value=value if isinstance(value, (int, float)) else np.NaN,
                        string_value=value if isinstance(value, str) else '',
                        tag=int(idx))
                    rates_history.append(rh)

            self.manager.session.add_all(rates_history)
            self.manager.session.commit()

        except Exception as e:
            self.manager.session.rollback()
            raise e

    def get_JapanExchange_Derivatives_ex2(self, path):
        try:
            rates = []
            rates_history = []
            source = self.__get_or_create(
                Source,
                name=path.split('/')[-1]
            )
            futures_id = self.manager.session.query(Category.id).filter(Category.name == 'futures').first()
            call_id = self.manager.session.query(Category.id).filter(Category.name == 'call').first()
            put_id = self.manager.session.query(Category.id).filter(Category.name == 'put').first()

            data = pd.read_csv(path, encoding="ISO-8859-1", header=2)

            tag = {'FUT': [futures_id, 'futures'],
                   'CAL': [call_id, 'call'],
                   'PUT': [put_id, 'put']}

            for _, row in data.iterrows():
                # in python only nan is not equal to itself
                if row['Put^Call'] != row['Put^Call']:
                    row['Put^Call'] = 'FUT'
                if row['Underlying Name'] != row['Underlying Name']:
                    row['Underlying Name'] = 'unnamed'

                rate = self.__get_or_create(
                    Rates,
                    name=row['Underlying Name'],
                    category_id=tag[row['Put^Call']][0],
                    source_id=source.id,
                    tag=tag[row['Put^Call']][1]
                )
                rate_history = RatesHistory(
                    rates_id=rate.id,
                    date=self.__convert_date(str(row['Contract Month'])),
                    float_value=row['Strike Price'],
                    string_value='Settlement Price: ' + str(row['Settlement Price']) +
                                 ' Volatility: ' + str(row['Volatility ']) +
                                 ' Underlying index: ' + str(row['Underlying Index']),
                    tag=tag[row['Put^Call']][1]
                )
                rates.append(rate)
                rates_history.append(rate_history)

        except Exception as e:
            self.manager.session.rollback()
            raise e

    def get_Kospi_ex1(self, path):
        try:
            rates = []
            rates_history = []
            source = self.__get_or_create(
                Source,
                name=path.split('/')[-1]
            )
            futures_id = self.manager.session.query(Category.id).filter(Category.name == 'futures').first()
            call_id = self.manager.session.query(Category.id).filter(Category.name == 'call').first()
            put_id = self.manager.session.query(Category.id).filter(Category.name == 'put').first()

            data = pd.read_excel(path, sheetname=2)
            for _, row in data.iterrows():
                cat = (futures_id, 'futures') if row['fSTL_V'] else (call_id, 'call') if row['cSTL_V'] else (put_id, 'put')
                rate = self.__get_or_create(
                    Rates,
                    name='kospi',
                    category_id=cat[0],
                    source_id=source.id,
                    tag=cat[1]
                )
                rate_history = RatesHistory(
                    rates_id=rate.id,
                    date=row['DATE1'],
                    float_value=row['STRIKE'],
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

    def get_CBR_ex3(self, start: datetime.datetime, end: datetime.datetime):
        try:
            client = Client('http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL')
            resp = client.service.BiCurBase(start, end)
            source = self.__get_or_create(Source, name='cbr.ru/DailyInfoWebServ/DailyInfo')
            cbr_id = self.manager.session.query(Category.id).filter(Category.name == 'cbr').first()
            rate = self.__get_or_create(
                Rates,
                name='bivalcur',
                category_id=cbr_id,
                source_id=source.id,
                tag='bivalcur'
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

'''
#example of usage

if __name__ == '__main__':
    from manager.reader import DBManager
    from models.models import *
    import datetime
    manager = DBManager('opentrm')
    cats = [Category(name='futures', description='azaza'),
            Category(name='call', description='azaza'),
            Category(name='put', description='azaza'),
            Category(name='cbr', description='azaza')]
    manager.session.add_all(cats)
    etl = ETL(manager=manager)
    etl.get_Kospi_ex1('../Kospi Quotes Eikon Loader.xlsx')
    etl.get_JapanExchange_Derivatives_ex2('../rb_e20161027.txt.csv')
    etl.get_CBR_ex3(datetime.datetime(2016, 10, 10), datetime.datetime.now())
'''
