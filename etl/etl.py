import datetime
import pandas as pd
import lxml
import numpy as np

from manager.dbmanager import DBManager
from etl.yahoo_finance import YahooFinance
from etl.quandl        import Quandl
from models.models     import *

from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.pdfpage   import PDFPage
from pdfminer.layout    import LAParams

from io   import StringIO
from zeep import Client
from lxml import html

import numpy as np
import requests

import json
import os


from lxml import html


class ETL:
    def __init__(self, manager: DBManager):
        self.manager = manager

    def update_external_data(self, start='2000-01-01'):
        with open('portfolio.json') as data_file:
            portfolio = json.load(data_file)

        for asset_name in list(portfolio.keys()):
            portfolio_asset = portfolio[asset_name]
            asset_ticket = portfolio[asset_name]['ticket']

            if portfolio[asset_name]['source'] == 'Quandl':
                qu = Quandl(period_start=start)
                qu.update(asset=portfolio_asset, ticket=asset_ticket)

            elif portfolio[asset_name]['source'] == 'Yahoo Finance':
                yahoofin = YahooFinance(period_start=start)
                yahoofin.update(asset=portfolio_asset, ticket=asset_ticket)

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

    def __proc_wdbd(self, data, start_slice, end_slice, inf=False):
        data = data.iloc[:, start_slice:end_slice]
        col_nms = data.columns.values
        year = col_nms[0][4:8]
        data = data[data[col_nms[0]] != 0][:-3]
        wd = data[data[col_nms[1]] == 1][col_nms[0]]
        bd = data[data[col_nms[1]] == 0][col_nms[0]]
        if inf:
            print('Year {}:'.format(year))
            print('MOEX weekend days =', len(wd))
            print('MOEX business days =', len(bd))
            print('Total days =', len(wd) + len(bd))
        return wd, bd

    def __save_days(self, day_type, year, data):
        if day_type == 'bd':
            day_fl = 'BD'
            day_type = 'Business days'
        elif day_type == 'wd':
            day_fl = 'WD'
            day_type = 'Weekend days'

        category = pd.DataFrame([{'name': 'Exchanges work calendar', 'description': 'Business and Weekend days'},
                                 {'name': 'MCX', 'description': 'Moscow Exchange',
                                  'parent_name': 'Exchanges work calendar'},
                                 {'name': 'MCX {0}'.format(day_type),
                                  'description': 'Moscow Exchange {0}'.format(day_type), 'parent_name': 'MCX'}
                                 ])
        rate = pd.DataFrame([{'name': 'MCX_{0}_{1}'.format(day_fl, year),
                              'category_name': 'MCX {0}'.format(day_type),
                              'source': 'MOEX.COM',
                              'tag': '{0} in {1} year'.format(day_type, year)}
                             ], index=[1])

        df = pd.DataFrame()
        data.reset_index(drop=True, inplace=True)

        for idx in range(data.shape[0]):
            df = df.append(
                {'rates_name': 'MCX_{0}_{1}'.format(day_fl, year), 'date': data[idx].strftime("%Y-%m-%d"),
                 'float_value': None, 'string_value': data[idx].strftime("%Y-%m-%d"), 'tag': ""},
                ignore_index=True)

        self.manager.save_raw_data(category, rate, df, source='MOEX.COM')

    def load_moex_workingdays(self):
        data = pd.read_excel('MICEX_BD.xlsx')
        labels = ['Unnamed: 2', 'Unnamed: 5', 'Unnamed: 8', 'Unnamed: 11', 'Unnamed: 14', 'Unnamed: 17', 'Unnamed: 18', 'Unnamed: 19']
        data.drop(labels, axis=1, inplace=True)
        data.fillna(0, inplace=True)

        wd2012, bd2012 = self.__proc_wdbd(data=data, start_slice=0, end_slice=2)
        wd2013, bd2013 = self.__proc_wdbd(data=data, start_slice=2, end_slice=4)
        wd2014, bd2014 = self.__proc_wdbd(data=data, start_slice=4, end_slice=6)
        wd2015, bd2015 = self.__proc_wdbd(data=data, start_slice=6, end_slice=8)
        wd2016, bd2016 = self.__proc_wdbd(data=data, start_slice=8, end_slice=10)
        wd2017, bd2017 = self.__proc_wdbd(data=data, start_slice=10, end_slice=12)

        self.__save_days(day_type='bd', year='2012', data=bd2012)
        self.__save_days(day_type='bd', year='2013', data=bd2013)
        self.__save_days(day_type='bd', year='2014', data=bd2014)
        self.__save_days(day_type='bd', year='2015', data=bd2015)
        self.__save_days(day_type='bd', year='2016', data=bd2016)
        self.__save_days(day_type='bd', year='2017', data=bd2017)

        self.__save_days(day_type='wd', year='2012', data=wd2012)
        self.__save_days(day_type='wd', year='2013', data=wd2013)
        self.__save_days(day_type='wd', year='2014', data=wd2014)
        self.__save_days(day_type='wd', year='2015', data=wd2015)
        self.__save_days(day_type='wd', year='2016', data=wd2016)
        self.__save_days(day_type='wd', year='2017', data=wd2017)

    def cbr_refinancing_rate(self):
        url = "https://www.finam.ru/analysis/macroevent/?dind=0&dpsd=1817681&fso=date+desc&str=1&ind=710&stdate=16.09.2013&endate=28.04.2017&sema=1&seman=5&timeStep=1"
        r = requests.get(url)
        tree = html.fromstring(r.text)
        dates = np.asarray([date.text for date in tree.xpath('//td[@class="sm"]')])
        rates = np.asarray([rate.text for rate in tree.xpath('//td[@align="right"]')])
        rates = rates[rates != np.array(None)][::2]
        rates = np.asarray([float(rate.split()[0]) for rate in rates])
        data = pd.DataFrame({'rate': rates}, index=[pd.to_datetime(dates)])

        data = data[~data.index.duplicated()]
        idx = pd.date_range(data.index[-1], pd.to_datetime(datetime.date.today()))
        data = data.sort_index().reindex(idx, method='pad')[::-1]

        rateshistory = pd.DataFrame()
        rate_name = 'CBR Refinancing Rate'

        for idx in data.index:
            rateshistory = rateshistory.append({'rates_name': rate_name, 'date': idx, 'float_value': data.get_value(idx, 'rate'), 'string_value': None, 'tag': None}, ignore_index=True)

        category = pd.DataFrame([
            {'name': 'Central Bank of Russia', 'description': 'Central Bank of Russia'},
            {'name': 'Refinancing Rate', 'description': 'From {0} to {1}'.format(pd.to_datetime(rateshistory.date[-1:].item()), rateshistory.date[0]), 'parent_name': 'Central Bank of Russia'}
        ])

        rates = pd.DataFrame([{'name': 'CBR Refinancing Rate', 'category_name': 'Refinancing Rate', 'source': 'Finam.ru', 'tag': None}], index=[1])

        self.manager.save_raw_data(category, rates, rateshistory, source='Finam.ru')

    def cbr_key_rate(self):
        url = "https://www.finam.ru/analysis/macroevent/?dind=0&dpsd=3980881&fso=date+desc&str=1&ind=1555&stdate=01.01.1991&endate=11.12.2015&sema=1&seman=5&timeStep=1"
        r = requests.get(url)
        tree = html.fromstring(r.text)
        dates = np.asarray([date.text for date in tree.xpath('//td[@class="sm"]')])
        rates = np.asarray([rate.text for rate in tree.xpath('//td[@align="right"]')])
        rates = rates[rates != np.array(None)][::2]
        rates = np.asarray([float(rate.split()[0]) for rate in rates])
        data = pd.DataFrame({'rate': rates}, index=[pd.to_datetime(dates)])

        data = data[~data.index.duplicated()]
        idx = pd.date_range(data.index[-1], pd.to_datetime(datetime.date.today()))
        data = data.sort_index().reindex(idx, method='pad')[::-1]

        rateshistory = pd.DataFrame()
        rate_name = 'CBR Key Rate'
        for idx in data.index:
            rateshistory = rateshistory.append({'rates_name': rate_name, 'date': idx, 'float_value': data.get_value(idx,'rate'), 'string_value': None, 'tag': None},ignore_index=True)

        category = pd.DataFrame([
            {'name': 'Central Bank of Russia', 'description': 'Central Bank of Russia'},
            {'name': 'Key Rate', 'description': 'From {0} to {1}'.format(pd.to_datetime(rateshistory.date[-1:].item()), rateshistory.date[0]), 'parent_name': 'Central Bank of Russia'}
            ])

        rates = pd.DataFrame([{'name': 'CBR Key Rate', 'category_name': 'Key Rate', 'source': 'Finam.ru', 'tag': None}], index=[1])

        self.manager.save_raw_data(category, rates, rateshistory, source='Finam.ru')

    def get_CBR_key_rate(self):
        client = Client('http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL')
        resp = client.service.MainInfoXML()
        bank_rate = float(resp.getchildren()[0].text)
        return bank_rate

    def get_Kospi_data_ex1(self, path, sheetname = 'DATA', Source = 'KRX', SaveToDB = True):
        # 1. Retrieve data from source
        KospiData = pd.read_excel(path, sheetname)
        # 2. Proceed and filter retrieved data
        KospiData = KospiData[['DATE1', 'YEAR', 'MONTH', 'STRIKE', 'cBID_V', 'cASK_V', 'cSTL_V', 'pBID_V', 'pASK_V', 'pSTL_V', 'fRIC', 'fSTL_V']].\
            dropna(how='all')

        OptionsData = KospiData[['DATE1', 'YEAR', 'MONTH', 'STRIKE', 'cBID_V', 'cASK_V', 'cSTL_V', 'pBID_V', 'pASK_V', 'pSTL_V']].\
            dropna(how='all', subset=['cBID_V', 'cASK_V', 'cSTL_V', 'pBID_V', 'pASK_V', 'pSTL_V']).\
            drop_duplicates()

        FuturesData = KospiData[['DATE1', 'YEAR', 'MONTH', 'fSTL_V']].\
            dropna(how='all', subset=['fSTL_V']).\
            drop_duplicates()

        # 3. Init DataFrames = Category, Rates, RatesHistory before data insertion ( for Options and Futures)
        Category = pd.DataFrame([{'name': 'Financial Markets', 'description': 'Financial Markets Data Branch'},
                                 {'name': 'Asia', 'description': 'Asia', 'parent_name': 'Financial Markets' },
                                 {'name': 'Korea', 'description': 'Korea', 'parent_name': 'Asia' },
                                 {'name': 'KRX', 'description': 'Korea Stock Exchange', 'parent_name': 'Korea' },
                                 {'name': 'KRX Derivatives', 'description': 'KRX Derivatives', 'parent_name': 'KRX' },
                                 ])
        Rates = pd.DataFrame()
        RatesHistory = pd.DataFrame()

        # 4. Insert Futures category in  Dataframe Category
        if FuturesData.empty == False:
            Category = Category.append([{'name': 'KOSPI Futures', 'description': 'KOSPI Index Futures', 'parent_name': 'KRX Derivatives' }])

        # 5. Import KOSPI Futures prices into Category, Rates, RatesHisory
        for row in FuturesData[['DATE1', 'YEAR', 'MONTH', 'fSTL_V']].values:
          Category = Category.append([{'name': 'KOSPI FUT Y:{}'.format(row[1]), 'description': 'KOSPI FUTURES YEAR: {}'.format(row[1]), 'parent_name': 'KOSPI Futures' },
                                      {'name': 'KOSPI FUT {}/{}'.format(row[2],row[1]), 'description': 'KOSPI FUTURES {}/{}'.format(row[2],row[1]), 'parent_name': 'KOSPI FUT Y:{}'.format(row[1])}])
          Rates = Rates.append([{'name': 'FUTSPriceKOSPI({}/{})'.format(row[2], row[1]), 'category_name': 'KOSPI FUT {}/{}'.format(row[2],row[1]), 'tag': 'KOSPI FUT {}/{}'.format(row[2], row[1]), 'source' : Source}])
          RatesHistory = RatesHistory.append([{'rates_name': 'FUTSPriceKOSPI({}/{})'.format(row[2], row[1]), 'date': row[0], 'float_value' : row[3], 'string_value' : None, 'tag': 'KOSPI FUT {}/{} price date: {}'.format(row[2], row[1], row[0])}])

        # 6. Insert Options category in Dataframe Category
        if OptionsData.empty == False:
            Category = Category.append([{'name': 'KOSPI Options', 'description': 'KOSPI Index Options', 'parent_name': 'KRX Derivatives' }])

        # 7. Import KOSPI Options prices into Category, Rates, RatesHisory
        for row in OptionsData[['DATE1', 'YEAR', 'MONTH', 'STRIKE',  'cBID_V', 'cASK_V', 'cSTL_V', 'pBID_V', 'pASK_V', 'pSTL_V']].values:
            Category = Category.append([{'name': 'KOSPI OPT Y:{}'.format(row[1]), 'description': 'KOSPI OPTIONS YEAR: {}'.format(row[1]), 'parent_name': 'KOSPI Options' },
                                        {'name': 'KOSPI OPT {}/{}'.format(row[2],row[1]), 'description': 'KOSPI OPTIONS {}/{}'.format(row[2],row[1]), 'parent_name': 'KOSPI OPT Y:{}'.format(row[1])},
                                        {'name': 'KOSPI OPT {}/{} STRIKE = {}'.format(row[2],row[1], row[3]), 'description': 'KOSPI OPTIONS {}/{} STRIKE = {}'.format(row[2],row[1], row[3]), 'parent_name': 'KOSPI OPT {}/{}'.format(row[2],row[1])},])
            Rates = Rates.append([{'name': 'OptPriceCallBid({}/{}){}'.format(row[2], row[1], row[3]), 'category_name': 'KOSPI OPT {}/{} STRIKE = {}'.format(row[2],row[1], row[3]), 'tag': 'KOSPI CALL OPTION {}/{} STRIKE = {} BID PRICE'.format(row[2],row[1], row[3]), 'source' : Source},
                                   {'name': 'OptPriceCallAsk({}/{}){}'.format(row[2], row[1], row[3]), 'category_name': 'KOSPI OPT {}/{} STRIKE = {}'.format(row[2],row[1], row[3]), 'tag': 'KOSPI CALL OPTION {}/{} STRIKE = {} ASK PRICE'.format(row[2],row[1], row[3]), 'source' : Source},
                                   {'name': 'OptPriceCallSetl({}/{}){}'.format(row[2], row[1], row[3]), 'category_name': 'KOSPI OPT {}/{} STRIKE = {}'.format(row[2],row[1], row[3]), 'tag': 'KOSPI CALL OPTION {}/{} STRIKE = {} SETTLEMENT PRICE'.format(row[2],row[1], row[3]), 'source' : Source},
                                   {'name': 'OptPricePutBid({}/{}){}'.format(row[2], row[1], row[3]), 'category_name': 'KOSPI OPT {}/{} STRIKE = {}'.format(row[2],row[1], row[3]), 'tag': 'KOSPI PUT OPTION {}/{} STRIKE = {} BID PRICE'.format(row[2],row[1], row[3]), 'source' : Source},
                                   {'name': 'OptPricePutAsk({}/{}){}'.format(row[2], row[1], row[3]), 'category_name': 'KOSPI OPT {}/{} STRIKE = {}'.format(row[2],row[1], row[3]), 'tag': 'KOSPI PUT OPTION {}/{} STRIKE = {} ASK PRICE'.format(row[2],row[1], row[3]), 'source' : Source},
                                   {'name': 'OptPricePutSetl({}/{}){}'.format(row[2], row[1], row[3]), 'category_name': 'KOSPI OPT {}/{} STRIKE = {}'.format(row[2],row[1], row[3]), 'tag': 'KOSPI PUT OPTION {}/{} STRIKE = {} SETTLEMENT PRICE'.format(row[2],row[1], row[3]), 'source' : Source}
                                   ])
            RatesHistory = RatesHistory.append([{'rates_name': 'OptPriceCallBid({}/{}){}'.format(row[2], row[1], row[3]), 'date': row[0], 'float_value' : row[3], 'string_value' : None, 'tag': 'KOSPI CALL OPTION {}/{} STRIKE = {} BID PRICE price date: {}'.format(row[2], row[1], row[3], row[0])},
                                                 {'rates_name': 'OptPriceCallAsk({}/{}){}'.format(row[2], row[1], row[3]), 'date': row[0], 'float_value' : row[3], 'string_value' : None, 'tag': 'KOSPI CALL OPTION {}/{} STRIKE = {} ASK PRICE date: {}'.format(row[2], row[1], row[3], row[0])},
                                                 {'rates_name': 'OptPriceCallSetl({}/{}){}'.format(row[2], row[1], row[3]), 'date': row[0], 'float_value' : row[3], 'string_value' : None, 'tag': 'KOSPI CALL OPTION {}/{} STRIKE = {} SETTLEMENT PRICE date: {}'.format(row[2], row[1], row[3], row[0])},
                                                 {'rates_name': 'OptPricePutBid({}/{}){}'.format(row[2], row[1], row[3]), 'date': row[0], 'float_value' : row[3], 'string_value' : None, 'tag': 'KOSPI PUT OPTION {}/{} STRIKE = {} BID PRICE date: {}'.format(row[2], row[1], row[3], row[0])},
                                                 {'rates_name': 'OptPricePutAsk({}/{}){}'.format(row[2], row[1], row[3]), 'date': row[0], 'float_value' : row[3], 'string_value' : None, 'tag': 'KOSPI PUT OPTION {}/{} STRIKE = {} ASK PRICE date: {}'.format(row[2], row[1], row[3], row[0])},
                                                 {'rates_name': 'OptPricePutSetl({}/{}){}'.format(row[2], row[1], row[3]), 'date': row[0], 'float_value' : row[3], 'string_value' : None, 'tag': 'KOSPI PUT OPTION {}/{} STRIKE = {} SETL PRICE date: {}'.format(row[2], row[1], row[3], row[0])},
                                                 ])

        Category = Category.drop_duplicates()
        Rates = Rates.drop_duplicates()
        RatesHistory = RatesHistory.drop_duplicates()

        # 6. Save to database prepared data
        if SaveToDB:
            self.manager.save_raw_data(Category, Rates, RatesHistory, Source)
        return [Category, Rates, RatesHistory]

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
                    category_id=tag[row['Put^Call']][0],
                    source_id=source.id,
                    tag=tag[row['Put^Call']][1]
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

    def get_PDF_case_1(self, scan_path, file_extention = 'pdf', source='PDF_case_1'):
            rsrcmgr = PDFResourceManager()
            sio = StringIO()
            codec = 'utf-8'
            laparams = LAParams()
            device = TextConverter(rsrcmgr, sio, codec=codec, laparams=laparams)
            interpreter = PDFPageInterpreter(rsrcmgr, device)

            Category = pd.DataFrame([{'Name': 'CASE-1', 'Description': 'CASE #1 RAW DATA'},
                                     {'Name': 'Doc scans', 'Description': 'Documents scan row data', 'Parent_id': 1 }],
                      index=[1, 2])
            Rates = pd.DataFrame([{'Name': 'CASE-1-TextLayer', 'category_id': 2, 'Source': source, 'tag': 'CASE-1-TextLayer'},
                                   ],
                            index= [1])
            RatesHistory = pd.DataFrame()

            source_files = [f.lower() for f in os.listdir(scan_path) if file_extention in f.lower()]

            for f in source_files:
                fp = open(scan_path + f, 'rb')
                for page in PDFPage.get_pages(fp):
                    interpreter.process_page(page)
                fp.close()
                # Get text from StringIO
                text = sio.getvalue()
                #print(text)
                RatesHistory= RatesHistory.append([{'rates_id': 1, 'date': None, 'float_value': None, 'string_value': text, 'tag': f}])
            # Cleanup
            device.close()
            sio.close()
            RatesHistory = RatesHistory.reset_index()
            #print(Category, Rates, RatesHistory)
            DBManager.save_raw_data(Category, Rates, RatesHistory)
            #return (Category, Rates, RatesHistory)

#IL would be better to have this stuff as 'get_IrisFisher_Data'

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
#IL:should be using by save_raw_data()
            self.manager.session.add_all(rates_history)
            self.manager.session.commit()

        except Exception as e:
            self.manager.session.rollback()
            raise e

'''
#example of usage

if __name__ == '__main__':
    from manager.DBManager import DBManager
    from models.models import *
    import datetime
    DB = DBManager(db_name = 'ONN2', user='postgres', host='localhost', password = 'kplus12')
    cats = [Category(name='futures', description='azaza'),
            Category(name='call', description='azaza'),
            Category(name='put', description='azaza'),
            Category(name='cbr', description='azaza')]
    DB.session.add_all(cats)
    etl = ETL(manager=DB)
    etl.get_Kospi_ex1("C:/Users/IvanL/OpenTRM/Presales/FinTech/Test Examples/Kopi Quote Eikon Loader.xlsx")
    etl.get_JapanExchange_Derivatives_ex2('../rb_e20161027.txt.csv')
    etl.get_CBR_ex3(datetime.datetime(2016, 10, 10), datetime.datetime.now())
'''
