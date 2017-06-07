from manager.dbmanager import DBManager

from zeep import Client
from lxml import html

import pandas as pd
import datetime
import requests


class CBR:
    """
    Class CBR provides access to information and data manipulation of CBR data.
    """
    def __init__(self):
        self.manager = DBManager()
        self.actions_map = {
            "refinancing_rate": self.refinancing_rate,
            "key_rate": self.key_rate,
            "bival_curse": self.bival_curse,
        }

    def prepare_to_save(self, data, rate_name, source):
        """
        Prepare to save trading calendar for given year

        Parameters
        ----------
        data: Series
        rate_name: string
        source: string

        Returns
        -------
        list(DataFrame, DataFrame, DataFrame)
        """
        category = pd.DataFrame([
            {'name': 'Central_Bank_of_Russia', 'description': 'Central Bank of Russia'},
            {'name': rate_name, 'description': 'From {0} to {1}'.format(data.index[0], data.index[-1]), 'parent_name': 'Central_Bank_of_Russia'}
        ])

        rates = pd.DataFrame([{'name': rate_name, 'category_name': rate_name, 'source': source, 'tag': None}],
                             index=[1])

        rateshistory = pd.DataFrame()
        for idx in data.index:
            rateshistory = rateshistory.append(
                {'rates_name': rate_name, 'date': idx, 'float_value': data[idx], 'string_value': None, 'tag': None},
                ignore_index=True)

        return [category, rates, rateshistory]

    def save(self, category, rates, rateshistory, source):
        """
        Save to Database

        Parameters
        ----------
        category: DataFrame
        rates: DataFrame
        rateshistory: DataFrame
        source: string
        """
        self.manager.save_raw_data(category, rates, rateshistory, source)

    def parse_finam(self, url, rate_name, start, end):
        """
        Parse Finam.ru site

        Parameters
        ----------
        url: string
        rate_name: string
        start: datetime
        end: datetime

        Returns
        -------
        Series
        """
        tree = html.fromstring(requests.get(url).text)
        dates = [date.text for date in tree.xpath('//td[@class="sm"]')]
        rates = [rate.text for rate in tree.xpath('//td[@align="right"]')]
        rates = [float(rate.split()[0]) for rate in filter(lambda k: '\xa0' in k, filter(None, rates))]

        data = pd.Series(data=rates, index=[pd.to_datetime(dates)])
        data = data[~data.index.duplicated()]
        all_datetime_index = pd.date_range(start=data.index[-1], end=pd.to_datetime(datetime.date.today()))
        data = data.sort_index().reindex(all_datetime_index, method='pad')

        data = data[start: end]

        return data

    def refinancing_rate(self, start, end, save_to_db):
        """
        Load Refinancing Rate of Central Bank of Russia from Finam.ru

        Parameters
        ----------
        start: datetime
        end: datetime
        save_to_db: bool

        Returns
        -------
        If save_to_db is False, function will return Series of rate with date index
        """
        url = "https://www.finam.ru/analysis/macroevent/?dind=0&dpsd=1817681&fso=date+desc&str=1&ind=710&" \
              "stdate=16.09.2013&endate=28.04.2017&sema=1&seman=5&timeStep=1"
        rate_name = 'CBR_Refinancing_Rate'
        source = 'finam.ru'

        data = self.parse_finam(url, rate_name, start, end)

        if save_to_db:
            category, rates, rateshistory = self.prepare_to_save(data=data, rate_name=rate_name, source=source)
            self.save(category=category, rates=rates, rateshistory=rateshistory, source=source)
        else:
            return data

    def key_rate(self, start, end, save_to_db):
        """
        Load Central Bank of Russia Key Rate from Finam.ru

        Parameters
        ----------
        start: datetime
        end: datetime
        save_to_db: bool

        Returns
        -------
        If save_to_db is False, function will return Series of rate with date index
        """
        url = "https://www.finam.ru/analysis/macroevent/?dind=0&dpsd=3980881&fso=date+desc&str=1&ind=1555&" \
              "stdate=01.01.1991&endate=11.12.2015&sema=1&seman=5&timeStep=1"
        rate_name = 'CBR_Key_Rate'
        source = 'finam.ru'

        data = self.parse_finam(url, rate_name, start, end)

        if save_to_db:
            category, rates, rateshistory = self.prepare_to_save(data=data, rate_name=rate_name, source=source)
            self.save(category=category, rates=rates, rateshistory=rateshistory, source=source)
        else:
            return data

    def bival_curse(self, start, end, save_to_db):
        """
        Load Bival Curse from Central Bank of Russia SOAP server

        Parameters
        ----------
        start: datetime
        end: datetime
        save_to_db: bool

        Returns
        -------
        If save_to_db is False, function will return Series of rate with date index
        """
        start = datetime.datetime(1993, 1, 1) if start is None else start
        end = datetime.datetime.now() if end is None else end

        client = Client('http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL')
        response = client.service.BiCurBase(start, end)

        data = []
        header = ['D0', 'VAL']
        for tbl in response['_value_1'].getchildren()[0].xpath('//BCB'):
            row = [tbl.xpath(col)[0].text for col in header]
            data.append(row)

        data = pd.DataFrame(data, columns=['date', 'curse'])
        data = data.set_index(pd.DatetimeIndex(data['date']))['curse'].astype(float)

        rate_name = 'Bival_Curse_Base'
        source = 'cbr.ru'

        if save_to_db:
            category, rates, rateshistory = self.prepare_to_save(data, rate_name, source)
            self.save(category=category, rates=rates, rateshistory=rateshistory, source=source)
        else:
            return data

    def get(self, request, start=None, end=None, save_to_db=False):
        """
        Binding function to query data.

        Parameters
        ----------
        request: string
        start: datetime
        end: datetime
        save_to_db: bool
        """
        for options, action in self.actions_map.items():
            if request in options:
                return action(start, end, save_to_db)