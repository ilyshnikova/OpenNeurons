from manager.dbmanager import DBManager

from io import BytesIO
import pandas as pd
import quandl as qu

import requests
import datetime
import zipfile
import os

qu_api_key=""
qu.ApiConfig.api_key = qu_api_key

class Quandl:
    """
    Class Quandl provides access to information and data manipulation of Quandl data.
    """
    def __init__(self):
        self.manager = DBManager()
        self.actions_map = {}
        self.exchanges = {'MCX': pd.DataFrame([{'name': 'Financial Markets', 'description': 'Financial Markets Data Branch'},
                                               {'name': 'Europe', 'description': 'Europe', 'parent_name': 'Financial Markets'},
                                               {'name': 'Russia', 'description': 'Russia', 'parent_name': 'Europe'},
                                               {'name': 'MCX', 'description': 'Moscow Exchange', 'parent_name': 'Russia'}])
                          ,}

    def category_information(self, request):
        """
        Parameters
        ----------
        request: string

        Returns
        -------
        list(string, string, string)
        """
        database = request.split('/')[0]
        exchange = request.split('/')[1].split("_")[0]
        ticker = request.split('/')[1].split("_")[1]
        db_file_location = 'local_files/'+database+"-datasets-codes.csv"

        if os.path.isfile(db_file_location) is False:
            url = 'https://www.quandl.com/api/v3/databases/' + database + '/codes?' + qu_api_key
            request = requests.get(url)
            z = zipfile.ZipFile(BytesIO(request.content))

            output = open(db_file_location, "wb")
            output.write(z.open(name=database + '-datasets-codes.csv').read())
            output.close()

        with open(db_file_location) as file:
            content = file.readlines()

        content = [row.strip().split(',') for row in content]
        content = pd.DataFrame(content).iloc[:, :2]
        content.columns = ['ticker', 'description']

        category_description = content[content['ticker'] == request]['description'].values[0]
        category_name = exchange+':'+ticker

        return [category_name, category_description, exchange]

    def prepare_to_save_quandl_data(self, request, data):
        """
        Parameters
        ----------
        request: string
        data: DataFrame

        Returns
        ----------
        list(DataFrame, DataFrame, DataFrame)
        """
        category_name, category_description, exchange_symbol = self.category_information(request)

        category = self.exchanges.get(exchange_symbol, pd.DataFrame())
        category = category.append({'name': category_name, 'description': category_description, 'parent_name': exchange_symbol},
                                    ignore_index=True)

        rates = pd.DataFrame()
        rateshistory = pd.DataFrame()
        for rate in data.columns.values:
            rates = rates.append({'name': category_name+"_"+rate, 'category_name': category_name, 'tag': None},
                                 ignore_index=True)
            for idx in range(data.shape[0]):
                rateshistory = rateshistory.append(
                    {'rates_name': category_name+"_"+rate, 'date': data.index[idx].strftime("%Y-%m-%d"),
                     'float_value': data[rate][idx], 'string_value': None, 'tag': None},
                    ignore_index=True)

        return [category, rates, rateshistory]

    def access_quandl_databases(self, request, start, end, save_to_db):
        """
        Parameters
        ----------
        request: string
        start: datetime
        end: datetime
        SaveToDB: bool

        Returns
        -------
        DataFrame of Quandl data
        """
        data = pd.DataFrame(qu.get(request, start_date=start, end_date=end, collapse='daily'))

        if save_to_db:
            category, rates, rateshistory = self.prepare_to_save_quandl_data(request, data)
            self.save(category=category, rates=rates, rateshistory=rateshistory, source='Quandl.com')
        else:
            return data

    def save(self, category, rates, rateshistory, source):
        """
        Parameters
        ----------
        category: DataFrame
        rates: DataFrame
        rateshistory: DataFrame
        source: string
        """
        self.manager.save_raw_data(category, rates, rateshistory, source)

    def get(self, request, start=None, end=None, save_to_db=False):
        """
        Binding function to query data

        Parameters
        ----------
        request: string
        start: datetime
        end: datetime
        save_to_db: bool
        """
        if len(request.split("/")) == 2:
            return self.access_quandl_databases(request, start, end, save_to_db)


