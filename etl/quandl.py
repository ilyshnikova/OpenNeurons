from io                import BytesIO

from manager.dbmanager import DBManager

import pandas as pd
import quandl as qu

import requests
import datetime
import zipfile

qu_api_key=""
qu.ApiConfig.api_key = qu_api_key


class Quandl:
    def __init__(self, period_start):
        self.start = period_start
        self.end = datetime.date.today().strftime("%Y-%m-%d")
        self.manager = DBManager()

    def update(self, asset, ticket):
        category = (pd.DataFrame(asset).T).reset_index()
        category = category.iloc[:-2, 1:]
        self.get(start=self.start, end=self.end, SaveToDB=True, category=category, ticket=ticket)

    def get_info(self, q_ticket):
        try:
            database, ticket = q_ticket.split("/")
            # This API call is used to download all of the dataset codes and dataset names available in this database.
            url = 'https://www.quandl.com/api/v3/databases/' + database + '/codes?' + qu_api_key
            r = requests.get(url)
            z = zipfile.ZipFile(BytesIO(r.content))
            df = pd.read_csv(z.open(name=database + '-datasets-codes.csv'), header=None)
            return df[df[0].str.contains(q_ticket + '$')]

        except Exception as e:
            raise e

    def save(self, dfData, category):
        Rates = pd.DataFrame()
        RatesHistory = pd.DataFrame()

        trg_category = category['name'].iloc[-1:].values[0]

        for rate in dfData.columns.values:
            Rates = Rates.append({'name': trg_category + "_" + rate, 'category_name': trg_category, 'tag': None}, ignore_index=True)
            for idx in range(dfData.shape[0]):
                RatesHistory = RatesHistory.append(
                    {'rates_name': trg_category + "_" + rate, 'date': dfData.index[idx].strftime("%Y-%m-%d"),
                     'float_value': dfData[rate][idx], 'string_value': None, 'tag': None}, ignore_index=True)

        self.manager.save_raw_data(category=category, rates=Rates, rateshistory=RatesHistory, source='Quandl')
        return [category, Rates, RatesHistory]

    def get(self, ticket, start, end, collapse='daily', SaveToDB=False, category=None):
        if len(ticket.split("/")) == 2:
            dfData = pd.DataFrame(qu.get(ticket, start_date=start, end_date=end, collapse=collapse))
        else:
            raise Exception("Correct the quandl ticket")

        if start < dfData.index[0]:
            print('Date range start = {0}'.format(dfData.index[0].strftime("%Y-%m-%d")))

        if SaveToDB:
            self.save(dfData, category)
        else:
            return dfData

