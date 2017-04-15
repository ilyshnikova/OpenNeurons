from io                import BytesIO

from manager.dbmanager import DBManager

import pandas as pd
import quandl as qu

import requests
import datetime
import zipfile
import json

api_key = ""


class Quandl:
    def __init__(self):
        self.start = '2000-01-01'
        self.end = datetime.date.today().strftime("%Y-%m-%d")
        self.manager = DBManager()

    def update(self):
        with open('portfolio.json') as data_file:
            portfolio = json.load(data_file)

        for asset_name in list(portfolio.keys()):
            category = (pd.DataFrame(portfolio[asset_name]).T).reset_index()
            category = category.iloc[:-1, 1:]
            q_ticket = portfolio[asset_name]['source']
            print('send')
            self.get(Category=category, q_ticket=q_ticket, start=self.start, end=self.end)

    def get_info(self, q_ticket):
        try:
            database, ticket = q_ticket.split("/")
            # This API call is used to download all of the dataset codes and dataset names available in this database.
            url = 'https://www.quandl.com/api/v3/databases/' + database + '/codes?' + api_key
            r = requests.get(url)
            z = zipfile.ZipFile(BytesIO(r.content))
            df = pd.read_csv(z.open(name=database + '-datasets-codes.csv'), header=None)
            print(df)
            return df[df[0].str.contains(q_ticket + '$')]

        except Exception as e:
            raise e

    def get(self, Category, q_ticket, start, end, collapse='daily', SaveToDB=True):
        if len(q_ticket.split("/")) == 2:
            dfData = pd.DataFrame(qu.get(q_ticket, start_date=start, end_date=end, collapse=collapse, returns="numpy"))
        else:
            raise Exception("Correct the quandl ticket")

        if dfData.empty == True:
            raise Exception('Package is empty')

        Rates = pd.DataFrame()
        RatesHistory = pd.DataFrame()

        trg_category = Category['name'].iloc[-1:].values[0]
        print('trg_category:', trg_category)

        for rate in dfData.columns.values[1:]:
            Rates = Rates.append({'name': trg_category + "_" + rate, 'category_name': trg_category, 'tag': None}, ignore_index=True)
            print(Rates)
            for idx in range(dfData.shape[0]):
                RatesHistory = RatesHistory.append(
                    {'rates_name': trg_category + "_" + rate, 'date': dfData['Date'][idx], 'float_value': dfData[rate][idx],
                     'string_value': None, 'tag': None}, ignore_index=True)
            print(RatesHistory)

        print("Category:", Category)
        if SaveToDB:
            self.manager.save_raw_data(category=Category, rates=Rates, rateshistory=RatesHistory, source='Quandl')

        return [Category, Rates, RatesHistory]