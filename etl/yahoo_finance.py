from manager.dbmanager import DBManager

from pandas_datareader import data

import pandas as pd
import quandl as qu

import datetime

class YahooFinance:
    def __init__(self, period_start):
        self.start = period_start
        self.end = datetime.date.today().strftime("%Y-%m-%d")
        self.manager = DBManager()

    def update(self, asset, ticket):
        category = (pd.DataFrame(asset).T).reset_index()
        category = category.iloc[:-2, 1:]
        self.get(Category=category, ticket=ticket, start=self.start, end=self.end)

    def save(self, Category, dfData):
        Rates = pd.DataFrame()
        RatesHistory = pd.DataFrame()

        trg_category = Category['name'].iloc[-1:].values[0]

        for rate in dfData.columns.values[1:]:
            Rates = Rates.append({'name': trg_category + "_" + rate, 'category_name': trg_category, 'tag': None},
                                 ignore_index=True)
            for idx in range(dfData.shape[0]):
                RatesHistory = RatesHistory.append(
                    {'rates_name': trg_category + "_" + rate, 'date': dfData[rate].index[idx].strftime("%Y-%m-%d"),
                     'float_value': dfData[rate][idx],
                     'string_value': None, 'tag': None}, ignore_index=True)

        self.manager.save_raw_data(category=Category, rates=Rates, rateshistory=RatesHistory, source='Yahoo Finance')

        return [Category, Rates, RatesHistory]

    def get(self, ticket, start, end, SaveToDB=False, Category=None):
        dfData = data.DataReader(ticket, 'yahoo', start=start, end=end)

        if SaveToDB:
            self.save(Category, dfData)
        else:
            return dfData


