from docutils.io import InputError

from manager.dbmanager import DBManager
from models.models import *

from datetime import datetime as dtt

import pandas as pd
import numpy as np


class DataPreprocessing:
    def __init__(self, manager: DBManager):
        self.manager = manager

    def check_moex_gaps(self, data, SaveToDB=False, category=None, rates=None, source=None):
        if type(data) == pd.Series:
            indexx = pd.Index(pd.to_datetime(data.index))
            data = pd.DataFrame(data)
            data = data.set_index(indexx)

        if data.shape[1] != 1:
            raise InputError(data, 'Shape more than 1')

        bd_moex = pd.Series(self.manager.session.query(RatesHistory.date).join(Rates).join(Category).filter(
            Category.name == 'MCX Business days').all())
        # wd_moex = pd.Series(self.manager.session.query(RatesHistory.date).join(Rates).join(Category).filter(
        #     Category.name == 'MCX Weekend days').all())

        bd_moex = bd_moex.apply(lambda x: x[0])

        str_data_dates = data.index.astype(str)
        str_bd_moex = bd_moex.astype(str)

        st = str_bd_moex[str_bd_moex == str_data_dates[0]].index[0]
        nd = str_bd_moex[str_bd_moex == str_data_dates[-1:][0]].index[0]
        str_bd_moex = str_bd_moex[st:nd]

        if str_bd_moex.equals(str_data_dates):
            print('No working days gaps')
        else:
            print('Equals dates with market calendar? -', str_bd_moex.equals(str_data_dates))
            print('Input data len =', len(str_data_dates))
            print('MOEX working period len =', len(str_bd_moex))

            comp = str_bd_moex.isin(str_data_dates)
            mis_dates = np.array([])
            for i in comp[comp==False].index:
                print('Missing: ', str_bd_moex[i])
                mis_dates = np.append(mis_dates, str_bd_moex[i])

            mis_dates = pd.Index(pd.to_datetime(mis_dates))
            dfMis_dates = pd.DataFrame(None, index=[mis_dates])

            data = data.append(dfMis_dates)
            data = data.sort_index()

            for date in mis_dates:
                nan_idx = data.index.get_loc(date)
                data.iloc[nan_idx] = data.iloc[nan_idx - 1]

        if SaveToDB:
            rateshistory = pd.DataFrame()
            rate_name = rates.name.values[0]
            col_name = data.columns.values[0]
            for idx in data.index:
                rateshistory = rateshistory.append(
                    {'rates_name': rate_name, 'date': idx, 'float_value': data.get_value(idx, col_name), 'string_value': None, 'tag': 'CL'}, ignore_index=True)

            self.manager.save_raw_data(category, rates, rateshistory, source)

        return data

    def prct_change(self, data, shift=252, CheckData=False, rates=None, SaveToDB=False, category=None, source=None):
        if type(data) == pd.Series:
            indexx = pd.Index(pd.to_datetime(data.index))
            data = pd.DataFrame(data)
            data = data.set_index(indexx)

        if data.shape[1] != 1:
            raise InputError(data, 'Shape more than 1')

        if CheckData:
            dfRatesHistory = pd.DataFrame(self.manager.session.query(RatesHistory.rates_id, Rates.name.label('rates_name'),
                                                                     RatesHistory.date, RatesHistory.float_value,
                                                                     RatesHistory.string_value, RatesHistory.tag). \
                                                                     join(Rates). \
                                                                     filter(Rates.name == rates.name[0]). \
                                                                     filter(RatesHistory.tag == 'CH[{0}]'.format(shift)). \
                                                                     all())
            if dfRatesHistory.empty:
                raise Exception('No available data with name {0}'.format(rates.name[0]))
            else:
                return dfRatesHistory
        else:
            data = data.pct_change(periods=shift)
            data = data*365/shift*100

            if SaveToDB:
                rateshistory = pd.DataFrame()
                rate_name = rates.name.values[0]
                col_name = data.columns.values[0]
                for idx in data.index:
                    rateshistory = rateshistory.append(
                        {'rates_name': rate_name, 'date': idx, 'float_value': data.get_value(idx, col_name), 'string_value': None, 'tag': 'CH[{0}]'.format(shift)}, ignore_index=True)

                self.manager.save_raw_data(category, rates, rateshistory, source)

                return data
            else:
                return data



