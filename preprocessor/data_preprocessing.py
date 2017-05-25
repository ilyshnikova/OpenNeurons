from docutils.io import InputError

from manager.dbmanager import DBManager
from models.models import *

from datetime import datetime as dtt

import pandas as pd
import numpy as np


class DataPreprocessing:
    def __init__(self, manager: DBManager):
        self.manager = manager

    def __find_nearest_weekend_idx(self, wd_moex, date, mode):
        deltas = wd_moex - date
        if mode == "left":
            deltas_ = deltas[deltas <= pd.Timedelta('0 days 00:00:00.0')]
            if len(deltas_):
                idx_closest_date = np.argmax(deltas_)
        elif mode == "right":
            deltas_ = deltas[deltas >= pd.Timedelta('0 days 00:00:00.0')]
            if len(deltas_):
                idx_closest_date = np.argmin(deltas_)

        return idx_closest_date

    def check_anomalies(self, rate_name, tag=None, SaveToDB=False, Inform=False):
        data = self.manager.get_raw_data(RateName=rate_name, Tag=tag)[2][['date', 'float_value']]
        data = data.set_index(data['date'])['float_value']
        indexx = pd.Index(pd.to_datetime(data.index))
        data = pd.DataFrame(data)
        data = data.set_index(indexx)

        data_diff = data.diff(periods=1)
        data_val = abs(data_diff.float_value)[1:]
        bound = data_val.mean() + 3 * data_val.std()
        anomaly_idx = list(data_val[data_val > bound].index)[::2]

        if len(anomaly_idx) > 0:
            if Inform:
                print('Abnormal values exist')
                for idx in anomaly_idx:
                    print('Anomaly falue: "date": ', idx, ' "value": ', data.get_value(idx, 'float_value'))

            data = data.drop(anomaly_idx, axis=0)

            if SaveToDB:
                category = self.manager.get_raw_data(rate_name)[0][['description', 'name', 'parent_name']]
                rates = self.manager.get_raw_data(rate_name)[1][['category_name', 'name', 'source', 'tag']]

                rateshistory = pd.DataFrame()
                rate_name = rates.name.values[0]
                col_name = data.columns.values[0]
                for idx in data.index:
                    rateshistory = rateshistory.append(
                        {'rates_name': rate_name, 'date': idx, 'float_value': data.get_value(idx, col_name), 'string_value': None, 'tag': 'CA'}, ignore_index=True)

                source = rates['source'].values[0]
                self.manager.save_raw_data(category, rates, rateshistory, source)
                try:
                    tag = self.manager.session.query(Rates.tag).filter(Rates.name == rate_name).one()
                    if tag[0] is None:
                        tag_new = 'CA'
                    else:
                        tag_new = tag[0] + '|CA'
                    self.manager.session.query(Rates).filter(Rates.name == rate_name).update({"tag": tag_new})
                    self.manager.session.commit()
                except Exception as e:
                    self.session.rollback()
                    raise e

            return data
        else:
            print('According to the rule of two sigma, no anomalies were found')
            print('Data will not be saved')
            return data

    def check_moex_gaps(self, rate_name, tag=None, SaveToDB=False, Inform=False):
        data = self.manager.get_raw_data(RateName=rate_name, Tag=tag)[2][['date', 'float_value']]
        data = data.set_index(data['date'])['float_value']
        indexx = pd.Index(pd.to_datetime(data.index))
        data = pd.DataFrame(data)
        data = data.set_index(indexx)

        if data.shape[1] != 1:
            raise InputError(data, 'Shape more than 1')

        bd_moex = pd.Series(self.manager.session.query(RatesHistory.date).join(Rates).join(Category).filter(Category.name == 'MCX Business days').all())
        wd_moex = pd.Series(self.manager.session.query(RatesHistory.date).join(Rates).join(Category).filter(Category.name == 'MCX Weekend days').all())

        bd_moex = bd_moex.apply(lambda x: x[0])
        wd_moex = wd_moex.apply(lambda x: x[0])
        wd_moex = pd.to_datetime(wd_moex)

        str_data_dates = data.index.astype(str)
        str_bd_moex = bd_moex.astype(str)

        st = str_bd_moex[str_bd_moex == str_data_dates[0]].index[0]
        nd = str_bd_moex[str_bd_moex == str_data_dates[-1:][0]].index[0]
        str_bd_moex = str_bd_moex[st:nd]

        if str_bd_moex.equals(str_data_dates):
            print('No working days gaps')
        else:
            if Inform:
                print('Equals dates with market calendar? -', str_bd_moex.equals(str_data_dates))
                print('Input data len =', len(str_data_dates))
                print('MOEX working period len =', len(str_bd_moex))

            comp = str_bd_moex.isin(str_data_dates)
            mis_dates = np.array([])
            for i in comp[comp==False].index:
                if Inform:
                    print('Missing: ', str_bd_moex[i])
                mis_dates = np.append(mis_dates, str_bd_moex[i])

            mis_dates = pd.Index(pd.to_datetime(mis_dates))
            dfMis_dates = pd.DataFrame(None, index=[mis_dates])

            data = data.append(dfMis_dates)
            data = data.sort_index()

            for date in mis_dates:
                nan_idx = data.index.get_loc(date)
                data.iloc[nan_idx] = data.iloc[nan_idx - 1]

        wk_st_idx = self.__find_nearest_weekend_idx(wd_moex, data.index[0], mode='right')
        wk_nd_idx = self.__find_nearest_weekend_idx(wd_moex, data.index[-1:][0], mode='left')

        wd_moex = wd_moex[wk_st_idx : wk_nd_idx]

        comp = pd.Series(data.index.isin(wd_moex))
        mis_dates = np.array([])
        for i in comp[comp==True].index:
            print('Erroneous business day: ', data.index[i])
            mis_dates = np.append(mis_dates, data.index[i])

        mis_dates = pd.Index(pd.to_datetime(mis_dates))
        data = data.drop(mis_dates, axis=0)

        if SaveToDB:
            category = self.manager.get_raw_data(rate_name)[0][['description', 'name', 'parent_name']]
            rates = self.manager.get_raw_data(rate_name)[1][['category_name', 'name', 'source', 'tag']]

            rateshistory = pd.DataFrame()
            rate_name = rates.name.values[0]
            col_name = data.columns.values[0]
            for idx in data.index:
                rateshistory = rateshistory.append(
                    {'rates_name': rate_name, 'date': idx, 'float_value': data.get_value(idx, col_name), 'string_value': None, 'tag': 'CG'}, ignore_index=True)

            source = rates['source'].values[0]
            self.manager.save_raw_data(category, rates, rateshistory, source)
            try:
                tag = self.manager.session.query(Rates.tag).filter(Rates.name == rate_name).one()
                if tag[0] is None:
                    tag_new = 'CG'
                else:
                    tag_new = tag[0] + '|CG'
                self.manager.session.query(Rates).filter(Rates.name == rate_name).update({"tag": tag_new})
                self.manager.session.commit()
            except Exception as e:
                self.session.rollback()
                raise e

        return data

    def prct_change(self, rate_name, tag=None, shift=1, resample='D', period_start=None, period_end=None, SaveToDB=False):
        data = self.manager.get_raw_data(RateName=rate_name, Tag=tag)[2][['date', 'float_value']]
        data = data.set_index(data['date'])['float_value']
        indexx = pd.Index(pd.to_datetime(data.index))
        data = pd.DataFrame(data)
        data = data.set_index(indexx)

        if data.shape[1] != 1:
            raise InputError(data, 'Shape more than 1')

        if resample != 'D':
            data = data.resample(resample, how=lambda x: x[-1])
        data = data.pct_change(periods=1)
        if resample == 'W':
            data = data*7/365*100
        elif resample == 'M':
            data = data*30/365*100
        elif resample == 'D':
            data = data*1/365*100

        if SaveToDB:
            category = self.manager.get_raw_data(rate_name)[0][['description', 'name', 'parent_name']]
            rates = self.manager.get_raw_data(rate_name)[1][['category_name', 'name', 'source', 'tag']]

            rateshistory = pd.DataFrame()
            rate_name = rates.name.values[0]
            col_name = data.columns.values[0]
            for idx in data.index:
                rateshistory = rateshistory.append(
                    {'rates_name': rate_name, 'date': idx, 'float_value': data.get_value(idx, col_name),
                     'string_value': None, 'tag': 'PC[{0}]'.format(shift)}, ignore_index=True)

            source = rates['source'].values[0]
            self.manager.save_raw_data(category, rates, rateshistory, source)
            try:
                tag = self.manager.session.query(Rates.tag).filter(Rates.name == rate_name).one()
                if tag[0] is None:
                    tag_new = 'PC[{0}]'.format(shift)
                else:
                    tag_new = tag[0] + '|PC[{0}]'.format(shift)
                self.manager.session.query(Rates).filter(Rates.name == rate_name).update({"tag": tag_new})
                self.manager.session.commit()
            except Exception as e:
                self.session.rollback()
                raise e

            return data
        else:
            return data




# if resample != 'D':
#     data = data.resample(resample, how=lambda x: x[-1])
# data = data.pct_change(periods=1)
# if resample == 'W':
#     data = data*7/365*100
# elif resample == 'M':
#     data = data*30/365*100
# elif resample == 'D':
#     data = data*1/365*100