from docutils.io import InputError

from manager.dbmanager import DBManager
from models.models import *

from datetime import datetime as dtt

import pandas as pd
import numpy as np


class DataPreprocessing:
    def __init__(self):
        self.manager = DBManager()

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

    def prepare_and_save(self, data, rate_name, data_tag):
        """
        data: Series
        rate_name: string
        data_tag: string
        """
        category = self.manager.get_raw_data(rate_name)[0][['description', 'name', 'parent_name']]
        rates = self.manager.get_raw_data(rate_name)[1][['category_name', 'name', 'source', 'tag']]
        rateshistory = pd.DataFrame()
        for idx in data.index:
            rateshistory = rateshistory.append(
                {'rates_name': rate_name, 'date': idx, 'float_value': data[idx], 'string_value': None, 'tag': data_tag},
                ignore_index=True)

        source = rates['source'].values[0]
        self.manager.save_raw_data(category, rates, rateshistory, source)
        try:
            tag = self.manager.session.query(Rates.tag).filter(Rates.name == rate_name).one()
            if tag[0] is None:
                tag_new = data_tag
            else:
                tag_new = tag[0] + '|{0}'.format(data_tag)
            self.manager.session.query(Rates).filter(Rates.name == rate_name).update({"tag": tag_new})
            self.manager.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e

    def check_anomalies(self, rate_name, data=None, tag=None, start=None, end=None, save_to_db=False):
        """
        Parameters
        ----------
        rate_name: string
        data: Series
        tag: string
        start: datetime
        end: datetime
        save_to_db: bool

        Returns
        -------
        If save_to_db is False, function will return Series of clean data from anomalies
        """
        if data is None:
            data = self.manager.get_timeseries(rate_name=rate_name, tag=tag, start=start, end=end)

        abs_daily_return = abs(data.diff(periods=1)).dropna()
        bound = abs_daily_return.mean() + 3 * abs_daily_return.std()
        anomaly_idx = list(abs_daily_return[abs_daily_return > bound].index)[::2]

        if len(anomaly_idx) == 0:
            print('No anomalies were found by the method of three sigma')
            print('Data will not be saved or returd')
        else:
            for idx in anomaly_idx:
                data.set_value(idx, None).fillna(method='ffill')
            if save_to_db:
                self.prepare_and_save(data=data, rate_name=rate_name, data_tag='CA')
            else:
                return data

    def check_moex_gaps(self, rate_name, tag=None, start=None, end=None, save_to_db=False):
        """
        Parameters
        ----------
        rate_name: string
        tag: string
        save_to_db: bool

        Returns
        -------
        If save_to_db is False, function will return Series of clean data from time gaps
        """
        data = self.manager.get_timeseries(rate_name=rate_name, tag=tag)
        data = data[start:end]

        bdays = pd.read_sql(self.manager.session.query(RatesHistory.date). \
                         join(Rates). \
                         join(Category). \
                         filter(Category.name == 'MCX_Business_Days').statement,
                         self.manager.session.bind, parse_dates={'date': None}).date

        wdays = pd.read_sql(self.manager.session.query(RatesHistory.date). \
                            join(Rates). \
                            join(Category). \
                            filter(Category.name == 'MCX_Weekend_Days').statement,
                            self.manager.session.bind, parse_dates={'date': None}).date

        bdays_left_slice = bdays[bdays == data.index[0]].index[0]
        bdays_right_slice = bdays[bdays == data.index[-1]].index[0]
        bdays = bdays[bdays_left_slice:bdays_right_slice + 1]

        bdays_gaps_indicator = bdays.isin(data.index)
        if bdays_gaps_indicator[bdays_gaps_indicator == False].empty:
            print('No business days gaps')
        else:
            bdays_detected_gaps = [bdays[idx] for idx in bdays_gaps_indicator[bdays_gaps_indicator == False].index]
            for date in bdays_detected_gaps:
                data = data.append(pd.Series(None, index=[date]))
            data = data.sort_index().fillna(method='ffill')

        wdays_left_slice = self.__find_nearest_weekend_idx(wdays, data.index[0], mode='right')
        wdays_right_slice = self.__find_nearest_weekend_idx(wdays, data.index[-1:][0], mode='left')
        wdays = wdays[wdays_left_slice:wdays_right_slice + 1]

        wdays_gaps_indicator = data.index.isin(wdays)
        if wdays_gaps_indicator[wdays_gaps_indicator == True].size == 0:
            print('No weekend days gaps')
        else:
            wdays_detected_gaps = pd.DatetimeIndex([wdays[idx] for idx in wdays_gaps_indicator[wdays_gaps_indicator == True].index])
            data = data.drop(wdays_detected_gaps, axis=0)

        if save_to_db:
            self.prepare_and_save(data=data, rate_name=rate_name, data_tag='CG')
        else:
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