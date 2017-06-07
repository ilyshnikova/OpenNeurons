from manager.dbmanager import DBManager

import pandas as pd
import numpy as np

class MOEX:
    """
    Class MOEX provides access to information and data manipulation of MOEX data.
    """
    def __init__(self):
        self.manager = DBManager()
        self.actions_map = {
            "trading_calendar": self.trading_calendar,
        }

    def prepare_to_save_trading_calendar(self, days_category, year, data):
        """
        Prepare to save trading calendar for given year

        Parameters
        ----------
        days_category: string
        year: string
        data: series

        Returns:
        ----------
        list(DataFrame, DataFrame, DataFrame)
        """
        days_category_ = days_category.split()[0] + '_' + days_category.split()[1]
        category = pd.DataFrame([{'name': 'Exchanges Trading Calendar', 'description': 'Business and Weekend days'},
                                 {'name': 'MCX', 'description': 'Moscow Exchange', 'parent_name': 'Exchanges Trading Calendar'},
                                 {'name': 'MCX_{0}'.format(days_category_), 'description': 'Moscow Exchange {0}'.format(days_category), 'parent_name': 'MCX'}])

        rates = pd.DataFrame([{'name': 'MCX_{0}_{1}'.format(days_category_, year), 'category_name': 'MCX_{0}'.format(days_category_),
                              'source': 'MOEX.COM', 'tag': '{0} in {1} year'.format(days_category, year)}], index=[1])

        rateshistory = pd.DataFrame()
        data.reset_index(drop=True, inplace=True)

        for idx in range(len(data)):
            rateshistory = rateshistory.append(
                    {'rates_name': 'MCX_{0}_{1}'.format(days_category_, year), 'date': data[idx].strftime("%Y-%m-%d"),
                     'float_value': None, 'string_value': data[idx].strftime("%Y-%m-%d"), 'tag': None},
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

    def trading_calendar(self, start, end, save_to_db):
        """
        Uploading from xlsx file the MOEX trading calendar from 2012 year to 2017 year

        Parameters
        ----------
        start: datetime
        end: datetime
        save_to_db: bool

        Returns
        -------
        Aggregated DataFrame of selected years
        """
        data = pd.read_excel('local_files/MICEX_BD.xlsx')

        start = pd.to_datetime('2012') if start is None else start
        end = pd.to_datetime('2017') if end is None else end
        years = [str(year) for year in range(start.year, end.year + 1)]

        frames = []
        names = []
        for year in years:
            year_dates = data.filter(regex=year)
            year_dates = year_dates[~year_dates['YEAR_' + year].isnull()].fillna(0)
            business_days = year_dates[year_dates['FD_' + year] == False]['YEAR_' + year]
            weekend_days = year_dates[year_dates['FD_' + year] == True]['YEAR_' + year]

            if save_to_db:
                bd_category, bd_rates, bd_rateshistory = self.prepare_to_save_trading_calendar('Business Days', year, business_days)
                self.save(category=bd_category, rates=bd_rates, rateshistory=bd_rateshistory, source='MOEX.COM')
                wd_category, wd_rates, wd_rateshistory = self.prepare_to_save_trading_calendar('Weekend Days', year, weekend_days)
                self.save(category=wd_category, rates=wd_rates, rateshistory=wd_rateshistory, source='MOEX.COM')
            else:
                frames.append(business_days)
                frames.append(weekend_days)
                names.append('business_days_{0}'.format(year))
                names.append('weekend_days_{0}'.format(year))

        if save_to_db is False:
            agregate_data = pd.concat(frames, axis=1)
            agregate_data.columns = names
            return agregate_data



        # if save_to_db:
        #     for year in years:
        #         year_dates = data.filter(regex=year)
        #         year_dates = year_dates[~year_dates['YEAR_' + year].isnull()].fillna(0)
        #
        #         business_days = year_dates[year_dates['FD_' + year] == False]['YEAR_' + year]
        #         weekend_days = year_dates[year_dates['FD_' + year] == True]['YEAR_' + year]
        #
        #         self.prepare_to_save_trading_calendar('Business Days', year, business_days)
        #         self.prepare_to_save_trading_calendar('Weekend Days', year, weekend_days)
        # else:
        #     frames = []
        #     names = []
        #
        #     for year in years:
        #         year_dates = data.filter(regex=year)
        #         year_dates = year_dates[~year_dates['YEAR_' + year].isnull()].fillna(0)
        #
        #         business_days = year_dates[year_dates['FD_' + year] == False]['YEAR_' + year]
        #         weekend_days = year_dates[year_dates['FD_' + year] == True]['YEAR_' + year]
        #
        #         frames.append(business_days)
        #         frames.append(weekend_days)
        #         names.append('business_days_{0}'.format(year))
        #         names.append('weekend_days_{0}'.format(year))
        #
        #     agregate_data = pd.concat(frames, axis=1)
        #     agregate_data.columns = names
        #
        #     return agregate_data

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
        for options, action in self.actions_map.items():
            if request in options:
                return action(start, end, save_to_db)