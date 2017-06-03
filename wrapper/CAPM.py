from manager.dbmanager import DBManager

import pandas as pd
import numpy  as np
import warnings
warnings.filterwarnings("ignore", category=np.VisibleDeprecationWarning)


class CAPM:
    def __init__(self):
        self.manager = DBManager()

    def effective_annual_interest_rate(self, data, horizon):
        data = (1 + data/(horizon/365))**(horizon/365)
        return data

    def calcs_beta(self, market_data, stock_data, deep, horizon, save_beta=False):
        """
        Parameters
        ----------
        market_data: Series
        stock_data: Series
        deep: int
        horizon: int

        Returns
        -------
        Series
        """
        market_data_new2old = market_data[::-1]
        stock_data_new2old = stock_data[::-1]

        betas = []
        for idx in range(0, len(market_data)-deep-1):
            market_deep = market_data_new2old[idx:idx+deep]
            stock_deep = stock_data_new2old[idx:idx+deep]

            erm = (-market_deep.diff(-horizon) / market_deep).dropna() * 100
            eri = (-stock_deep.diff(-horizon) / stock_deep).dropna() * 100

            beta = erm.cov(eri) / erm.var()
            betas.append(beta)

        betas = pd.Series(betas, index=market_data_new2old.index[:len(market_data_new2old) - deep - 1])
        return betas

    def prepare_and_save_beta(self, rate_name, market_rate_name, stock_rate_name, data):
        """
        Parameters
        ----------
        rate_name: string
        market_rate_name: string
        stock_rate_name: string
        data: Series
        """
        category = pd.DataFrame(
            [{'name': 'Indicators', 'description': 'Financial indicators'},
             {'name': 'Beta',
              'description': 'Coefficient indicates whether the investment is more or less volatile than the market as a whole',
              'parent_name': 'Indicators'},
             {'name': '{0} & {1}'.format(market_rate_name, stock_rate_name), 'description': None}], index=[1, 2, 3])

        rates = pd.DataFrame(
            [{'name': rate_name, 'category_name': '{0} & {1}'.format(market_rate_name, stock_rate_name), 'source': None, 'tag': None}],
            index=[1])

        ratehistory = pd.DataFrame()
        for idx in data.index:
            ratehistory = ratehistory.append(
                {'rates_name': rate_name, 'date': idx, 'float_value': data[idx], 'string_value': None, 'tag': None},
                ignore_index=True)

        self.manager.save_raw_data(category, rates, ratehistory, None)

    def prepare_and_save_data(self, rate_name, data, prediction):
        """
        Parameters
        ----------
        rate_name: string
        data: DataFrame
        prediction: Series
        """
        model_desc = pd.DataFrame({'model_name': 'CAPM', 'description': 'Capital Asset Pricing Model', 'model_type': None},
                              index=[0])
        dataset_desc = pd.DataFrame({'name': rate_name}, index=[0])

        data.columns = ['x{0}'.format(idx) for idx in range(1, data.shape[1] + 1)]

        self.manager.save_dataset(model_inform=model_desc, dataset_inform=dataset_desc, dataset=data, target=None)
        self.manager.save_model_prediction(model=model_desc, dataset=dataset_desc, prediction=pd.DataFrame(prediction))

    def run(self, market_rate_name, stock_rate_name, rfr_rate_name, deep=250, horizon=7, market_data=None, stock_data=None,
            rfr_data=None, period_start=None, period_end=None, market_tag=None, stock_tag=None, save_beta=False, save_to_db=False):
        """

        Parameters
        ----------
        market: string
        market_tag: string
        market_data: Series
        stock: string
        stock_tag: string
        stock_data: Series
        risk_free_rate: string
        period_start: datetime
        period_end: datetime
        deep: int
        horizon: int
        save_to_db: bool
        save_beta: bool

        Returns
        -------
        Series
        """
        if market_data is None:
            market_data = self.manager.get_timeseries(rate_name=market_rate_name, tag=market_tag, start=period_start, end=period_end)
        if stock_data is None:
            stock_data = self.manager.get_timeseries(rate_name=stock_rate_name, tag=stock_tag, start=period_start, end=period_end)

        betas = self.calcs_beta(market_data, stock_data, deep, horizon, save_beta)
        market_pct_change = (market_data.pct_change(periods=horizon)[::-1] * 100)[:len(market_data)-deep-1]

        if rfr_data is None:
            rfr_data = self.manager.get_timeseries(rate_name=rfr_rate_name, start=period_start, end=period_end)[::-1]
        rfr_data = rfr_data[rfr_data.index.isin(betas.index)]
        rfr_data = self.effective_annual_interest_rate(rfr_data, horizon)

        capm_data = pd.concat([market_pct_change, betas, rfr_data], axis=1, keys=['market', 'beta', 'rfr'])

        capm_prediction = capm_data.apply(lambda x: x['rfr'] + x['beta'] * (x['market'] - x['rfr']), axis=1)

        rate_name = '{0} & {1} | {2} : {3} | H={4} | D={5}'.format(market_rate_name, stock_rate_name,
                                                                   period_start.strftime("%Y-%m-%d"),
                                                                   period_end.strftime("%Y-%m-%d"),
                                                                   horizon, deep)

        if save_beta:
            self.prepare_and_save_beta(rate_name=rate_name, market_rate_name=market_rate_name, stock_rate_name=stock_rate_name, data=betas)
        if save_to_db:
            self.prepare_and_save_data(rate_name=rate_name, data=capm_data, prediction=capm_prediction)
        return capm_prediction
