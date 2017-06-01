from manager.dbmanager import DBManager

import pandas as pd
import numpy  as np
import warnings
warnings.filterwarnings("ignore", category=np.VisibleDeprecationWarning)


class CAPM:
    def __init__(self, market, stock, risk_free_rate, period_start, period_end, Deep=250, Horizon=7):
        self.market = market
        self.stock = stock
        self.risk_free_rate = risk_free_rate
        self.period_start = period_start
        self.period_end = period_end
        self.deep = Deep
        self.horizon = Horizon
        self.manager = DBManager()

    def beta(self, market, stock):
        market = market[:self.deep]
        stock = stock[:self.deep]
        erm = -market[::-1].diff(-self.horizon) / market[::-1] * self.horizon / 365 * 100
        eri = -stock[::-1].diff(-self.horizon) / stock[::-1] * self.horizon / 365 * 100
        beta = erm.cov(eri) / erm.var()
        return beta

    def run(self, SaveToDB_Beta=False, SaveToDB=False):
        market = self.manager.get_raw_data(RateName=self.market, Tag='CG')[2][['date', 'float_value']]
        market = market.set_index(pd.DatetimeIndex(market['date']))['float_value']
        market_ = market.loc[self.period_start:self.period_end]

        stock = self.manager.get_raw_data(RateName=self.stock, Tag='CG')[2][['date', 'float_value']]
        stock = stock.set_index(pd.DatetimeIndex(stock['date']))['float_value']
        # stock_ = stock.loc[self.period_start:self.period_end]

        # beta = self.beta(market_, stock_)
        beta = self.beta(market, stock)

        dfMarket = market_.pct_change(periods=self.horizon)[::-1].dropna() * 100
        dfBeta = pd.Series([beta], index=dfMarket.index)

        rfr = self.manager.get_raw_data(RateName=self.risk_free_rate)[2][['date', 'float_value']]
        rfr = rfr.set_index(pd.DatetimeIndex(rfr['date']))['float_value']
        dfRfr = rfr[rfr.index.isin(dfMarket.index)]

        capm_data = pd.concat([dfMarket, dfBeta, dfRfr], axis=1)
        capm_data.columns = ['market', 'beta', 'rfr']

        capm_prediction = capm_data.apply(lambda a: a['rfr'] + a['beta'] * (a['market'] - a['rfr']), axis=1)


        rate_name = '{0} & {1} | {2} : {3} | H={4} | D={5}'.format(self.market, self.stock, self.period_start.strftime("%Y-%m-%d"),
                                                         self.period_end.strftime("%Y-%m-%d"), self.horizon, self.deep)


        if SaveToDB_Beta:
            beta_category = pd.DataFrame(
                [{'name': 'Indicators', 'description': 'Financial indicators'},
                 {'name': 'Beta', 'description': 'Coefficient indicates whether the investment is more or less volatile than the market as a whole', 'parent_name': 'Indicators'},
                 {'name': '{0} & {1}'.format(self.market, self.stock), 'description': None}], index=[1, 2, 3])

            beta_rate = pd.DataFrame([{'name': rate_name, 'category_name': '{0} & {1}'.format(self.market, self.stock), 'source': None, 'tag': None}], index=[1])

            beta_ratehistory = pd.DataFrame()
            for idx in dfBeta.index:
                beta_ratehistory = beta_ratehistory.append({'rates_name': rate_name, 'date': idx, 'float_value': dfBeta[idx], 'string_value': None, 'tag': None}, ignore_index=True)

            self.manager.save_raw_data(beta_category, beta_rate, beta_ratehistory, None)


        if SaveToDB:
            model = pd.DataFrame({'model_name': 'CAPM', 'description': 'Capital Asset Pricing Model', 'model_type': None}, index=[0])
            dataset = pd.DataFrame({'name': rate_name}, index=[0])

            capm_data.columns = ['x{0}'.format(idx) for idx in range(1, capm_data.shape[1]+1)]

            self.manager.save_dataset(model=model, dataset=dataset, X=capm_data, y=None)
            self.manager.save_model_prediction(model=model, dataset=dataset, prediction=pd.DataFrame(capm_prediction))

        return capm_prediction

    def evaluate(self):
        pass
