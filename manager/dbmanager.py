from sqlalchemy import text, func
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, aliased, joinedload

from models.models import *
import pandas as pd
import numpy as np
import json

class DBManager:
    def __init__(self):
        self.engine = create_engine(
            'postgresql://{user}:{password}@{host}:5432/{db_name}'.format(user=self.__get_engine_attr()['user'],
                                                                          password=self.__get_engine_attr()['password'],
                                                                          host=self.__get_engine_attr()['host'],
                                                                          db_name=self.__get_engine_attr()['db_name'],
            echo=True
        ))
        self.session = sessionmaker(bind=self.engine)()
        Base.metadata.create_all(self.engine)

    def __get_engine_attr(self):
        with open('../config.json') as data_file:
            config = json.load(data_file)

        return {'user': config['database']['user'],
                'password': config['database']['password'],
                'host': config['database']['host'],
                'db_name': config['database']['db_name']}

    def __get_or_create(self, model, **kwargs):
        instance = self.session.query(model).filter_by(**kwargs).first()
        if instance:
            return instance
        else:
            instance = model(**kwargs)
            self.session.add(instance)
            return self.session.query(model).filter_by(**kwargs).one()

    def get_raw_data(self, rate_name_input):

        # Get Category
        # and child Category
        # : DataFrame
        query = self.session.query(Category.name)
        query = query.join(Rates)
        query = query.filter(Rates.name == rate_name_input)
        ctg_name = query.subquery('ctg_name')

        ctg_ch  = aliased(Category)

        query   = self.session.query(Category.name, Category.description, Category.parent_id)
        query   = query.join(ctg_ch.children, aliased=True)
        query   = query.filter(Category.name == ctg_name.c.name)
        records = query.all()

        ctg_data = pd.DataFrame()
        for name, description, parent_id in records:
            ctg_data = ctg_data.append({'name': name, 'description': description, 'parent_id': parent_id},
                                       ignore_index=True)

        # Get Rates
        # : DataFrame
        # Example output:
        query  = self.session.query(Rates.name, Source.name, Rates.category_id, Rates.tag)
        query  = query.join(Source)
        query  = query.filter(Rates.name == rate_name_input)
        rate = query.first()

        rts_data = pd.DataFrame({'name': rate_name_input, 'source': rate.name, 'category_id': rate.category_id, 'tag': rate.tag},
                                index=[0])
        # for r_name, source, cat_id, tag in records:
        #     rts_data = rts_data.append({'name': r_name, 'source': source, 'category_id': cat_id, 'tag': tag},
        #                                 ignore_index=True)

        # Get RatesHistory
        # : DataFrame
        query   = self.session.query( RatesHistory.tag, RatesHistory.float_value, RatesHistory.string_value)
        query   = query.join(Rates)
        query   = query.filter(Rates.name == rate_name_input)
        query   = query.order_by(RatesHistory.tag)
        records = query.all()

        data = pd.DataFrame()
        for tag, vl1, vl2 in records:
            data = data.append({'tag': tag, 'float_value': vl1, 'string_value': vl2},
                               ignore_index=True)

        # To DataFrame
        dic = {k: g['float_value'].tolist() for k, g in data.groupby("tag")}
        float_data = pd.DataFrame.from_dict(dic).T
        dic = {k: g['string_value'].tolist() for k, g in data.groupby("tag")}
        str_data = pd.DataFrame.from_dict(dic).T

        # Concatenate RH_float + RH_str
        raw_data = pd.concat([float_data, str_data], axis=1)

        # Clean data
        raw_data = raw_data.replace('', np.nan, regex=True)
        raw_data = raw_data.dropna(axis=1, how='all')

        # Add columns name
        query = self.session.query(Rates.name)
        query = query.filter(Rates.name == rate_name_input)
        rate_name = query.one()

        raw_data.columns = rate_name
        return [ctg_data, rts_data, raw_data]

    def save_raw_data(self, category, rates, rateshistory, path):
        try:
            source = self.__get_or_create(
                Source,
                name=path.split('/')[-1]
            )

            category_prnt = self.__get_or_create(
                Category,
                name=category['name'][0],
                description=category['description'][0],
            )

            for idx in range(1, category.shape[0]):
                category_chld = self.__get_or_create(
                    Category,
                    name        = category['name'][idx],
                    description = category['description'][idx],
                    parent_id   = category['parent_id'][idx]
                )

            categ = aliased(Category)
            categ_chd = aliased(Category)

            query  = self.session.query(func.max(RatesHistory.tag))
            query  = query.join(Rates)
            query  = query.join(categ_chd)
            query  = query.join(categ, categ_chd.parent_id == categ.id)
            query  = query.filter(categ.name == category['name'][0])
            query  = query.filter(categ_chd.name == category['name'][1])
            max_id = query.scalar()

            if max_id is None:
                max_id = 0
            else:
                max_id = max_id + 1

            rate_f = self.__get_or_create(
                Rates,
                name        = str(rates['name'][0]),
                category_id = int(rates['category_id'][0]),
                source_id   = int(source.id),
                tag         = str(rates['tag'][0])
            )

            rates_history = []
            for idx in range(rateshistory.shape[0]):
                value = rateshistory.get_value(idx, rates['name'][0])
                rh = RatesHistory(
                    rates_id     = int(rate_f.id),
                    float_value  = int(value) if isinstance(value, (int, float)) else np.NaN,
                    string_value = str(value) if isinstance(value, str) else '',
                    tag          = int(max_id+idx)
                )
                rates_history.append(rh)

            self.session.add_all(rates_history)
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            raise e

    def save_dataset(self, ft_data, trg_data, model_df, data_set_df):
        try:
            model = self.__get_or_create(
                Model,
                model_name  = str(model_df['model_name'][0]),
                description = str(model_df['description'][0]),
                model_type  = str(model_df['model_type'][0])
            )

            print(model.model_name, model.description, model.model_type)

            dataset = self.__get_or_create(
                DataSet,
                name = str(data_set_df['name'][0])
            )

            print(dataset.name)

            model2dataset = self.__get_or_create(
                Model2Dataset,
                model_id   = model.id,
                dataset_id = dataset.id
            )

            query  = self.session.query(func.max(DataSetValues.vector_id))
            query  = query.join(DataSetComponent)
            query  = query.join(DataSet)
            query  = query.filter(DataSet.name==data_set_df['name'][0])
            max_id = query.scalar()

            if max_id == None:
                max_id = 0
            else:
                max_id = max_id + 1

            print(max_id)

            datasetvalues = []
            for i in range(ft_data.shape[1]):
                component = self.__get_or_create(
                    DataSetComponent,
                    dataset_id=dataset.id,
                    component_type='I',
                    component_index=i + 1,
                    component_name='X'+str(i+1)
                )
                for idx in range(ft_data.shape[0]):
                    data_val = DataSetValues(
                        component_id=component.id,
                        dataset_id=dataset.id,
                        vector_id=max_id + idx,
                        value=ft_data.iloc[idx, i]
                    )
                    datasetvalues.append(data_val)

            for i in range(trg_data.shape[1]):
                component = self.__get_or_create(
                    DataSetComponent,
                    dataset_id=dataset.id,
                    component_type='O',
                    component_index=i + 1,
                    component_name='O' + str(i + 1)
                )
                for idx in range(trg_data.shape[0]):
                    data_val = DataSetValues(
                        component_id=component.id,
                        dataset_id=dataset.id,
                        vector_id=max_id + idx,
                        value=trg_data.iloc[idx, i]
                    )
                    datasetvalues.append(data_val)

            self.session.add_all(datasetvalues)
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            raise e

    def get_dataset(self, dataset_name):

        # Get all "component_id" input values
        query = self.session.query(DataSetComponent.id)
        query = query.join(DataSet)
        query = query.filter(DataSet.name == dataset_name)
        query = query.filter(DataSetComponent.component_type == 'I')
        records = query.all()
        out_id = [id[0] for id in records]

        # Get input data
        # : DataFrame
        query = self.session.query(DataSetValues.vector_id, DataSetValues.value)
        query = query.filter(DataSetValues.component_id.in_(out_id))
        query = query.order_by(DataSetValues.vector_id)
        records = query.all()

        ft_data = pd.DataFrame()
        for vec_id, val in records:
            ft_data = ft_data.append({'vec_id': vec_id, 'val': val},
                                     ignore_index=True)

        dic = {k: g["val"].tolist() for k, g in ft_data.groupby("vec_id")}
        X = pd.DataFrame.from_dict(dic).T

        # Get all "component_id" output values
        query = self.session.query(DataSetComponent.id)
        query = query.join(DataSet)
        query = query.filter(DataSet.name == dataset_name)
        query = query.filter(DataSetComponent.component_type == 'O')
        records = query.all()
        out_id = [id[0] for id in records]

        # Get output data
        # : DataFrame
        query = self.session.query(DataSetValues.vector_id, DataSetValues.value)
        query = query.filter(DataSetValues.component_id.in_(out_id))
        query = query.order_by(DataSetValues.vector_id)
        records = query.all()

        ft_data = pd.DataFrame()
        for vec_id, val in records:
            ft_data = ft_data.append({'vec_id': vec_id, 'val': val},
                                     ignore_index=True)

        dic = {k: g["val"].tolist() for k, g in ft_data.groupby("vec_id")}
        y = pd.DataFrame.from_dict(dic).T

        return [X, y]



































