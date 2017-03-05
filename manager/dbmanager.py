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
            self.session.commit()
            return self.session.query(model).filter_by(**kwargs).one()

    def get_raw_data(self, instruction):

        ctg_name = self.session.query(Category.name).\
                                join(Rates).\
                                filter(Rates.name == instruction).\
                                subquery('ctg_name')

        # Get Category
        # and child Category
        # : DataFrame
        #
        #Example output:
        #            Description             Name  Parent_id
        #1  Iris Fisher Row Data    Iris Row Data        NaN
        #2  Iris Fisher Row Data  Iris Attributes        1.0
        #3  Iris Fisher Row Data     Iris Classes        1.0
        ctg_data = pd.DataFrame()
        ctg_ch = aliased(Category)
        for name, description, parent_id in \
                self.session.query(Category.name, Category.description, Category.parent_id). \
                        join(ctg_ch.children, aliased=True). \
                        filter(Category.name == ctg_name.c.name):
            ctg_data = ctg_data.append({'name': name,
                                        'description': description,
                                        'parent_id': parent_id},
                                       ignore_index=True)

        # Get Rates
        # : DataFrame
        # Example output:
        #                      Name  Source  category_id         tag
        #1  X1 (Длина чашелистника)  Manual            2  IRIS INPUT
        rts_data = pd.DataFrame()
        for r_name, source, cat_id, tag in \
                self.session.query(Rates.name, Source.name, Rates.category_id, Rates.tag). \
                             join(Source). \
                             filter(Rates.name == instruction):
            rts_data = rts_data.append({'name': r_name,
                                        'source': source,
                                        'category_id': cat_id,
                                        'tag': tag},
                                        ignore_index=True)

        # Get RatesHistory
        # : DataFrame
        data = pd.DataFrame()
        for tag, vl1, vl2 in \
                self.session.query(
                    RatesHistory.tag,
                    RatesHistory.float_value,
                    RatesHistory.string_value). \
                        join(Rates). \
                        join(Category). \
                        filter(Category.id == rts_data.get_value(0, 'category_id')). \
                        order_by(RatesHistory.tag):
            data = data.append({'tag': tag,
                                'float_value': vl1,
                                'string_value': vl2},
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
        names_clmn =  [ name[0] for name in self.session.query(Rates.name).\
                                            filter(Rates.category_id == rts_data.get_value(0, 'category_id'))\
                                            .all()
                      ]

        raw_data.columns = names_clmn
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
                description=category['description'][0]
            )

            category_attr = self.__get_or_create(
                Category,
                name=category['name'][1],
                description=category['description'][1],
                parent_id=category_prnt.id
            )

            category_cls = self.__get_or_create(
                Category,
                name=category['name'][2],
                description=category['description'][2],
                parent_id=category_prnt.id
            )

            categ = aliased(Category)
            categ_chd = aliased(Category)

            max_id = self.session.query(func.max(RatesHistory.tag)). \
                join(Rates). \
                join(categ_chd). \
                join(categ, categ_chd.parent_id == categ.id). \
                filter(categ.name == category['name'][0]). \
                filter(categ_chd.name == category['name'][1]). \
                scalar()

            if max_id is None:
                max_id = 0
            if max_id is not None:
                max_id = max_id + 1

            rates_history = []
            for name in rateshistory.columns.values:
                rate_f = self.__get_or_create(
                    Rates,
                    name=name,
                    category_id=category_cls.id if name == 'target' else category_attr.id,
                    source_id=source.id,
                    tag='test')

                for idx in range(rateshistory.shape[0]):
                    value = rateshistory.get_value(idx, name)
                    rh = RatesHistory(
                        rates_id=rate_f.id,
                        float_value=value if isinstance(value, (int, float)) else np.NaN,
                        string_value=value if isinstance(value, str) else '',
                        tag=max_id + idx)
                    rates_history.append(rh)

            self.session.add_all(rates_history)
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            raise e

    def save_dataset(self, ft_data, trg_data, model_name, dataset_name):
        try:
            model = self.__get_or_create(
                Model,
                model_name=model_name,
                description='test',
                model_type='K'
            )

            dataset = self.__get_or_create(
                DataSet,
                name=dataset_name
            )

            model2dataset = self.__get_or_create(
                Model2Dataset,
                model_id=model.id,
                dataset_id=dataset.id
            )

            max_id = self.session.query(func.max(DataSetValues.vector_id)).\
                    join(DataSetComponent). \
                    join(DataSet) .\
                    filter(DataSet.name==dataset_name). \
                    scalar()

            if max_id == None:
                max_id = 0
            if max_id is not None:
                max_id = max_id + 1

            datasetvalues = []
            for i in range(ft_data.shape[1]):
                component = self.__get_or_create(
                    DataSetComponent,
                    dataset_id=dataset.id,
                    component_type='I',
                    component_index=i + 1,
                    component_name=str('X'+str(i+1))
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
                    component_name=str('O' + str(i + 1))
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
        try:
            # Get all "component_id" input values
            out_id = [id[0] for id in self.session.query(DataSetComponent.id).\
                        join(DataSet).\
                        filter(DataSet.name == dataset_name).\
                        filter(DataSetComponent.component_type == 'I').all()]

            # Get input data
            # : DataFrame
            ft_data = pd.DataFrame()
            for vec_id, val in \
                    self.session.query(DataSetValues.vector_id, DataSetValues.value).\
                            filter(DataSetValues.component_id.in_(out_id)).\
                            order_by(DataSetValues.vector_id):
                ft_data = ft_data.append({'vec_id': vec_id,
                                          'val': val},
                                         ignore_index=True)

            dic = {k: g["val"].tolist() for k, g in ft_data.groupby("vec_id")}
            X = pd.DataFrame.from_dict(dic).T

            # Get all "component_id" output values
            out_id = [id[0] for id in
                      self.session.query(DataSetComponent.id). \
                          join(DataSet). \
                          filter(DataSet.name == 'Iris data'). \
                          filter(DataSetComponent.component_type == 'O').all()]

            # Get output data
            # : DataFrame
            ft_data = pd.DataFrame()
            for vec_id, val in \
                    self.session.query(DataSetValues.vector_id, DataSetValues.value). \
                            filter(DataSetValues.component_id.in_(out_id)). \
                            order_by(DataSetValues.vector_id):
                ft_data = ft_data.append({'vec_id': vec_id,
                                          'val': val},
                                         ignore_index=True)

            dic = {k: g["val"].tolist() for k, g in ft_data.groupby("vec_id")}
            y = pd.DataFrame.from_dict(dic).T

            return [X, y]

        except Exception as e:
            self.session.rollback()
            raise e



































