from sqlalchemy import text, func
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, aliased, joinedload

import json
import sys, inspect
from sqlalchemy import insert

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
        with open('config.json') as data_file:
            config = json.load(data_file)

        return {'user': config['database']['user'],
                'password': config['database']['password'],
                'host': config['database']['host'],
                'db_name': config['database']['db_name']}

    def __get_or_create(self, model, **kwargs):
        instance = self.session.query(model).filter_by(**kwargs).all()

        if len(instance) > 1:
            raise Exception("Multiple return")

        if len(instance) == 1:
            return instance[0]
        else:
            instance = model(**kwargs)

            self.session.add(instance)

            try:
                result = self.session.query(model).filter_by(**kwargs).one()
                return result
            except Exception as e:
                print(e)
                return None

    def __get_parent_category(self, childCategory, nestedlevel = 0):
        # get parent category hierarchy for particular Category (childCategory)
        if (childCategory.empty)|(nestedlevel >= 250):
            return childCategory
        AllCategory = childCategory
        for parent_id in childCategory['parent_id'].values:
            if parent_id != None:
                    prntCategory = aliased(Category)
                    ParentCategory = pd.DataFrame(self.session.query(Category.id, Category.name, Category.description, Category.parent_id, prntCategory.name.label('parent_name')).\
                                                  outerjoin(prntCategory, prntCategory.id == Category.parent_id).\
                                                  filter(Category.id == parent_id.astype(str)).\
                                                  all())
                    nestedlevel += 1
                    ParentCategory = self.__get_parent_category(ParentCategory, nestedlevel)
                    AllCategory = AllCategory.append(ParentCategory, ignore_index=True)
        return AllCategory

    def __set_parent_category(self, dfCategory, category_name = None, nested_level = 0):
        OutCategory = None
        if (category_name == None)|(category_name == '')|(nested_level >= 250):
            return OutCategory
        for rc in dfCategory[(dfCategory['name']==category_name)][['name', 'description', 'parent_name']].values:
            OutCategory = self.__get_or_create(Category, name = rc[0], description = rc[1], parent = self.__set_parent_category(dfCategory, rc[2], nested_level+1))
        return OutCategory

    # Example get_raw_data with static data
    def get_iris_sample_raw_data(self, RateName = None):
       Category = None
       Rates = None
       RatesHistory = None
       if RateName == 'X1 (Длина чашелистника)':
           Category = pd.DataFrame([{'name': 'Iris Row Data', 'description': 'Iris Fisher Row Data'},
                                    {'name': 'Iris Attributes', 'description': 'Iris Fisher Row Data', 'parent_name': 'Iris Row Data' },
                                    {'name': 'Iris Classes', 'description': 'Iris Fisher Row Data', 'parent_name': 'Iris Row Data' }],
                     index=[1, 2, 3])
           Rates = pd.DataFrame([{'name': 'X1 (Длина чашелистника)', 'category_name': 'Iris Attributes', 'source': 'Manual', 'tag': 'IRIS INPUT'},
                                  ],
                           index= [1])
           RatesHistory = pd.DataFrame([{'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.3, 'string_value': None, 'tag': '1'},
                                        {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.4, 'string_value': None, 'tag': '2'},
                                        {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.4, 'string_value': None, 'tag': '3'},
                                        {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.4, 'string_value': None, 'tag': '4'},
                                        {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.5, 'string_value': None, 'tag': '5'},
                                        {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.6, 'string_value': None, 'tag': '6'},
                                        {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.6, 'string_value': None, 'tag': '7'},
                                        {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.6, 'string_value': None, 'tag': '8'},
                                        {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.6, 'string_value': None, 'tag': '9'},
                                        {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.7, 'string_value': None, 'tag': '10'}])
       return [Category, Rates, RatesHistory]

    def get_raw_data(self, RateName, CategoryName = None, Tag = None):
        # seems not to be working with parent depth more than 1 
        # Category name?? Tag ??
        ParentCategory = aliased(Category)

        category = self.session.query(Category.id, Category.name, Category.description, Category.parent_id, ParentCategory.name.label('parent_name')).\
                                  join(Rates).\
                                  outerjoin(ParentCategory, ParentCategory.id == Category.parent_id).\
                                  filter(Rates.name == RateName).\
                                  all()

        dfCategory = pd.DataFrame(self.session.query(Category.id, Category.name, Category.description, Category.parent_id, ParentCategory.name.label('parent_name')).\
                                  join(Rates).\
                                  outerjoin(ParentCategory, ParentCategory.id == Category.parent_id).\
                                  filter(Rates.name == RateName).\
                                  all())

        dfCategory = self.__get_parent_category(dfCategory)

        dfRates = pd.DataFrame(self.session.query(Rates.id, Rates.name, Rates.category_id, Category.name.label('category_name'), Rates.tag, Source.name.label('source'), Rates.source_id).\
                               filter(Rates.name == RateName).\
                               join(Source).\
                               join(Category).\
                               all())

        dfRatesHistory = pd.DataFrame(self.session.query(RatesHistory.rates_id, Rates.name.label('rates_name'), RatesHistory.date, RatesHistory.float_value, RatesHistory.string_value, RatesHistory.tag).\
                                      join(Rates).\
                                      filter(Rates.name == RateName). \
                                      filter(RatesHistory.tag == Tag).\
                                      all())

        return [dfCategory, dfRates, dfRatesHistory]

    def save_raw_data(self, category, rates, rateshistory, source):
        NewRawData = []

        for rrh in rateshistory[['rates_name', 'date', 'float_value', 'string_value', 'tag']].values:
            rowc = self.__set_parent_category(dfCategory = category,
                                              category_name = rates[(rates['name'] == rrh[0])][['category_name']].values[0][0])
            rows = self.__get_or_create(Source,
                                       name = source)
            rowr = self.__get_or_create(Rates,
                                       name = rrh[0],
                                       source = rows,
                                       tag = rates[(rates['name'] == rrh[0])][['tag']].values[0][0],
                                       category = rowc)
            rowrh = self.__get_or_create(RatesHistory,
                                        date = rrh[1],
                                        float_value = rrh[2],
                                        string_value = rrh[3],
                                        tag = rrh[4],
                                        rates = rowr)
            NewRawData.append(rowrh)

        try:
            self.session.add_all(NewRawData)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e

    def save_dataset(self, model, dataset, X, y):
        ModelData = []
        for comp_name in X:
            vec_id = 0
            for comp_val in X[comp_name]:
                modelr = self.__get_or_create(Model,
                                          model_name=model['model_name'][0],
                                          description=model['description'][0],
                                          model_type=model['model_type'][0])
                datasetr = self.__get_or_create(DataSet, name=dataset['name'][0])
                model2dataset = self.__get_or_create(Model2Dataset,
                                                 model_id=modelr.id,
                                                 dataset_id=datasetr.id)
                dscomponent = self.__get_or_create(DataSetComponent,
                                               dataset_id=datasetr.id,
                                               component_type='I',
                                               component_index=str(comp_name)[-1],
                                               component_name=str(comp_name))
                dsvalue = self.__get_or_create(DataSetValues,
                                           component_id=dscomponent.id,
                                           dataset_id=datasetr.id,
                                           vector_id = vec_id,
                                           value=comp_val.astype(float))
                ModelData.append(dsvalue)
                vec_id = vec_id + 1

        if y is not None:
            for comp_name in y:
                vec_id = 0
                for comp_val in y[comp_name]:
                    modelr = self.__get_or_create(Model,
                                                  model_name=model['model_name'][0],
                                                  description=model['description'][0],
                                                  model_type=model['model_type'][0])
                    datasetr = self.__get_or_create(DataSet, name=dataset['name'][0])
                    model2dataset = self.__get_or_create(Model2Dataset,
                                                         model_id=modelr.id,
                                                         dataset_id=datasetr.id)
                    dscomponent = self.__get_or_create(DataSetComponent,
                                                       dataset_id=datasetr.id,
                                                       component_type='O',
                                                       component_index=str(comp_name)[-1],
                                                       component_name=str(comp_name))
                    dsvalue = self.__get_or_create(DataSetValues,
                                                   component_id=dscomponent.id,
                                                   dataset_id=datasetr.id,
                                                   vector_id=vec_id,
                                                   value=comp_val.astype(float))
                    ModelData.append(dsvalue)
                    vec_id = vec_id + 1

        try:
            self.session.add_all(ModelData)
            self.session.commit()

        except Exception as e:
            self.session.rollback()
            raise e

    def save_model_prediction(self, model, dataset, prediction):
        ModelData = []
        vec_id = 0
        for comp_val in prediction[prediction.columns.values[0]]:
            modelr = self.__get_or_create(Model,
                                      model_name=model['model_name'][0],
                                      description=model['description'][0],
                                      model_type=model['model_type'][0])
            datasetr = self.__get_or_create(DataSet, name=dataset['name'][0])
            model2dataset = self.__get_or_create(Model2Dataset,
                                             model_id=modelr.id,
                                             dataset_id=datasetr.id)
            dscomponent = self.__get_or_create(DataSetComponent,
                                           dataset_id=datasetr.id,
                                           component_type='P',
                                           component_index=1,
                                           component_name='P')
            dsvalue = self.__get_or_create(DataSetValues,
                                       component_id=dscomponent.id,
                                       dataset_id=datasetr.id,
                                       vector_id = vec_id,
                                       value=comp_val.astype(float))
            ModelData.append(dsvalue)
            vec_id = vec_id + 1

        try:
            self.session.add_all(ModelData)
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

            comp_I_names = list(self.session.query(DataSetComponent.component_name).join(DataSet). \
                              filter(DataSet.name == dataset_name, DataSetComponent.component_type == 'I').all())
            comp_I_names = [name[0] for name in comp_I_names]

            dic = {k: g["val"].tolist() for k, g in ft_data.groupby("vec_id")}
            X = pd.DataFrame.from_dict(dic).T
            X.columns = comp_I_names

            # Get all "component_id" output values
            out_id = [id[0] for id in
                      self.session.query(DataSetComponent.id). \
                          join(DataSet). \
                          filter(DataSet.name == dataset_name). \
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

            comp_O_names = list(self.session.query(DataSetComponent.component_name).join(DataSet). \
                                filter(DataSet.name == dataset_name, DataSetComponent.component_type == 'O').all())
            comp_O_names = [name[0] for name in comp_O_names]

            dic = {k: g["val"].tolist() for k, g in ft_data.groupby("vec_id")}
            y = pd.DataFrame.from_dict(dic).T
            y.columns = comp_O_names

            return [X, y]

        except Exception as e:
            self.session.rollback()
            raise e

    def get_model_prediction(self, dataset_name):
        try:
            prd_id = self.session.query(DataSetComponent.id).join(DataSet). \
                                        filter(DataSet.name == dataset_name,
                                               DataSetComponent.component_type == 'P').one()[0]

            pred_data = pd.DataFrame()
            for vec_id, val in \
                    self.session.query(DataSetValues.vector_id, DataSetValues.value). \
                            filter(DataSetValues.component_id == prd_id). \
                            order_by(DataSetValues.vector_id):
                pred_data = pred_data.append({'P': val}, ignore_index=True)

            return pred_data

        except Exception as e:
            self.session.rollback()
            raise e

    def drop_all_tables(self):
        metadata = Base.metadata
        all_tables = list(reversed(metadata.sorted_tables))
        for table in all_tables:
            self.engine.execute(table.delete())

    def load_tables_from_json(self, file_path):
        clsmembers = dict(inspect.getmembers(sys.modules['models.models'], inspect.isclass))
        with open(file_path, 'r') as fh:
            data = json.load(fh)

        for table in data.keys():
            vals_to_insert = []

            for d in data[table]["data"]:
                vals_to_insert.append(dict(zip(data[table]["head"], d)))

            table_object = clsmembers[table]

            ins = insert(table_object).values(vals_to_insert)
            self.engine.connect().execute(ins)

        self.session.commit()