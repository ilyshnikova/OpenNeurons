from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.models import Base
import pandas as pd
import numpy as np


class DBManager:
    def __init__(self, db_name, user='postgres', host='localhost'):
        self.user = user
        self.host = host
        self.db_name = db_name
        self.engine = create_engine(
            'postgresql://{user}@{host}:5432/{db_name}'.format(user=user, host=host, db_name=db_name),
            echo=True
        )
        self.session = sessionmaker(bind=self.engine)()
        Base.metadata.create_all(self.engine)

    def get_raw_data(self, instruction):

        attributes_sql_raw = \
            text("""
                  select rh.tag as tag,
                         rh.rates_value_double as double_val,
                         rh.rates_value_char as char_val
                    from rates_history rh
                      join rates r
                        on rh.rates_id = r.rates_id
                      join category c
                        on r.category_category_id = c.category_id
                  where c.name = :category_name
            """)

        attributes_names = \
            text("""
                  select r.name
                    from rates r
                      join category c
                        on r.category_category_id = c.category_id
                  where c.category_name = :category_name
                 """)

        raw_data = pd.DataFrame(self.engine.execute(attributes_sql_raw, instruction).fetchall())
        columns = ['id', 'val_double', 'val_char']
        raw_data.columns = columns

        appended_data = []
        for i in columns[1:]:
            dict = {k: g[i].tolist() for k, g in raw_data.groupby("id")}
            appended_data.append(pd.DataFrame.from_dict(dict).T)
        data = pd.concat(appended_data, axis=1)

        # check for empty columns
        data = data.replace('', np.nan, regex=True)
        data = data.dropna(axis=1, how='all')

        # create list of value names
        val_names = list(self.engine.execute(attributes_names, instruction).fetchall())
        val_names = [x[0].encode('utf-8') for x in val_names]
        data.columns = val_names

        return data

    def get_dataset(self, instruction):
        attributes_sql_raw = \
            text("""
                  select dsv.vector_id as id,
                         dsv.data_set_value as features
                    from data_set_values as dsv
                      join data_set_component dsc
                        on dsv.component_id = dsc.component_id
                      join data_set ds
                        on dsc.data_set_id = ds.data_set_id
                      join model_2_data_set mds
                        on ds.data_set_id = mds.data_set_id
                      join model m
                        on mds.model_id = m.model_id
                  where m.model_name like :model_name
                    and ds.data_set_name like :data_set_name
                    and dsc.component_type = 'I'
                 """)

        attributes_names = \
            text("""
                  select dsc.component_name
                    from data_set_component as dsc
                      join data_set ds
                        on dsc.data_set_id = ds.data_set_id
                      join model_2_data_set mds
                        on ds.data_set_id = mds.data_set_id
                      join model m
                        on  mds.model_id = m.model_id
                  where m.model_name like :model_name
                    and ds.data_set_name like :data_set_name
                    and dsc.component_type = 'I'
                 """)

        raw_data = pd.DataFrame(
            self.engine.execute(attributes_sql_raw, instruction).fetchall()
        )
        columns = ['id', 'val_double']
        raw_data.columns = columns
        dic = {k: g["val_double"].tolist() for k, g in raw_data.groupby("id")}
        X = pd.DataFrame.from_dict(dic).T

        val_names = list(
            self.engine.execute(attributes_names, instruction).fetchall()
        )
        val_names = [x[0].encode('utf-8') for x in val_names]
        X.columns = val_names

        return X
