from sklearn.model_selection    import train_test_split
from sqlalchemy                 import text
import pandas   as pd
import numpy    as np

def get_library(lib_name):
    return __import__(lib_name)

def save_model_keras(model_name, model):
    name_json = model_name + '.json'
    with open(name_json, "w") as json_file:
        json_file.write(model.to_json())

def save_models_weights_keras(model_name, loaded_model):
    name_h5 = model_name + '_weights.h5'
    print("Saved model to disk")
    return loaded_model.save_weights(name_h5)

def load_model_keras(lib_name, model_name ):
    keras = get_library(lib_name)
    name_json = model_name + '.json'
    json_file = open(name_json, 'r')
    loaded_model_json = json_file.read()
    json_file.close()
    model = keras.models.model_from_json(loaded_model_json)
    return model

class DataReader():
    def __init__(self, connect):
        self.connect = connect

    def one_hot_encoder(self, data):
        return np.array(pd.get_dummies(data))

    def train_test_split(self, X, y, train_size, random_state):
        train_X, test_X, train_y, test_y = train_test_split(X, y,
                                                            train_size=train_size,
                                                            random_state=random_state)
        return train_X, test_X, train_y, test_y

    # Processing "nxm" data
    # function attributes:
    #   sql raw for the withdrawal data values;
    #   sql raw for the withdrawal names of variables;
    #   dictionary of instructions
    def rh_multiple_processing(self, multiple_sql_raw, names_sql_raw, instruction):

        features = pd.DataFrame(self.connect.execute(multiple_sql_raw, instruction).fetchall())
        features.columns = ['id', 'val_double', 'val_char']
        dic1 = {k: g["val_double"].tolist() for k, g in features.groupby("id")}
        dic2 = {k: g["val_char"].tolist() for k, g in features.groupby("id")}
        doub = pd.DataFrame.from_dict(dic1).T
        char = pd.DataFrame.from_dict(dic2).T
        data = pd.concat([doub, char], axis=1)
        data = data.replace('', np.nan, regex=True)
        data = data.dropna(axis=1, how='all')

        val_names = list(self.connect.execute(names_sql_raw, instruction).fetchall())
        val_names = [x[0].encode('utf-8') for x in val_names]
        data.columns = val_names

        data = data.as_matrix()

        return data

    def rh_target_processing(self, target_sql_raw, instruction):
        targets = pd.DataFrame(self.connect.execute(target_sql_raw, instruction).fetchall())

        y = np.array(targets[0], dtype=str)

        return y

    def dsv_multiple_processing(self, multiple_sql_raw, names_sql_raw, instruction):

        features = pd.DataFrame(self.connect.execute(multiple_sql_raw, instruction).fetchall())
        features.columns = ['id', 'val_double']
        dic = {k: g["val_double"].tolist() for k, g in features.groupby("id")}
        data = pd.DataFrame.from_dict(dic).T

        val_names = list(self.connect.execute(names_sql_raw, instruction).fetchall())
        val_names = [x[0].encode('utf-8') for x in val_names]
        data.columns = val_names

        data = data.as_matrix()

        return data

    def dsv_target_processing(self, target_sql_raw, instruction):
        targets = pd.DataFrame(self.connect.execute(target_sql_raw, instruction).fetchall())
        targets.columns = ['id', 'val_double']
        dic = {k: g["val_double"].tolist() for k, g in targets.groupby("id")}
        data = pd.DataFrame.from_dict(dic).T

        y = np.array(data[0], dtype=str)

        return y

    def get_training_data(self, get_instructions_sql):
        # In database search category_name 'Iris Row Data'
        attributes_sql_raw = \
            text("""
                  with cat_id as (
                      SELECT child.category_id as c_id
                        from category child
                          left join category as parent
                                 on child.parent_category_id = parent.category_id
                      where parent.category_name = :category_name
                        and child.category_name like '%Attributes%')

                  select rh.tag as tag,
                       rh.rates_value_double as double_val,
                       rh.rates_value_char as char_val
                    from rates_history rh
                      join rates r
                        on rh.rates_id = r.rates_id
                      join cat_id c
                        on r.category_category_id = c.c_id
            """)

        features_names = \
            text("""
                  with cat_id as (
                      SELECT child.category_id as c_id
                        from category child
                   left join category as parent
                          on child.parent_category_id = parent.category_id
                       where parent.category_name = :category_name
                         and child.category_name like '%Attributes%')

                  select r.rates_name
                    from rates r
                      join cat_id c
                        on r.category_category_id = c.c_id

                 """)

        X = self.rh_multiple_processing(attributes_sql_raw, features_names, get_instructions_sql)

        targets_sql_raw = \
            text("""
                  with cat_id as (
                      SELECT child.category_id as c_id
                        from category child
                          left join category as parent
                                 on child.parent_category_id = parent.category_id
                      where parent.category_name = :category_name
                        and child.category_name like '%Targets%')

                  select rh.rates_value_double as double_val
                    from rates_history rh
                      join rates r
                        on rh.rates_id = r.rates_id
                      join cat_id c
                        on r.category_category_id = c.c_id
                 """)

        y = self.rh_target_processing(targets_sql_raw, get_instructions_sql)

        return X, y

    def load_trainig_data(self, instruction):

        input_sql_raw = \
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

        features_names = \
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

        X = self.dsv_multiple_processing(input_sql_raw, features_names, instruction)

        target_sql_raw = \
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
                    and dsc.component_type = 'O'

                  """)

        y = self.dsv_target_processing(target_sql_raw, instruction)

        return X, y

class Classifier():
    def __init__(self, lib_name, model_name, model_description, model_args):
        self.init_libs = {'keras': 'keras'}

        try:
            self.lib_name = self.init_libs[lib_name]
        except KeyError as e:
            raise ValueError('Undefined library: {}'.format(e.args[0]))

        self.model_name = model_name
        self.model_description = model_description
        self.denses = model_args
        self.lib = get_library(self.lib_name)
        self.created_model = self.create_model()

    def create_model(self):
        if self.lib_name == self.init_libs['keras']:
            keras = self.lib
            model = keras.models.Sequential()

            for i in xrange(len(self.denses)):
                print(self.denses[i])
                model.add(keras.layers.Dense(
                    self.denses[i]['neurons'],
                    input_dim=self.denses[i]['input_dim'],
                    init=self.denses[i]['init'],
                    activation=self.denses[i]['activation']
                ))

            save_model_keras(self.model_name, model)

    def fit(self, train_X, train_y_ohe, compile_args, fit_args):
        if self.lib_name == self.init_libs['keras']:

            loaded_model = load_model_keras(lib_name = self.lib_name,
                                      model_name=self.model_name)

            loaded_model.compile(optimizer=compile_args['optimizer'],
                                 loss=compile_args['loss'],
                                 metrics=[compile_args['metrics']])

            loaded_model.fit(train_X, train_y_ohe,
                             nb_epoch=fit_args['nb_epoch'],
                             batch_size=fit_args['batch_size'],
                             verbose=fit_args['verbose'])

            save_models_weights_keras(self.model_name, loaded_model)

    def evaluate(self, test_X, test_y_ohe, compile_args, evaluate_args):
        if self.lib_name == self.init_libs['keras']:

            loaded_model = load_model_keras(lib_name = self.lib_name,
                                      model_name=self.model_name)

            name_h5 = self.model_name + '_weights.h5'
            loaded_model.load_weights(name_h5)
            print("Loaded model from disk")

            loaded_model.compile(optimizer=compile_args['optimizer'],
                                 loss=compile_args['loss'],
                                 metrics=[compile_args["metrics"]])

            loss, accuracy = loaded_model.evaluate(test_X, test_y_ohe,
                                                   verbose = evaluate_args['verbose'])

            return loss, accuracy







