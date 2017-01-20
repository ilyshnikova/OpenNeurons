from InitModule.Wrapper          import Classifier, DataReader
from sqlalchemy                  import create_engine, MetaData

import pandas  as pd
import requests
import os

# change in category.category_name 'Iris Classes' -> 'Iris Targets'

get_instructions_sql = {"category_name": 'Iris Row Data'}
load_data_sql = {"model_name": '%PERCEPTRON%',
                 "data_set_name": '%IRIS%'}
model_args = ({'neurons' : 16, 'input_dim' : 4, 'init' : 'normal', 'activation' : 'relu'},
              {'neurons' : 3, 'input_dim' : 0, 'init' : 'normal', 'activation' : 'sigmoid'})
compile_args = {'loss': 'categorical_crossentropy',
                'optimizer': 'adam',
                'metrics': 'accuracy'}
fit_args = {'nb_epoch': 100,
            'batch_size': 1,
            'verbose': 0}
evaluate_args = {'verbose': 0}

class ETL:
    def __init__(self, db_name, user='postgres', host='localhost'):
        self.user = user
        self.host = host
        self.db_name = db_name
        self.engine = create_engine(
            'postgresql://{user}@{host}:5432/{db_name}'.format(user=user, host=host, db_name=db_name)
        )

    def write_to_db(self, path, table_name):
        extension = path.split('.')[-1]
        if extension == 'csv':
            data = pd.read_csv(path, encoding="ISO-8859-1", header=2)
        elif extension == 'xlsx':
            data = pd.read_excel(path, sheetname=2)
        data.to_sql(table_name, self.engine, if_exists='append')
        os.remove('path')

    def get_table(self, table_name):
        return pd.read_sql_table(table_name, self.engine)

    def get_cb_data(self, url, path):
        r = requests.get(url)
        with open(path, 'w') as file:
            file.write(r.text)
            return r.text

    def get_metadata(self):
        # The return value of create_engine() is our connection object
        con = self.engine
        # We then bind the connection to MetaData()
        meta = MetaData(bind=con, reflect=True)
        return con, meta

etl = ETL(db_name='opentrm')
conn, meta = etl.get_metadata()

dr = DataReader(connect=conn)
X, y = dr.get_training_data(get_instructions_sql)

train_X, test_X, train_y, test_y = dr.train_test_split(X, y, train_size=0.5, random_state=0)
train_y_ohe = dr.one_hot_encoder(train_y)
test_y_ohe = dr.one_hot_encoder(test_y)

clf = Classifier(lib_name='keras',
                 model_name='iris_fisher',
                 model_description='test',
                 model_args = model_args)

clf.fit(train_X, train_y_ohe,
            compile_args = compile_args,
            fit_args = fit_args)

loss, accuracy = clf.evaluate(test_X, test_y_ohe,
                                  compile_args = compile_args,
                                  evaluate_args = evaluate_args)

print("Accuracy = {:.2f}".format(accuracy))







