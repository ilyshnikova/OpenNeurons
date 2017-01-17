from InitModule.Wrapper          import Classifier

from sklearn.model_selection     import train_test_split
from pandas                      import get_dummies
from sqlalchemy                  import create_engine, MetaData

import numpy   as np
import pandas  as pd
import requests
import os

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

def one_hot_encod_pandas(arr):
    return np.array(get_dummies(arr))

etl = ETL(db_name='opentrm')
conn, meta = etl.get_metadata()

features_sql = """
            select x1.tag, x1.x, x2.x, x3.x, x4.x from
            (select rh.tag as tag, rh.rates_value_double as x
            from rates_history rh
            join rates r on rh.rates_id = r.rates_id
            join category c on r.category_category_id = c.category_id
            where c.category_id = 2 and r.rates_id = 1) as x1
            join
            (select rh.tag as tag, rh.rates_value_double as x
            from rates_history rh
            join rates r on rh.rates_id = r.rates_id
            join category c on r.category_category_id = c.category_id
            where c.category_id = 2 and r.rates_id = 2) as x2 on x1.tag = x2.tag
            join
            (select rh.tag as tag, rh.rates_value_double as x
            from rates_history rh
            join rates r on rh.rates_id = r.rates_id
            join category c on r.category_category_id = c.category_id
            where c.category_id = 2 and r.rates_id = 3) as x3 on x1.tag = x3.tag
            join
            (select rh.tag as tag, rh.rates_value_double as x
            from rates_history rh
            join rates r on rh.rates_id = r.rates_id
            join category c on r.category_category_id = c.category_id
            where c.category_id = 2 and r.rates_id = 4) as x4 on x1.tag = x4.tag
"""

X = pd.DataFrame(conn.execute(features_sql).fetchall())
X.drop(0, inplace=True, axis=1)

target_sql = """
            select rh.rates_value_char
            from rates_history rh
            join rates r on rh.rates_id = r.rates_id
            join category c on r.category_category_id = c.category_id
            where c.category_id = 3 and r.rates_id = 5

"""

y = pd.DataFrame(conn.execute(target_sql).fetchall())

X = X.as_matrix()
y = np.array(y[0], dtype=str)

train_X, test_X, train_y, test_y = train_test_split(X, y, train_size=0.5, random_state=0)

train_y_ohe = one_hot_encod_pandas(train_y)
test_y_ohe = one_hot_encod_pandas(test_y)

model_args = ({'neurons' : 16, 'input_dim' : 4, 'init' : 'normal', 'activation' : 'relu'},
              {'neurons' : 3, 'input_dim' : 0, 'init' : 'normal', 'activation' : 'sigmoid'})
compile_args = {'loss': 'categorical_crossentropy', 'optimizer' : 'adam', 'metrics' : 'accuracy'}
fit_args = {'nb_epoch': 100, 'batch_size' : 1, 'verbose' : 0}
evaluate_args = {'verbose' : 0}

clf = Classifier(lib_name='keras',
                 model_name='iris_fisher',
                 model_description='test',
                 model_args = model_args)

clf.fitting(train_X, train_y_ohe,
            compile_args = compile_args,
            fit_args = fit_args)

loss, accuracy = clf.evaluate(test_X, test_y_ohe,
                                  compile_args = compile_args,
                                  evaluate_args = evaluate_args)

print("Accuracy = {:.2f}".format(accuracy))







