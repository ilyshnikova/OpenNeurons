from wrapper.kerasclassifier          import KerasClassifier
from manager.dbmanager                import DBManager
from etl.etl                          import ETL
from models.models                    import *

from sklearn.model_selection          import train_test_split
from pandas                           import get_dummies

import numpy    as np
import pandas   as pd

load_data_instr = {"category_name": 'Iris Fisher'}
get_raw_data_intsr = {"attr_name": 'sepal_length',
                      "cls_name": 'target'}
build_args = {
        'build_args': [
            {'neurons' : 16, 'input_dim' : 4, 'init' : 'normal', 'activation' : 'relu'},
            {'neurons' : 3, 'input_dim' : 0, 'init' : 'normal', 'activation' : 'sigmoid'}
            ],
        'compile_args': {'loss': 'categorical_crossentropy', 'optimizer': 'adam', 'metrics': 'accuracy'}
}
compile_args = {'loss': 'categorical_crossentropy', 'optimizer': 'adam', 'metrics': 'accuracy'}
fit_args = {'nb_epoch': 100, 'batch_size': 1, 'verbose': 0}
evaluate_args = {'verbose': 0}
predict_args = {}
save_dataset_instr = {'model_name': 'Iris Keras',
                      'dataset_name': 'Iris Data'}


# Connection to DataBase
# and assemble Scheme
manager = DBManager()

# Connect to ETL
etl = ETL(manager=manager)

path='local_files/iris.csv'
# Examples of insert data
# cats = [Category(name='futures', description='azaza'),
#             Category(name='call', description='azaza'),
#             Category(name='put', description='azaza'),
#             Category(name='cbr', description='azaza')]
# manager.session.add_all(cats)
# etl.get_Kospi_ex1('../Kospi Quotes Eikon Loader.xlsx')
# etl.get_JapanExchange_Derivatives_ex2('../rb_e20161027.txt.csv')
# etl.get_CBR_ex3(datetime.datetime(2016, 10, 10), datetime.datetime.now())
# etl.load_supervised_data(path=path, ctg_name=load_data_instr["category_name"])

# Get Raw data
datax = manager.get_raw_data(instruction=get_raw_data_intsr['attr_name'])
X = datax[2]

datax = manager.get_raw_data(instruction=get_raw_data_intsr['cls_name'])
y = datax[2]

insert_data = pd.concat([X, y], axis=1)

# # Save Raw data
# manager.save_raw_data(category=datax[0], rates=datax[1], rateshistory=insert_data, path=path)

##### user's code #####
X = X.as_matrix()
train_X, test_X, train_y, test_y = train_test_split(X, y, train_size=0.7, random_state=42)
train_y_ohe = np.array(get_dummies(train_y), dtype=np.float64)
test_y_ohe = np.array(get_dummies(test_y), dtype=np.float64)
#######################

# Simple Example
m = KerasClassifier(name='iris', args=build_args)
m.fit(train_X, train_y_ohe, fit_args=fit_args)
loss, accuracy = m.evaluate(test_X, test_y_ohe, evaluate_args)
prediction = m.predict(train_X)
print(loss, accuracy)
print(prediction)

##### user's code #####
ft_data = np.append(train_X, test_X, axis=0)
ft_data = pd.DataFrame(data=ft_data)
trg_data = np.append(train_y_ohe, test_y_ohe, axis=0)
trg_data = pd.DataFrame(data=trg_data)
#######################

# Save dataset
manager.save_dataset(ft_data=ft_data,
                     trg_data=trg_data,
                     model_name=save_dataset_instr['model_name'],
                     dataset_name=save_dataset_instr['dataset_name'])

# # Get dataset
# X, y = manager.get_dataset(dataset_name='Iris data')





