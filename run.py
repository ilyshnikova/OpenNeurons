from wrapper.kerasclassifier          import KerasClassifier
from manager.reader                   import Reader

from sklearn.model_selection          import train_test_split
from pandas                           import get_dummies

import numpy as np

get_instructions_sql = {"category_name": 'Iris Row Data'}
load_data_sql = {"model_name": '%PERCEPTRON%',
                 "data_set_name": '%IRIS%'}
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

# Read raw data
#
# rd = Reader('opentrm')
# X, y = rd.get_raw_data(instruction=get_instructions_sql)

##### user's code #####
# X = X.as_matrix()
# train_X, test_X, train_y, test_y = train_test_split(X, y, train_size=0.7, random_state=42)
# train_y_ohe = np.array(get_dummies(train_y))
# test_y_ohe = np.array(get_dummies(test_y))
#######################

# Simple Example
#
# m = KerasClassifier(name='iris', args=build_args)
# m.fit(train_X, train_y_ohe, fit_args=fit_args)
# loss, accuracy = m.evaluate(test_X, test_y_ohe, evaluate_args)
# prediction = m.predict(train_X)
# print(loss)
# print("/n")
# print(prediction)

# Read dataset
#
# X_l, y_l = rd.get_dataset(instruction=load_data_sql)

##### user's code #####
# X_l = X_l.as_matrix()
# y_l = y_l.as_matrix()
#######################




