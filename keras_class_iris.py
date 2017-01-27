from InitModule.Wrapper          import Classifier

from sklearn.model_selection     import train_test_split
from pandas                      import get_dummies

import seaborn as sns
import numpy   as np

iris = sns.load_dataset("iris")
X = iris.values[:, :4]
y = iris.values[:, 4]
train_X, test_X, train_y, test_y = train_test_split(X, y, train_size=0.5, random_state=0)

def one_hot_encod_pandas(arr):
    return np.array(get_dummies(arr))

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






















