from .inheritance import Model
from .inheritance import Classifier


class KerasClassifier(Classifier):

    def __init__(self, name=None, args=None):
        if name is None:
            raise TypeError("Specify model's name")

        Model.__init__(self, name, approach='keras')
        self.keras = self.get_library()

        if args is None:
            self.loaded_model = self.load_model()
        else:
            self.build_args = args['build_args']
            self.compile_args = args['compile_args']
            self.build_model()

    def get_library(self):
        return __import__(self.approach)

    def save_model(self):
        mod_name = self.name + '.h5'
        self.model.save(mod_name)

    def load_model(self):
        mod_name = self.name + '.h5'
        return self.keras.models.load_model(mod_name)

    def build_model(self):
        model = self.keras.models.Sequential()


        for i in range(len(self.build_args)):
            model.add(self.keras.layers.Dense(
                self.build_args[i]['neurons'],
                input_dim=self.build_args[i]['input_dim'],
                init=self.build_args[i]['init'],
                activation=self.build_args[i]['activation']
            ))

        model.compile(optimizer=self.compile_args['optimizer'],
                      loss=self.compile_args['loss'],
                      metrics=[self.compile_args['metrics']])

        self.model = model

    def fit(self, X, y, fit_args):

        self.model.fit(X, y,
                       nb_epoch=fit_args['nb_epoch'],
                       batch_size=fit_args['batch_size'],
                       verbose=fit_args['verbose'])

        self.save_model()

    def evaluate(self, X, y, evaluate_args):

        loaded_model = self.load_model()

        loss, accuracy = loaded_model.evaluate(X, y,
                                               verbose=evaluate_args['verbose'])
        return (loss, accuracy)

    def predict(self, X):

        loaded_model = self.load_model()

        prediction = loaded_model.predict_classes(X)

        return prediction



