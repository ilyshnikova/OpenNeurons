from .inheritance import Classifier


class KerasClassifier(Classifier):
    def __init__(self, name, args):
        super().__init__(name, approach='keras')
        self.keras = __import__(self.approach)
        self.build_args = args.get('build_args')
        self.compile_args = args.get('compile_args')
        self.model = self.build_model() if args else self.load_model()

    def save_model(self):
        mod_name = self.name + '.h5'
        self.model.save(mod_name)

    def load_model(self):
        mod_name = self.name + '.h5'
        return self.keras.models.load_model(mod_name)

    def build_model(self):
        model = self.keras.models.Sequential()

        for arg in self.build_args:
            model.add(self.keras.layers.Dense(
                arg['neurons'],
                input_dim=arg['input_dim'],
                init=arg['init'],
                activation=arg['activation']
            ))

        model.compile(optimizer=self.compile_args['optimizer'],
                      loss=self.compile_args['loss'],
                      metrics=[self.compile_args['metrics']])
        return model

    def fit(self, X, y, fit_args: 'keys: nb_epoch, batch_size and verbose'):
        self.model.fit(X, y, **fit_args)
        self.save_model()

    def evaluate(self, X, y, evaluate_args: 'keys: verbose'):
        loss, accuracy = self.model.evaluate(X, y, **evaluate_args)
        return loss, accuracy

    def predict(self, X):
        return self.model.predict_classes(X)

