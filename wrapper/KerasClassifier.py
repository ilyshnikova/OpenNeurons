import keras as keras


class KerasClassifier:
    def __init__(self, name):
        self.name = name

    def save_model(self):
        name_json = self.name + '_architecture.json'
        with open(name_json, "w") as json_file:
            json_file.write(self.model.to_json())

        name_h5 = self.name + '_weights.h5'
        self.model.save_weights(name_h5)

    def load_model(self):
        name_json = self.name + '_architecture.json'
        model = keras.models.model_from_json(open(name_json).read())
        name_h5 = self.name + '_weights.h5'
        model.load_weights(name_h5)
        model.compile(optimizer=self.compile_args['optimizer'],
                      loss=self.compile_args['loss'],
                      metrics=[self.compile_args['metrics']])
        return model

    def build_model(self, build_args, compile_args):
        model = keras.models.Sequential()

        for i in range(len(build_args)):
            model.add(keras.layers.Dense(
                build_args[i]['neurons'],
                input_dim=build_args[i]['input_dim'],
                init=build_args[i]['init'],
                activation=build_args[i]['activation']
            ))

        model.compile(optimizer=compile_args['optimizer'],
                      loss=compile_args['loss'],
                      metrics=[compile_args['metrics']])

        self.compile_args = compile_args
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




