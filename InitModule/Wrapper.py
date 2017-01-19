def get_library(lib_name):
    return __import__(lib_name)

def save_model(model_name, model):
    name_json = model_name + '.json'
    with open(name_json, "w") as json_file:
        json_file.write(model.to_json())

def save_models_weights(model_name, loaded_model):
    name_h5 = model_name + '.h5'
    print("Saved model to disk")
    return loaded_model.save_weights(name_h5)

def load_model(lib_name, model_name ):
    keras = get_library(lib_name)
    name_json = model_name + '.json'
    json_file = open(name_json, 'r')
    loaded_model_json = json_file.read()
    json_file.close()
    model = keras.models.model_from_json(loaded_model_json)
    return model


class Classifier():
    def __init__(self, lib_name, model_name, model_description, model_args):
        self.init_libs = {'keras': 'keras', 'pybrain': 'pybrain'}

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

            save_model(self.model_name, model)

    def fitting(self, train_X, train_y_ohe, compile_args, fit_args):
        if self.lib_name == self.init_libs['keras']:

            loaded_model = load_model(lib_name = self.lib_name,
                                      model_name=self.model_name)

            loaded_model.compile(optimizer=compile_args['optimizer'],
                                 loss=compile_args['loss'],
                                 metrics=[compile_args['metrics']])

            loaded_model.fit(train_X, train_y_ohe,
                             nb_epoch=fit_args['nb_epoch'],
                             batch_size=fit_args['batch_size'],
                             verbose=fit_args['verbose'])

            save_models_weights(self.model_name, loaded_model)

    def evaluate(self, test_X, test_y_ohe, compile_args, evaluate_args):
        if self.lib_name == self.init_libs['keras']:

            loaded_model = load_model(lib_name = self.lib_name,
                                      model_name=self.model_name)

            name_h5 = self.model_name + '.h5'
            loaded_model.load_weights(name_h5)
            print("Loaded model from disk")

            loaded_model.compile(optimizer=compile_args['optimizer'],
                                 loss=compile_args['loss'],
                                 metrics=[compile_args["metrics"]])

            loss, accuracy = loaded_model.evaluate(test_X, test_y_ohe,
                                                   verbose = evaluate_args['verbose'])

            return loss, accuracy









