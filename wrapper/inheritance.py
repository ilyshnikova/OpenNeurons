class Model(object):
    def __init__(self, name, approach):
        self.name = name
        self.approach = approach

    def predict(self, X):
        pass


class NonDeterministicMethods(Model):
    def save_model(self):
        pass

    def load_model(self):
        pass


class MachineLearning(NonDeterministicMethods):
    pass


class Supervised(MachineLearning):
    pass


class Classifier(Supervised):
    pass
