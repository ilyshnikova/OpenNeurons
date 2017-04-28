from wrapper.kerasclassifier import KerasClassifier
from manager.dbmanager import DBManager
from etl.etl import ETL
from etl.quandl import Quandl as q

from sklearn.model_selection          import train_test_split
from pandas                           import get_dummies

import numpy as np
import pandas as pd
import datetime
from models.models import *


if __name__ == '__main__':
    # Connection to DataBase and assemble Scheme
    DB = DBManager()
    etl = ETL(manager=DB)

##### LOADING DATA FROM VARIOUS SOURCES

    # Download local files for superviesd learning
    load_data_instr = {"category_name": 'Iris Fisher'}
    etl.load_supervised_data(path='local_files/iris.csv', ctg_name=load_data_instr["category_name"])

    # Define categories for JapanExchange_Derivatives_ex2
    cats = [Category(name='futures', description='azaza'),
            Category(name='call', description='azaza'),
            Category(name='put', description='azaza'),
            Category(name='cbr', description='azaza')]
    DB.session.add_all(cats)

    # Import Future Data
    c, r, rh = etl.get_Kospi_data_ex1('../Kospi Quotes Eikon Loader.xlsx')

    # Download file 'rb_e20161027.txt.csv'
    etl.get_JapanExchange_Derivatives_ex2('../rb_e20161027.txt.csv')

    # Import data from pdf
    path = "../Examples/Acts 2016/"
    etl.get_PDF_case_1(path)

    # Receiving daily data from the CBR (exchange rates, discount prices of precious metals ...)
    etl.get_CBR_ex3(datetime.datetime(2016, 10, 10), datetime.datetime.now())

    # Define categories for Quandl data
    Category = pd.DataFrame([{'name': 'Financial Markets', 'description': 'Financial Markets Data Branch'},
                             {'name': 'Europe', 'description': 'Europe', 'parent_name': 'Financial Markets'},
                             {'name': 'Russia', 'description': 'Russia', 'parent_name': 'Europe'},
                             {'name': 'MCX', 'description': 'Moscow Exchange', 'parent_name': 'Russia'},
                             {'name': 'MCX Index', 'description': 'MCX Index', 'parent_name': 'MCX'},
                             {'name': 'MCX:MOEX', 'description': 'MOEX Index', 'parent_name': 'MCX Index'}
                             ])

    # Get single data from Quandl
    dfMICEX = q.get(q_ticket='GOOG/MCX_MOEX',start='2016-12-01', end='2017-01-31', Category=Category, SaveToDB=True)

##### SAVE RAW DATA

    # Define categories for export
    Category = pd.DataFrame([{'name': 'Iris Row Data', 'description': 'Iris Fisher Row Data'},
                             {'name': 'Iris Attributes', 'description': 'Iris Fisher Row Data', 'parent_name': 1 },
                             {'name': 'Iris Classes', 'description': 'Iris Fisher Row Data', 'parent_name': 1 }], index=[1, 2, 3])

    # Define rates for export
    Rates = pd.DataFrame([{'name': 'X1 (Длина чашелистника)', 'category_name': 'Iris Attributes', 'source': 'Manual', 'tag': 'IRIS INPUT'}], index= [1])

    RatesHistory = pd.DataFrame([{'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.3, 'string_value': None, 'tag': '1'},
                                 {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.4, 'string_value': None, 'tag': '2'},
                                 {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.4, 'string_value': None, 'tag': '3'},
                                 {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.4, 'string_value': None, 'tag': '4'},
                                 {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.5, 'string_value': None, 'tag': '5'},
                                 {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.6, 'string_value': None, 'tag': '6'},
                                 {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.6, 'string_value': None, 'tag': '7'},
                                 {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.6, 'string_value': None, 'tag': '8'},
                                 {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.6, 'string_value': None, 'tag': '9'},
                                 {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.7, 'string_value': None, 'tag': '10'}])

    DB.save_raw_data(Category, Rates, RatesHistory, source='MANUAL')

##### TESTING PERFORMANCE get_raw_data()

    # Get Raw data
    dtStart = datetime.datetime.now()

    datax = DB.get_raw_data(RateName='sepal_length')

    dtEnd = datetime.datetime.now()
    print('get_raw_data exec time: {}'.format(dtEnd - dtStart))

    dtStart = datetime.datetime.now()
    c, r, rh = etl.get_Kospi_data_ex1(path = "local_files/Kospi Quotes Eikon Loader 28102016.xlsx", SaveToDB = True)

    dtEnd = datetime.datetime.now()
    print('etl.get_Kospi_data exec time: {}'.format(dtEnd - dtStart))

##### EXAMPLE 1: IRIS FISHER
##### FORMATION OF TRAINING SAMPLE

    x1 = DB.get_raw_data(RateName='sepal_length')
    x1 = pd.DataFrame(x1[2].float_value)
    x2 = DB.get_raw_data(RateName='sepal_width')
    x2 = pd.DataFrame(x2[2].float_value)
    x3 = DB.get_raw_data(RateName='petal_length')
    x3 = pd.DataFrame(x3[2].float_value)
    x4 = DB.get_raw_data(RateName='petal_width')
    x4 = pd.DataFrame(x4[2].float_value)
    X = pd.concat([x1, x2, x3, x4], axis=1)

    y = DB.get_raw_data(RateName='target')
    y = pd.DataFrame(y[2].string_value)

    X = X.as_matrix()
    train_X, test_X, train_y, test_y = train_test_split(X, y, train_size=0.7, random_state=42)
    train_y_ohe = np.array(get_dummies(train_y), dtype=np.float64)
    test_y_ohe = np.array(get_dummies(test_y), dtype=np.float64)

##### BUILD MODEL

    # Set model parameters
    build_args = {
        'build_args': [
            {'neurons': 16, 'input_dim': 4, 'init': 'normal', 'activation': 'relu'},
            {'neurons': 3, 'input_dim': 0, 'init': 'normal', 'activation': 'sigmoid'}
        ],
        'compile_args': {'loss': 'categorical_crossentropy', 'optimizer': 'adam', 'metrics': 'accuracy'}
    }
    fit_args = {'nb_epoch': 100, 'batch_size': 1, 'verbose': 0}
    compile_args = {'loss': 'categorical_crossentropy', 'optimizer': 'adam', 'metrics': 'accuracy'}
    evaluate_args = {'verbose': 0}

    m = KerasClassifier(name='iris', args=build_args)
    m.fit(train_X, train_y_ohe, fit_args=fit_args)
    loss, accuracy = m.evaluate(test_X, test_y_ohe, evaluate_args)
    prediction = m.predict(train_X)

##### SAVE DATASET & PREDICTION

    # Define column names
    ft_data = np.append(train_X, test_X, axis=0)
    ft_data = pd.DataFrame(data=ft_data, columns=['x1', 'x2', 'x3', 'x4'])
    trg_data = np.append(train_y_ohe, test_y_ohe, axis=0)
    trg_data = pd.DataFrame(data=trg_data, columns=['o1', 'o2', 'o3'])

    # Set model parameters for import
    Model = {'model_name': 'Iris Classifier Test', 'description': 'IC_TEST', 'type': 'K'}
    dataset_name = 'Iris Data'

    DB.save_dataset(model=Model, dataset_name=dataset_name, X=ft_data, y=trg_data)

    # Save model prediction
    DB.save_model_prediction(model=Model, dataset_name=dataset_name, prediction=pd.DataFrame(prediction))

##### GET DATASET & PREDICTION

    [X_, y_] = DB.get_dataset(dataset_name='Iris Data')

    prd = DB.get_model_prediction(dataset_name='Iris Data')





