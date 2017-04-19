from wrapper.kerasclassifier import KerasClassifier
from manager.dbmanager import DBManager
from etl.etl import ETL
#IL Remove bellow, doesn't using it
#from models.models import *

from sklearn.model_selection          import train_test_split
from pandas                           import get_dummies

import numpy as np
import pandas as pd
import datetime

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

save_dataset_instr = {'model_name': 'Iris Keras', 'dataset_name': 'Iris Data'}

#example of usage

if __name__ == '__main__':
    ## Connection to DataBase and assemble Scheme
    DB = DBManager()
    etl = ETL(manager=DB)

####### Ex0 - IRIS FISHER CASE
    ## Examples of insert data
    # cats = [Category(name='futures', description='azaza'),
    #         Category(name='call', description='azaza'),
    #         Category(name='put', description='azaza'),
    #         Category(name='cbr', description='azaza')]
    # manager.session.add_all(cats)
    #
    # etl.get_Kospi_ex1('../Kospi Quotes Eikon Loader.xlsx')
    #
    # etl.get_JapanExchange_Derivatives_ex2('../rb_e20161027.txt.csv')
    #
    # etl.get_CBR_ex3(datetime.datetime(2016, 10, 10), datetime.datetime.now())
    #
    # etl.load_supervised_data(path='local_files/iris.csv', ctg_name=load_data_instr["category_name"])

    ## Get single data from Quandl
    # Category = pd.DataFrame([{'name': 'Financial Markets', 'description': 'Financial Markets Data Branch'},
    #                          {'name': 'Europe', 'description': 'Europe', 'parent_name': 'Financial Markets'},
    #                          {'name': 'Russia', 'description': 'Russia', 'parent_name': 'Europe'},
    #                          {'name': 'MCX', 'description': 'Moscow Exchange', 'parent_name': 'Russia'},
    #                          {'name': 'MCX Index', 'description': 'MCX Index', 'parent_name': 'MCX'},
    #                          {'name': 'MCX:MOEX', 'description': 'MOEX Index', 'parent_name': 'MCX Index'}
    #                          ])
    #
    # dfMICEX = q.get(q_ticket='GOOG/MCX_MOEX',start='2016-12-01', end='2017-01-31', Category=Category, SaveToDB=True)

    ## Get Raw data
    # dtStart = datetime.datetime.now()
    #
    # datax = DB.get_raw_data(RateName='sepal_length')
    #
    # dtEnd = datetime.datetime.now()
    # print('get_raw_data exec time: {}'.format(dtEnd - dtStart))
    #
    # dtStart = datetime.datetime.now()
    # c, r, rh = etl.get_Kospi_data_ex1(path = "local_files/Kospi Quotes Eikon Loader 28102016.xlsx", SaveToDB = True)
    #
    # dtEnd = datetime.datetime.now()
    # print('etl.get_Kospi_data exec time: {}'.format(dtEnd - dtStart))

    ## Save Raw data
    # Category = pd.DataFrame([{'name': 'Iris Row Data', 'description': 'Iris Fisher Row Data'},
    #                          {'name': 'Iris Attributes', 'description': 'Iris Fisher Row Data', 'parent_name': 1 },
    #                          {'name': 'Iris Classes', 'description': 'Iris Fisher Row Data', 'parent_name': 1 }], index=[1, 2, 3])
    # Rates = pd.DataFrame([{'name': 'X1 (Длина чашелистника)', 'category_name': 'Iris Attributes', 'source': 'Manual', 'tag': 'IRIS INPUT'}], index= [1])
    # RatesHistory = pd.DataFrame([{'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.3, 'string_value': None, 'tag': '1'},
    #                              {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.4, 'string_value': None, 'tag': '2'},
    #                              {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.4, 'string_value': None, 'tag': '3'},
    #                              {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.4, 'string_value': None, 'tag': '4'},
    #                              {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.5, 'string_value': None, 'tag': '5'},
    #                              {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.6, 'string_value': None, 'tag': '6'},
    #                              {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.6, 'string_value': None, 'tag': '7'},
    #                              {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.6, 'string_value': None, 'tag': '8'},
    #                              {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.6, 'string_value': None, 'tag': '9'},
    #                              {'rates_name': 'X1 (Длина чашелистника)', 'date': None, 'float_value': 4.7, 'string_value': None, 'tag': '10'}])
    #
    # DB.save_raw_data(Category, Rates, RatesHistory, source='MANUAL')

    ## __Here user's code block__ #
    # X = X.as_matrix()
    # train_X, test_X, train_y, test_y = train_test_split(X, y, train_size=0.7, random_state=42)
    # train_y_ohe = np.array(get_dummies(train_y), dtype=np.float64)
    # test_y_ohe = np.array(get_dummies(test_y), dtype=np.float64)
    ###

    ## Example: Iris Fisher classifier
    # m = KerasClassifier(name='iris', args=build_args)
    # m.fit(train_X, train_y_ohe, fit_args=fit_args)
    # loss, accuracy = m.evaluate(test_X, test_y_ohe, evaluate_args)
    # prediction = m.predict(train_X)
    # print(loss, accuracy)
    # print(prediction)

    ## __Here user's code block__ #
    # ft_data = np.append(train_X, test_X, axis=0)
    # ft_data = pd.DataFrame(data=ft_data)
    # trg_data = np.append(train_y_ohe, test_y_ohe, axis=0)
    # trg_data = pd.DataFrame(data=trg_data)
    ###

    ## Save Dataset
    # DB.save_dataset(ft_data=ft_data, trg_data=trg_data, model_name=save_dataset_instr['model_name'], dataset_name=save_dataset_instr['dataset_name'])

    ## Get Dataset
    # X, y = manager.get_dataset(dataset_name='Iris data')

####### Ex1 - KOSPI instruments from Excel
    # etl.get_Kospi_ex1("local_files/Kospi Quotes Eikon Loader.xlsx")

####### Ex2 - Japan Exchange Derivatives from CSV
    # etl.get_JapanExchange_Derivatives_ex2('../rb_e20161027.txt.csv')

####### Ex3 - Get CB RF public data from cbr.ru web site
    # etl.get_CBR_ex3(datetime.datetime(2016, 10, 10), datetime.datetime.now())

####### Case1 - PDF CASE    
    # path = "../Examples/Acts 2016/"
    # etl.get_PDF_case_1(path)




