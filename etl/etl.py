import pandas as pd
import requests
import os

from sqlalchemy import create_engine

class ETL:
    def __init__(self, db_name, user='postgres', host='localhost'):
        self.user = user
        self.host = host
        self.db_name = db_name
        self.engine = create_engine(
            'postgresql://{user}@{host}:5432/{db_name}'.format(user=user, host=host, db_name=db_name)
        )

    def write_to_db(self, path, table_name):
        # deletes file after loading table to db
        extension = path.split('.')[-1]
        if extension == 'csv':
            data = pd.read_csv(path, encoding="ISO-8859-1", header=2)
        elif extension == 'xlsx':
            data = pd.read_excel(path, sheetname=2)
        data.to_sql(table_name, self.engine, if_exists='append')
        os.remove('path')

    def get_table(self, table_name):
        return pd.read_sql_table(table_name, self.engine)
    
    def get_cb_data(self, url, path):
        '''
        XML is a tree-like structure, while a Pandas DataFrame
        is a 2D table-like structure. So there is no automatic way
        to convert between the two. You have to understand the XML
        structure and know how you want to map its data onto a 2D table,
        so now, when we don't know which xml is needed we would return it
        as text and save to file
        '''
        
        r = requests.get(url)
        with open(path, 'w') as file:
            file.write(r.text)
            
        return r.text
