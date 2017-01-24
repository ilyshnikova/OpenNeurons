import datetime
import os
import pandas as pd
import requests
import lxml

from sqlalchemy import create_engine
from zeep import Client


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

    def get_table(self, table_name):
        return pd.read_sql_table(table_name, self.engine)
    
    def get_bicurbase(self, start : datetime.datetime, end : datetime.datetime):
        client = Client('http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?WSDL')
        resp = client.service.BiCurBase(datetime.datetime(2016, 10, 18, 0, 0, 0, 0), datetime.datetime.today())
        
        header = ['D0', 'VAL']
        table = []
        for tbl in resp['_value_1'].getchildren()[0].xpath('//BCB'):
            row = [tbl.xpath(col)[0].text for col in header]
            table.append(row)
        return pd.DataFrame(table, columns=header)
