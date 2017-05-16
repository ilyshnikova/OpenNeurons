from manager.dbmanager import DBManager
from etl.etl import ETL

manager = DBManager()
etl = ETL(manager=manager)
etl.get_Kospi_data_ex1("local_files/kospi.xlsx")
