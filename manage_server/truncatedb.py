from manager.dbmanager import DBManager

manager = DBManager()
manager.drop_all_tables()
manager.session.flush()
