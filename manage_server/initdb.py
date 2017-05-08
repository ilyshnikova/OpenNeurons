from manager.dbmanager import DBManager

manager = DBManager()
manager.session.flush()
