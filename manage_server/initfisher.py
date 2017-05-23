from manager.dbmanager import DBManager
manager = DBManager()
manager.load_tables_from_json("fisher.json")
