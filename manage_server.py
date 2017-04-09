import sys

def print_usage():
    print("""Usage:
    manage_server.py initdb|runserver|checkdeps
        initdb      -   initialized database structure
        runserver   -   run Web server
        checkdeps   -   check dependencies required to run
        initfisher  -   init fisher dataset in database
        initkospi   -   init kospi dataset in database
        truncatedb  -   truncate database
    """)


def init_database():
    from sqlalchemy import text
    from manager.dbmanager import DBManager
    try:
        manager = DBManager()
    except OperationalError as e:
        error_message = str(e)
        sys.stderr.write("Can't connect to database: {}\n".format(error_message))
        sys.exit(1)

    manager.session.flush()


def truncate_database():
    from sqlalchemy import text
    from manager.dbmanager import DBManager
    try:
        manager = DBManager()
    except OperationalError as e:
        error_message = str(e)
        sys.stderr.write("Can't connect to database: {}\n".format(error_message))
        sys.exit(1)
    manager.drop_all_tables()
    manager.session.flush()


def init_fisher():
    from sqlalchemy import text
    from manager.dbmanager import DBManager


    try:
        base = DBManager()
    except OperationalError as e:
        error_message = str(e)
        sys.stderr.write("Can't connect to database: {}\n".format(error_message))
        sys.exit(1)

    base.load_tables_from_json("fisher.json")
#    clsmembers = dict(inspect.getmembers(sys.modules['models.models'], inspect.isclass))
#    data = json.load(open("fisher.json", 'r'))
#
#    for table in data.keys():
#        vals_to_insert = []
#
#        for d in data[table]["data"]:
#            vals_to_insert.append(dict(zip(data[table]["head"], d)))
#
#        table_object = clsmembers[table]
#
#        ins = insert(table_object).values(vals_to_insert)
#        base.engine.connect().execute(ins)
#
#    base.session.commit()


def init_kospi():
    from manager.dbmanager import DBManager
    DB = DBManager()
    etl = ETL(manager=DB)
    etl.get_Kospi_data_ex1("local_files/kospi.xlsx")


def run_server():
    from server.server import server_main
    server_main()


def check_deps():
    # TODO use try/catch to detect all required packages
    pass


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_usage()
    elif "initdb" == sys.argv[1]:
        init_database()
    elif "runserver" == sys.argv[1]:
        run_server()
    elif "checkdeps" == sys.argv[1]:
        check_deps()
    elif "initfisher" == sys.argv[1]:
        init_fisher()
    elif "initkospi" == sys.argv[1]:
        init_kospi()
    elif "truncatedb" == sys.argv[1]:
        truncate_database()
    else:
        print_usage()
