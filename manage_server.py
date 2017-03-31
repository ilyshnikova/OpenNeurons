import sys

def print_usage():
    print("""Usage:
    manage_server.py initdb|runserver|checkdeps
        initdb      -   initialized database structure
        runserver   -   run Web server
        checkdeps   -   check dependencies required to run
    """)


def init_database():
    from manager.dbmanager import DBManager
    manager = DBManager()
    manager.session.flush()


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
    else:
        print_usage()
