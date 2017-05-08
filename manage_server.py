import sys
from subprocess import call

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

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_usage()
        exit(0)
    script = sys.argv[1]
    call(["python",  "manage_server/{script}.py".format(script=script)])
