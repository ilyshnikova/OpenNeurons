import sys
from manager.dbmanager import DBManager
import hashlib

from models.models import Admins

from sqlalchemy import insert

def print_usage():
    print("""Usage:
        manage_server.py addadminuser --username <username> --password <password>
    """)


if len(sys.argv) < 5:
    print_usage()
    exit(0)

username = ''
password = ''

for i in range(4):
    if (sys.argv[i + 1] == '--username'):
        username = sys.argv[i + 2]
    elif (sys.argv[i + 1] == '--password'):
        password = sys.argv[i + 2]

if username == '':
    print("Username is empty")

if password == '':
    print('Password is empty')

auth = hashlib.md5()
auth.update(password.encode('utf-8'))
auth = auth.hexdigest()

base = DBManager()
ins = insert(Admins).values([{'username': username, 'auth': auth}])
base.engine.connect().execute(ins)
