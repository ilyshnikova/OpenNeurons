from flask import Flask
from manager.dbmanager import DBManager
import json

app = Flask(__name__)

base = DBManager()

with open('config.json') as data_file:
    config = json.load(data_file)
