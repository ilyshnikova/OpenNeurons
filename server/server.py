# -*- coding: utf8 -*-

from flask import Flask, send_from_directory, render_template, request
import json
import os

from sqlalchemy import *

app = Flask(__name__)

def add_default_values(elements, request):
    for element in elements:
        value = request.args.get(element['id'])
        if value:
            element['default'] = value

    return elements

user='postgres'
host='localhost'
db_name='trmdb'

engine = create_engine('postgresql://{user}:postgres@{host}:5432/{db_name}'.format(user=user, host=host, db_name=db_name))
conn = engine.connect()

metadata = MetaData(engine)

models_table = Table('model', metadata, autoload=True)


@app.route('/files/<path:path>')
def send_file(path):
    return send_from_directory('files', path)

@app.route('/')
def ok():
    return render_template(
        'menu.html',
        elements=[
            {
                'title' : 'Просмотр моделей',
                'href' : '/models',
            },
            {
                'title' : 'Просмотр лингвистических переменных',
                'href' : '/ling-vars',
                'disabled' : True,
            },
            {
                'title' : 'Просмотр правил базы знаний нечетких высказываний',
                'href' : '/rules',
                'disabled' : True,
            },
        ],
    )


@app.route('/models')
def show_models():
    s = select([models_table])
    result = conn.execute(s)
    options = []

    for row in result:
        print row
        options += [{'title': 'Data analysing model : "' + str(row['model_name']) + '"', 'id': str(int(row['model_id']))}]

    print options

    elements = [
        {
            'title': 'Модели',
            'id': 'models',
            'type' : 'choice',
            'options' : options,
            'default': options[0]['id'],
        },
    ]
    return_url="/"

    if request.args.get('result'):
        s = select([models_table]).where(models_table.c.model_id == request.args.get('models'))
        result = conn.execute(s).__iter__().next()

        return render_template(
            "output.html",
            elements=add_default_values(elements, request),
            output_elements=[
                {
                    'title' : 'Model name',
                    'value' : result['model_name']
                },
                {
                    'title' : 'Description',
                    'value' : result['description'],
                },
                {
                    'title' : 'Model type',
                    'value' : result['model_type']
                },
            ],
            return_url=return_url,
        )
    else:
        return render_template(
            "input.html",
            elements=elements,
            return_url=return_url,
        )

if __name__ == "__main__":
    os.chdir("/root/open_trm/")
    app.jinja_env.tests['equalto'] = lambda value, other : value == other
    app.run(
        host='::',
        port=5001,
        use_reloader=False,
        debug=True,
        threaded=False,
    )
