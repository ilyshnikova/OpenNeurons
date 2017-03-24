# -*- coding: utf8 -*-

from flask import Flask, send_from_directory, render_template, request
import json
import os
from helpers import *

from sqlalchemy import *

import sys

from manager.dbmanager import *

app = Flask(__name__)

def add_default_values(elements, request):
    for element in elements:
        value = request.args.get(element['id'])
        if value:
            element['default'] = value

    return elements

with open('config.json') as data_file:
        config = json.load(data_file)

base = DBManager()

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
                'title': 'Модель -> выборка',
                'href': '/chose_dataset'
            },
            {
                'title' : 'Rates',
                'href' : '/rates',
            },
        ],
    )


@app.route('/models')
def show_models():

    options = get_models_pretty_list(base, Model)
    print(options)

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
        (model_info, datasets) = get_model_info(base, request.args.get('models'), Model, Model2Dataset, DataSet)

        return render_template(
            "models.html",
            elements=add_default_values(elements, request),
            model_desc=model_info,
            datasets=datasets,
            return_url=return_url,
        )
    else:
        return render_template(
            "input.html",
            elements=elements,
            return_url=return_url,
        )

@app.route('/dataset')
def show_dataset():

    return_url="/models?models=%s&result=1" % request.args.get('models')

    (head, dataset) = get_dataset(base, request.args.get('id'), DataSetComponent, DataSetValues)

    return render_template(
        "dataset.html",
        table=dataset,
        name=request.args.get('name'),
        head=head,
        return_url=return_url,
    )


@app.route('/chose_dataset')
def show_chose_dataset():
    return_url="/"

    options = get_models_pretty_list(base, Model)

    elements = [
        {
            'title': 'Модели',
            'id': 'models',
            'type' : 'choice',
            'options' : options,
            'default': options[0]['id'],
        },
    ]

    if request.args.get('result') == '1':
        (datasets, checked_datasets) = get_all_dataset_for_model(base, request.args.get('models'), Model, Model2Dataset, DataSet)

        return render_template(
            "checkbox_list.html",
            elements=add_default_values(elements, request),
            return_url=return_url,
            cb_list=datasets,
            datasets_ids=','.join(checked_datasets),
            result=2,
        )
    elif request.args.get('result') == '2':

        add_datasets_to_model(base, request.args.get('models'), Model, Model2Dataset, DataSet, request.args.get('datasets_ids').split(','))

        (datasets, checked_datasets) = get_all_dataset_for_model(
            base,
            request.args.get('models'),
            Model, Model2Dataset, DataSet)

        return render_template(
            "checkbox_list.html",
            return_url=return_url,
            elements=add_default_values(elements, request),
            cb_list=datasets,
            datasets_ids=','.join(checked_datasets),
            result=2,
        )

    else:
        return render_template(
            "input.html",
            elements=elements,
            result=1,
            return_url=return_url,
        )

def RepresentsInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


@app.route('/rates')
def rates():
    return_url="/"

    head = ['Data', 'Value', 'Tag']

    rates = [
        {'id': 1, 'name': '1_node', 'all_childs': [{'id': 2, 'name': '1.1', 'has_childs': False}, {'id': 3, 'name': '1.2', 'has_childs': False}], 'has_childs': True},
        {'id': 4, 'name': '2_node', 'all_childs': [{'id': 5, 'name': '2.1', 'has_childs': False}, {'id': 6, 'name': '2.2', 'has_childs': False}], 'has_childs': True},
    ]

    tabs = {
        '-1': [],
        '1' : [
            {'id':1, 'name': '1_rate', 'tag': '1_tag', 'source': '1_source', 'head': head, 'table': [[1,2,3], [4,5,6], [7,8,9]]},
            {'id':2, 'name': '11_rate', 'tag': '11_tag', 'source': '11_source', 'head': head, 'table': [[11,2,3], [4,5,6], [7,8,9]]}],
        '2' : [{'id': 1, 'name': '2_rate', 'tag': '2_tag', 'source': '2_source', 'head': head, 'table': [[1,2,3], [4,5,6], [7,8,9]]}],
        '3' : [{'id': 1, 'name': '3_rate', 'tag': '3_tag', 'source': '3_source', 'head': head, 'table': [[1,2,3], [4,5,6], [7,8,9]]}],
        '4' : [{'id': 1, 'name': '4_rate', 'tag': '4_tag', 'source': '4_source', 'head': head, 'table': [[1,2,3], [4,5,6], [7,8,9]]}],
    }

#    import pdb; pdb.set_trace()
    if 'node' in request.args:
        return render_template(
            "tree.tmpl",
            tabs=tabs.get(request.args.get('node'), []),
            nodes=rates,
            return_url=return_url,
        )
    else:
        return render_template(
            "tree.tmpl",
            tabs=tabs["1"],
            nodes=rates,
            return_url=return_url,
        )


if __name__ == "__main__":
#    os.chdir("/root/open_trm/")
    app.jinja_env.tests['equalto'] = lambda value, other : value == other
    app.run(
        host=config['server']['host'],
        port=int(config['server']['port']),
        use_reloader=False,
        debug=True,
        threaded=False,
    )
