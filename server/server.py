# -*- coding: utf8 -*-
import json

from flask import send_from_directory, render_template, request, \
    make_response

from .helpers import add_datasets_to_model, \
    get_all_dataset_for_model, get_dataset, get_model_info, \
    get_models_pretty_list, get_rates, update_model

from .env import app, base, config

from .authorization import check_auth, authenticate, requires_auth, \
    hash_auth, is_admin

import pandas as pd
import numpy as np

from pandas import get_dummies

from wrapper.kerasclassifier import KerasClassifier
from etl.etl import ETL
from sklearn.model_selection import train_test_split


def add_default_values(elements, req):
    for element in elements:
        value = req.args.get(element['id'])
        if value:
            element['default'] = value

    return elements


@app.route('/files/<path:path>')
def send_file(path):
    return send_from_directory('files', path)


@app.route('/')
def main():
    return render_template(
        'menu.html',
        elements=[
            {
                'title': 'Просмотр моделей',
                'href': '/models',
            },
            {
                'title': 'Модель -> выборка',
                'href': '/chose_dataset'
            },
            {
                'title': 'Rates',
                'href': '/rates',
            },
            {
                'title': 'Моделирование',
                'href': '/modeling',
            },
            {
                'title': 'Авторизироваться как администратор',
                'href': '/authorize',
            },
        ],
    )


@app.route('/models')
def show_models():
    admin = is_admin(request)
    if request.args.get("update"):
        if admin:
            update_model(base, request.args.get("model_id"), request.args.get("model_name"), request.args.get("description"), request.args.get("model_type"));
        else:
            return authorize_as_admin()



    options = get_models_pretty_list(base)
    print(options)

    elements = [
        {
            'title': 'Модели',
            'id': 'models',
            'type': 'choice',
            'options': options,
            'default': options[0]['id'],
        },
    ]
    return_url = "/"

    if request.args.get('result'):
        model_info, datasets = get_model_info(base, request.args.get('models'))

        return render_template(
            "models.html",
            elements=add_default_values(elements, request),
            model_desc=model_info,
            datasets=datasets,
            return_url=return_url,
            is_admin=admin,
            model_id=request.args.get('models')
        )
    else:
        return render_template(
            "input.html",
            elements=elements,
            return_url=return_url,
        )


@app.route('/dataset')
def show_dataset():
    return_url = "/models?models=%s&result=1" % request.args.get('models')

    head, dataset = get_dataset(base, request.args.get('id'))

    return render_template(
        "dataset.html",
        table=dataset,
        name=request.args.get('name'),
        head=head,
        return_url=return_url,
    )


@app.route('/chose_dataset')
def show_chose_dataset():
    return_url = "/"

    options = get_models_pretty_list(base)

    elements = [
        {
            'title': 'Модели',
            'id': 'models',
            'type': 'choice',
            'options': options,
            'default': options[0]['id'],
        },
    ]

    admin = is_admin(request)

    if request.args.get('result') == '1':
        model_id = request.args.get('models')
        datasets, checked_datasets = get_all_dataset_for_model(base, model_id)

        return render_template(
            "checkbox_list.html",
            elements=add_default_values(elements, request),
            return_url=return_url,
            cb_list=datasets,
            datasets_ids=','.join(checked_datasets),
            result=2,
            is_admin=admin,
        )
    elif request.args.get('result') == '2':
        model_id = request.args.get('models')
        datasets_ids = request.args.get('datasets_ids').split(',')
        if is_admin == 1:
            add_datasets_to_model(base, model_id, datasets_ids)

        datasets, checked_datasets = get_all_dataset_for_model(
            base,
            request.args.get('models'),
        )

        return render_template(
            "checkbox_list.html",
            return_url=return_url,
            elements=add_default_values(elements, request),
            cb_list=datasets,
            datasets_ids=','.join(checked_datasets),
            result=2,
            is_admin=admin,
        )

    else:
        return render_template(
            "input.html",
            elements=elements,
            result=1,
            return_url=return_url,
        )


@app.route('/rates')
def rates():
    return_url = "/"
    category_id = request.args.get('node')
    rate_id = request.args.get('rate')
    cats, rate, tabs = get_rates(base, category_id, rate_id)

    if 'node' in request.args:
        return render_template(
            "tree.tmpl",
            tabs=tabs,
            cur_tab=rate,
            nodes=cats,
            category=category_id,
            cur_rate=rate_id,
            return_url=return_url,
        )
    else:
        return render_template(
            "tree.tmpl",
            tabs=tabs,
            cur_tab=rate,
            nodes=cats,
            return_url=return_url,
        )

@app.route('/authorize')
def authorize_as_admin():
    elements = [
        {
            'title': 'Логин',
            'id': 'login',
            'type': 'input',
        }, {
            'title': 'Пароль',
            'id': 'password',
            'type': 'input',
        }
    ]
    return_url = "/"

    if request.args.get('result'):
        auth = request.args.get("password") # не передавать прям вот в таком виде
        username = request.args.get("login")
        if check_auth(username, auth, True):
            resp = make_response(main())
            resp.set_cookie('auth', hash_auth(auth))
            resp.set_cookie('login', username)
            return resp
        else:
            return render_template(
                "login.html",
                elements=elements,
                return_url=return_url,
                error=1
            )
    else:
        return render_template(
            "login.html",
            elements=elements,
            return_url=return_url,
        )


def server_main():
    app.jinja_env.tests['equalto'] = lambda value, other: value == other
    app.run(
        host=config['server']['host'],
        port=int(config['server']['port']),
        use_reloader=False,
        debug=True,
        threaded=False,
    )

@app.route('/modeling')
def modeling():

    options = [
        {
            'title': 'iris',
            'id': 1,
        }
    ]
    print(options)

    elements = [
        {
            'title': 'Network layers number',
            'id': 'layers_n',
            'type': '',
            'default': 1,
        },
        {
            'title': 'Neuron number',
            'id': 'nn',
            'type': '',
            'default': 10,
        },

        {
            'title': 'Activation functions list',
            'id': 'func',
            'type': '',
            'default': 'sigmoid',
        },
        {
            'title': 'Metrics',
            'id': 'metrics',
            'type': '',
            'default': 'accuracy',
        },
        {
            'title': 'Loss',
            'id': 'loss',
            'type': '',
            'default': 'categorical_crossentropy',
        },
        {
            'title': 'Epoch number',
            'id': 'ep',
            'type': '',
            'default': '100',
        },
        {
            'title': 'Datasets',
            'id': 'dataset',
            'type': '',
            'options': options,
            'default': options[0]['id'],
        },
    ]
    return_url = "/"

    if request.args.get('result'):

        dataset_to_comps = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width'] # more tables???
        model_info, datasets = get_model_info(base, request.args.get('models'))

        neurons = request.args.get('nn').split(',')
        input_dim = [len(dataset_to_comps)] +  [0] * (len(neurons) - 1)
        activation = request.args.get('func').split(',')

        etl = ETL(manager=base)
        load_data_instr = {"category_name": 'Iris Fisher'}
        path = 'local_files/iris.csv'
        etl.load_supervised_data(path=path, ctg_name=load_data_instr["category_name"])


#        x1 = base.get_raw_data(RateName=dataset_to_comps[0])
#        x1 = pd.DataFrame(x1[2].float_value)
#        x2 = base.get_raw_data(RateName=dataset_to_comps[1])
#        x2 = pd.DataFrame(x2[2].float_value)
#        x3 = base.get_raw_data(RateName=dataset_to_comps[2])
#        x3 = pd.DataFrame(x3[2].float_value)
#        x4 = base.get_raw_data(RateName=dataset_to_comps[3])
#        x4 = pd.DataFrame(x4[2].float_value)

        X = pd.read_csv(path)
        y = X['species']
        X = X.drop('species', axis=1)

        X = X.as_matrix()
        train_X, test_X, train_y, test_y = train_test_split(X, y, train_size=0.7, random_state=42)
        train_y_ohe = np.array(get_dummies(train_y), dtype=np.float64)
        test_y_ohe = np.array(get_dummies(test_y), dtype=np.float64)

#        build_args = {
#            'build_args': [
#                {'neurons': neurons[i], 'input_dim': input_dim[i], 'activation': activation[i], 'init': 'normal'} for i in range(len(neurons))
##                {'neurons' : 16, 'input_dim' : 4, 'init' : 'normal', 'activation' : 'relu'},
##                {'neurons' : 3, 'input_dim' : 0, 'init' : 'normal', 'activation' : 'sigmoid'}
#                ],
#            'compile_args': {
#                    'loss': request.args.get('loss'),
#                    'optimizer': 'adam',
#                    'metrics': request.args.get('metrics')
#            }
#        }
#        compile_args = {
#                    'loss': request.args.get('loss'),
#                    'optimizer': 'adam',
#                    'metrics': request.args.get('metrics')
#            }
#        fit_args = {'nb_epoch': request.args.get('ep'), 'batch_size': 1, 'verbose': 0}
#        evaluate_args = {'verbose': 0}
#        predict_args = {}

        build_args = {
            'build_args': [
                {'neurons' : 16, 'input_dim' : 4, 'init' : 'normal', 'activation' : 'relu'},
                {'neurons' : 3, 'input_dim' : 0, 'init' : 'normal', 'activation' : 'sigmoid'}
                ],
            'compile_args': {'loss': 'categorical_crossentropy', 'optimizer': 'adam', 'metrics': 'accuracy'}
        }
        compile_args = {'loss': 'categorical_crossentropy', 'optimizer': 'adam', 'metrics': 'accuracy'}
        fit_args = {'epochs': 100, 'batch_size': 1, 'verbose': 1}
        evaluate_args = {'verbose': 0}
        predict_args = {}


        print(build_args)

        m = KerasClassifier(name='iris', args=build_args)
        history = m.fit(train_X, train_y_ohe, fit_args=fit_args)
        loss, accuracy = m.evaluate(test_X, test_y_ohe, evaluate_args)
        prediction = m.predict(train_X)

        loss_data = history.history['loss'][1:]

        return render_template(
            "modeling.html",
            elements=elements,
            return_url=return_url,
            loss=request.args.get('loss'),
            loss_data=list(zip(list(range(len(loss_data) - 1)), loss_data))
        )
    else:

        return render_template(
            "input.html",
            elements=elements,
            return_url=return_url,
        )



if __name__ == "__main__":
    server_main()
