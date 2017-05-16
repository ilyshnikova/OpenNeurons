# -*- coding: utf8 -*-
import json

from flask import send_from_directory, render_template, request, \
    make_response

from .helpers import add_datasets_to_model, \
    get_all_dataset_for_model, get_dataset, get_model_info, \
    get_models_pretty_list, get_rates

from .env import app, base, config

from .authorization import check_auth, authenticate, requires_auth, \
    hash_auth
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
                'title': 'Авторизироваться как администратор',
                'href': '/authorize',
            },
        ],
    )


@app.route('/models')
def show_models():
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
    import pdb;pdb.set_trace()
    auth = request.cookies.get("auth", '')
    username = request.cookies.get("login", '')
    is_admin = 0
    if check_auth(username, auth):
        is_admin = 1


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
            is_admin=is_admin,
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
            is_admin=is_admin,
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

if __name__ == "__main__":
    server_main()
