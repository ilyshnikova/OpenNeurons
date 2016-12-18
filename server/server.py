# -*- coding: utf8 -*-

from flask import Flask, send_from_directory, render_template, request
import json
import os

app = Flask(__name__)

def add_default_values(elements, request):
    for element in elements:
        value = request.args.get(element['id'])
        if value:
            element['default'] = value

    return elements

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
    elements = [
        {
            'title': 'Модели',
            'id': 'models',
            'type' : 'choice',
            'options' : [
                {
                    'title' : 'First algorithm model',
                    'id' : 'first',
                },
                {
                    'title' : 'Second algorithm model',
                    'id' : 'second',
                },
                {
                    'title' : 'Third algorithm model',
                    'id' : 'third',
                },
            ],
            'default': 'first',
        },
    ]
    return_url="/"

    if request.args.get('result'):
        return render_template(
            "output.html",
            elements=add_default_values(elements, request),
            output_elements=[
                {
                    'title' : 'какое то значение',
                    'value' : 1234,
                },
                {
                    'title' : 'еще какое то значение',
                    'value' : 5678,
                },
                {
                    'title' : 'и еще значение',
                    'value' : 901,
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
