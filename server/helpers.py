import json
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import numpy as np

def get_models_pretty_list(base, table):
    s = select([table])
    result = base.engine.connect().execute(s)
    models = []

    for row in result:
        models += [{'title': 'Data analysing model : "' + str(row['model_name']) + '"', 'id': str(int(row['id']))}]

    return models


def get_model_info(base, model_id, models_table, model_2_data_table, dataset_table):
    session = sessionmaker(bind=base.engine)()
    models = session.query(models_table, model_2_data_table).\
            join(model_2_data_table, model_2_data_table.model_id == models_table.id).\
            filter(models_table.id == model_id)

    dataset_list = []
    model_info = []

    if models is None:
        return ([],[])

    for model in models:
        model_info = [{
                'title' : 'Model name',
                'value' : model[0].model_name
            },
            {
                'title' : 'Description',
                'value' : model[0].description,
            },{
                'title' : 'Model type',
                'value' : model[0].model_type,
            }]

        s = select([dataset_table]).where(dataset_table.id == model[0].id)
        data_set = base.engine.connect().execute(s).first()

        if data_set is not None:
            dataset_list.append(
                [{
                    # for template
                    'title' : 'Dataset name',
                    'value' : data_set.name,
                    'keywords': "id=%d&models=%d&name=%s" % (data_set.id, model[0].id, data_set.name),
                    # additional info
                    'id': data_set.id,
                    'name': data_set.name,
                }]
            )

    return (model_info, dataset_list)


def get_dataset(base, dataset_id, dataset_comp_table, dataset_values_table):
    s = select([dataset_comp_table]).where(dataset_comp_table.dataset_id == dataset_id).order_by(dataset_comp_table.id)
    dataset_comps = base.engine.connect().execute(s)

    head = []
    table = []

    for comp in dataset_comps:
        s = select([dataset_values_table]).\
                where(dataset_values_table.dataset_id == dataset_id).\
                where(dataset_values_table.component_id == comp.id).\
                order_by(dataset_values_table.vector_id)
        vector = base.engine.connect().execute(s)

        col = []
        prev_vect_id = 0
        for i in vector:
            if prev_vect_id + 1 == i.vector_id:
                col.append(i.value)
            else:
                while  prev_vect_id < i.vector_id and prev_vect_id + 1 != i.vector_id:
                    col.append(0)
                    prev_vect_id += 1
            prev_vect_id += 1


        table.append(col)
        head.append("%s(%s)" % (comp.component_name, comp.component_type))


    table = np.array(table).transpose().tolist()

    return (head, table)


def get_all_dataset_for_model(base, model_id, models_table, model_2_data_table, dataset_table):
    datasets = []
    checked_datasets = []

    models_datasets = get_model_info(base, model_id, models_table, model_2_data_table, dataset_table)[1];

    for ds in models_datasets:
        ds = ds[0]
        checked_datasets.append(str(ds['id']))

    s = select([dataset_table])
    dss = base.engine.connect().execute(s)
    for ds in dss:
        datasets.append({
            'id': ds.id,
            'name': ds.name,
            'checked' : 1 if str(ds.id) in checked_datasets else 0,
        })
    print(datasets)
    return datasets, checked_datasets

def RepresentsInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def add_datasets_to_model(base, model_id, models_table, model_2_data_table, dataset_table, datasets):
    data_to_insert = []

    models_datasets = get_model_info(base, model_id, models_table, model_2_data_table, dataset_table)[1];

    to_delete = []
    to_insert = []
    models_dataset_list = []

    for ds in models_datasets:
        ds = ds[0]
        if not str(ds['id']) in datasets:
            to_delete.append(int(ds['id']))
        models_dataset_list.append(str(ds['id']))

    for ds in datasets:
        if RepresentsInt(ds) and not ds in models_dataset_list:
            to_insert.append({'dataset_id': ds, 'model_id': model_id})

    if len(to_delete):
        session = sessionmaker(bind=base.engine)()
        ds = session.query(model_2_data_table).filter(model_2_data_table.model_id == model_id, model_2_data_table.dataset_id.in_(to_delete))

        for row in ds.all():
            session.delete(row)
        session.commit()

    if len(to_insert):
        ins = insert(model_2_data_table).values(to_insert)
        base.engine.connect().execute(ins)


def get_rates(base, category_table, rates_table, rates_history_table, source_table):
#    categories = [
#        {'id': 1, 'name': '1_node', 'all_childs': [{'id': 2, 'name': '1.1', 'has_childs': False}, {'id': 3, 'name': '1.2', 'has_childs': False}], 'has_childs': True},
#        {'id': 4, 'name': '2_node', 'all_childs': [{'id': 5, 'name': '2.1', 'has_childs': False}, {'id': 6, 'name': '2.2', 'has_childs': False}], 'has_childs': True},
#    ]
#
#
#    rates = {
#        '-1': [],
#        '1' : [
#            {'id':1, 'name': '1_rate', 'tag': '1_tag', 'source': '1_source', 'head': head, 'table': [[1,2,3], [4,5,6], [7,8,9]]},
#            {'id':2, 'name': '11_rate', 'tag': '11_tag', 'source': '11_source', 'head': head, 'table': [[11,2,3], [4,5,6], [7,8,9]]}],
#        '2' : [{'id': 1, 'name': '2_rate', 'tag': '2_tag', 'source': '2_source', 'head': head, 'table': [[1,2,3], [4,5,6], [7,8,9]]}],
#        '3' : [{'id': 1, 'name': '3_rate', 'tag': '3_tag', 'source': '3_source', 'head': head, 'table': [[1,2,3], [4,5,6], [7,8,9]]}],
#        '4' : [{'id': 1, 'name': '4_rate', 'tag': '4_tag', 'source': '4_source', 'head': head, 'table': [[1,2,3], [4,5,6], [7,8,9]]}],
#    }   #id, name, tag, head table

    categories = []
    rates = {-1: []}
    head = ['Deta', 'Value', 'Tag']

    # categories
    s = select([category_table])
    result = base.engine.connect().execute(s)

    cats = {}

    for row in result:
        if row.id in cats:
            cats[row.name] = row.name
        else:
            cats[row.id] = {'id': row.id, 'name': row.name, 'all_childs': []}

        if row.parent_id is None:
            categories.append(cats[row.id])
        else:
            if row.parent_id in cats:
                cats[row.parent_id]['has_childs'] = True
                cats[row.parent_id]['all_childs'].append(cats[row.id])
            else:
                cats[row.parent_id] = {'id': row.parent_id, 'all_childs': [cats[row.id]], 'has_childs': True}


    # sources
    s = select([source_table])
    result = base.engine.connect().execute(s)

    sources = {}

    for source in result:
        sources[source.id] = source.name

    # rates
    s = select([rates_table]).\
            order_by(rates_table.category_id)
    result = base.engine.connect().execute(s)

    for rate in result:
        s = select([rates_history_table])
        history = base.engine.connect().execute(s)
        history_table = []
        for row in history:
            history_table.append([row.date, row.string_value if row.string_value != '' else row.float_value, row.tag])

        if rate.category_id not in rates:
            rates[rate.category_id] = []

        rates[rate.category_id].append(
            {'id': rate.source_id, 'name': rate.name, 'tag': row.tag, 'source': sources[rate.source_id], 'head': head, 'table': history_table}
        )

    return (categories, rates)
