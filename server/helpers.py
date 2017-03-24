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


def get_all_dataset_for_model(base, model_id, models_table, model_2_data_table, dataset_table, checked_datasets=[]):
    datasets = []

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
            to_insert.append({'data_set_id': ds, 'model_id': model_id})

    if len(to_delete):
        session = sessionmaker(bind=base.engine)()
        ds = session.query(model_2_data_table).filter(model_2_data_table.model_id == model_id, model_2_data_table.dataset_id.in_(to_delete))

        for row in ds.all():
            session.delete(row)
        session.close()

    if len(to_insert):
        ins = insert(model_2_data_table).values(to_insert)
        base.engine.connect().execute(ins)
