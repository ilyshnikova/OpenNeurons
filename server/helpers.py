from sqlalchemy import select, insert
from sqlalchemy.orm import sessionmaker
import numpy as np
from models.models import Model, Model2Dataset, \
    DataSet, DataSetComponent, DataSetValues, \
    Category, Rates, RatesHistory, Source


def get_models_pretty_list(base):
    s = select([Model])
    result = base.engine.connect().execute(s)
    models = []

    for row in result:
        models += [{
            'title': 'Data analysing model : "' + str(row['model_name']) + '"',
            'id': str(int(row['id']))
        }]

    return models


def get_model_info(base, model_id):
    session = sessionmaker(bind=base.engine)()
    models = session.query(Model, Model2Dataset). \
        join(Model2Dataset, Model2Dataset.model_id == Model.id). \
        filter(Model.id == model_id)

    dataset_list = []
    model_info = []

    if models is None:
        return [], []

    for model in models:
        model_info = [{
                'title': 'Model name',
                'value': model[0].model_name
            },
            {
                'title': 'Description',
                'value': model[0].description,
            }, {
                'title': 'Model type',
                'value': model[0].model_type,
            }]

        s = select([DataSet]).where(DataSet.id == model[0].id)
        data_set = base.engine.connect().execute(s).first()

        if data_set is not None:
            keywords = "id=%d&models=%d&name=%s" % (
                data_set.id,
                model[0].id,
                data_set.name
            )
            dataset_list.append(
                [{
                    # for template
                    'title': 'Dataset name',
                    'value': data_set.name,
                    'keywords': keywords,
                    # additional info
                    'id': data_set.id,
                    'name': data_set.name,
                }]
            )

    return model_info, dataset_list


def get_dataset(base, dataset_id):
    s = select([DataSetComponent]) \
        .where(DataSetComponent.dataset_id == dataset_id) \
        .order_by(DataSetComponent.id)
    dataset_comps = base.engine.connect().execute(s)

    head = []
    table = []

    for comp in dataset_comps:
        s = select([DataSetValues]). \
            where(DataSetValues.dataset_id == dataset_id). \
            where(DataSetValues.component_id == comp.id). \
            order_by(DataSetValues.vector_id)
        vector = base.engine.connect().execute(s)

        col = []
        prev = 0
        for i in vector:
            if prev + 1 == i.vector_id:
                col.append(i.value)
            else:
                while prev < i.vector_id and prev + 1 != i.vector_id:
                    col.append(0)
                    prev += 1
            prev += 1

        table.append(col)
        head.append("%s(%s)" % (comp.component_name, comp.component_type))

    table = np.array(table).transpose().tolist()

    return head, table


def get_all_dataset_for_model(base, model_id):
    datasets = []
    checked_datasets = []

    models_datasets = get_model_info(base, model_id)[1]

    for ds in models_datasets:
        ds = ds[0]
        checked_datasets.append(str(ds['id']))

    s = select([DataSet])
    dss = base.engine.connect().execute(s)
    for ds in dss:
        datasets.append({
            'id': ds.id,
            'name': ds.name,
            'checked': 1 if str(ds.id) in checked_datasets else 0,
        })
    print(datasets)
    return datasets, checked_datasets


def can_be_represented_as_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def add_datasets_to_model(base, model_id, datasets):
    models_datasets = get_model_info(base, model_id)[1]

    to_delete = []
    to_insert = []
    models_dataset_list = []

    for ds in models_datasets:
        ds = ds[0]
        if not str(ds['id']) in datasets:
            to_delete.append(int(ds['id']))
        models_dataset_list.append(str(ds['id']))

    for ds in datasets:
        if can_be_represented_as_int(ds) and ds not in models_dataset_list:
            to_insert.append({'dataset_id': ds, 'model_id': model_id})

    if to_delete:
        session = sessionmaker(bind=base.engine)()
        ds = session.query(Model2Dataset) \
            .filter(Model2Dataset.model_id == model_id,
                    Model2Dataset.dataset_id.in_(to_delete))

        for row in ds.all():
            session.delete(row)
        session.commit()

    if to_insert:
        ins = insert(Model2Dataset).values(to_insert)
        base.engine.connect().execute(ins)


def get_rates(base, category_id, cur_rate_id):
    categories = []
    rates = {}
    head = ['Deta', 'Value', 'Tag']

    # categories
    s = select([Category])
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
                cats[row.parent_id] = {
                    'id': row.parent_id,
                    'all_childs': [cats[row.id]],
                    'has_childs': True
                }

    if category_id is None:
        return categories, [], []

    # sources
    s = select([Source])
    result = base.engine.connect().execute(s)

    sources = {}

    for source in result:
        sources[source.id] = source.name

    # rates
    s = select([Rates]). \
        where(Rates.category_id == category_id)
    result = base.engine.connect().execute(s)

    tabs = []

    for rate in result:
        no_valid_rate = cur_rate_id is None and not rates == {}
        current_rate_id = int(cur_rate_id) if cur_rate_id else None
        rate_id = int(rate.id)
        rate_id_match = cur_rate_id is not None and current_rate_id == rate_id
        if no_valid_rate or rate_id_match:
            s = select([RatesHistory]).where(RatesHistory.rates_id == rate.id)
            history = base.engine.connect().execute(s)
            history_table = []
            for row in history:
                history_table.append([
                    row.date, row.string_value
                    if row.string_value != ''
                    else row.float_value, row.tag
                ])

            rates = {
                'id': rate.id,
                'name': rate.name,
                'tag': rate.tag,
                'source': sources[rate.source_id],
                'head': head,
                'table': history_table
            }
        tabs.append({'id': rate.id, 'name': rate.name})

    return categories, rates, tabs

