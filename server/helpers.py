import json
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
import numpy as np

def get_models_pretty_list(engine, conn, table):
    s = select([table])
    result = conn.execute(s)
    models = []

    for row in result:
        models += [{'title': 'Data analysing model : "' + str(row['model_name']) + '"', 'id': str(int(row['model_id']))}]

    return models


def get_model_info(engine, conn, model_id, models_table, model_2_data_table, dataset_table):
    session = sessionmaker(bind=engine)()
    models = session.query(models_table, model_2_data_table).\
            join(model_2_data_table, model_2_data_table.c.model_id == models_table.c.model_id).\
            filter(models_table.c.model_id == model_id)

#    import pdb; pdb.set_trace()
    dataset_list = []
    model_info = []

    for model in models:
        model_info = [{
                'title' : 'Model name',
                'value' : model.model_name
            },
            {
                'title' : 'Description',
                'value' : model.description,
            },{
                'title' : 'Model type',
                'value' : model.model_type,
            }]

        s = select([dataset_table]).where(dataset_table.c.data_set_id == model.data_set_id)
        data_set = conn.execute(s).first()

        dataset_list.append(
            [{
                'title' : 'Dataset name',
                'value' : data_set.data_set_name,
                'keywords': "id=%d&models=%d&name=%s" % (data_set.data_set_id, model.model_id, data_set.data_set_name),
            }]
        )

    return (model_info, dataset_list)


def get_dataset(engine, conn, dataset_id, dataset_comp_table, dataset_values_table):
    s = select([dataset_comp_table]).where(dataset_comp_table.c.data_set_id == dataset_id).order_by(dataset_comp_table.c.component_id)
    dataset_comps = conn.execute(s)

    head = []
    table = []

    for comp in dataset_comps:
        s = select([dataset_values_table]).\
                where(dataset_values_table.c.data_set_id == dataset_id).\
                where(dataset_values_table.c.component_id == comp.component_id).\
                order_by(dataset_values_table.c.vector_id)
        vector = conn.execute(s)

        col = []
        prev_vect_id = 0
        for i in vector:
            if prev_vect_id + 1 == i.vector_id:
                col.append(i.data_set_value)
            else:
                while  prev_vect_id < i.vector_id and prev_vect_id + 1 != i.vector_id:
                    col.append(0)
                    prev_vect_id += 1
            prev_vect_id += 1


        table.append(col)
        head.append("%s(%s)" % (comp.component_name, comp.component_type))


    table = np.array(table).transpose().tolist()

    return (head, table)
