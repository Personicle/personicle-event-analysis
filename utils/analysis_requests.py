# from config import *
import pandas.io.sql as sqlio
import pandas as pd
import os
import copy
from sqlalchemy import select, inspect

from database.postgres import loadSession, engine, generate_table_class
from utils.analysis_request_schema import ANALYSIS_TABLE_SCHEMA
import logging
# session = loadSession()

ANALYSIS_TABLE_COLUMNS = ["user_id", "anchor", "aggregate_function", "query_interval",
                          "antecedent_name", "antecedent_table", "antecedent_parameter", "antecedent_type", "antecedent_interval",
                          "consequent_name", "consequent_table", "consequent_parameter", "consequent_type", "consequent_interval"]


def object_as_dict(obj):
    return {c.key: getattr(obj, c.key)
            for c in inspect(obj).mapper.column_attrs}


def get_analysis_requests():
    model_class = generate_table_class(os.environ.get("ANALYSIS_TABLENAME"),
                                       copy.deepcopy(ANALYSIS_TABLE_SCHEMA))

    session = loadSession()
    # results = session.execute(query)
    results = []

    select_query = select(model_class)
    result = session.execute(select_query).scalars().all()

    # analysis_query = "select * from {}".format(
    #     os.environ.get("ANALYSIS_TABLENAME"))
    # result = sqlio.read_sql(analysis_query, engine)
    print(result)

    # for i in model_class.query.all():
    for i in result:  # .to_dict('records'):
        record = object_as_dict(i)
        # record = i
        print("sending record: {}".format(record))
        if all(j in record.keys() for j in ANALYSIS_TABLE_COLUMNS):
            # record['unique_analysis_id'] = str(record['unique_analysis_id'])
            logging.info(record)
            results.append(record)

    return results
