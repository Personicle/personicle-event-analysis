# from config import *
import pandas.io.sql as sqlio
import pandas as pd
import os
import copy
from sqlalchemy import select, inspect

from database.postgres import loadSession, engine, generate_table_class
from utils.analysis_request_schema import ANALYSIS_TABLE_SCHEMA

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

    for i in session.query(model_class).all():
        record = object_as_dict(i)
        if all(j in record.keys() for j in ANALYSIS_TABLE_COLUMNS):
            results.append(record)

    return results
