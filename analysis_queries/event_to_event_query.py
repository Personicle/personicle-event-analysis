import numpy as np
import pandas as pd

from sqlalchemy import select
import pandas.io.sql as sqlio
import json
import logging

import sqlite3 as sq3
from sqlite3 import InterfaceError

from database.postgres import engine
from .personicle_functions import *
from .utility_functions_sleepanalysis import *
from datetime import datetime, timedelta

# logger = logging.getLogger(__main__):


def e2e_scatterplot(time_interval_begin, time_interval_end, antecedent, antecedent_parameter, consequent, consequent_parameter, user_id, anchor="CONSEQUENT", aggregation_function="SUM"):
    # Query to fetch the event data
    print("Running analysis for {}:{} [{},{}] {}:{}".format(antecedent,
          antecedent_parameter, time_interval_begin, time_interval_end, consequent, consequent_parameter))
    query_events = "select * from  personal_events where user_id='{}'".format(
        user_id)
    events_stream = sqlio.read_sql_query(query_events, engine)

    events_stream = events_overlap(events_stream)

    events_stream['duration'] = (
        events_stream['end_time'] - events_stream['start_time']) / pd.Timedelta(hours=1)
    events_stream['end_date'] = pd.to_datetime(
        events_stream['end_time']).dt.date
    events_stream['end_date'] = events_stream['end_date'].astype(str)

    # Matching events with event data

    # es1=data_stream.copy()
    # es1=events_stream[~(events_stream.event_name.isin(['Sleep']))]
    es1 = events_stream[(events_stream.event_name.isin([antecedent]))]
    es1['metric'] = es1['parameter2'].apply(
        lambda x: json.loads(x).get(antecedent_parameter))
    print(f"{antecedent} stream")
    print(es1[["event_name", "metric"]])
    es2 = events_stream[(events_stream.event_name.isin([consequent]))]

    es2['metric'] = es2['parameter2'].apply(
        lambda x: json.loads(x).get(consequent_parameter))
    print(f"{consequent} Stream")
    print(es2[["event_name", "metric"]])

    if es1 is None or es1.shape[0] == 0 or es2 is None or es2.shape[0] == 0:
        return None

    # es2=events_stream[(events_stream.event_name.isin(['Sleep']))&(events_stream.duration<=15)] #sleep
    if time_interval_begin is not None and time_interval_end is not None:
        es1['interval_start'] = es1['start_time'] + \
            timedelta(seconds=time_interval_begin)
        es1['interval_end'] = es1['end_time'] + \
            timedelta(seconds=time_interval_end)
    else:
        es1['interval_start'] = es1['start_time']
        es1['interval_end'] = es1['end_time']

    try:
        es1['parameter2'] = es1['parameter2'].apply(lambda x: json.dumps(x))
        es2['parameter2'] = es2['parameter2'].apply(lambda x: json.dumps(x))

    except KeyError:
        pass

    # convert datetime to datetime string for sqlite
    for f in ['start_time', 'end_time', 'timestamp', 'interval_start', 'interval_end']:
        if f in es1.columns:
            es1[f] = es1[f].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S %z"))
        if f in es2.columns:
            es2[f] = es2[f].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S %z"))

    # convert the dfs to in-memory sqlite tables, join the tables, then read as df
    conn = sq3.connect(':memory:')
    # #write the tables
    try:
        es1.to_sql('antecedent', conn, index=False)
        es2.to_sql('consequent', conn, index=False)
    except InterfaceError as e:
        print("Eventstream 1")
        print(es1.head())
        print(es1.dtypes)

        print("Eventstream 2")
        print(es2.head())
        print(es2.dtypes)
        raise e

    # Matching query to match event streams

    # es1=steps(datastream), es2=sleep
    es1 = 'antecedent'
    es2 = 'consequent'

    qry = f"""
        select
            {es1}.user_ID,
            {es1}.event_name antecedent_name,
            {es1}.start_time antecedent_start_time,
            {es1}.end_time antecedent_end_time,
            {es1}.metric antecedent_metric,
            {es2}.event_name consequent_name,
            {es2}.start_time consequent_start_time,
            {es2}.end_time consequent_end_time,
            {es2}.metric as consequent_metric,
            {es2}.parameter2 {es2}_parameter2
        from
            {es2} left join {es1} on
            (
            (
            {es1}.user_id={es2}.user_id
            )
            and

            (
            ({es2}.start_time between {es1}.interval_start and {es1}.interval_end)

            or

            ({es2}.end_time between {es1}.interval_start and {es1}.interval_end)
            )
            )



        """
    logging.info("query = {}".format(qry))
    sleep_event_matched_data = pd.read_sql_query(qry, conn)

    # Missing value imputation of event data

    for col in ['antecedent_name', 'antecedent_start_time', 'antecedent_end_time', 'consequent_name', 'consequent_start_time', 'consequent_end_time']:
        sleep_event_matched_data[col].fillna(0, inplace=True)

    for f in ['antecedent_start_time', 'antecedent_end_time', 'consequent_start_time', 'consequent_end_time']:
        sleep_event_matched_data[f] = pd.to_datetime(
            sleep_event_matched_data[f], infer_datetime_format=True)

    # Computing activity duration on the matched event data

    sleep_event_matched_data['antecedent_duration'] = sleep_event_matched_data['antecedent_end_time'] - \
        sleep_event_matched_data['antecedent_start_time']
    sleep_event_matched_data['antecedent_duration'] = sleep_event_matched_data['antecedent_duration'] / \
        np.timedelta64(1, 'm')

    sleep_event_matched_data['consequent_duration'] = sleep_event_matched_data['consequent_end_time'] - \
        sleep_event_matched_data['consequent_start_time']
    sleep_event_matched_data['consequent_duration'] = sleep_event_matched_data['consequent_duration'] / \
        np.timedelta64(1, 'h')

    sleep_event_matched_data = sleep_event_matched_data[(
        sleep_event_matched_data.antecedent_name != 0)].copy()

    logging.info("matched data")
    print(sleep_event_matched_data)
    # Return none if no data is matched
    if sleep_event_matched_data is None or sleep_event_matched_data.shape[0] == 0:
        return None

    # Use the provided aggregation function
    if aggregation_function == "SUM":
        agg_func = np.sum
    else:
        agg_func = np.mean

    # Use the provided anchor
    if anchor == "CONSEQUENT":
        pivot_sleep = sleep_event_matched_data.pivot_table(index=['user_id', 'consequent_metric'], columns=[
            'antecedent_name'], values=['antecedent_metric'], aggfunc=np.sum).fillna(0).reset_index()
        # COlumn name modification
        new_cols = [('{1} {0}'.format(*tup)) for tup in pivot_sleep.columns]

        # assign it to the dataframe (assuming you named it pivoted
        pivot_sleep.columns = new_cols

        pivot_sleep.columns = pivot_sleep.columns.str.strip()

        pivot_sleep.columns = pivot_sleep.columns. str. replace(
            ' ', '').str. replace('antecedent_metric', '')

        pivot_sleep.columns = map(str.lower, pivot_sleep.columns)

        pivot_sleep = pd.melt(pivot_sleep, id_vars=[
            'user_id', 'consequent_metric']).copy()

        pivot_sleep.rename(
            columns={'variable': 'antecedent_name', 'value': 'antecedent_metric'}, inplace=True)
    else:
        pivot_sleep = sleep_event_matched_data.pivot_table(index=['user_id', 'antecedent_metric'], columns=[
            'consequent_name'], values=['consequent_metric'], aggfunc=np.sum).fillna(0).reset_index()
        # COlumn name modification
        new_cols = [('{1} {0}'.format(*tup)) for tup in pivot_sleep.columns]

        # assign it to the dataframe (assuming you named it pivoted
        pivot_sleep.columns = new_cols

        pivot_sleep.columns = pivot_sleep.columns.str.strip()

        pivot_sleep.columns = pivot_sleep.columns. str. replace(
            ' ', '').str. replace('consequent_metric', '')

        pivot_sleep.columns = map(str.lower, pivot_sleep.columns)

        pivot_sleep = pd.melt(pivot_sleep, id_vars=[
            'user_id', 'antecedent_metric']).copy()

        pivot_sleep.rename(
            columns={'variable': 'consequent_name', 'value': 'consequent_metric'}, inplace=True)

    print("pivoted data")
    print(pivot_sleep)

    # Generating scatterplot data to be sent to the application
    scatterplot_insights = pivot_sleep.copy()

    dic = {}

    for user_id in scatterplot_insights.user_id.unique():
        user_df = scatterplot_insights[scatterplot_insights['user_id'] == user_id]
        l = []
        for i in range(user_df.shape[0]):
            row = user_df.iloc[i, :]
            # print(row)
            l.append([row['antecedent_metric'], row['consequent_metric']])

        dic[user_id] = {'XAxis': {'Measure': antecedent, 'unit': "Minute"}, 'YAxis': {
            'Measure': consequent,  # "Sleep",
            'unit': "hours"
        }, 'data': l}

    scatterplot_data = pd.DataFrame(dic.items())
    scatterplot_data.rename(
        columns={0: 'user_id', 1: 'correlation_result'}, inplace=True)

    # Will be automated based on the analysis
    scatterplot_data['unique_analysis_id'] = "temp"
    scatterplot_data['timestamp_added'] = strftime(
        "%Y-%m-%d %H:%M:%S", gmtime())
    scatterplot_data['viewed'] = False

    # this data can be added to the analysis results table
    return scatterplot_data
