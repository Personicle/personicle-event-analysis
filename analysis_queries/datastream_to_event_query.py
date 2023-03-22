# -*- coding: utf-8 -*-
"""
Created on Mon Oct  3 12:19:18 2022

@author: nbadam
"""

import numpy as np
import pandas as pd
# from base_schema import *
# from db_connection import *
from sqlalchemy import select
import pandas.io.sql as sqlio
import json

import sqlite3 as sq3
from sqlite3 import InterfaceError

from .personicle_functions import *
from .utility_functions_sleepanalysis import *


def run_d2e_analysis(time_interval_begin, time_interval_end, consequent_event, antecedent_name, antecedent_datastream_tablename, effect_activity, start_analysis, end_analysis,  user_id, anchor="CONSEQUENT", aggregation_function="SUM", chunk_days=30):
    """
    time_interval_begin: beginning of matching ime interval
    time_interval_end: end of matching time interval
    antecedent_event: Antecedent event name
    consequent_name: Consequent stream name
    consequent_datastream_tablename: Consequent stream table name
    effect_activity: Consequent stream name
    start_analysis: Starting timestamp for getting data stream
    end_analysis: End timestamp for getting data stream
    chunk_days: Number of days used to query the chunks of data stream
    """
    analysis_result = None
    cur_window_begin = start_analysis
    cur_window_end = cur_window_begin + timedelta(days=chunk_days)
    while True:
        # In the loop, iterate over data streams in 1 week chunks and match each chunk with events
        # the matched results need to be accumulated and added to the database
        result = d2e_scatterplot(time_interval_begin, time_interval_end, consequent_event,
                                 antecedent_name, antecedent_datastream_tablename,
                                 antecedent_name, start_analysis, end_analysis, user_id, anchor=anchor, aggregation_function=aggregation_function)
        # print(result)
        if analysis_result:
            analysis_result['data'].extend(
                result['correlation_result'].iloc[0]['data'])
        else:
            analysis_result = result['correlation_result'].iloc[0]
        cur_window_begin = cur_window_end + timedelta(days=1)
        cur_window_end = cur_window_begin + timedelta(days=chunk_days)
        if cur_window_begin > end_analysis:
            result['correlation_result'].iloc[0] = analysis_result
            break
    return result


def d2e_scatterplot(time_interval_begin, time_interval_end, consequent_event, antecedent_name, antecedent_datastream_tablename, effect_activity, query_start, query_end,  user_id, anchor="CONSEQUENT", aggregation_function="SUM"):
    """
    time_interval_begin: beginning of matching ime interval
    time_interval_end: end of matching time interval
    consequent_event: Consequent event name
    antecedent_name: Antecedent stream name
    antecedent_datastream_tablename: Antecedent stream table name
    effect_activity: Consequent stream name (Deprecated)
    query_start: Starting timestamp for getting data stream
    query_end: End timestamp for getting data stream
    anchor: Whether antecedent or consequent is the anchor for the query
    aggregation_function: aggregation function to be used for aggregating matched data; Supports SUM (default), MEAN
    """
    # READ EVENTS DATA
    query_events = "select * from  personal_events where event_name='{}' and user_id='{}'".format(
        consequent_event, user_id)
    events_stream = sqlio.read_sql_query(query_events, engine)

    # EVENTS PRE-PROCESSING; NEED TO REFACTOR
    events_stream = events_overlap(events_stream)

    if events_stream is None or events_stream.shape[0] == 0:
        return None

    events_stream['duration'] = (
        events_stream['end_time'] - events_stream['start_time']) / pd.Timedelta(hours=1)
    events_stream['end_date'] = pd.to_datetime(
        events_stream['end_time']).dt.date
    events_stream['end_date'] = events_stream['end_date'].astype(str)

    # READ DATASTREAM
    data_stream = datastream(
        antecedent_datastream_tablename, user_id, query_start, query_end).copy()
    if data_stream is None or data_stream.shape[0] == 0:
        return None

    # CONVERT DATA STREAM TO INTERVAL FORMAT; NEED TO ADD DATA STREAM SYNCHRONIZATION FOR SOLVING THE DOUBLE COUNTING PROBLEM WITH DATA STREAMS
    data_stream = timestamp_modify(data_stream).copy()

    # AGGREGATE DATA SRTEAM TO REQUIRED INTERVAL; DEFINED BY aggregation_window; THIS SHOULD BE A PARAMETER SET BY THE USER
    data_stream = datastream_aggregate(
        data_stream, aggregation_window='DAILY', agg_func=aggregation_function)
    data_stream['event_name'] = antecedent_name

    # Matching events with sleep data; ANTECEDENT (ES1) IS A DATA STREAM AND CONSEQUENT (ES2) IS AN EVENT STREAM
    es1 = data_stream.copy()
    es2 = events_stream

    # SETTING UP THE MATCHING INTERVAL
    if time_interval_begin is not None and time_interval_end is not None:
        es1['interval_start'] = es1['start_time'] + \
            timedelta(hours=time_interval_begin)
        es1['interval_end'] = es1['end_time'] + \
            timedelta(hours=time_interval_end)
    else:
        es1['interval_start'] = es1['start_time']
        es1['interval_end'] = es1['end_time']
    # es2['event_name']=es1['unit'].map(eventname_unit)

    try:
        es1['parameter2'] = es1['parameter2'].apply(lambda x: json.dumps(x))
        es2['parameter2'] = es2['parameter2'].apply(lambda x: json.dumps(x))

    except KeyError:
        pass

    # convert datetime to datetime string for sqlite
    for f in ['start_time', 'end_time', 'timestamp', 'interval_start', 'interval_end']:
        print(f)
        if f in es1.columns:
            es1[f] = es1[f].apply(lambda x: None if pd.isna(
                x) else x.strftime("%Y-%m-%d %H:%M:%S %z"))
        if f in es2.columns:
            es2[f] = es2[f].apply(lambda x: None if pd.isna(
                x) else x.strftime("%Y-%m-%d %H:%M:%S %z"))

    # convert the dfs to in-memory sqlite tables, join the tables, then read as df

    conn = sq3.connect(':memory:')

    # print(es1)
    # print(es2)
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

    # es1=steps(datastream), es2=sleep
    es1 = 'antecedent'
    es2 = 'consequent'

    qry = f"""
        select  
            {es1}.user_ID,
            {es1}.event_name antecedent_name,
            {es1}.start_time antecedent_start_time,
            {es1}.end_time antecedent_end_time,
             {es1}.value antecedent_value,
            {es2}.event_name consequent_name,
            {es2}.start_time consequent_start_time,
            {es2}.end_time consequent_end_time,
            {es2}.parameter2 {es2}_parameter2
            
            
    
        from
            {es2}  join {es1} on
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

    d2e_matched_data = pd.read_sql_query(qry, conn)
    # print(d2e_matched_data)

    for col in ['antecedent_name', 'antecedent_start_time', 'antecedent_end_time', 'consequent_start_time', 'consequent_end_time']:
        d2e_matched_data[col].fillna(0, inplace=True)

    for f in ['antecedent_start_time', 'antecedent_end_time', 'consequent_start_time', 'consequent_end_time']:
        d2e_matched_data[f] = pd.to_datetime(
            d2e_matched_data[f], infer_datetime_format=True)

    d2e_matched_data['antecedent_duration'] = d2e_matched_data['antecedent_end_time'] - \
        d2e_matched_data['antecedent_start_time']
    d2e_matched_data['antecedent_duration'] = d2e_matched_data['antecedent_duration'] / \
        np.timedelta64(1, 'm')

    d2e_matched_data['consequent_duration'] = d2e_matched_data['consequent_end_time'] - \
        d2e_matched_data['consequent_start_time']
    d2e_matched_data['consequent_duration'] = d2e_matched_data['consequent_duration'] / \
        np.timedelta64(1, 'm')

    d2e_matched_data = d2e_matched_data[(
        d2e_matched_data.antecedent_name != 0)].copy()

    # if d2e_matched_data.antecedent_name.unique()=='steps': #will change to list soon

    if d2e_matched_data.antecedent_name.unique() in (activity_cumulative):
        pivot_matched_data = d2e_matched_data.pivot_table(index=['user_id', 'consequent_duration'], columns=[
            'antecedent_name'], values=['antecedent_duration', 'antecedent_value'], aggfunc=np.sum).fillna(0).reset_index()

    else:
        pivot_matched_data = d2e_matched_data.pivot_table(index=['user_id', 'consequent_duration'], columns=[
            'antecedent_name'], values=['antecedent_duration', 'antecedent_value'], aggfunc=np.mean).fillna(0).reset_index()

    new_cols = [('{1} {0}'.format(*tup)) for tup in pivot_matched_data.columns]

    # assign it to the dataframe (assuming you named it pivoted
    pivot_matched_data.columns = new_cols

    # resort the index, so you get the columns in the order you specified
    # pivot_matched_data.sort_index(axis='columns').head()

    pivot_matched_data.columns = pivot_matched_data.columns.str.strip()

    pivot_matched_data.columns = pivot_matched_data.columns. str. replace(
        ' ', '').str. replace('antecedent_duration', '')

    pivot_matched_data.rename(columns={
        pivot_matched_data.columns[-1]: "antecedent_value", pivot_matched_data.columns[-2]: "antecedent_duration"}, inplace=True)

    pivot_matched_data['antecedent_name'] = d2e_matched_data.antecedent_name.unique()[
        0]
    pivot_matched_data['activity_units'] = "antecedent_unit"

    pivot_matched_data.columns = map(str.lower, pivot_matched_data.columns)

    # Generating Scatterplot data to be sent to the application
    scatterplot_insights = pivot_matched_data.copy()

    dic = {}

    for user_id in scatterplot_insights.user_id.unique():
        user_df = scatterplot_insights[scatterplot_insights['user_id'] == user_id]
        l = []
        for i in range(user_df.shape[0]):
            row = user_df.iloc[i, :]
            # print(row)
            l.append([row['antecedent_value'], row['consequent_duration']])

        dic[user_id] = {'XAxis': {'Measure': antecedent_name, 'unit': data_stream.unit.iloc[0]}, 'YAxis': {
            'Measure': consequent_event,
            'unit': "Minutes"
        }, 'data': l}

    scatterplot_data = pd.DataFrame(dic.items())
    scatterplot_data.rename(
        columns={0: 'user_id', 1: 'correlation_result'}, inplace=True)

    scatterplot_data['timestampadded'] = strftime(
        "%Y-%m-%d %H:%M:%S", gmtime())
    scatterplot_data['view'] = 'No'

    return scatterplot_data
