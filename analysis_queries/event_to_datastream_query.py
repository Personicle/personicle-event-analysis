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

from database.postgres import engine
from datetime import datetime, timedelta


def run_e2d_analysis(time_interval_begin, time_interval_end, antecedent_event, consequent_name, consequent_datastream_tablename, effect_activity, start_analysis, end_analysis,  user_id, anchor="CONSEQUENT", aggregation_function="SUM", chunk_days=30):
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
        result = e2d_scatterplot(time_interval_begin, time_interval_end, antecedent_event,
                                 consequent_name, consequent_datastream_tablename,
                                 consequent_name, start_analysis, end_analysis, user_id, anchor=anchor, aggregation_function=aggregation_function)
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


def e2d_scatterplot(time_interval_begin, time_interval_end, antecedent_event, consequent_name, consequent_datastream_tablename, effect_activity, query_start, query_end,  user_id, anchor="CONSEQUENT", aggregation_function="SUM"):
    """
    time_interval_begin: beginning of matching ime interval
    time_interval_end: end of matching time interval
    antecedent_event: Antecedent event name
    consequent_name: Consequent stream name
    consequent_datastream_tablename: Consequent stream table name
    effect_activity: Consequent stream name
    query_start: Starting timestamp for getting data stream
    query_end: End timestamp for getting data stream
    """
    # Reading the query from te events table
    query_events = "select * from  personal_events where event_name='{}' and user_id='{}'".format(
        antecedent_event, user_id)
    events_stream = sqlio.read_sql_query(query_events, engine)

    # print(events_stream)
    data_stream = datastream(
        consequent_datastream_tablename, user_id, query_start, query_end)
    if data_stream is None:
        return None

    data_stream = timestamp_modify(data_stream).copy()
    data_stream['event_name'] = consequent_name
    events_stream = events_overlap(events_stream)

    events_stream['duration'] = (
        events_stream['end_time'] - events_stream['start_time']) / pd.Timedelta(hours=1)
    events_stream['end_date'] = pd.to_datetime(
        events_stream['end_time']).dt.date
    events_stream['end_date'] = events_stream['end_date'].astype(str)

    # Getting list of dates that have high intense activities
    # to be removed to check Step count effect

    # # Parameterize these as well for activity exclusion and should be based on event stream
    # dates_tobe_excluded = events_stream[events_stream.event_name.isin(
    #     ['Running', 'Biking', 'Strength training', 'Training', 'FunctionalStrengthTraining', 'Rowing', 'activity', 'Afternoon Ride', 'Cycling', 'Evening Run', 'Afternoon Run'])]

    # events_stream = events_stream[~events_stream.end_date.isin(
    #     dates_tobe_excluded.end_date.unique())].copy()

    # Matching events with data streams
    es2 = data_stream.copy()
    # es2=events_stream[(events_stream.event_name.isin(['Sleep']))&(events_stream.duration<=15)] #sleep
    es1 = events_stream
    # events_stream[(events_stream.event_name.isin(
    #     [effect_activity]))]  # sleep

    if time_interval_begin is not None and time_interval_end is not None:
        es1['interval_start'] = es1['start_time'] + \
            timedelta(seconds=time_interval_begin)
        es1['interval_end'] = es1['end_time'] + \
            timedelta(seconds=time_interval_end)
    else:
        es1['interval_start'] = es1['start_time']
        es1['interval_end'] = es1['end_time']
    # es1['event_name'] = es1['unit'].map(eventname_unit)

    try:
        # es1['parameter2'] = es1['parameter2'].apply(lambda x: json.dumps(x))
        es1['parameter2'] = es1['parameter2'].apply(lambda x: json.dumps(x))

    except KeyError:
        print("Parameter field not found")
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

    # Matching query to match data and event streams
    # {es2}.parameter2 {es2}_parameter2
    es1 = 'antecedent'
    es2 = 'consequent'

    qry = f"""
        select  
            {es1}.user_ID,
            {es1}.event_name activity_name,
            {es1}.start_time activity_start_time,
            {es1}.end_time activity_end_time,
             {es2}.value count,
            {es2}.event_name effect_event_name,
            {es2}.start_time effect_start_time,
            {es2}.end_time effect_end_time
            
        from
            {es1} join {es2} on
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

    sleep_event_matched_data = pd.read_sql_query(qry, conn)

    # Converting to datetime format

    for col in ['activity_name', 'activity_start_time', 'activity_end_time', 'effect_start_time', 'effect_end_time']:
        sleep_event_matched_data[col].fillna(0, inplace=True)

    for f in ['activity_start_time', 'activity_end_time', 'effect_start_time', 'effect_end_time']:
        sleep_event_matched_data[f] = pd.to_datetime(
            sleep_event_matched_data[f], infer_datetime_format=True)

    # Computing activity duration of matched events

    sleep_event_matched_data['activity_duration'] = sleep_event_matched_data['activity_end_time'] - \
        sleep_event_matched_data['activity_start_time']
    sleep_event_matched_data['activity_duration'] = sleep_event_matched_data['activity_duration'] / \
        np.timedelta64(1, 'm')

    sleep_event_matched_data['effect_duration'] = sleep_event_matched_data['effect_end_time'] - \
        sleep_event_matched_data['effect_start_time']
    sleep_event_matched_data['effect_duration'] = sleep_event_matched_data['effect_duration'] / \
        np.timedelta64(1, 'm')

    sleep_event_matched_data = sleep_event_matched_data[(
        sleep_event_matched_data.activity_name != 0)].copy()

    # Computing whether matched events should be summed up or averaged based on their nature
    print("Matched data columns")
    print(sleep_event_matched_data.columns)
    if aggregation_function == "SUM":
        pivot_sleep = sleep_event_matched_data.pivot_table(index=['user_id', 'activity_start_time', 'activity_end_time', 'activity_duration'], columns=[
                                                           'activity_name'], values=['count'], aggfunc=np.sum).fillna(0).reset_index()
    else:
        pivot_sleep = sleep_event_matched_data.pivot_table(index=['user_id', 'activity_start_time', 'activity_end_time', 'activity_duration'], columns=[
                                                           'activity_name'], values=['count'], aggfunc=np.mean).fillna(0).reset_index()

    # Column name modification
    print("Pivoted data")
    print(pivot_sleep)
    new_cols = [('{1} {0}'.format(*tup)) for tup in pivot_sleep.columns]
    new_cols = ["user_id", "antecedent_start_time",
                "antecedent_end_time", "antecedent_duration", "consequent_value"]

    # assign it to the dataframe (assuming you named it pivoted
    pivot_sleep.columns = new_cols
    print(pivot_sleep.columns)
    # pivot_sleep.columns = pivot_sleep.columns.str.strip()
    # pivot_sleep.columns = pivot_sleep.columns. str. replace(
    #     ' ', '').str. replace('activity_duration', '')
    # pivot_sleep.rename(columns={
    #                    pivot_sleep.columns[-1]: "activity_value", pivot_sleep.columns[-2]: "activity_duration"}, inplace=True)

    pivot_sleep['antecedent_name'] = sleep_event_matched_data.activity_name.unique()[
        0]
    pivot_sleep['antecedent_units'] = "Minutes"
    # list(eventname_unit.keys())[list(eventname_unit.values()).index(sleep_event_matched_data.activity_name.unique()[0])]

    pivot_sleep.columns = map(str.lower, pivot_sleep.columns)

    # Generating scatterplot data that needs to be sent to the application

    scatterplot_insights = pivot_sleep.copy()
    dic = {}

    for user_id in scatterplot_insights.user_id.unique():
        user_df = scatterplot_insights[scatterplot_insights['user_id'] == user_id]
        l = []
        for i in range(user_df.shape[0]):
            row = user_df.iloc[i, :]
            # print(row)
            l.append([row['antecedent_duration'], row['consequent_value']])

        dic[user_id] = {'XAxis': {'Measure': antecedent_event, 'unit': "Total Minutes"}, 'YAxis': {
            'Measure': consequent_name,  # 'Sleep'
            'unit': "Total {}".format(consequent_name) if aggregation_function == "SUM" else "Average {}".format(consequent_name)
        }, 'data': l}

    scatterplot_data = pd.DataFrame(dic.items())
    scatterplot_data.rename(
        columns={0: 'user_id', 1: 'correlation_result'}, inplace=True)

    # Assigning analysis id by activity type
    scatterplot_data['timestampadded'] = strftime(
        "%Y-%m-%d %H:%M:%S", gmtime())
    scatterplot_data['view'] = 'No'

    return scatterplot_data
