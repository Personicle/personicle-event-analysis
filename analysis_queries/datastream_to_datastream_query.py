# -*- coding: utf-8 -*-
"""
Created on Wed Nov  2 15:05:53 2022

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
from .utils.datastream_aggregation import aggregate_datastream, generate_query

# Defining the antecedent and consequent parameters


def __aggregate_matched_data(matched_data, anchor="ANTECEDENT", aggregation_function="SUM"):
    assert anchor in ["ANTECEDENT", "CONSEQUENT"], "INVALID ANCHOR PROVIDED"
    if matched_data is None or matched_data.shape[0] == 0:
        return None

    if anchor == "ANTECEDENT":
        grouped_df = matched_data.groupby(["user_id", "antecedent_name", "consequent_name", "antecedent_start_time",
                                           "antecedent_end_time", "antecedent_value"], as_index=False)
        value_column = "consequent_value"
        matched_start_time = "consequent_start_time"
        matched_end_time = "consequent_end_time"
    else:
        grouped_df = matched_data.groupby(["user_id", "antecedent_name", "consequent_name", "consequent_start_time",
                                           "consequent_end_time", "consequent_value"], as_index=False)
        value_column = "antecedent_value"
        matched_start_time = "antecedent_start_time"
        matched_end_time = "antecedent_end_time"

    if aggregation_function == "SUM":
        aggregated_df = grouped_df.agg(
            {value_column: "sum", matched_start_time: "min", matched_end_time: "max"})
    elif aggregation_function == "MEAN":
        aggregated_df = grouped_df.agg(
            {value_column: "mean", matched_start_time: "min", matched_end_time: "max"})
    else:
        return None

    return aggregated_df


def run_d2d_analysis(time_interval_begin, time_interval_end, consequent_name, consequent_datastream_tablename, antecedent_name, antecedent_datastream_tablename, effect_activity,
                     start_analysis, end_analysis,  user_id,
                     anchor="ANTECEDENT", aggregation_function="SUM", chunk_days=30, antecedent_aggregation_function="SUM", consequent_aggregation_function="SUM", antecedent_aggregation_interval="DAILY", consequent_aggregation_interval="DAILY"):
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
    antecedent_aggregation_function="SUM"
    consequent_aggregation_function="SUM"
    antecedent_aggregation_interval = "DAILY"
    consequent_aggregation_interval = "DAILY"
    """
    analysis_result = None
    cur_window_begin = start_analysis
    cur_window_end = cur_window_begin + timedelta(days=chunk_days)
    while True:
        # In the loop, iterate over data streams in 1 week chunks and match each chunk with events
        # the matched results need to be accumulated and added to the database
        result = d2d_scatterplot(time_interval_begin, time_interval_end, consequent_name, consequent_datastream_tablename,
                                 antecedent_name, antecedent_datastream_tablename,
                                 antecedent_name, cur_window_begin, cur_window_end, user_id, anchor=anchor, query_aggregation_function=aggregation_function,
                                 antecedent_aggregation_function=antecedent_aggregation_function, consequent_aggregation_function=consequent_aggregation_function,
                                 antecedent_aggregation_window=antecedent_aggregation_interval, consequent_aggregation_window=consequent_aggregation_interval)
        # print(result)
        if result is not None and result.shape[0] > 0:
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


def d2d_scatterplot(time_interval_begin, time_interval_end, consequent_name, consequent_datastream_tablename, antecedent_name, antecedent_datastream_tablename, effect_activity, query_start, query_end,  user_id, anchor="CONSEQUENT", query_aggregation_function="SUM",
                    antecedent_aggregation_function="SUM", antecedent_aggregation_window="DAILY", consequent_aggregation_function="SUM", consequent_aggregation_window="DAILY"):
    """
    time_interval_begin: beginning of matching ime interval
    time_interval_end: end of matching time interval
    consequent_name: Consequent stream name
    consequent_datastream_tablename: Consequent stream table name
    antecedent_name: Antecedent stream name
    antecedent_datastream_tablename: Antecedent stream table name
    effect_activity: Consequent stream name (Deprecated)
    query_start: Starting timestamp for getting data stream
    query_end: End timestamp for getting data stream
    anchor: Whether antecedent or consequent is the anchor for the query
    query_aggregation_function: aggregation function to be used for aggregating matched data; Supports SUM (default), MEAN
    antecedent_aggregation_function: aggregation function to find aggregate values for antecedent and create matching windows if antecedent is the anchor "SUM" (default), "MEAN" 
    antecedent_aggregation_window: aggregation window to find aggregate values for antecedent and create matching windows if antecedent is the anchor "DAILY" (default), "WEEKLY", "HOURLY"
    consequent_aggregation_function: aggregation function to find aggregate values for consequent and create matching windows if consequent is the anchor "SUM"(default), "MEAN" 
    consequent_aggregation_window: aggregation window to find aggregate values for consequent and create matching windows if consequent is the anchor "DAILY" (default), "WEEKLY", "HOURLY"
    """

    antecedent_datastream = data_stream = datastream(
        antecedent_datastream_tablename, user_id, query_start, query_end).copy()
    if antecedent_datastream is None or antecedent_datastream.shape[0] == 0:
        return None

    consequent_query_start = query_start + \
        timedelta(seconds=time_interval_begin)
    consequent_query_end = query_end + timedelta(seconds=time_interval_end)

    consequent_datastream = datastream(
        consequent_datastream_tablename, user_id, consequent_query_start, consequent_query_end).copy()
    if consequent_datastream is None or consequent_datastream.shape[0] == 0:
        return None

    antecedent_datastream = timestamp_modify(antecedent_datastream).copy()

    consequent_datastream = timestamp_modify(consequent_datastream).copy()

    # consequent_datastream['end_date'] = consequent_datastream['end_time'].dt.date

    # antecedent_datastream['end_date'] = antecedent_datastream['end_time'].dt.date

    # Defining whether activities should be summed or averaged based upon their nature

    if anchor == "ANTECEDENT":
        antecedent_datastream = aggregate_datastream(
            antecedent_datastream, antecedent_aggregation_window, antecedent_aggregation_function)
    elif anchor == 'CONSEQUENT':
        consequent_datastream = aggregate_datastream(
            consequent_datastream, consequent_aggregation_window, consequent_aggregation_function)
    else:
        return

    es1 = antecedent_datastream.copy()
    es2 = consequent_datastream.copy()

    print("Antecedent")
    print(antecedent_datastream)

    print("Consequent")
    print(consequent_datastream)

    if anchor == "ANTECEDENT":
        es1['interval_start'] = es1['start_time'] + \
            timedelta(seconds=time_interval_begin)
        es1['interval_end'] = es1['end_time'] + \
            timedelta(seconds=time_interval_end)
    elif anchor == "CONSEQUENT":
        es2['interval_end'] = es2['end_time'] - \
            timedelta(seconds=time_interval_begin)
        es2['interval_start'] = es2['start_time'] - \
            timedelta(seconds=time_interval_end)
    else:
        print("INVALID ANCHOR VALUE: {}".format(anchor))
        return None
    es1['event_name'] = antecedent_name
    es2['event_name'] = consequent_name

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

    qry = generate_query(anchor)

    print("Data matching query")
    print(qry)

    matched_data = pd.read_sql_query(qry, conn)
    print("Matched Data")
    print(matched_data)

    matched_data = __aggregate_matched_data(
        matched_data, anchor=anchor, aggregation_function=query_aggregation_function)

    # MOdify the column names to match the refactored query
    for col in ['antecedent_name', 'antecedent_start_time', 'antecedent_end_time', 'antecedent_value', 'consequent_name', 'consequent_end_time',
                'consequent_start_time', 'consequent_value']:
        matched_data[col].fillna(0, inplace=True)

    for f in ['antecedent_start_time', 'antecedent_end_time', 'consequent_end_time']:
        matched_data[f] = pd.to_datetime(
            matched_data[f], infer_datetime_format=True)

    # Generating scatterplot data

    scatterplot_insights = matched_data[(matched_data.antecedent_name != 0)][[
        'user_id', 'antecedent_value', 'consequent_value', 'antecedent_name', 'consequent_name']].copy()

    scatterplot_insights['antecedent_units'] = antecedent_datastream.unit[~pd.isna(
        antecedent_datastream.unit)].unique()[0]
    scatterplot_insights['consequent_units'] = consequent_datastream.unit[~pd.isna(
        consequent_datastream.unit)].unique()[0]

    dic = {}

    for user_id in scatterplot_insights.user_id.unique():
        user_df = scatterplot_insights[scatterplot_insights['user_id'] == user_id]
        l = []
        for i in range(user_df.shape[0]):
            row = user_df.iloc[i, :]
            # print(row)
            l.append([row['antecedent_value'], row['consequent_value']])

        dic[user_id] = {'XAxis': {'Measure': scatterplot_insights.antecedent_name.unique()[0], 'unit': scatterplot_insights['antecedent_units'].unique()[0]}, 'YAxis': {
            'Measure': scatterplot_insights.consequent_name.unique()[0],
            'unit': scatterplot_insights['consequent_units'].unique()[0]
        }, 'data': l}

    scatterplot_data = pd.DataFrame(dic.items())
    scatterplot_data.rename(
        columns={0: 'user_id', 1: 'correlation_result'}, inplace=True)

    # assigning Analysis id based on activity type
    # scatterplot_data['analysis_id'] = scatterplot_insights['cause_name'].map(
    #     activity_analysisid)   # Will be automated based on the analysis
    scatterplot_data['timestampadded'] = strftime(
        "%Y-%m-%d %H:%M:%S", gmtime())
    scatterplot_data['view'] = 'No'

    return scatterplot_data
