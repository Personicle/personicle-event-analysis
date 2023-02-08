import logging
from datetime import datetime, timedelta
import pandas.io.sql as sqlio
import json

import azure.functions as func

from database.postgres import engine
from utils.schema_info import get_datastream_schema
from database.write_analysis_to_db import write_analysis_results
from analysis_queries.event_to_event_query import e2e_scatterplot

"""
Function for processing the analysis requests;
Analysis requests are read from the queue; this function is responsible for 
1. parsing the request
2. get the requested data for the analysis
3. run the analysis query
4. add the result to the corresponding output table

Assumptions:
1. Data type is numeric
2. Correct table and data type is provided in the request

Sample queue item:
{"unique_analysis_id": "9a1b5dac-fb70-42bb-b051-4d9fe3e08ff9", 
"user_id": "00u50rvywd8mGuJw75d7", 
"anchor": "CONSEQUENT", 
"aggregate_function": "SUM", 
"query_interval": [0, 1, "weeks"], 
"antecedent_name": "Running", 
"antecedent_table": "test", 
"antecedent_parameter": "duration", 
"antecedent_type": "EVENT", 
"antecedent_interval": null, 
"consequent_name": "weight", 
"consequent_table": "test", 
"consequent_parameter": null, 
"consequent_type": "DATASTREAM", 
"consequent_interval": "DAILY"}
"""

INTERVAL_MULTIPLIER = {
    'minutes': 60,
    'hours': 60*60,
    "days": 24*60*60,
    'weeks': 7*824*60*60
}


def main(msg: func.QueueMessage) -> None:
    logging.info('Python queue trigger function processed a queue item: %s',
                 msg.get_body().decode('utf-8'))

    analysis_request = json.loads(msg.get_body().decode('utf-8'))

    current_time = datetime.utcnow()
    end_time = current_time
    # find min and max times for both streams, identify the date ranges from these values
    if analysis_request['antecedent_table'] == "test":
        analysis_request['antecedent_table'] = "personal_events"

    if analysis_request['antecedent_table'] == "personal_events":
        antecedent_query = "select min(start_time) as first_time, max(end_time) as last_time from {} where event_name='{}'".format(
            analysis_request['antecedent_table'], analysis_request['antecedent_name'])
    elif "interval" in analysis_request['antecedent_table']:
        antecedent_query = "select min(start_time) as first_time, max(end_time) as last_time from {}".format(
            analysis_request['antecedent_table'])
    else:
        antecedent_query = "select min(timestamp) as first_time, max(timestamp) as last_time from {}".format(
            analysis_request['antecedent_table'])
    # logging.info(antecedent_query)
    antecedent_interval = sqlio.read_sql(antecedent_query, engine)
    # logging.info(f"query={antecedent_query}, result = {antecedent_interval}")

    if analysis_request['consequent_table'] == "test":
        analysis_request['consequent_table'] = "personal_events"

    if analysis_request['consequent_table'] == "personal_events":
        consequent_query = "select min(start_time) as first_time, max(end_time) as last_time from {} where event_name='{}'".format(
            analysis_request['consequent_table'], analysis_request['consequent_name'])
    elif "interval" in analysis_request['consequent_table']:
        consequent_query = "select min(start_time) as first_time, max(end_time) as last_time from {}".format(
            analysis_request['consequent_table'])
    else:
        consequent_query = "select min(timestamp) as first_time, max(timestamp) as last_time from {}".format(
            analysis_request['consequent_table'])
    # logging.info(antecedent_query)
    consequent_interval = sqlio.read_sql(consequent_query, engine)
    # logging.info(f"query={consequent_query}, result = {consequent_interval}")
    logging.info(
        f"antecedent query={consequent_query}, result = {consequent_interval} \n consequent query={antecedent_query}, result = {antecedent_interval}")

    # find interval for running the query
    if antecedent_interval['first_time'].iloc[0] is None or consequent_interval['first_time'].iloc[0] is None:
        logging.info("No data found. Exiting.")
        return
    start_analysis = min(
        antecedent_interval['first_time'].iloc[0], consequent_interval['first_time'].iloc[0])
    end_analysis = max(
        antecedent_interval['last_time'].iloc[0], consequent_interval['last_time'].iloc[0])

    logging.info(f"Analysis duration from {start_analysis} to {end_analysis}")
    # find matching interval
    matching_interval_begin = None if analysis_request['query_interval'][0] is None else analysis_request['query_interval'][0] * \
        INTERVAL_MULTIPLIER[analysis_request['query_interval'][2]]
    matching_interval_end = None if analysis_request['query_interval'][1] is None else analysis_request['query_interval'][1] * \
        INTERVAL_MULTIPLIER[analysis_request['query_interval'][2]]

    if analysis_request['antecedent_type'] == "EVENT" and analysis_request['consequent_type'] == "EVENT":
        logging.info("run e2e query")
        df = e2e_scatterplot(matching_interval_begin, matching_interval_end, analysis_request['antecedent_name'], analysis_request['antecedent_parameter'], analysis_request['consequent_name'],
                             analysis_request['consequent_parameter'], analysis_request['user_id'], anchor=analysis_request['anchor'], aggregation_function=analysis_request['aggregate_function'])
        # print(df['correlation_result'].iloc[0])
        if df is None or df.shape[0] == 0:
            logging.info("No results found for query id {}".format(
                analysis_request['unique_analysis_id']))
        else:
            write_analysis_results(analysis_request['user_id'],
                                   df['correlation_result'].iloc[0], analysis_request['unique_analysis_id'])
    else:
        while True:
            break
    return
