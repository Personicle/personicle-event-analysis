import pandas as pd
from datetime import timedelta, datetime

AGGREGATION_INTERVALS = ["DAILY", "HOURLY", "WEEKLY"]


def generate_query(anchor):
    """
    Generate query to match the 2 datasets
    the generated dataset will have the following fields:
    userId
    antecedent_name,
    antecedent_start_time,
    antecedent_end_time,
    antecedent_value,
    consequent_name,
    consequent_start_time,
    consequent_end_time,
    consequent_value
    """
    es1 = 'antecedent'
    es2 = 'consequent'

    if anchor == "ANTECEDENT":
        # Matching query to match overlapping data streams
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
                {es2}.value consequent_value,
                {es1}.interval_start interval_start,
                {es1}.interval_end interval_end
            from
                {es1} left join {es2} on
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
    elif anchor == "CONSEQUENT":
        qry = f"""
            select  
                {es2}.user_ID,
                {es1}.event_name antecedent_name,
                {es1}.start_time antecedent_start_time,
                {es1}.end_time antecedent_end_time,
                {es1}.value antecedent_value,
                {es2}.event_name consequent_name,
                {es2}.start_time consequent_start_time,
                {es2}.end_time consequent_end_time,
                {es2}.value consequent_value,
                {es2}.interval_start interval_start,
                {es2}.interval_end interval_end
            from
                {es2} left join {es1} on
                (
                (
                {es1}.user_id={es2}.user_id
                )
                and
                
                (
                ({es1}.start_time between {es2}.interval_start and {es2}.interval_end)
                
                or 
            
                ({es1}.end_time between {es2}.interval_start and {es2}.interval_end)
                )
                )
            """
    else:
        qry = None

    return qry


def __find_aggregation_interval(data_stream, aggregation_window):
    """
    Find respective aggregation intervals for each row in a data stream
    """
    assert aggregation_window in AGGREGATION_INTERVALS, "Invalid aggregation interval provided"

    if aggregation_window == "DAILY":
        aggregation_interval = timedelta(days=1)
        data_stream['interval_start'] = data_stream['start_time'].apply(
            lambda x: datetime.strptime(x.strftime("%Y-%m-%d 00:00:00"), "%Y-%m-%d %H:%M:%S"))

    elif aggregation_window == "WEEKLY":
        aggregation_interval = timedelta(days=7)
        data_stream['interval_start'] = data_stream['start_time'].apply(
            lambda x: x - timedelta(days=x.weekday()))

    elif aggregation_window == "HOURLY":
        aggregation_interval = timedelta(hours=1)
        data_stream['interval_start'] = data_stream['start_time'].apply(
            lambda x: datetime.strptime(x.strftime("%Y-%m-%d %H:00:00"), "%Y-%m-%d %H:%M:%S"))
        pass
    else:
        return None
    data_stream['interval_end'] = data_stream['interval_start'] + \
        aggregation_interval
    return data_stream


def aggregate_datastream(data_stream, aggregation_window, aggregation_function):
    """
    Helper method to aggregate ther anchor datastream using the provided aggregation window and aggregation function
    """
    if data_stream is None or data_stream.shape[0] == 0:
        return None

    interval_datastream = __find_aggregation_interval(
        data_stream, aggregation_window)

    if aggregation_function == "SUM":
        aggregated_dataset = interval_datastream.groupby(
            ['user_id', 'interval_start', 'interval_end', 'unit'], as_index=False).agg({"value": "sum"})
    elif aggregation_function == "MEAN":
        aggregated_dataset = interval_datastream.groupby(
            ['user_id', 'interval_start', 'interval_end', 'unit'], as_index=False).agg({"value": "mean"})
    else:
        return None

    aggregated_dataset = aggregated_dataset.rename(
        columns={"interval_start": "start_time", "interval_end": "end_time"})

    return aggregated_dataset
