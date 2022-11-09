import datetime
import logging
from typing import List
import json

import azure.functions as func

from utils.analysis_requests import get_analysis_requests

"""
Timer trigger module for event analysis queries in personicle
This module should connect to personicle database and read the analysis table
if the frequency of a record in the table matches the frequency of the timer trigger then
A request should be generated for that analysis -> (ie request sent to the analysis requests queue)
"""


def main(mytimer: func.TimerRequest, analysisRequest: func.Out[List[str]]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    analysis_requests = get_analysis_requests()
    generated_requests = []
    for i in analysis_requests:
        logging.info(i)
        generated_requests.append(json.dumps(i))
    analysisRequest.set(generated_requests)

    return
