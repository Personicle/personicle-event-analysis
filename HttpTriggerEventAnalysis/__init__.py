import logging
from typing import List
import json

import azure.functions as func

from utils.analysis_requests import get_analysis_requests


def main(req: func.HttpRequest, analysisQueue: func.Out[List[str]]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    analysis_requests = []
    analysis_requests = get_analysis_requests()
    generated_requests = ["testing http trigger"]
    for i in analysis_requests:
        logging.info(i)
        generated_requests.append(json.dumps(i))
    analysisQueue.set(generated_requests)

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200
        )
