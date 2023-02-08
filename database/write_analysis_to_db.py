from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, exists, inspect
import traceback
import copy
import json
from datetime import datetime
import logging
import numpy as np

from database.postgres import generate_table_class, loadSession
from utils.analysis_request_schema import ANALYSIS_RESULT_SCHEMA

logger = logging.getLogger(__name__)

ANALYSIS_RESULT_TABLE = "user_analysis_results"


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


def write_analysis_results(user_id, analysis_result, analysis_id):
    """
    Add analysis results to the database
    user_id: personicle user id,
    analysis_results: results of the analysis in a well formatted JSON object,
    analysis_id: unique analysis id
    """
    session = loadSession()
    table_name = ANALYSIS_RESULT_TABLE
    try:
        model_class = generate_table_class(
            table_name, copy.deepcopy(ANALYSIS_RESULT_SCHEMA))

        # insert a row in the db
        results_row = {
            "user_id": user_id,
            "correlation_result": json.dumps(analysis_result, cls=NpEncoder),
            "unique_analysis_id": analysis_id,
            "timestamp_added": datetime.utcnow(),
            "viewed": False
        }
        statement = insert(model_class).values(results_row)
        statement = statement.on_conflict_do_update(index_elements=[model_class.unique_analysis_id], set_=results_row)\
            .returning(model_class.unique_analysis_id)
        orm_stmt = (
            select(model_class)
            .from_statement(statement)
            .execution_options(populate_existing=True)
        )

        print(statement)

        return_values = session.execute(orm_stmt,)
        # for r in return_values.scalar():
        print("Result: {}".format(return_values.scalar().unique_analysis_id))
        logger.info("Result: {}".format(return_values))

        session.commit()
        session.close()
    except Exception as e:
        session.rollback()
        session.close()
        logger.error(e)
        logger.error(traceback.format_exc())
        logger.debug("{},{},{}".format(
            user_id, analysis_result, analysis_id))
