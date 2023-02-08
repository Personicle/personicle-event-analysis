from sqlalchemy import BigInteger, Column, Enum, Float, TIMESTAMP
from sqlalchemy.types import Integer, Numeric, String, Boolean
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy_utils import CompositeType

from datetime import datetime
import uuid

ANALYSIS_TABLE_SCHEMA = {
    # "individual_id": Column(String, primary_key=True),
    # "timestamp": Column(TIMESTAMP, primary_key=True),
    # "source": Column(String, primary_key=True),
    # "value": Column(Integer),
    # "unit": Column(String),
    # "confidence": Column(String, default=None)
    "unique_analysis_id": Column(UUID(as_uuid=False), primary_key=True),
    "user_id": Column(UUID(as_uuid=False)),
    "anchor": Column(String),
    "aggregate_function": Column(String),
    "query_interval": Column(CompositeType(
        'TIME_INTERVAL', [
            Column('start_interval', Integer),
            Column('end_interval', Integer),
            Column('unit', String)
        ]
    )),
    "antecedent_name": Column(String),
    "antecedent_table": Column(String),
    "antecedent_parameter": Column(String),
    "antecedent_type": Column(String),
    "antecedent_interval": Column(String),
    "consequent_name": Column(String),
    "consequent_table": Column(String),
    "consequent_parameter": Column(String),
    "consequent_type": Column(String),
    "consequent_interval": Column(String),
    "consequent_interval": Column(String)
}

ANALYSIS_RESULT_SCHEMA = {
    "user_id": Column(String),
    "correlation_result": Column(JSON),
    "unique_analysis_id": Column(UUID(as_uuid=False), primary_key=True),
    "timestamp_added": Column(TIMESTAMP, default=datetime.utcnow()),
    "viewed": Column(Boolean, default=False)
}
