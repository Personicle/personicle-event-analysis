from asyncio.log import logger
import logging
import os
import traceback
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import register_composites

# config_object = ConfigParser()
# config_object.read("config.ini")
# database = config_object["CREDENTIALS_DATABASE"]

logger = logging.getLogger(__name__)

engine = create_engine("postgresql://{username}:{password}@{dbhost}/{dbname}".format(username=os.environ.get('DATABASE_USERNAME'), password=os.environ.get('DATABASE_PASSWORD'),
                                                                                     dbhost=os.environ.get('DATABASE_HOST'), dbname=os.environ.get('DATABASE_NAME')),
                       pool_pre_ping=True)


Base = declarative_base(engine)

TABLE_MODELS = {}


def generate_table_class(table_name: str, base_schema: dict):
    if table_name in TABLE_MODELS:
        return TABLE_MODELS[table_name]
    try:
        base_schema['__tablename__'] = table_name
        base_schema['__table_args__'] = {'extend_existing': True}
        generated_model = type(table_name, (Base, ), base_schema)
        generated_model.__table__.create(bind=engine, checkfirst=True)
        TABLE_MODELS[table_name] = generated_model
        register_composites(engine.connect())
    except Exception as e:
        logger.error(traceback.format_exc())
        generated_model = None
    return generated_model


def loadSession():
    Session = sessionmaker(bind=engine)
    session = Session()
    return session
