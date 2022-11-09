from configparser import ConfigParser
import os

if os.environ.get("DEV_ENVIRONMENT", "local") in ["local", "staging"]:
    print("in dev environment")
    config_object = ConfigParser()

    config_object.read("config.ini")
    DB_CONFIG = config_object['CREDENTIALS_DATABASE']

    os.environ["DATABASE_USERNAME"] = DB_CONFIG['USERNAME']
    os.environ["DATABASE_PASSWORD"] = DB_CONFIG["PASSWORD"]
    os.environ["DATABASE_HOST"] = DB_CONFIG['HOST']
    os.environ["DATABASE_NAME"] = DB_CONFIG['NAME']
    os.environ["ANALYSIS_TABLENAME"] = DB_CONFIG['ANALYSIS_TABLENAME']
else:
    pass
