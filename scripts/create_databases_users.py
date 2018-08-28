#!/usr/bin/env python


# Copyright (C) 2018:
#     Majeri Kasmaei Chervine,  chervine.majeri@elca.ch 
#     Sonia Bogos, sonia.bogos@elca.ch
#
import argparse
import logging
import json
import jsonschema
import sys

from influxdb import InfluxDBClient

# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)

version="1.0"
prog_name = sys.argv[0]
parser = argparse.ArgumentParser(prog="{pn} {v}".format(pn=prog_name, v=version))
usage = """{pn} [options]
Script that creates the influx database and user. The name and credentials of the user are given in a json file.
""".format(
    pn=prog_name
)

parser.add_argument(
    '--influx-config-file',
    dest="json_file",
    help='Path of the json config file for influx database and user: Ex : ../keycloak.json',
    required=True
)

parser.add_argument(
    '--influxdb-credentials',
    dest="cred_file",
    help='Path of the json credentials file for influx admin: Ex : ../admin.json',
    required=True
)

parser.add_argument(
    '--debug',
    dest="debug",
    default=False,
    action="store_true",
    help='Enable debug'
)

parser.add_argument(
    '--influxdb-host',
    dest="host",
    type=str,
    required=False,
    default='127.0.0.1',
    help='hostname of InfluxDB http API'
)

parser.add_argument(
    '--influxdb-port',
    dest="port",
    type=int,
    required=False,
    default=8086,
    help='port of InfluxDB http API'
)


def validate_json(json_file, json_schema):
    #Validate the incoming json file
    try:
        jsonschema.validate(
            json_file,
            json_schema
        )
    except jsonschema.ValidationError as e:
        logger.debug("Error : {m}".format(m=e))
        raise jsonschema.ValidationError
    except jsonschema.SchemaError as e:
        logger.debug("Error : {m}".format(m=e))
        raise jsonschema.SchemaError


if __name__ == "__main__":
    """
    :return:
    """

    # parse args
    ##
    args = parser.parse_args()

    # debug
    ##
    debug = args.debug

    # set debug level
    ##
    logger = logging.getLogger("influx_tools.scripts.populate")
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    # Influx config file
    ##

    db_json_schema = {
        "$schema": "http://json-schema.org/schema#",
        "required": ["db_name"],
        "additionalProperties": True,
        "type": "object",
        "properties": {
            "db_name": {"type": "string"},
            "db_user": {"type": "string"},
            "db_password": {"type": "string"}
        },

    }

    influx_json_file = args.json_file
    logger.info("Loading influx json file from {path}".format(path=influx_json_file))

    try:
        with open(influx_json_file) as json_data:
            config = json.load(json_data)
    except IOError as e:
        logger.debug(e)
        raise IOError("Config file {path} not found".format(path=influx_json_file))
    else:
        logger.debug(
            json.dumps(
                config,
                sort_keys=True,
                indent=4,
                separators=(',', ': ')
            )
        )

    validate_json(config, db_json_schema)
    db_name = config.get('db_name')

    # Influx admin credentials file
    ##

    admin_json_schema = {
        "$schema": "http://json-schema.org/schema#",
        "required": ["user", "password"],
        "additionalProperties": True,
        "type": "object",
        "properties": {
            "user": {"type": "string"},
            "password": {"type": "string"}
        },
    }

    credentials_file = args.cred_file
    logger.info("Loading credentials json file from {path}".format(path=credentials_file))

    try:

        with open(credentials_file) as json_data:
            admin = json.load(json_data)

    except IOError as e:
        logger.debug(e)
        raise IOError("Config file {path} not found".format(path=credentials_file))
    else:
        logger.debug(
            json.dumps(
                admin,
                sort_keys=True,
                indent=4,
                separators=(',', ': ')
            )
        )

    validate_json(admin, admin_json_schema)
    admin_user = admin.get('user')
    admin_password = admin.get('password')

    # Influxdb connection
    ##
    host = args.host
    port = args.port

    try:
        client = InfluxDBClient(host=host, port=port, username=admin_user, password=admin_password)
        client.create_database(db_name)
        logger.info("Create influx database {name}".format(name=db_name))
        if config.get('db_user') and config.get('db_password'):
            db_user = config.get('db_user')
            db_password = config.get('db_password')
            client.create_user(db_user, db_password)
            client.grant_privilege('all', db_name, db_user)
            logger.info("Created user {user} with all privileges on database {db}".format(user=db_user, db=db_name))
    except Exception as e:
        logger.debug(e)
        raise e
    finally:
        client.close()
        logger.info("Influxdb client: closed HTTP session")
