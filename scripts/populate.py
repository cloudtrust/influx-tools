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
#import influxdb

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
Script that creates the influx admin, influx databases and users.
    Json file that indicates what databases need to be created is stored within /scripts
""".format(
    pn=prog_name
)

parser.add_argument(
    '--influx-config-file',
    dest="json_file",
    help='Paths of the json config file for influx: Ex : ../influx.json',
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
        "required": ["admin", "databases_users"],
        "additionalProperties": True,
        "type": "object",
        "properties": {
            "admin": {
                "type": "object",
                "properties": {
                    "user": {"type": "string"},
                    "password": {"type": "string"}
                },
                "required": ["user", "password"]
                },
            "databases_users": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "db_name": {"type": "string"},
                        "db_user": {"type": "string"},
                        "db_password": {"type": "string"}
                    },
                    "required": ["db_name"]
                }
            }
        }
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
    admin_user = config['admin']['user']
    admin_password = config['admin']['password']
    dbs_users = config['databases_users']

    # Influxdb connection
    ##
    host = args.host
    port = args.port

    try:
        client = InfluxDBClient(host=host, port=port)

        # create admin
        client.create_user(admin_user, admin_password, True)
        client.switch_user(admin_user, admin_password)
        logger.info("Created admin user {user}".format(user=admin_user))

        # create the list of databases and its users
        for db in dbs_users:
            client.create_database(db['db_name'])
            logger.info("Created database {name}".format(name=db['db_name']))
            if 'db_user' in db:
                client.create_user(db['db_user'], db['db_password'])
                client.grant_privilege('all', db['db_name'], db['db_user'])
                logger.info("Created user {user} with all privileges on database {db}".format(user=db['db_user'],
                                                                                      db=db['db_name']))
    # except influxdb.exceptions.InfluxDBClientError as e:
    #     logger.debug(e)
    #
    # except influxdb.exceptions.InfluxDBServerError as e:
    #     logger.debug(e)
    except Exception as e:
        logger.debug(e)
        raise e
    finally:
        client.close()
	    logger.info("Influxdb client: closed HTTP session")
