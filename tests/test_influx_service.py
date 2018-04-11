#!/usr/bin/env python

# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#

import re
import pytest
import logging
import time
import json
import influxdb

from sh import docker
from influxdb import InfluxDBClient


# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)
logger = logging.getLogger("influx_tools.tests.test_influx_service")
logger.setLevel(logging.INFO)


@pytest.mark.usefixtures('settings', 'influxdb_settings', scope='class')
class TestServiceInflux():
    """
        Class to test the influx servuce.
    """

    def test_influxdb(self, influxdb_settings):
        """
        Test to check if influxdb is functional.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        # test if one can do modifications on influxdb
        host = influxdb_settings['host_ip']
        user = influxdb_settings['user']
        password = influxdb_settings['password']
        port = influxdb_settings['port']
        db = influxdb_settings['database']

        test_value = "my_test_text"
        measurement_name = "cpu_test"
        new_db = "database_test"
        list_db = "SHOW DATABASES;"
        list_measurement = "SELECT * FROM {measure};".format(measure=measurement_name)
        json_measurement = [
            {
                "measurement": measurement_name,
                "tags": {
                    "host": "server",
                    "region": "us-west"
                },
                "time": "2018-02-03T12:00:00Z",
                "fields": {
                    "Float_value": 0.64,
                    "Int_value": 3,
                    "String_value": test_value,
                    "Bool_value": True
                }
            }
        ]

        client = InfluxDBClient(host=host, port=port, username=user, password=password, database=db)

        # create a database and check if it is in the list of databases
        client.create_database(new_db)
        logger.debug("Influx: CREATE DATABASE {db};".format(db=new_db))

        dbs = client.query(list_db)
        logger.debug("Influx: {query}".format(query=list_db))

        res = json.dumps(dbs.raw)
        logger.debug(res)

        assert re.search(new_db, res) is not None

        client.switch_database(new_db)

        assert client.write_points(json_measurement) is True
        logger.debug("Influx: INSERT {measure};".format(measure=json_measurement))

        series = client.query(list_measurement)
        logger.debug("Influx: {query}".format(query=list_measurement))

        res = json.dumps(series.raw)
        logger.debug(res)

        assert re.search(test_value, res) is not None

        # delete the newly created database and measurement and check that the delete is done
        client.drop_measurement(measurement_name)
        logger.debug("Influx: DROP MEASUREMENT {measure};".format(measure=measurement_name))

        client.drop_database(new_db)
        logger.debug("Influx: DROP DATABASE {db};".format(db=new_db))

        dbs = client.query(list_db)
        logger.debug("Influx: {query}".format(query=list_db))

        res = json.dumps(dbs.raw)
        logger.debug(res)

        assert re.search(new_db, res) is None

    def test_data_consistency(self, settings, influxdb_settings):
        """
        Test to check that the modifications done in influxdb are present after the container was stopped.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """
        host = influxdb_settings['host_ip']
        user = influxdb_settings['user']
        password = influxdb_settings['password']
        port = influxdb_settings['port']
        db = influxdb_settings['database']

        container_name = settings['container_name']
        service_name = settings['service_name']

        new_db = "database_test"
        list_db = "show databases;"

        client = InfluxDBClient(host=host, port=port, username=user, password=password, database=db)

        # create a database
        client.create_database(new_db)
        logger.debug("Influx: CREATE DATABASE {db};".format(db=new_db))

        # stop and restart container
        stop_container = docker.bake("stop", container_name)
        logger.debug(stop_container)
        stop_container()

        restart_container = docker.bake("restart", container_name)
        logger.debug(restart_container)
        restart_container()

        # wait until influxdb is running
        influxdb_is_up = False
        while (influxdb_is_up == False):
            time.sleep(1)
            check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
            logger.debug(check_service)

            try:
                influxdb_status = check_service().exit_code
                if (influxdb_status == 0):
                    influxdb_is_up = True
                    logger.info("{service} is running".format(service=service_name))
            except Exception as e:
                pass

        # check that the previously created db exists
        dbs = client.query(list_db)
        logger.debug("Influx: {query}".format(query=list_db))

        res = json.dumps(dbs.raw)
        logger.debug(res)

        assert re.search(new_db, res) is not None

        # delete the newly created database and check that the delete is done
        client.drop_database(new_db)
        logger.debug("Influx: DROP DATABASE {db};".format(db=new_db))

        dbs = client.query(list_db)
        logger.debug("Influx: {query}".format(query=list_db))

        res = json.dumps(dbs.raw)
        logger.debug(res)

        assert re.search(new_db, res) is None

    def test_authentication_influxdb(self, influxdb_settings):
        """Test to check that authentification is enforced by influxdb."""

        host = influxdb_settings['host_ip']
        user = influxdb_settings['user']
        port = influxdb_settings['port']
        db = influxdb_settings['database']

        list_db = "SHOW DATABASES;"

        client = InfluxDBClient(host=host, port=port, username=user, database=db)

        with pytest.raises(influxdb.exceptions.InfluxDBClientError):
            client.query(list_db)

        client = InfluxDBClient(host=host, port=port, database=db)

        with pytest.raises(influxdb.exceptions.InfluxDBClientError):
            client.query(list_db)

