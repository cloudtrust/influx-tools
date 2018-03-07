#!/usr/bin/env python

# Copyright (C) 2018:
#     Sonia Bogos, sonia.bogos@elca.ch
#

import re
import pytest
import logging
import time
import calendar
import datetime
import json
import influxdb

import dateutil.parser

from sh import docker
from influxdb import InfluxDBClient


# logging
logging.basicConfig(
    format='%(asctime)s %'
           '(name)s %(levelname)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)
logger = logging.getLogger("influx_tools.tests.test_influx_container")
logger.setLevel(logging.INFO)

@pytest.mark.usefixtures('settings', scope='class')
class TestContainerInflux():
    """
        Class to test the influx container.
    """

    def test_systemd_running_influxdb(self, settings):
        """
        Test to check if systemd is running influxdb.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        command_influxdb = (
        "busctl", "get-property", "org.freedesktop.systemd1", "/org/freedesktop/systemd1/unit/influxdb_2eservice",
        "org.freedesktop.systemd1.Unit", "ActiveState")
        active_status = '"active"'

        # docker exec -it busctl get-property
        check_service = docker.bake("exec", "-i", container_name, command_influxdb)
        logger.debug(check_service)

        # check the return value
        influxdb_status = check_service().stdout.decode("utf-8")

        status = re.search(active_status, influxdb_status)
        assert status is not None

    def test_systemd_running_monit(self, settings):
        """
        Test to check if systemd is running monit.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        command_monit = (
        "busctl", "get-property", "org.freedesktop.systemd1", "/org/freedesktop/systemd1/unit/monit_2eservice",
        "org.freedesktop.systemd1.Unit", "ActiveState")
        active_status = '"active"'

        # docker exec -it busctl get-property
        check_service = docker.bake("exec", "-i", container_name, command_monit)
        logger.debug(check_service)

        # check the return value
        monit_status = check_service().stdout.decode("utf-8")

        status = re.search(active_status, monit_status)
        assert status is not None

    def test_systemd_running_nginx(self, settings):
        """
        Test to check if systemd is running nginx.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        command_monit = (
        "busctl", "get-property", "org.freedesktop.systemd1", "/org/freedesktop/systemd1/unit/nginx_2eservice",
        "org.freedesktop.systemd1.Unit", "ActiveState")
        active_status = '"active"'

        # docker exec -it busctl get-property
        check_service = docker.bake("exec", "-i", container_name, command_monit)
        logger.debug(check_service)

        # check the return value
        nginx_status = check_service().stdout.decode("utf-8")

        status = re.search(active_status, nginx_status)
        assert status is not None

    def test_systemd_running_influxdb2(self, settings):
        """
        Test to check if systemd is running influxdb.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        service_name = settings['service_name']

        # docker exec -it busctl get-property
        check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
        logger.debug(check_service)

        # check the return value
        try:
            influxdb_status = check_service().exit_code
            assert influxdb_status == 0
        except Exception as e:
            pytest.fail("Exit code of systemctl is not 0; influxdb is not active")
            logger.error(e)

    def test_systemd_running_monit2(self, settings):
        """
        Test to check if systemd is running monit.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        service_name = "monit"

        # docker exec -it busctl get-property
        check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
        logger.debug(check_service)

        # check the return value
        try:
            monit_status = check_service().exit_code
            assert monit_status == 0
        except Exception as e:
            pytest.fail("Exit code of systemctl is not 0; monit is not active")
            logger.error(e)

    def test_systemd_running_nginx2(self, settings):
        """
        Test to check if systemd is running nginx.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        service_name = settings['webserver']

        # docker exec -it busctl get-property
        check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
        logger.debug(check_service)

        # check the return value
        try:
            nginx_status = check_service().exit_code
            assert nginx_status == 0
        except Exception as e:
            pytest.fail("Exit code of systemctl is not 0; monit is not active")
            logger.error(e)

    def test_container_running(self, settings):
        """
        Test to check if the container is running.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        running_status = 'running'
        container_name = settings['container_name']

        # docker inspect --format='{{.State.Status}} container
        check_status = docker.bake("inspect", "--format='{{.State.Status}}'", container_name)
        logger.debug(check_status)

        status = re.search(running_status, check_status().stdout.decode("utf-8"))
        assert status is not None

    def test_influxdb(self, settings, influxdb_settings):
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

        new_db = "database_test"
        list_db = "show databases;"
        list_measurement = "SELECT * FROM cpu_test;"
        test_value = "my_test_text"
        measurement_name = "cpu_test"
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
        dbs = client.query(list_db)
        res = json.dumps(dbs.raw)
        assert re.search(new_db, res) is not None
        client.switch_database(new_db)

        assert client.write_points(json_measurement)is True

        series = client.query(list_measurement)
        res = json.dumps(series.raw)
        assert re.search(test_value, res) is not None

        # delete the newly created database and measurement and check that the delete is done
        client.drop_measurement(measurement_name)
        client.drop_database(new_db)
        dbs = client.query(list_db)
        res = json.dumps(dbs.raw)
        assert re.search(new_db, res) is None

    def test_authentication_influxdb(self, influxdb_settings):
        """Test to check that authentification is enforced by influxdb."""

        host = influxdb_settings['host_ip']
        user = influxdb_settings['user']
        port = influxdb_settings['port']
        db = influxdb_settings['database']

        list_db = "show databases;"

        client = InfluxDBClient(host=host, port=port, username=user, database=db)

        with pytest.raises(influxdb.exceptions.InfluxDBClientError):
            client.query(list_db)

        client = InfluxDBClient(host=host, port=port, database=db)
        
        with pytest.raises(influxdb.exceptions.InfluxDBClientError):
            client.query(list_db)


    def test_monit_restarts_stopped_influxdb(self, settings):
        """
        Test to check if monit restarts a stopped influxdb.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        service_name = settings['service_name']
        max_timeout = settings['influxdb_timeout']

        # stop influxdb
        stop_service = docker.bake("exec", "-i", container_name, "systemctl", "stop", service_name)
        logger.debug(stop_service)
        stop_service()

        tic_tac = 0
        influxdb_is_up = False

        while (tic_tac < max_timeout) and (influxdb_is_up == False):
            # check if monit started influxdb
            time.sleep(1)
            check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
            logger.info("Check to see if {service} started after {time} seconds".format(service=service_name, time=tic_tac))
            logger.debug(check_service)

            try:
                influxdb_status = check_service().exit_code
                if (influxdb_status == 0):
                    influxdb_is_up = True
                    logger.info("{service} is running".format(service=service_name))

            except Exception as e:
                tic_tac = tic_tac + 1
        assert influxdb_is_up == True

    def test_monit_restarts_stopped_nginx(self, settings):
        """
        Test to check if monit restarts a stopped nginx.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        service_name = settings['webserver']
        max_timeout = settings['nginx_timeout']

        # stop nginx
        stop_service = docker.bake("exec", "-i", container_name, "systemctl", "stop", service_name)
        logger.debug(stop_service)
        stop_service()

        tic_tac = 0
        nginx_is_up = False

        while (tic_tac < max_timeout) and (nginx_is_up == False):
            # check if monit started nginx
            time.sleep(1)
            check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
            logger.info("Check to see if {service} started after {time} seconds".format(service=service_name, time=tic_tac))
            logger.debug(check_service)

            try:
                nginx_status = check_service().exit_code
                if (nginx_status == 0):
                    nginx_is_up = True
                    logger.info("{service} is running".format(service=service_name))

            except Exception as e:
                tic_tac = tic_tac + 1
        assert nginx_is_up == True

    def test_monit_restarts_killed_influxdb(self, settings):
        """
        Test to check if monit restarts a killed influxdb.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        service_name = settings['service_name']
        max_timeout = settings['influxdb_timeout']

        # kill influxdb
        stop_service = docker.bake("exec", "-i", container_name, "systemctl", "kill", service_name)
        logger.debug(stop_service)
        stop_service()

        tic_tac = 0
        influxdb_is_up = False

        while (tic_tac < max_timeout) and (influxdb_is_up == False):
            # check if monit started influxdb
            time.sleep(1)
            check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
            logger.info("Check to see if {service} started after {time} seconds".format(service=service_name, time=tic_tac))
            logger.debug(check_service)

            try:
                influxdb_status = check_service().exit_code
                if (influxdb_status == 0):
                    influxdb_is_up = True
                    logger.info("{service} is running".format(service=service_name))

            except Exception as e:
                tic_tac = tic_tac + 1
        assert influxdb_is_up == True

    def test_monit_restarts_killed_nginx(self, settings):
        """
        Test to check if monit restarts a stopped nginx.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        service_name = settings['webserver']
        max_timeout = settings['nginx_timeout']

        # kill nginx
        stop_service = docker.bake("exec", "-i", container_name, "systemctl", "kill", service_name)
        logger.debug(stop_service)
        stop_service()

        tic_tac = 0
        nginx_is_up = False

        while (tic_tac < max_timeout) and (nginx_is_up == False):
            # check if monit started nginx
            time.sleep(1)
            check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
            logger.info("Check to see if {service} started after {time} seconds".format(service=service_name, time=tic_tac))
            logger.debug(check_service)

            try:
                nginx_status = check_service().exit_code
                if (nginx_status == 0):
                    nginx_is_up = True
                    logger.info("{service} is running".format(service=service_name))

            except Exception as e:
                tic_tac = tic_tac + 1
        assert nginx_is_up == True

    def test_no_error_monit_log(self, settings):
        """
        Test to check that when running the container systemd starts influxdb and there is no error in the monit logs.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        path_monit_log = "/var/log/monit.log"
        error_status ='error'

        # stop and restart the container
        stop_docker = docker.bake("stop", container_name)
        logger.debug(stop_docker)
        stop_docker()

        restart_docker = docker.bake("start", container_name)
        logger.debug(restart_docker)
        restart_docker()
        time.sleep(2)

        # docker inspect --format='{{.State.Status}} container
        check_status = docker.bake("inspect", "--format='{{.State.StartedAt}}'", container_name)
        logger.debug(check_status)
        last_started_date = dateutil.parser.parse(check_status().stdout.rstrip()).replace(tzinfo=None)

        get_monit_log = docker.bake("exec", container_name, "cat", path_monit_log)
        logger.debug(get_monit_log)
        monit_log = get_monit_log().stdout.decode("utf-8")

        # parse the line of the log file and check if the logs done after the start of the container contain any error
        lines = monit_log.splitlines()
        for line in lines:
            start = line.index("[") + len("[")
            end = line.index("]", start)
            monit_date = line[start : end].split()
            monit_time = monit_date[3].split(":")
            logged_date = datetime.datetime(datetime.datetime.now().year, list(calendar.month_abbr).index(monit_date[1]), int(monit_date[2]),
                           int(monit_time[0]), int(monit_time[1]), int(monit_time[2]))
            if logged_date > last_started_date:
                monit_priority = line[end+1 : line.index(":", end)]
                assert re.search(error_status, monit_priority) is None
            # making the assumption that we are in the same year

    def test_systemd_restarts_monit(self, settings):
        """
        Test to check that if monit is down then systemd will restart it.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        service_name = "monit"
        max_timeout = settings['monit_timeout']

        # kill monit
        stop_service = docker.bake("exec", "-i", container_name, "systemctl", "kill", service_name)
        logger.debug(stop_service)
        stop_service()

        tic_tac = 0
        monit_is_up = False

        while (tic_tac < max_timeout) and (monit_is_up == False):
            # check if systemd starts monit
            time.sleep(1)
            check_service = docker.bake("exec", "-i", container_name, "systemctl", "status", service_name)
            logger.info("Check to see if {service} started after {time} seconds".format(service=service_name, time=tic_tac))
            logger.debug(check_service)

            try:
                monit_status = check_service().exit_code
                if (monit_status == 0):
                    monit_is_up = True
                    logger.info("{service} is running".format(service=service_name))

            except Exception as e:
                tic_tac = tic_tac + 1
        assert monit_is_up == True

    def test_container_exposed_ports(self, settings):
        """
        Test to check if the correct ports are exposed.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """

        container_name = settings['container_name']
        ports = settings['ports']

        check_ports = docker.bake("inspect", "--format='{{.Config.ExposedPorts}}'", container_name)
        logger.debug(check_ports)
        exposed_ports = check_ports().stdout.decode("utf-8")

        for port in ports:
            assert re.search(port, exposed_ports) is not None

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
                logger.info("{service} is not yet running".format(service=service_name))

        # check that the previously created db exists
        dbs = client.query(list_db)
        res = json.dumps(dbs.raw)
        assert re.search(new_db, res) is not None

        # delete the newly created database and check that the delete is done
        client.drop_database(new_db)
        dbs = client.query(list_db)
        res = json.dumps(dbs.raw)
        assert re.search(new_db, res) is None

    def test_monit_always_restarts2(self, settings):
        """
        Test to check if monit is configured to always restart.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """
        container_name = settings['container_name']
        service_name = "monit"
        restart_status = "Restart=always"

        check_monit_restart = docker.bake("exec", "-i", container_name, "systemctl", "cat", service_name)
        logger.debug(check_monit_restart)
        monit_restart = check_monit_restart().stdout.decode("utf-8")
        assert re.search(restart_status, monit_restart) is not None

    def test_monit_always_restarts(self, settings):
        """
        Test to check if monit is configured to always restart.
        :param settings: settings of the container, e.g. container name, service name, etc.
        :return:
        """
        container_name = settings['container_name']

        command_monit = (
            "busctl", "get-property", "org.freedesktop.systemd1", "/org/freedesktop/systemd1/unit/monit_2eservice",
            "org.freedesktop.systemd1.Service", "Restart")
        restart_status = '"always"'

        # docker exec -it busctl get-property
        check_monit_restart = docker.bake("exec", "-i", container_name, command_monit)
        logger.debug(check_monit_restart)

        # check the return value
        monit_restart = check_monit_restart().stdout.decode("utf-8")

        status = re.search(restart_status, monit_restart)
        assert status is not None
