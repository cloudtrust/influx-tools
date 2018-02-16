#!/usr/bin/env python

import argparse
import requests
from requests.auth import HTTPBasicAuth
from time import sleep

parser = argparse.ArgumentParser(description="Creates the influx user")
parser.add_argument('user', type=str)
parser.add_argument('password', type=str)
parser.add_argument('database', type=str)
args = parser.parse_args()

while True:
    try:
        r = requests.get('http://localhost:8086/ping')
        if r.status_code == 204:
            break
    except Exception as e:
        print("Influx not reachable {}".format(e))
        sleep(2)

exit_code=0
r = requests.post('http://localhost:8086/query', data= { 'q': "CREATE USER {} WITH PASSWORD '{}' WITH ALL PRIVILEGES".format(args.user,args.password) })
print(r.text)
if r.status_code != 200:
    exit_code += 1

r = requests.post('http://localhost:8086/query', 
        auth=(args.user, args.password),
        data={
            'q': 'CREATE DATABASE {}'.format(args.database),
        },
    )
print(r.text)
if r.status_code != 200:
    exit_code += 1

exit(exit_code)
