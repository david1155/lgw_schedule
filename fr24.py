import json
import logging
import os
import sys
import zlib

from amqp_consumer import AMQPConsumer

from datetime import datetime
from time import time
import mysql.connector

# Connect to MariaDB Platform
try:
    conn = mysql.connector.connect(
        user="lgw",
        password="123242",
        host="172.20.88.88",
        port=3306,
        database="lgw_schedule"

    )
except mysql.connector.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

# --- Azure Event Hub credentials to access Flightradar24 Live Event feed -------------------------
# -------------------------------------------------------------------------------------------------
# --- Credentials below will be provided by Flightradar24 -----------------------------------------

CONSUMER_GROUP = os.environ.get('EVENT_HUB_CONSUMER_GROUP', '$Default')
EVENTHUB = os.environ.get('EVENT_HUB_NAME', 'uxusoft')
USER = os.environ.get('EVENT_HUB_SAS_POLICY', 'uxusoft-consumer')
KEY = os.environ.get('EVENT_HUB_SAS_KEY', 'OQAokO2Wd4qea0f7oa0FekwiXmhL6q8Vinbbl0FovVk=')
NAMESPACE = os.environ.get('EVENT_HUB_NAMESPACE', 'fr24-position-feed-1')
# -------------------------------------------------------------------------------------------------
# --- Optional, if set can be used to store queue offset in Azure cloud storage -------------------
STORAGE_ACCOUNT_NAME = os.environ.get('AZURE_STORAGE_ACCOUNT', '')
STORAGE_KEY = os.environ.get('AZURE_STORAGE_ACCESS_KEY', '')
LEASE_CONTAINER_NAME = os.environ.get('AZURE_LEASE_CONTAINER_NAME', '')
# -------------------------------------------------------------------------------------------------
# --- Client name can be chosen by you to facilitate debugging if needed --------------------------
CLIENTNAME = os.environ.get('CLIENTNAME', 'test1')

flights = {}


def on_receive_callback(gzip_data):
    content = read_content(gzip_data)
    # print('Aircraft count', content['full_count'])
    del content['full_count']
    del content['version']
    if not content:
        raise RuntimeError('No flights included in response?')
    # print('TODO: Replace this with desired flight handling.')

    for flight_id in content.keys():
        inspect_flight(flight_id, content[flight_id])
        # pick first flight in list as sample content to display
        break


def read_content(gzip_data):
    """
    Read compressed content, decompress and return parsed JSON
    """
    # print(f"Received {len(gzip_data)} bytes")

    # expand compressed data and check byte lengths
    json_data = zlib.decompress(gzip_data)

    # parse JSON content
    content = json.loads(json_data)
    return content


def inspect_flight(flight_id, values):
    """
    Parse list of values for a flight
    """
    # flight_id == "0" -> heartbeat message, ignore
    if flight_id == "0":
        return
    # join the field names with provided values and list them
    names = ['addr', 'lat', 'lon', 'track', 'alt', 'speed',
             'squawk', 'radar_id', 'model', 'reg', 'last_update', 'origin',
             'destination', 'flight', 'on_ground', 'vert_speed', 'callsign', 'source_type', 'eta']

    # extend when Extended Mode-S etc included
    if len(values) > len(names):
        names.append('enhanced')
    dest = values[12]
    flight = values[16]
    # eta = values[18]
    flight_iata = values[13]
    origin = values[11]
    eta = datetime.utcfromtimestamp(values[18])
    last_update = datetime.utcfromtimestamp(values[10])

    # check if destination is LGW, ETA is in future and speed > 50 kts
    if dest == "LGW" and values[18] > int(time()) and values[5] > 50:
        # # debug
        # if flight in flights:
        #     print(f'FR24: {datetime.utcnow()}   {flight} >> OLD ETA: {flights[flight]} NEW ETA: {eta} ({values})')
        # else:
        #     print(f'FR24: {datetime.utcnow()}   {flight} >> NEW ETA: {eta} ({values})')

        # update flight ETA if changed
        if flight in flights:
            old_eta = flights[flight]
            # 1. ETA changed
            if eta != old_eta:
                flights[flight] = eta
                update_table(flight, flight_iata, origin, eta, last_update)
            else:
                # 2. ETA not changed
                pass
        else:
            # 3. new flight, not in table
            flights[flight] = eta
            update_table(flight, flight_iata, origin, eta, last_update)


def update_table(flight, flight_iata, origin, eta, last_update):
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO fr24 (timestamp, flight_icao, flight_iata, origin, eta, last_update) VALUES (%s, %s, %s, %s, %s, %s);",
            (datetime.utcnow(), flight, flight_iata, origin, eta, last_update))
        # cur.close()
        conn.commit()
    except mysql.connector.Error as e:
        print(f"Error: {e}")

    # print("Flight ID", flight_id)
    # for (key, val) in zip(names, values):
    #     print(key, json.dumps(val))


def consume_amqp():
    consumer = AMQPConsumer(namespace=NAMESPACE, eventhub=EVENTHUB, user=USER, key=KEY,
                            consumer_group=CONSUMER_GROUP, storage_container=LEASE_CONTAINER_NAME,
                            storage_account=STORAGE_ACCOUNT_NAME, storage_key=STORAGE_KEY)
    consumer.set_callback(on_receive_callback)
    consumer.consume()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)sZ %(message)s')
    print(__doc__)
    consume_amqp()
