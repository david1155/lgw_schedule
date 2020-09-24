from threading import Timer

import mysql.connector

from _lgw_website_scraper import lgw_web
from datetime import datetime, timedelta

timetable = {}

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

# create dict with list of flights
flights_fr24 = {}


def update_table(flight_icao, origin, status, status_time, sched_time):
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO lgw_web (timestamp, flight_icao, origin, status, status_time, sched_time) VALUES (%s, %s, %s, %s, %s, %s);",
            (datetime.utcnow(), flight_icao, origin, status, status_time, sched_time))
        cur.close()
        conn.commit()
    except mysql.connector.Error as e:
        print(f"Error: {e}")


def scrape_diff_db():
    '''
    Scrape London Gatwick Airport arrivals schedule from website
    and put in MariaDB database
    '''
    global timetable

    Timer(30.0, scrape_diff_db).start()
    scraped = lgw_web()

    for k, v in scraped.items():
        # convert time to datetime (here, because unable to pass datetime through function return)
        now = datetime.today()
        # workaround fix timezone
        v['sched_time'] = datetime.fromtimestamp(int(v['sched_time'])) - timedelta(hours=2)
        # workaround with date in status, may be not correct when day changes. and convert to UTC timezone
        if v['status_time'] != '':
            v['status_time'] = datetime.strptime(v["status_time"], '%H:%M').replace(year=now.year, month=now.month,
                                                                                    day=v[
                                                                                        'sched_time'].day) - timedelta(
                hours=1)
        else:
            # workaround for MySQL DB to insert empty datetime value
            v['status_time'] = None

        params = ({'flight': k, 'origin': v['origin'], 'status': v['status'], 'status_time': v['status_time'],
                   'sched_time': v['sched_time']})

        # 1. check if flight exists in local schedule and changed
        if k in timetable:
            if timetable[k] != params:
                timetable[k] = params
                update_table(k, v['origin'], v['status'], v['status_time'], v['sched_time'])
            else:
                # 2. if not changed
                pass
        else:
            # 3. create if not exists
            timetable[k] = params
            update_table(k, v['origin'], v['status'], v['status_time'], v['sched_time'])


scrape_diff_db()
