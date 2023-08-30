import re

import requests
from bs4 import BeautifulSoup


def lgw_web():
    flights = {}
    url = 'https://www.gatwickairport.com/flights/?type=arrivals'
    page = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36'})

    soup = BeautifulSoup(page.content, "html5lib")

    flights_raw = soup.find_all('tr', {'class': re.compile('flight-info-row')})

    for row in flights_raw:
        utc_time = re.findall('data-flight-time=\"(\d+)\"', str(row))[0][:10]
        data = [cell.get_text().strip() for cell in row("td")]
        status_text = data[4]
        status = re.findall("^(.*?)\d.*", status_text)

        origin = data[2]

        status = '' if not status else status[0].strip()
        params = dict({'origin': origin, 'sched_time': utc_time, 'status': status})
        # add status time if present
        if re.findall("^.*?(\d.*)", status_text):
            status_time = re.findall("^.*?(\d.*)", status_text)[0]
            params['status_time'] = status_time
        else:
            params['status_time'] = ''

        # write flight to dictionary
        flights[data[3]] = params

    return {
        flight: {
            'origin': info['origin'],
            'status': info['status'],
            'status_time': info['status_time'],
            'sched_time': info['sched_time'],
        }
        for flight, info in flights.items()
    }
