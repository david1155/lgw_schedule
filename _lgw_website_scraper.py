import re

import requests
from bs4 import BeautifulSoup


def lgw_web():
    flights = {}
    final_schedule = {}
    url = 'https://www.gatwickairport.com/flights/?type=arrivals'
    page = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36'})

    soup = BeautifulSoup(page.content, "html5lib")

    flights_raw = soup.find_all('tr', {'class': re.compile('flight-info-row')})

    for row in flights_raw:
        data = []
        utc_time = re.findall('data-flight-time=\"(\d+)\"', str(row))[0][:10]
        for cell in row("td"):
            data.append(cell.get_text().strip())

        status_text = data[4]
        status = re.findall("^(.*?)\d.*", status_text)

        origin = data[2]

        if not status:
            status = ''
        else:
            status = status[0].strip()

        params = {}
        params.update({'origin': origin, 'sched_time': utc_time, 'status': status})

        # add status time if present
        if re.findall("^.*?(\d.*)", status_text):
            status_time = re.findall("^.*?(\d.*)", status_text)[0]
            params.update({'status_time': status_time})
        else:
            params.update({'status_time': ''})

        # write flight to dictionary
        flights[data[3]] = params

    for flight, info in flights.items():
        final_schedule[flight] = {'origin': info['origin'], 'status': info['status'], 'status_time': info['status_time'],
                                  'sched_time': info['sched_time']}

    return final_schedule
