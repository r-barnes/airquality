#!/usr/bin/env python3
import urllib
import urllib3
import xml.etree.cElementTree as et
from bs4 import BeautifulSoup
import re

# Code values for air quality parameters in the database
paramkeys = {'OZONE': 1, 'PM10': 2, 'PM2.5': 3, 'TEMP':4, 'BARPR': 5, 'SO2': 6, 'RHUM': 7, 'WS': 
8, 'WD': 9, 'CO': 10, 'NOY': 11, 'NO2Y': 12, 'NO': 13, 'NOX': 14, 'NO2': 15, 'PRECIP': 16, 'SRAD'
: 17, 'BC': 18, 'EC': 19, 'OC': 20}

# Used to convert descriptions of air quality parameters from the UK Air website
# to the corresponding codes in the database.

# TODO: There are two fields for WD and two for WS. Is this a problem?

ukairkeys = {'1,3-butadiene (1,3-butadiene)' : None, 
             'Carbon monoxide (CO)' : 'CO', 
             'Modelled Temperature (Temp)' : 'TEMP',
             'Modelled Wind Direction (Dir)' : 'WD', 
             'Modelled Wind Speed (Speed)' : 'WS', 
             'Nitric oxide (NO)' : 'NO', 
             'Nitrogen dioxide (NO<sub>2</sub>)' : 'NO2', 
             'Nitrogen oxides as nitrogen dioxide (NO<sub>X</sub>asNO<sub>2</sub>)' : 'NOX', 
             'Non-volatile PM<sub>10</sub> (Hourly measured) (Non-volatile PM<sub>10</sub>)' : None, 
             'Non-volatile PM<sub>2.5</sub> (Hourly measured) (Non-volatile PM<sub>2.5</sub>)' : None, 
             'Ozone (O<sub>3</sub>)' : 'OZONE', 
             'PM<sub>10</sub> particulate matter (Hourly measured) (PM<sub>10</sub>)' : 'PM10', 
             'PM<sub>2.5</sub> particulate matter (Hourly measured) (PM<sub>2.5</sub>)' : 'PM2.5', 
             'Sulphur dioxide (SO<sub>2</sub>)' : 'SO2', 
             'Volatile PM<sub>10</sub> (Hourly measured) (Volatile PM<sub>10</sub>)' : None, 
             'Volatile PM<sub>2.5</sub> (Hourly measured) (Volatile PM<sub>2.5</sub>)' : None, 
             'Wind Direction (DIR)' : 'WD', 
             'Wind Speed (SPED)' : 'WS', 
             'benzene (benzene)' : None}

# Load a web page using GET with optional parameters
def fetch_html(url, fields = None):
    http = urllib3.PoolManager()
    if fields:
        r = http.request('GET', url, fields)
    else:
        r = http.request('GET', url)
    html = r.data.decode('unicode_escape')
    return html   

# Parse the station name
def get_station_name(item):
    station_name = item.find('title').text
    return station_name

# Parse the station id
def get_station_id(item):
    regex = r'f_site_id=(.+)&'
    link = item.find('link').text
    station_id = re.search(regex, link).groups()[0]
    return station_id

# Parse the longitude and latitude    
def get_lon_lat(item):
    regex = r'Location: (\d+)&deg;(\d+)&acute;(\d+)\.(\d+)&quot;N\s+(\d+)&deg;(\d+)&acute;(\d+)\.(\d+)&quot;([WE])'
    description = item.find('description').text
    m = re.search(regex, description).groups()
    lat = float(m[0]) + float(m[1])/60 + float(m[2])/3600 + float(m[3])/360000
    lon = float(m[4]) + float(m[5])/60 + float(m[6])/3600 + float(m[7])/360000
    if m[8] == 'W':
       lon = -lon
    return (lon, lat)

# Retrieve the station list
def loadStations():
    url = 'http://uk-air.defra.gov.uk/assets/rss/current_site_levels.xml'
    html = fetch_html(url)
    root = et.fromstring(html)
    items = root.find('channel').findall('item')
    for item in items:
        station_name = get_station_name(item)
        station_id = get_station_id(item)
        lon, lat = get_lon_lat(item)
        elev = get_elev(lon, lat)
        yield (station_id, station_name, lon, lat, elev)

# TODO: get elevation from longitude and latitude
# Using -9999 as missing value
def get_elev(lon, lat):
    return -9999

# Convert date and time to the database format
def get_date_time(date, time):
    day, month, year = date.split('/')
    return ("%s-%s-%s %s" % (year, month, day, time))

# Retrieve most recent hourly air quality data
def loadData():
    url = 'http://uk-air.defra.gov.uk/assets/rss/current_site_levels.xml'
    html = fetch_html(url)
    root = et.fromstring(html)
    items = root.find('channel').findall('item')
    for item in items:
        station_id = get_station_id(item)
        link = item.find('link').text
        html = fetch_html(link)
        soup = BeautifulSoup(html)
        tables = soup.findAll('tbody')
        if len(tables) == 0:
            continue
        for tr in tables[0].findAll('tr'):
            param, date, time, val, unit, period, comment = \
                [repr(td)[4:-5] for td in tr.findAll('td')]
            date_time = get_date_time(date, time)
            if param in ukairkeys and ukairkeys[param] != None:
                param_id = paramkeys[ukairkeys[param]]
                yield (station_id, date_time, param_id, val)
