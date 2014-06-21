# Scrape UK air quality data

import urllib
import urllib2
from BeautifulSoup import BeautifulSoup
import webbrowser
import json
import datetime

COLUMNS = ['End Date', 'End Time', 'PM10', 'Dir', 'Speed', 'Temp', 'NO', 'NO2', 'NOx as NO2',
           'Nonvolatile PM10', 'Nonvolatile PM2.5', 'O3', 'PM2.5', 'Volatile PM10', 'Volatile PM2.5']

def retrieve_data(monitoring_station, **options):
    url = "http://uk-air.defra.gov.uk/data/data_selector"        
    params = {
        'q': '515237',
        'f_statistic_type_id': '9999',
        'action': 'step6',
        'f_limit_was': '1',
        'submit': 'Save selection',
        'f_preset_date': '1',
        'f_site_id[]': monitoring_station,
    }
    if options:
        params = dict(params.items() + options.items())

    data = urllib.urlencode(params)
    results = urllib2.urlopen(url, data)
    
    return results

def get_table(html):
    soup = BeautifulSoup(html)
    table = soup.findAll('table')[0]
    return table

def get_rows(table):
    return table.findAll('tr')

def get_fields(row):
    fields = [field.contents[0] for field in row.findAll('td') if len(field.findAll('span')) == 0]
    fields[1] = fields[1][:8]
    return ', '.join(fields)

def scrape_UK(monitoring_station):
    utcnow = datetime.datetime.utcnow()
    fname = 'airquality_UK_' + monitoring_station + '_' + utcnow.strftime("%Y%m%d")
    with open(fname, "w") as f:
        output = []
        output.append(', '.join(COLUMNS))
        html = retrieve_data(monitoring_station)
        table = get_table(html)
        rows = get_rows(table)[2:]
        for row in rows:
            fields = get_fields(row)
            output.append(fields)
        f.writelines('\n'.join(output)+'\n')

def get_stations():
    url = "http://uk-air.defra.gov.uk/data/data_selector?q=515239&s=s&o=s&l=1#mid"
    html = urllib2.urlopen(url)
    soup = BeautifulSoup(html)
    select = soup.findAll('select')[0]
    return [(str(option['value']), str(option.contents[0])) for option in select.findAll('option')]

def write_station_list():
    stations = get_stations()
    output = ['Code, Name']
    output += ["%s, %s" % t for t in get_stations()]
    with open("UK_station_list", "w") as f:
        f.writelines('\n'.join(output))
    
if __name__ == "__main__":
    stations = get_stations()
    for station, name in stations:
        scrape_UK(station)




