# Scrape UK air quality data

import urllib
import urllib2
from BeautifulSoup import BeautifulSoup
import webbrowser
import json
import datetime
import csv

code_dict = {'PM<sub>10</sub>': 'PM10',
             'Dir': 'WD',
             'Speed': 'WS',
             'Temp': 'TEMP',
             'NO': 'NO',
             'NO<sub>2</sub>': 'NO2',
             'NO<sub>X</sub>asNO<sub>2</sub>': 'NOX',
             'O<sub>3</sub>': 'OZONE',
             'PM<sub>2.5</sub>': 'PM10'}

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

def get_values(row):
    values = [v.contents[0] for v in row.findAll('td') if len(v.findAll('span')) == 0]
    values[1] = values[1][:8]
    return values

def get_column_heads(row):
    return [''.join(map(str, td.contents)) for td in row.findAll('td') if td.contents[0] != 'Status/units']

def scrape_UK():
    stations = get_stations()
    lat_long = get_lat_long(stations)
    for station_code, station_name in stations:
        if station_code in lat_long:
            longitude, latitude, altitude = lat_long[station_code]
            insert_station(station_code, longitude, latitude, altitude)
        html = retrieve_data(station_code)
        table = get_table(html)
        rows = get_rows(table)
        column_heads = get_column_heads(rows[1])
        for row in rows[2:]:
            values = get_values(row)
            for attribute, value in zip(column_heads, values):
                insert_pollution_data(station_code, attribute, value)

def insert_pollution_data(station_code, attribute, value):
    if (attribute in code_dict) and (value != 'No data') and (value != '&nbsp;'):
        print "INSERT (%s, %s, %s)" % (station_code, code_dict[attribute], value)
    
    
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

def get_lat_long(stations):
    with open('AirBase_v8_stations.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        next(reader) # Skip header row
        dic = {}
        for row in reader:
            station_european_code = row[0]
            station_local_code = row[1]
            country_iso_code = row[2]
            longitude = row[12]
            latitude = row[13]
            altitude = row[14]
            if country_iso_code == 'GB':
                dic[station_local_code] = (longitude, latitude, altitude)
        return dic

def insert_station(station_code, longitude, latitude, altitude):
    # print "Insert station: %s, %s, %s, %s" % (station_code, longitude, latitude, altitude)
    pass

if __name__ == "__main__":
    scrape_UK()
    print attributes
        




