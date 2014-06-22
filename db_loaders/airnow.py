#!/usr/bin/env python3
#select pg_size_pretty(pg_database_size('airquality'));
#http://stackoverflow.com/questions/396455/python-postgresql-psycopg2-interface-executemany
import ftplib
import psycopg2
import sys
import io
import urllib
import urllib3
from bs4 import BeautifulSoup
import csv
import re

paramkeys = {'OZONE': 1, 'PM10': 2, 'PM2.5': 3, 'TEMP':4, 'BARPR': 5, 'SO2': 6, 'RHUM': 7, 'WS': 8, 'WD': 9, 'CO': 10, 'NOY': 11, 'NO2Y': 12, 'NO': 13, 'NOX': 14, 'NO2': 15, 'PRECIP': 16, 'SRAD': 17, 'BC': 18, 'EC': 19, 'OC': 20}

def get_html(url, fields = None):
    http = urllib3.PoolManager()
    if fields:
        r = http.request('GET', url, fields)
    else:
        r = http.request('GET', url)
    html = r.data.decode('unicode_escape')
    return html

#Example: timestamp('25/12/2014', '13:00:00') = '2014-12-25 13:00:00'
def timestamp(date, time):
    dd, mm, yy = date.split('/')
    return "%s-%s-%s %s" % (yy, mm, dd, time)

class BaseMonkey:
    def __init__(self, system):
        systems = {'airnow': 1, 'ukair': 2}
        if system in systems:
            self.system = systems[system]
        else:
            raise Exception('Unrecognised system')

        self.db    = psycopg2.connect(database='airquality', user='airq', password='nach0s', host='localhost')
        self.db.set_isolation_level(0) #Set conneciton to autocommit
        self.dbcursor = self.db.cursor()

        self.data_to_insert = set()

    def commit(self, columns=[]):
        prepped_data = ["\t".join([str(y) for y in x]) for x in self.data_to_insert]
        prepped_data = "\n".join(prepped_data)
        prepped_data = io.StringIO(prepped_data)
        prepped_data.seek(0)
        if columns:
            self.dbcursor.copy_from(prepped_data, self.table, columns=columns)
        else:
            self.dbcursor.copy_from(prepped_data, self.table)
        self.data_to_insert = set()

class StationMonkey(BaseMonkey):
    def __init__(self, system):
        super().__init__(system)
        self.table = 'stations'

    def insert(self, stationid, name, lat, lon, elev):
        lat=float(lat)
        lon=float(lon)
        if not (-180<=lon and lon<=180 and -90<=lat and lat<=90):
                return
        datum = (stationid, self.system, name, lat, lon, elev)
        self.data_to_insert.add(datum)

    def commit(self):
        super().commit(columns=('stationid','system','name','lat','lon','elev'))
        self.dbcursor.execute("UPDATE stations SET pt = POINT(lon,lat);")

class MeasureMonkey(BaseMonkey):
    def __init__(self, system):
        super().__init__(system)
        self.table = 'measurements'

    def insert(self, stationid, thedate, param, value):
        datum = (stationid, self.system, thedate, param, value)
        self.data_to_insert.add(datum)


class AirNow:
    def __init__(self):
        pass

    def _connectFTP(self):
        print("Connecting via FTP...")
        self.ftp = ftplib.FTP(host='ftp.airnowapi.org', user='leapingleopard', passwd='xi7MxOiwrRR_vKeT')

    def loadStations(self, command):
        db = StationMonkey('airnow')

        self._connectFTP()
        self.ftp.cwd('Locations')

        print("Retrieving stations list")
        data = []
        self.ftp.retrlines('RETR monitoring_site_locations.dat', data.append)

        #CSV fields are: AQSID|parameter name|site code|site name|status|agency id|agency name|EPA region|latitude|longitude|elevation|GMT offset|country code|CMSA code|CMSA name|MSA code|MSA name|state code|state name|county code|county name|city code|city name
        for line in data:
            line = line.split('|')

            stationid = line[0]
            name      = line[3]
            lat       = line[8]
            lon       = line[9]
            elev      = line[10]
            db.insert(stationid,name,lat,lon,elev)

        db.commit()

    def loadData(self, command):
        db = MeasureMonkey('airnow')

        self._connectFTP()
        self.ftp.cwd('HourlyData')

        print("Retrieving file list...")
        files=list(self.ftp.mlsd('*dat'))
        files.sort(key=lambda x: x[0], reverse=True)

        if   command=='latestonline':
            files = files[0:1]
        elif command=='bulkonline':
            pass
        else:
            raise Exception("Unrecognised command")

        for f in files:
            filename = f[0]
            data     = []
            print("Retrieving file " + filename)
            self.ftp.retrlines('RETR '+filename, data.append)

            #These are the CSV file headers
            #"valid_date", "valid_time", "aqsid", "sitename", "GMT_offset", "parameter_name", "reporting_units", "value", "data_source"
            print('Parsing data...')
            for d in data:
                d=d.strip()
                if len(d)==0:
                        continue
                d         = d.split('|')
                stationid = d[2]
                thedate   = d[0] +' '+ d[1] #TODO: What format is this?

                param     = d[5]
                if param in paramkeys:
                    param = paramkeys[param]
                else:
                    sys.stderr.write("Unknown param key: %s\n" % (param))
                    continue

                value     = d[7]

                db.insert(stationid, thedate, param, value)

            db.commit()

class UKAir(AirNow):
    def __init__(self):
        pass
            
    def loadStations(self, command):
        db = StationMonkey('ukair')

        print("Retrieving stations list")
        
        # Scrape stations list from uk-air.defra.gov.uk
        url = "http://uk-air.defra.gov.uk/data/data_selector?q=515239&s=s&o=s&l=1#mid"
        html = get_html(url)
        soup = BeautifulSoup(html)
        select = soup.findAll('select')[0]
        stations = {}
        for option in select.findAll('option'):
            code = str(option['value'])
            name = str(''.join(option.contents))
            stations[code] = name
        
        # Read latitude, longitude, elevation from AirBase      
        with open('AirBase_v8_stations.csv', 'rt') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t')
            next(reader) # Skip header row
            dic = {}
            for row in reader:
                country_iso_code = row[2]
                if country_iso_code == 'GB':
                    code = row[1]
                    name = row[4]
                    longitude = float(row[12])
                    latitude = float(row[13])
                    altitude = float(row[14])
                    dic[code] = (longitude, latitude, altitude)
                    if not (code in stations):
                        stations[code] = name
                    
        # Insert stations
        for code, name in stations.items():
            if code in dic:
                lat = dic[code][1]
                lon = dic[code][0]
                elev = dic[code][2]
                db.insert(code,name,lat,lon,elev)
        db.commit()
        
        # Save station list as tab-separated text file
        with open('uk-stations.txt', 'w') as f:
            output = ['code\tname']
            for code, name in stations.items():
                output.append("%s\t%s" % (code, name))
            f.writelines('\n'.join(output)) 
            
    def loadData(self, command):
        print "Not implemented"
        


if len(sys.argv)==1:
    print("Syntax: %s <API NAME> <COMMAND>" % (sys.argv[0]))
    print("\nAPIS:")
    print("\tairnow dataload <FILE>")
    print("\tairnow dataload latestonline")
    print("\tairnow dataload bulkonline")
    print("\tairnow stationload <FILE>")
    print("\tairnow stationload online")
    sys.exit(-1)

apis = {'airnow': AirNow,
        'ukair': UKAir}


if not sys.argv[1] in apis:
    print("API not found")
    sys.exit(-1)
else:
    api = apis[sys.argv[1]]()
    if len(sys.argv)!=4:
        raise Exception('Need a command and argument')
    elif sys.argv[2]=='dataload':
        api.loadData(sys.argv[3])
    elif sys.argv[2]=='stationload':
        api.loadStations(sys.argv[3])
    else:
        raise Exception('Unrecognised command')
