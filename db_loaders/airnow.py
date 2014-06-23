#!/usr/bin/env python3
#select pg_size_pretty(pg_database_size('airquality'));
#http://stackoverflow.com/questions/396455/python-postgresql-psycopg2-interface-executemany
import ftplib
import psycopg2
import sys
import io
import urllib
import urllib3
import ukair

paramkeys = {'OZONE': 1, 'PM10': 2, 'PM2.5': 3, 'TEMP':4, 'BARPR': 5, 'SO2': 6, 'RHUM': 7, 'WS': 8, 'WD': 9, 'CO': 10, 'NOY': 11, 'NO2Y': 12, 'NO': 13, 'NOX': 14, 'NO2': 15, 'PRECIP': 16, 'SRAD': 17, 'BC': 18, 'EC': 19, 'OC': 20}

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
        for row in ukair.loadStations():
            # print ('insert %s' % repr(row))
            db.insert(*row)
        db.commit()
             
    def loadData(self, command):
        db = MeasureMonkey('ukair')
        for row in ukair.loadData():
            # print ('insert %s' % repr(row))
            db.insert(*row)
        db.commit()
        
        


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
