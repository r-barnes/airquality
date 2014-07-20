#!/usr/bin/env python3
#select pg_size_pretty(pg_database_size('airquality'));
#http://stackoverflow.com/questions/396455/python-postgresql-psycopg2-interface-executemany
import ftplib
import psycopg2
import sys
import io
import urllib.request
import json
from bs4 import BeautifulSoup

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

  def fastcommit(self, columns=[]):
    prepped_data = ["\t".join([str(y) for y in x]) for x in self.data_to_insert]
    prepped_data = "\n".join(prepped_data)
    prepped_data = io.StringIO(prepped_data)
    prepped_data.seek(0)
    try:
      if columns:
        self.dbcursor.copy_from(prepped_data, self.table, columns=columns)
      else:
        self.dbcursor.copy_from(prepped_data, self.table)
    except psycopg2.IntegrityError:
      print("This file duplicated data already in the table. Skipping file.")
    self.data_to_insert = set()

  def commit(self, columns=[]):
    vals    = ','.join(["%s"]*len(columns))
    columns = ','.join(columns)
    for d in self.data_to_insert:
      try:
        self.dbcursor.execute("INSERT INTO "+self.table+" ("+columns+") VALUES ("+vals+")", d)
      except psycopg2.IntegrityError:
        print("Duplicate value",d)
        continue
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

  def commit(self):
    super().commit(columns=('stationid','system','datetime','param','value'))


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

      if command=='bulkonline':
        db.fastcommit()
      else:
        db.commit()

class UKAir(AirNow):
  def __init__(self):
    self.param_translate={
      'Ozone (O3)':                                                'OZONE',
      'Nitric oxide (NO)':                                         'NO',
      'Nitrogen dioxide (NO2)':                                    'NO2',
      'Nitrogen oxides as nitrogen dioxide (NOXasNO2)':            'NOX', #TODO: Check this
      'PM10 particulate matter (Hourly measured) (PM10)':          'PM10',
      'Non-volatile PM10 (Hourly measured) (Non-volatile PM10)':   'NVPM10',
      'Volatile PM10 (Hourly measured) (Volatile PM10)':           'VPM10',
      'PM2.5 particulate matter (Hourly measured) (PM2.5)':        'PM2.5',
      'Non-volatile PM2.5 (Hourly measured) (Non-volatile PM2.5)': 'NVPM2.5',
      'Volatile PM2.5 (Hourly measured) (Volatile PM2.5)':         'VPM2.5',
      'Modelled Wind Direction (Dir)':                             'WD',
      'Modelled Wind Speed (Speed)':                               'WS',
      'Modelled Temperature (Temp)':                               'TEMP'
    }

  def getStationList(self):
    #URL     obtained from observing "http://uk-air.defra.gov.uk/interactive-map"
    url      = 'http://uk-air.defra.gov.uk/js/map_data?c=6db207cb3484c8be6cf0fe5c6785132f9185df7d'
    response = urllib.request.urlopen(url)
    data     = response.read()
    text     = data.decode('utf-8')
    text     = text.replace('markers = ','')
    text     = json.loads(text)
    return   text['aurn'] #There are other monitoring networks, but they are much smaller

  def loadStations(self, command):
    db       = StationMonkey('ukair')
    stations = self.getStationList()
    print("Found stations: %s" % (','.join('"'+x['site_id']+'"' for x in stations)))
    for station in stations:
      db.insert(station['site_id'], station['site_name'], station['latitude'], station['longitude'], None)
    db.commit()


  def loadData(self, command):
    db           = MeasureMonkey('ukair')
    station_list = ["ABD","ABD7","ARM6","AH","ACTH","BALM","BAR3","BPLE","BATH","BEL2","BEL1","BIL","AGRN","BIR1","BIRT","BLAR","BLC2","BOT","BORN","BRT3","BRS8","BUSH","CAM","CA1","CANT","CARD","CARL","MACK","CHAT","CHP","CHS7","COAL","CWMB","DERY","DUMB","DUMF","EA8","EB","ED3","ESK","EX","FW","GGWR","GLA4","GLKP","GLAZ","GRAN","GRA2","GDF","HG1","HAR","HM","HONI","HORE","HUL2","INV2","LB","LEAM","LEAR","LEED","LED6","LECU","LEOM","LERW","LIN3","LV6","LVP","BEX","CLL2","LON6","HG4","HRL","HR3","HIL","MY1","KC1","TED","TED2","HORS","LN","LH","MH","MAN3","MAN4","MKTH","MID","PEMB","NEWC","NCA3","NPT3","NTN3","NO12","NOTT","OX","OX8","PEEB","PLYM","PT4","PMTH","PRES","REA1","ROCH","ECCL","SASH","SDY","SCN2","CW","SHDG","SHE","SIB","SOUT","SEND","SK5","OSY","HOPE","EAGL","STOK","STOR","SV","SUN2","SWA1","THUR","TH2","WAL4","WAR","WEYB","WFEN","WIG5","TRAN","WREX","YW","YK10","YK11"]
    url          = "http://uk-air.defra.gov.uk/data/site-data?f_site_id=STATION_ID&view=last_hour"
    for station in station_list:
      thisurl  = url.replace('STATION_ID',station)
      response = urllib.request.urlopen(thisurl)
      data     = response.read()
      text     = data.decode('utf-8')
      bs       = BeautifulSoup(text)
      rows     = bs.tbody.find_all('tr')
      for row in rows:
        rowdata  = row.find_all('td')
        param    = rowdata[0].text
        date     = rowdata[1].text
        time     = rowdata[2].text
        value    = rowdata[3].text
        unit     = rowdata[4].text
        howoften = rowdata[5].text #How often the data is collected
        comment  = rowdata[6].text
        if param in self.param_translate and self.param_translate[param] in paramkeys:
          param = paramkeys[self.param_translate[param]]
        else:
          print("Unrecognised parameter %s in UKAir" % (param))
          continue
        date = date[6:10]+"-"+date[3:5]+"-"+date[0:2]+" "+time
        db.insert(station, date, param, value)

      db.commit()


apis = {'airnow': AirNow,
        'ukair': UKAir}

if len(sys.argv)==1:
  print("Syntax: %s <API NAME> <COMMAND>" % (sys.argv[0]))
  print("\nAPIS:")
  print("\tairnow dataload <FILE>")
  print("\tairnow dataload latestonline")
  print("\tairnow dataload bulkonline")
  print("\tairnow stationload <FILE>")
  print("\tairnow stationload online")
  print("\nAvailable APIs: %s" % (', '.join(list(apis))))
  sys.exit(-1)




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
