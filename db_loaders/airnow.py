#!/usr/bin/env python3
#select pg_size_pretty(pg_database_size('airquality'));
#http://stackoverflow.com/questions/396455/python-postgresql-psycopg2-interface-executemany
import ftplib
import psycopg2
import sys
import io

paramkeys = {'OZONE': 1, 'PM10': 2, 'PM2.5': 3, 'TEMP':4, 'BARPR': 5, 'SO2': 6, 'RHUM': 7, 'WS': 8, 'WD': 9, 'CO': 10, 'NOY': 11, 'NO2Y': 12, 'NO': 13, 'NOX': 14, 'NO2': 15, 'PRECIP': 16, 'SRAD': 17, 'BC': 18, 'EC': 19, 'OC': 20}

class MeasureMonkey:
	def __init__(self, system):
		systems = {'airnow': 1}
		if system in systems:
			self.system = systems[system]
		else:
			raise Exception('Unrecognised system')

		self.db    = psycopg2.connect(database='airquality', user='airq', password='nach0s', host='localhost')
		self.db.set_isolation_level(0) #Set conneciton to autocommit

		self.meascursor = self.db.cursor()
		self.meascursor.execute("PREPARE measurement_insert AS INSERT INTO measurements (stationid,	system,	datetime,	param, value) VALUES ($1, $2, $3, $4, $5)")

		self.data_to_insert = set()

	def insert(self, stationid, thedate, param, value):
		datum = (stationid, self.system, thedate, param, value)
		self.data_to_insert.add(datum)

	def commit(self):
		prepped_data = ["\t".join([str(y) for y in x]) for x in self.data_to_insert]
		prepped_data = "\n".join(prepped_data)
		prepped_data = io.StringIO(prepped_data)
		prepped_data.seek(0)
		self.meascursor.copy_from(prepped_data, 'measurements')
		self.data_to_insert = set()


class AirNow:
	def __init__(self):
		pass

	def _connectFTP(self):
		print("Connecting via FTP...")
		self.ftp = ftplib.FTP(host='ftp.airnowapi.org', user='leapingleopard', passwd='xi7MxOiwrRR_vKeT')

	def loadStations(self):
		pass

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
			raise Exception("Onrecognised command")

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




if len(sys.argv)==1:
	print("Syntax: %s <API NAME> <COMMAND>" % (sys.argv[0]))
	print("\nAPIS:")
	print("\tairnow dataload <FILE>")
	print("\tairnow dataload latestonline")
	print("\tairnow dataload bulkonline")
	print("\tairnow stationload <FILE>")
	print("\tairnow stationload online")
	sys.exit(-1)

apis = {'airnow': AirNow}

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