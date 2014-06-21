#!/usr/bin/env python3
#select pg_size_pretty(pg_database_size('airquality'));
#http://stackoverflow.com/questions/396455/python-postgresql-psycopg2-interface-executemany
import ftplib
import psycopg2
import sys
import io

#encoding = locale.getdefaultlocale()[1]

db    = psycopg2.connect(database='airquality', user='airq', password='nach0s', host='localhost')
db.set_isolation_level(0) #Set conneciton to autocommit
dbcur = db.cursor()

dbcur.execute("PREPARE measurement_insert AS INSERT INTO measurements (stationid,	system,	datetime,	param, value) VALUES ($1, $2, $3, $4, $5)")

print("Connecting via FTP...")
ftp   = ftplib.FTP(host='ftp.airnowapi.org', user='leapingleopard', passwd='xi7MxOiwrRR_vKeT')

ftp.cwd('HourlyData')

print("Retrieving file list...")
files=list(ftp.mlsd('*dat'))
#files=[('bob')]
files.sort(key=lambda x: x[0], reverse=True)

paramkeys = {'OZONE': 1, 'PM10': 2, 'PM2.5': 3, 'TEMP':4, 'BARPR': 5, 'SO2': 6, 'RHUM': 7, 'WS': 8, 'WD': 9, 'CO': 10, 'NOY': 11, 'NO2Y': 12, 'NO': 13, 'NOX': 14, 'NO2': 15, 'PRECIP': 16, 'SRAD': 17, 'BC': 18, 'EC': 19, 'OC': 20}

for f in files[0:]:
	filename = f[0]
	data     = []
	print("Retrieving file " + filename)
	ftp.retrlines('RETR '+filename, data.append)

	#with open('../data/2014052706.dat', 'rb') as fin:
#		data=fin.readlines()
#	data = map(lambda x: x.strip(), data)

	#"valid_date", "valid_time", "aqsid", "sitename", "GMT_offset", "parameter_name", "reporting_units", "value", "data_source"
	print('Parsing data...')
	data_to_insert = set()
	for d in data:
		#try:
	#		d         = d.decode(encoding)
#		except:
	#		continue
		d=d.strip()
		if len(d)==0:
			continue
		d         = d.split('|')
		stationid = d[2]
		system    = 1
		thedate   = d[0] +' '+ d[1] #TODO: What format is this?

		param     = d[5]
		if param in paramkeys:
			param=paramkeys[param]
		else:
			sys.stderr.write("Unknown param key: %s\n" % (param))
			continue

		value     = d[7]

		datum = (stationid, system, thedate, param, value)
		data_to_insert.add(datum)

	data_to_insert=["\t".join([str(y) for y in x]) for x in data_to_insert]
	data_to_insert="\n".join(data_to_insert)

	data_to_insert=io.StringIO(data_to_insert)
	data_to_insert.seek(0)
	dbcur.copy_from(data_to_insert, 'measurements')

	#try:
		#dbcur.execute("INSERT INTO measurements (stationid,	system,	datetime,	param, value) VALUES (%s, %s, %s, %s, %s)", data_to_insert)
	#	dbcur.execute("EXECUTE measurement_insert (%s, %s, %s, %s, %s)", data_to_insert)
	#except psycopg2.IntegrityError as excp:
#		print(excp.pgerror)

	db.commit()