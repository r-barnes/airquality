#!/usr/bin/env python3

import csv
import psycopg2
import json
import io

db       = psycopg2.connect(database='airquality', user='airq', password='nach0s', host='localhost')
db.set_isolation_level(0) #Set conneciton to autocommit
dbcursor = db.cursor()

input_file = csv.DictReader(open("zipcode.csv"))

dbcursor.execute('DELETE FROM zips')

prepped_data = ""
for row in input_file:
  prepped_data += ("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (row['zip'], row['city'], row['state'], row['latitude'], row['longitude'], row['timezone'], row['dst']))

prepped_data = io.StringIO(prepped_data)
prepped_data.seek(0)
dbcursor.copy_from(prepped_data, 'zips')