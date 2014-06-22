#!/usr/bin/env python3

import csv
import psycopg2
import json
import io

db       = psycopg2.connect(database='airquality', user='airq', password='nach0s', host='localhost')
db.set_isolation_level(0) #Set conneciton to autocommit
dbcursor = db.cursor()

input_file = csv.DictReader(open("zipcode.csv"))

prepped_data = ""
for row in input_file:
  row['latitude']  = float(row['latitude'])
  row['longitude'] = float(row['longitude'])
  row['timezone']  = int(row['timezone'])

  jdump         = json.dumps(row, ensure_ascii=False)
  prepped_data += row['zip']+"\t"+jdump+"\n"

prepped_data = io.StringIO(prepped_data)
prepped_data.seek(0)
dbcursor.copy_from(prepped_data, 'zips')