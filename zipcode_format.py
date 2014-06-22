#!/usr/bin/env python

import csv
import redis
import json

r_server = redis.Redis("localhost")

input_file = csv.DictReader(open("zipcode.csv"))

for row in input_file:
  row['latitude']  = float(row['latitude'])
  row['longitude'] = float(row['longitude'])
  row['timezone']  = int(row['timezone'])

  jdump = json.dumps(row, ensure_ascii=False)
  r_server.set("zip" + row['zip'], jdump)