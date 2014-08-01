#!/usr/bin/env python
import sys
import shapely.geometry
import fiona
import numpy as np
import rtree
import os
import cPickle

if len(sys.argv)==1:
  print "Syntax: %s <SHAPE FILE>" % (sys.argv[0])
  sys.exit(-1)

#In my testing, constructing the index with this generator function took 39.271s
#versus 2m24.470s inserting points one at a time. The generator allows us to
#benefit from pre-sorting.
print "Generating bounds..."
xbounds   = np.arange(-124.848974,-66.885444,0.05)
ybounds   = np.arange(24.396308,49.384358,0.05)
print "Generating cell boundaries..."
cells     = [(x, y, x+0.05, y+0.05) for x in xbounds for y in ybounds]
print "Generating cell coordinates..."
cellcoors = [(x,y) for x in range(len(xbounds)) for y in range(len(ybounds))]
cell_list = ( (i,x,None) for i,x in enumerate(cells))

if os.path.isfile('polys'):
  print "Loading cell polygons..."
  cellpolys = cPickle.load(open('polys', 'rb'))
else:
  print "Generating cell polygons..."
  cellpolys  = [shapely.geometry.Polygon([(x,y),(x,y+0.05),(x+0.05,y+0.05),(x+0.05,y)]) for x in xbounds for y in ybounds]
  print "Saving cell polygons..."
  cPickle.dump(cellpolys, open('polys', 'wb'))

print "Generating empty data matrix..."
values = np.zeros(shape=(len(xbounds),len(ybounds)))


if os.path.isfile('bin-index.idx'):
  print "Loading spatial index..."
  ridx = rtree.index.Rtree('bin-index')
else:
  print "Building spatial index..."
  ridx = rtree.index.Index('bin-index',cell_list)

print "Loading shapefile %s..." % (sys.argv[1])
c = fiona.open(sys.argv[1])
while True:
  try:
    feature = c.next()
  except StopIteration:
    break

  geom   = shapely.geometry.shape(feature['geometry'])
  isects = ridx.intersection(geom.bounds)
  for i in isects:
    x,y          = cellcoors[i]
    values[x,y] += geom.intersection(cellpolys[i]).area/geom.area