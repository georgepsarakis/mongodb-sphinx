#!/usr/bin/python
import sys, time, logging
from helper import *
from xmldoc import xmldoc
from mongoid import mongoid
from pymongo import Connection

# Get command line parameters
args = get_args()

# TODO: add parameters for logging
logging.basicConfig(filename = 'mongodb-sphinx.log', level = logging.DEBUG, format = '%(asctime)s %(message)s', datefmt = "%d-%m-%Y %H:%M:%S")

# Connection to the MongoDB server
c = Connection(args.host, args.port)
# Select database and collection
db = c[args.database]
# TODO: support for multiple collections
collection = db[args.collection]

# Get text fields and attributes
fields = args.text_field
attributes = get_attributes(args)

xml = xmldoc(nsmap = 'sphinx')
xml.l_start('docset')
xml.l_start('schema')

# define schema
for field in fields:
  xml.l_add('field', '', name = field)
for attr, meta in attributes.iteritems():
  meta['name'] = attr
  xml.l_add('attr', '', **meta)
xml.l_end('schema')

# Example: {'machine': 11412541, 'timestamp': 1335805916, 'pid': 27573, 'inc': 2}
ts_from = args.timestamp_from
ts_until = ts_from + args.step

while ( ts_until < args.timestamp_until ):
  id_limits = get_date_limits({ 'from' : ts_from, 'until' : ts_until })
  r = collection.find({"_id" : { "$gte" : id_limits[0] }, "_id" : {"$lte" : id_limits[1] } })
  # map doc_ids to _id values in key-value storage (?)
  doc_id = 1
  for row in r:
    xml.l_start('document', False, id = doc_id)
    xml.ns_disable()
    o = mongoid( row['_id'] )
    id_parts = o.decompose()
    for field in fields:
      if field in row:
	xml.l_add(field, row[field])
    id_parts['date_inserted'] = id_parts['timestamp']
    del id_parts['timestamp']
    for attr, v in id_parts.iteritems():
      xml.l_add(attr, v)
    for attr in attributes:
      if not attr in default_attributes:
	if attr in row:
	  v = row[attr]
	else:
	  v = ""
	xml.l_add(attr, v)
    xml.ns_enable()
    xml.l_end('document')
    doc_id += 1
    if doc_id % args.step == 0:
      logging.info("Fetched %d records"%(doc_id,))
      logging.info("Timestamp Limits [%d,%d]"%(ts_from, ts_until))
  ts_from = ts_until
  ts_until += args.step
xml.tostring()
