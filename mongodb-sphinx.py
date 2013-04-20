#!/usr/bin/python
import sys, time, logging
from libraries.helper import *
from libraries.xmldoc import xmldoc
from libraries.mongoid import mongoid
from pymongo import MongoClient

class MongoSphinx(object):
  Params = None
  MConnection = None
  XMLBuilder = None
  MCollection = None
  Fields = []
  Attributes = {}
  DefaultAttributes = [ 'date_inserted', 'machine', 'pid', 'inc' ]

  def __init__(self, args, **kwargs):
    start_time = time.time()
    self.Params = args
   # Connection to the MongoDB server
    self.MConnection = MongoClient(self.Params.host, self.Params.port)
    # Select database and collection
    db = self.MConnection[self.Params.database]
    # TODO: support for multiple collections
    self.MCollection = db[self.Params.collection]
    # Get text fields and attributes
    self.Fields = self.Params.text_fields
    self.Attributes = get_attributes(self.Params)
  
  def xmlpipe2(self):
    self.xmlschema()
    self.documents()

  ''' Define schema '''
  def xmlschema(self):
    self.XMLBuilder = xmldoc(nsmap = 'sphinx', encoding = 'utf-8', debug = False)
    self.XMLBuilder.l_start('docset')
    self.XMLBuilder.l_start('schema')
    for field in self.Fields:
      self.XMLBuilder.l_add('field', '', name = field)
    for attr, meta in self.Attributes.iteritems():
      meta['name'] = attr
      self.XMLBuilder.l_add('attr', '', **meta)
    self.XMLBuilder.l_end('schema')

  ''' Fetch records from MongoDB '''  
  def fetch(self, timestamp_from, timestamp_until):
    try:
      id_limits = get_id_boundaries({ 'from' : timestamp_from, 'until' : timestamp_until })
      if timestamp_until < self.Params.timestamp_until:
        operator = "$lt"
      else:
        operator = "$lte"
      condition = { "_id" : { "$gte" : id_limits[0] , operator : id_limits[1] } }
      return self.MCollection.find( condition )
    except Exception as e:
      print self.Params
      print 'Error while fetching MongoDB records: ', e
      sys.exit(1)
 
  def document_template(self):
    tags = []
    tags.append( { 'document' : { 'id' : '%(doc_id)s', 'has_children' : True } })
    for field in self.Fields:
      tags.append({ field : { 'has_children' : False } })
    for attr in self.DefaultAttributes:
      tags.append({ attr : { 'has_children' : False } })
    for attr in self.Attributes.keys():
      tags.append({ attr : { 'has_children' : False } })
    self.XMLBuilder.template(tags)
  
  def documents(self):
    timestamp_from = self.Params.timestamp_from
    timestamp_until = timestamp_from + self.Params.step
    start_time = time.time()
    self.document_template()
    doc_counter = 1    
    queries = 0
    mongotime = 0.
    flushtime = 0.
    xmlbuildtime = 0.
    default_attrs = {}
    looptime = 0.
    for attr in self.DefaultAttributes:
      default_attrs[attr] = ''
    while ( timestamp_until < self.Params.timestamp_until ):
      queries += 1
      mongotime -= time.time()
      r = self.fetch(timestamp_from, timestamp_until)
      mongotime += time.time()
      timestamp_from = timestamp_until
      timestamp_until += self.Params.step
      looptime -= time.time() 
      if r:
        for row in r:
          xmlbuildtime -= time.time()
          if self.Params.id_field is None:
            doc_id = doc_counter
          else:
            doc_id = row[self.Params.id_field]
          row['doc_id'] = str(doc_id)
          _id = mongoid( row['_id'] )
          _id_components = _id.decompose()
          row['_id'] = str(row['_id'])
          _id_components['date_inserted'] = _id_components['timestamp']
          del _id_components['timestamp']
          for field in self.Fields:
            if not field in row:
              row[field] = ''
          row = dict(row.items() + _id_components.items())
          row = dict(default_attrs.items() + row.items())
          self.XMLBuilder.add_by_template(row)
          xmlbuildtime += time.time()
          doc_counter += 1      
      r = None
      flushtime -= time.time()
      self.XMLBuilder.tostring()
      flushtime += time.time()
      if doc_counter % self.Params.step == 0:
        logging.info("Fetched %d records" % (doc_counter,))
        logging.info("Timestamp Limits [%d, %d]" % (timestamp_from, timestamp_until))
      looptime += time.time()
    self.XMLBuilder.tostring(True)
    logging.info("Total MongoDB Time: %12.6fsec for %d queries" % ( mongotime, queries ))
    logging.info("Total XML Build Time: %12.6fsec" % ( xmlbuildtime, ))
    logging.info("Total XML Resultset Loop Time: %12.6fsec" % ( looptime, ))
    logging.info("Total XML Flushing Time: %12.6fsec" % ( flushtime, ))

if __name__ == '__main__':
  start_time = time.time()
  # Get command line parameters
  args = get_args()
  # TODO: add parameters for logging
  logging.basicConfig(filename = 'progress.log', level = logging.DEBUG, format = '%(asctime)s %(message)s', datefmt = "%d-%m-%Y %H:%M:%S")
  MSphinx = MongoSphinx(args)
  MSphinx.xmlpipe2()
  logging.info('Total script time %12.6fsec' % (time.time() - start_time,))

