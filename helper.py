import sys, argparse, time
from datetime import date, datetime
from pymongo import objectid

default_attributes = [ "date_inserted", "machine", "pid", "inc" ]

def add_attribute(attr):
  (attr_name, attr_type) = map( str.strip, attr.split(',') )
  return { 'attribute' : attr_name, 'attribute-type' : attr_type }

def get_attributes(arguments):
  """
    -------- TODO --------
    Could storage of ID components be achieved with a single MVA ?
  """
  attributes = {
    "machine"        : { "type" : "int", "bits" : 32, "default" : 0 },
    "pid"            : { "type" : "int", "bits" : 32, "default" : 0 },
    "inc"            : { "type" : "int", "bits" : 32, "default" : 0 }
  }
  """
    This attribute stores the record insertion timestamp calculated from the decomposed _id value
  """
  attributes["date_inserted"] = { "type" : "timestamp" }
  for attr in arguments.attribute:
    attr_params = { 'type' : attr['attribute-type'] }
    if attr["attribute-type"] == 'int':
      attr_params['bits'] = 32
    attributes[attr['attribute']] = attr_params
  return attributes

def get_args():
  parser = argparse.ArgumentParser(prog = "mongodb-sphinx.py", add_help = False)
  parser.add_argument( "--version", action = 'version', version = '%(prog)s 0.1')
  parser.add_argument( "-f", "--timestamp-from", help = "Timestamp from", type = int, default = 0)
  parser.add_argument( "-u", "--timestamp-until", help = "Timestamp until", type = int, default = int(time.time()) )
  parser.add_argument( "-d", "--database", help = "MongoDB Database", required = True )
  parser.add_argument( "-c", "--collection", help = "MongoDB Database Collection", required = True )
  parser.add_argument( "-h", "--host", help = "MongoDB server host", default = "localhost" )
  parser.add_argument( "-p", "--port", help = "MongoDB server port", default = 27017, type = int )
  """
    -------- TODO ---------
    The field and attribute parameters (and all parameters in general)
    could be specified through a simple configuration file as well
    Or should be reading/parsing directly from a given Sphinx conf file
  """
  parser.add_argument( "-t", "--text-field", help = "Text fields for indexing. Can be specified multiple times.", action = 'append' )
  parser.add_argument( "-a", "--attribute", help = "Attributes (date_inserted,machine,pid,inc are reserved)", type = add_attribute, action = 'append' )
  parser.add_argument( "-s", "--step", help = "Indexing step (increment for timestamp values)", type = int, default = 250 )
  return parser.parse_args()

def get_date_limits(timestamps):
  try:
    date_from = datetime.fromtimestamp( timestamps['from'] )
    date_until = datetime.fromtimestamp( timestamps['until'] )
    _id_from = objectid.ObjectId.from_datetime(date_from)
    _id_until = objectid.ObjectId.from_datetime(date_until)
    return ( _id_from, _id_until )
  except Exception as e:
    print "Unable to convert timestamp to ID limits"
    print "Traceback:", e
    sys.exit()

