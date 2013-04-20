import sys, argparse, time
from datetime import date, datetime
from bson import objectid

default_attributes = [ "date_inserted", "machine", "pid", "inc" ]

def get_attributes(arguments):
  attributes = {
    "machine"        : { "type" : "int", "bits" : 32, "default" : 0 },
    "pid"            : { "type" : "int", "bits" : 32, "default" : 0 },
    "inc"            : { "type" : "int", "bits" : 32, "default" : 0 }
  }
  """ This attribute stores the record insertion timestamp calculated from the decomposed _id value  """
  attributes["date_inserted"] = { "type" : "timestamp" }
  for index, attr in enumerate(arguments.attributes):
    attr_params = { 'type' : arguments.attribute_types[index] }
    if attr == 'int':
      attr_params['bits'] = 32
    attributes[attr] = attr_params
  return attributes

def get_args():
  try:
    parser = argparse.ArgumentParser(prog = "mongodb-sphinx.py", description = 'MongoDB Full-text search with Sphinx XMLPipe2 Interface')
    parser.add_argument( "--version", action = 'version', version = '%(description)s 1.0-beta')
    parser.add_argument( "-d", "--database", help = "MongoDB Database", required = True )
    parser.add_argument( "-c", "--collection", help = "MongoDB Database Collection", required = True )
    parser.add_argument( "-H", "--host", help = "MongoDB server host", default = "localhost" )
    parser.add_argument( "-p", "--port", help = "MongoDB server port", default = 27017, type = int )
    parser.add_argument( "-f", "--timestamp-from", help = "Timestamp from", type = int, default = 0)
    parser.add_argument( "-u", "--timestamp-until", help = "Timestamp until", type = int, default = int(time.time()) )
    parser.add_argument( "-t", "--text-fields", help = "Text fields for indexing. Can be specified multiple times.", nargs = '+' )
    parser.add_argument( "-a", "--attributes", help = "Attributes (date_inserted,machine,pid,inc are reserved)", nargs = '*')
    parser.add_argument( "-s", "--step", help = "Indexing step (increment for timestamp values)", type = int, default = 500 )
    parser.add_argument( "--id-field", help = "Which field to use as a document ID. If left blank, an auto-increment integer will be used.",  )
    parser.add_argument( "--attribute-types", help = "Attribute types. You should specify all types for --attributes.", nargs = '*')
    args = parser.parse_args()
  except Exception as e:
    print e
    sys.exit(1)
  return args

'''
Get ObjectIDs for a certain timestamp range
'''
def get_id_boundaries(timestamps):
  try:
    date_from = datetime.fromtimestamp( timestamps['from'] )
    date_until = datetime.fromtimestamp( timestamps['until'] )
    _id_from = objectid.ObjectId.from_datetime(date_from)
    _id_until = objectid.ObjectId.from_datetime(date_until)
    return ( _id_from, _id_until )
  except Exception as e:
    print "Unable to convert timestamp to ID limits"
    print "Traceback:", e
    sys.exit(1)

