import cgi, sys

class xmldoc():
  """
    A simple class for generating XML documents
  """
  XML = ''
  NSMAP = False
  ENCODING = 'UTF-8'
  NS_SEPARATOR = ''
  NS = { 'nsmap' : NSMAP, 'ns_separator' : NS_SEPARATOR }
  OPEN_TAGS = []
  DEBUG = False
  Template = ''
  XMLBuffer = []

  def __init__(self, **kwargs):
    if 'nsmap' in kwargs:
      self.NSMAP = kwargs['nsmap']
      self.NS_SEPARATOR = ':'
    if 'encoding' in kwargs:     
      self.ENCODING = kwargs['encoding']
    if 'debug' in kwargs:
      self.DEBUG = kwargs['debug']
    self.XML = '<?xml version="1.0" encoding="' + self.ENCODING + '"?>'
    if self.DEBUG:
      self.XML += "\n"
  
  def ns_enable(self):
    self.NSMAP = self.NS['nsmap']
    self.NS_SEPARATOR = self.NS['ns_separator']

  def ns_disable(self):
    self.NS['nsmap'] = self.NSMAP
    self.NS['ns_separator'] = self.NS_SEPARATOR
    self.NSMAP = ''
    self.NS_SEPARATOR = ''
    
  def l_start(self, tag, selfclose = False, **kwargs):
    attr = ''
    for k, v in kwargs.iteritems():
      attr += ' ' + k + '="' + cgi.escape(unicode(v)) + '"'
    if selfclose:
      tag_end = ' />'
    else:
      tag_end = '>'
    self.XML += "<%s%s%s%s%s"%(self.NSMAP, self.NS_SEPARATOR, tag, attr, tag_end)
    if self.DEBUG:
      self.XML += "\n"
    if not selfclose:
      self.OPEN_TAGS.append(tag)
  
  def l_end(self, tag):
    self.XML += "</%s%s%s>"%(self.NSMAP, self.NS_SEPARATOR, tag)
    if self.DEBUG:
      self.XML += "\n"
    self.OPEN_TAGS.remove(tag)
  
  def l_text(self, s):
    self.XML += cgi.escape( unicode(s) )
    if self.DEBUG:
      self.XML += "\n"
  
  def l_add(self, tag, text, **kwargs):
    selfclose = (text == '')
    self.l_start(tag, selfclose, **kwargs)
    if not selfclose:
      self.l_text( text )
      self.l_end(tag)
  
  def cleanup(self):
    if self.OPEN_TAGS:
      self.OPEN_TAGS.reverse()
    while( len(self.OPEN_TAGS) ):
      self.l_end(self.OPEN_TAGS[0])
  
  def add_by_template(self, values):
    self.XMLBuffer.append( self.Template % values )

  def template(self, tags):
    xml = self.XML
    self.XML = '' 
    close_tags = []
    for tag_properties in tags:
      tag = tag_properties.keys()[0]
      attrs = tag_properties[tag]
      has_children = attrs['has_children']
      del attrs['has_children']      
      if has_children:
        self.l_start(tag, False, **attrs)
        close_tags.append(tag)
      else:
        self.l_add(tag, '%(' + tag + ')s', **attrs)
    close_tags.reverse()
    for tag in close_tags:
      self.l_end(tag)
    self.Template = self.XML
    self.XML = xml
    return self.Template
   
  def tostring(self, cleanup = False):
    try:
      if cleanup:
        self.cleanup()
      if self.XMLBuffer:
        self.XML += ''.join(self.XMLBuffer)
      sys.stdout.write(self.XML)
      sys.stdout.flush()
      self.XML = ''
      self.XMLBuffer = []
    except Exception as e:
      print e
      sys.exit(1)

