import cgi

class xmldoc():
  """
    A simple class for generating XML documents
  """
  XML = ''
  NSMAP = False
  ENC = 'UTF-8'
  NS_SEPARATOR = ''
  NS = { 'nsmap' : NSMAP, 'ns_separator' : NS_SEPARATOR }
  OPEN_TAGS = []

  def __init__(self, **kwargs):
    if 'nsmap' in kwargs:
      self.NSMAP = kwargs['nsmap']
      self.NS_SEPARATOR = ':'
    if 'enc' in kwargs:
      self.ENC = kwargs['enc']
    self.XML = "<?xml version=\"1.0\" encoding=\"" + self.ENC + "\"?>\n"
  
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
    self.XML += "<%s%s%s%s%s\n"%(self.NSMAP, self.NS_SEPARATOR, tag, attr, tag_end)
    if not selfclose:
      self.OPEN_TAGS.append(tag)
  
  def l_end(self, tag):
    self.XML += "</%s%s%s>\n"%(self.NSMAP, self.NS_SEPARATOR, tag)
    self.OPEN_TAGS.remove(tag)
  
  def l_text(self, s):
    self.XML += cgi.escape( unicode(s) ) + "\n"
  
  def l_add(self, tag, text, **kwargs):
    if text == '':
      selfclose = True
    else:
      selfclose = False
    self.l_start(tag, selfclose, **kwargs)
    if not selfclose:
      self.l_text( text )
      self.l_end(tag)
  
  def cleanup(self):
    if self.OPEN_TAGS:
      self.OPEN_TAGS.reverse()
    while( len(self.OPEN_TAGS) ):
      self.l_end(self.OPEN_TAGS[0])
   
  def tostring(self):
    self.cleanup()
    print self.XML.encode('utf-8')

