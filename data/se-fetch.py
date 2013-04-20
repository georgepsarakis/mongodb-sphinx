#!/usr/bin/python
import os, sys
import time 
import json
import codecs

fetch_url = r'wget -q -O- --header\="Accept-Encoding: gzip" "%(url)s" | gunzip > %(json)s'

url = 'http://api.stackexchange.com/2.1/posts?page=%d&pagesize=100&order=desc&sort=activity&site=stackoverflow'

''' Download 20x100 posts (questions & answers) from StackOverflow '''
fw = codecs.open('data.json', mode = 'w', encoding = 'utf-8')
for n in range(1, 21):
  print 'Downloading page', n
  cmd = fetch_url % { 'url' : url % n, 'json' : 'page.%d.json' % n }
  print cmd
  os.system(cmd)
  print 'Formatting JSON data for import ...'
  f = codecs.open('page.%d.json' %n, mode = 'r', encoding = 'utf-8')
  d = json.loads(''.join(f.readlines()))
  f.close()
  os.system("rm 'page.%d.json'" % n)
  for item in d['items']:
    fw.write(json.dumps(item) + "\n")
  time.sleep(10.0)

fw.close()
