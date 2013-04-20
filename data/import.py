#!/usr/bin/python
import os, sys

mongoimport = '''mongoimport --host localhost --db stackoverflow
                 --collection posts  --type json --file %s'''
mongoimport = mongoimport.replace("\n", ' ')

os.system(mongoimport % ('data.json',))
