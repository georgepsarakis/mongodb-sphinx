mongodb-sphinx
==============

MongoDB fulltext search implementation using the xmlpipe2 interface of the Sphinx fulltext search engine.

Tested with Sphinx v2.04, check the documentation on the xmlpipe2 interface http://sphinxsearch.com/docs/manual-2.0.4.html#xmlpipe2

Package Dependencies
--------------------
* argparse
* pymongo

CHANGELOG
---------

* 20/4/2013
    - Improved command line parameters
    - XML Flushing on each _--step_
    - Code Cleanup
    - OOP
    - Speed Optimization
    - Sample JSON Data Download from StackOverflow API
    - Sample JSON Data Importer to MongoDB


* 22/4/2013
    - Added multiple connection support. Needs testing on multi-core system.

TODO
----
Priority tasks are marked as bold:

* __Read parameters from the Sphinx configuration file (e.g. the indexer step, fields & attributes etc)__
* __Support multiple collections__
* __Unit Tests__
* __Create appropriate wrapper of sphinxapi.py for fetching search results__
* __Test with larger collections__
* MVA support
