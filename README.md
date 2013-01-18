mongodb-sphinx
==============

MongoDB fulltext search implementation using the xmlpipe2 interface of the Sphinx fulltext search engine

Tested with Sphinx v2.04, check the documentation on the xmlpipe2 interface http://sphinxsearch.com/docs/manual-2.0.4.html#xmlpipe2

Package Dependencies
--------------------
* argparse
* pymongo

TODO
----
Priority tasks are marked as bold:

* __Read parameters from the Sphinx configuration file (e.g. the indexer step, fields & attributes etc)__
* __Decomposed IDs: could they be stored in an MVA__
* More logging/debugging info
* __Support multiple collections__
* __Tests__
* __Create appropriate wrapper of sphinxapi.py for fetching search results__
* Sample BSON dump
* MVA support
