gsimporter
==========

gsimporter is a python library for using GeoServer's importer API.

Installing
==========

Once in pypi, put stuff here.

Getting Help
============

Need to host the manual. And put other details here.

Running Tests
=============

The tests are integration tests. These require having a running GeoServer instance with the community/importer modules installed. Because some of the tests use a postgres database, a data base is required to run.

The test suite will first attempt to verify a connection to GeoServer and a connection to the database. If the default values are not appropriate, provide them via environment variables on the command line or via `export`. For example::

  DB_DATASTORE_DATABASE=my_test_db python tests.py

A convenient way to deal with connection settings (besides setting things up to use the defaults) is to put them all in a bash (or other shell) script.

The tests are designed to create a workspace named `importer` and `importer2` for use in testing. `importer` will be set to the default workspace. As much as possible, things are cleaned up after test execution. This can be skipped by adding the command line argument `