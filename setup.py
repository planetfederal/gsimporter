#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name = "gsimporter",
    version = "1.0.0",
    description = "GeoServer Importer Client",
    keywords = "GeoServer Importer",
    license = "MIT",
    url = "https://github.com/opengeo/gsimporter",
    author = "Ian Schneider",
    author_email = "ischneider@opengeo.org",
    install_requires = [
        'httplib2',
    ],
    tests_require = [
        'gisdata>=0.5.4',
        'gsconfig>=0.6.3',
        'psycopg2',
        'OWSLib>=0.7.2',
        'unittest2',
    ],
    package_dir = {'':'src'},
    packages = find_packages('src'),
    test_suite = 'test.uploadtests'
)

