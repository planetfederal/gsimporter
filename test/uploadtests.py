import os
import sys

# add all eggs to path to support command line use
if not any(['gsconfig' in p for p in sys.path]):
    dn = os.path.dirname
    for f in os.listdir(dn(dn(os.path.abspath(__file__)))):
        if '.egg' in f:
            sys.path.append(f)

import csv
import contextlib
from geoserver import catalog
import gisdata
from gsimporter import Client
from gsimporter import NotFound
from gsimporter import BadRequest
from gsimporter import _util
from owslib import wms
from pprint import pprint
import psycopg2
import shutil
import socket
import tempfile
import time
import traceback
import unittest2 as unittest

"""
PREPARATION FOR THE TESTS
=========================

1) Create the Postgis user and DB

    $ sudo su - postgres
    $ createuser -P importer
      pw: importer
    $ createdb -O importer importer_test
    $ psql -c "ALTER USER importer WITH SUPERUSER;"
    $ psql -d importer_test -c "create extension postgis;"
    $ psql -d importer_test -c "grant all on spatial_ref_sys to public;"
    $ psql -d importer_test -c "grant all on geometry_columns to public;"

2) Lunch a local instance of GeoServer on 8080 port
3) Run the uploadtests

   $ GEOSERVER_BASE_URL=http://localhost:8080/ python setup.py test

"""


# Setup config
hasflag = lambda f: f in sys.argv and (sys.argv.remove(f) or True)
GEOSERVER_BASE_URL = os.getenv('GEOSERVER_BASE_URL', 'http://localhost:8080')
GEOSERVER_REST_URL = '%s/geoserver/rest' % GEOSERVER_BASE_URL
WORKSPACE = 'importer'
WORKSPACE2 = 'importer2'
SKIP_TEARDOWN = hasflag('--skip-teardown')
DB_CONFIG = dict(
    DB_DATASTORE_DATABASE='importer_test',
    DB_DATASTORE_NAME='importer_test',
    DB_DATASTORE_USER='importer',
    DB_DATASTORE_PASSWORD='importer',
    DB_DATASTORE_HOST='localhost',
    DB_DATASTORE_PORT='5432',
    DB_DATASTORE_TYPE='postgis',
)
DB_CONFIG.update([(k, os.getenv(k)) for k in DB_CONFIG if k in os.environ])
# make this global
DB_DATASTORE_NAME = DB_CONFIG['DB_DATASTORE_NAME']
client = None
gscat = None


# global utilities
def open_db_datastore_connection():
    params = [
        ('dbname', 'DB_DATASTORE_DATABASE'),
        ('user', 'DB_DATASTORE_USER'),
        ('password', 'DB_DATASTORE_PASSWORD'),
        ('port', 'DB_DATASTORE_PORT'),
        ('host', 'DB_DATASTORE_HOST'),
    ]
    return psycopg2.connect(' '.join(
        ["%s='%s'" % (k, DB_CONFIG[v]) for k, v in params]))
try:
    conn = open_db_datastore_connection()
except:
    traceback.print_exc()
    print 'Error connecting to the database, check your settings'
    pprint(DB_CONFIG)
    sys.exit(1)


def drop_table(name):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT DropGeometryTable ('%s')" % name)
    except:
        pass
    conn.commit()


def count_table(name):
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) from "%s"' % name)
    return cursor.fetchone()[0]


def vector_file(name):
    return os.path.join(gisdata.VECTOR_DATA, name)


def raster_file(name):
    return os.path.join(gisdata.RASTER_DATA, name)


def bad_file(name):
    return os.path.join(gisdata.BAD_DATA, name)


def get_wms(name):
    url = '%s/geoserver/wms' % GEOSERVER_BASE_URL
    get_capa = wms.WebMapService(url)
    if len(list(get_capa.contents)) > 0:
        return get_capa[name]
    else:
        return None


@contextlib.contextmanager
def csv_file(data):
    _, fname = tempfile.mkstemp(suffix='.csv')
    with open(fname, 'wb') as fp:
        writer = csv.writer(fp)
        writer.writerows(data)
    yield fname
    os.unlink(fname)


# Test suites
class BaseClientTest(unittest.TestCase):
    '''Basic client/importer tests'''

    def test_create(self):
        session = client.start_import()
        self.assertTrue(session.id >= 0)

    def test_delete(self):
        session = client.start_import()
        session.delete()
        try:
            client.get_session(session.id)
            self.fail('delete did not work')
        except NotFound:
            pass

    def test_create_with_id(self):
        current_id = client.start_import().id
        proposed = current_id + 100
        session = client.start_import(import_id=proposed)
        self.assertEqual(proposed, session.id)
        current_id = proposed

        # now with a lower one, expect incremented
        session = client.start_import(import_id=proposed - 1)
        self.assertEqual(current_id + 1, session.id)

        # and a normal increment
        session = client.start_import()
        self.assertEqual(current_id + 2, session.id)

    def test_transforms(self):
        # just verify client functionality - does it manage them properly
        # at some point, the server might add validation of fields...
        session = client.upload(
            vector_file('san_andres_y_providencia_poi.shp'))
        task, = session.tasks
        att_transform = lambda f, t='AttributeRemapTransform': {
            'type': t,
            'field': f,
            'target': 'java.lang.Integer'
        }

        t1 = att_transform('foo')
        t2 = att_transform('bar')
        t3 = att_transform('baz')

        # this is just to strip off the href :(
        compare_dict = lambda d1, d2: \
            all([d1[k] == d2[k] for k in d1 if k != 'href'])
        compare_list = lambda l1, l2: len(l1) == len(l2) and \
            all([compare_dict(*a) for a in zip(l1, l2)])

        def t(func, transforms, expect, **kwargs):
            task = func.__self__
            func(transforms, **kwargs)
            self.assertEqual(expect, task.transforms)
            task.reload()
            self.assertTrue(compare_list(expect, task.transforms))

        t(task.set_transforms, [t1], [t1])
        t(task.set_transforms, [t1, t2], [t1, t2])
        t(task.add_transforms, [t3], [t1, t2, t3])
        t(task.remove_transforms, ['baz'], [t1, t2], by_field='field')

        try:
            task.set_transforms([att_transform(f='f', t='Error')])
            self.fail('expected BadRequest')
        except BadRequest, br:
            self.assertEqual("Invalid transform type 'Error'", str(br))


class SingleImportTests(unittest.TestCase):
    '''Successful path, single file tests'''

    def setUp(self):
        self.drop_table = None
        self.expected_layer = None

    def tearDown(self):
        if SKIP_TEARDOWN:
            return
        if not self.expected_layer:
            return
        lyr = gscat.get_layer(self.expected_layer)
        try:
            lyr and gscat.delete(lyr) and gscat.delete(lyr.resource)

            if self.drop_table:
                drop_table(self.drop_table)
        except:
            pass

    def run_single_upload(self, vector=None, raster=None, target_store=None,
                          delete_existing=True, async=False, mosaic=False,
                          update_mode=None, change_layer_name=None,
                          expected_srs='', target_srs=None,
                          expect_session_state='COMPLETE',
                          expected_layer=None, workspace=None,
                          transforms=None, expected_atts=None):

        assert vector or raster
        file_func = raster_file if raster else vector_file
        file_name, = filter(None, [vector, raster])
        layer_name, ext = os.path.basename(file_name).rsplit('.', 1)
        file_name = file_func(file_name)
        if expected_layer is None:
            expected_layer = '%s:%s' % (WORKSPACE, layer_name)

        # pre-flight cleanup
        # if update_mode or otherwise specified, don't do any deleting
        if delete_existing and update_mode is None:
            lyr = gscat.get_layer(expected_layer)
            lyr and gscat.delete(lyr)
            drop_table(layer_name)

        # upload and verify state
        print 'uploading %s' % file_name
        session = client.upload(file_name, mosaic=mosaic)
        self.assertEqual(1, len(session.tasks))
        self.assertEqual('PENDING', session.state)
        if update_mode != 'APPEND':
            self.assertEqual(expected_layer,
                             session.tasks[0].get_target_layer_name())
        if expected_srs is None:
            self.assertEqual('NO_CRS', session.tasks[0].state)
        elif expected_srs:
            self.assertEqual(expected_srs, session.tasks[0].srs)

        if target_srs is not None:
            session.tasks[0].layer.set_srs(target_srs)

        if transforms:
            session.tasks[0].set_transforms(transforms)

        if change_layer_name:
            session.tasks[0].layer.set_target_layer_name(change_layer_name)
            session = session.reload()
            self.assertEqual(change_layer_name, session.tasks[0].layer.name)
            self.expected_layer = expected_layer = change_layer_name

        if target_store or workspace:
            session.tasks[0].target.change_datastore(target_store, workspace)
            session = session.reload()
            if target_store:
                self.assertEqual(session.tasks[0].target.name, target_store)
            if workspace:
                self.assertEqual(session.tasks[0].target.workspace_name,
                                 workspace)
            if vector:
                self.drop_table = layer_name
            if not target_store:
                expected_layer = '%s:%s' % (workspace or WORKSPACE, layer_name)

        if update_mode:
            session.tasks[0].set_update_mode(update_mode)

        # run import and poll if required
        session.commit(async=async)
        self.expected_layer = expected_layer
        if async:
            while True:
                time.sleep(.1)
                progress = session.tasks[0].get_progress()
                if progress['state'] == 'COMPLETE':
                    break
                if progress['state'] != 'RUNNING':
                    self.fail('expected async progress state to be RUNNING')

        # verify post import
        session = session.reload()
        self.assertEqual(expect_session_state, session.state)
        lyr = gscat.get_layer(expected_layer)
        self.assertTrue(lyr is not None,
                        msg='Expected to find layer "%s" in the catalog' %
                        expected_layer)
        if expected_atts:
            # @todo cannot check type via gsconfig, only names
            names = set(lyr.resource.attributes)
            self.assertTrue(names.issuperset(expected_atts.keys()))
        return expected_layer

    def test_single_shapefile_upload(self):
        self.run_single_upload(vector='san_andres_y_providencia_poi.shp')

    def test_single_shapefile_change_workspace(self):
        self.run_single_upload(vector='san_andres_y_providencia_poi.shp',
                               workspace=WORKSPACE2)

    def test_single_raster_upload(self):
        self.run_single_upload(raster='relief_san_andres.tif')

    def test_single_raster_upload_change_workspace(self):
        self.run_single_upload(raster='relief_san_andres.tif',
                               workspace=WORKSPACE2)

    def test_upload_to_db(self):
        self.run_single_upload(vector='san_andres_y_providencia_poi.shp',
                               target_store=DB_DATASTORE_NAME)

    @unittest.skip('Currently not handled on server')
    def test_upload_to_db_w_layer_name(self):
        self.run_single_upload(vector='san_andres_y_providencia_poi.shp',
                               target_store=DB_DATASTORE_NAME,
                               change_layer_name='my_layer')

    def test_upload_to_db_append(self):
        self.run_single_upload(vector='san_andres_y_providencia_poi.shp',
                               target_store=DB_DATASTORE_NAME)
        count = count_table('san_andres_y_providencia_poi')
        self.run_single_upload(vector='san_andres_y_providencia_poi.shp',
                               target_store=DB_DATASTORE_NAME,
                               update_mode='APPEND')
        self.assertEqual(count * 2,
                         count_table('san_andres_y_providencia_poi'))

    def test_upload_to_db_replace(self):
        self.run_single_upload(vector='san_andres_y_providencia_poi.shp',
                               target_store=DB_DATASTORE_NAME)
        count = count_table('san_andres_y_providencia_poi')
        self.run_single_upload(vector='san_andres_y_providencia_poi.shp',
                               target_store=DB_DATASTORE_NAME,
                               update_mode='REPLACE')
        self.assertEqual(count, count_table('san_andres_y_providencia_poi'))

    def test_upload_to_db_async(self):
        self.run_single_upload(vector='san_andres_y_providencia_highway.shp',
                               target_store=DB_DATASTORE_NAME, async=True)

    def test_upload_with_bad_files(self):
        shp_files = _util.shp_files(vector_file(
                                    'san_andres_y_providencia_poi.shp'))
        _, junk = tempfile.mkstemp(suffix='.junk')
        try:
            shp_files.append(junk)
            zip_file = _util.create_zip(shp_files)
            ly_name = 'importer:san_andres_y_providencia_poi'
            self.run_single_upload(vector=zip_file,
                                   expected_layer=ly_name)
        finally:
            os.unlink(junk)
            os.unlink(zip_file)

    def test_mosaic(self):
        tmpdir = tempfile.mkdtemp()
        src = raster_file('relief_san_andres.tif')
        fmt = 'relief_san_andres_%s.tif'
        paths = []
        for year in range(2000, 2010):
            path = os.path.join(tmpdir, fmt % year)
            shutil.copy(src, path)
            paths.append(path)
        zip_file = _util.create_zip(paths)
        try:
            layer_name = self.run_single_upload(raster=zip_file, mosaic=True)
            # Avoid Layer DELETE on tearDown
            self.expected_layer = None
        finally:
            os.unlink(zip_file)

        lyr = gscat.get_layer(layer_name)
        self.assertTrue(lyr is not None,
                        msg='Expected to find layer "%s" in the catalog' %
                        layer_name)
        wms = get_wms(layer_name)
        # Importer now creates a default style named importer_{layer_name}
        # get_wms(layer_name) now returns a value
        self.assertTrue(wms is not None,
                             msg='Expected layer "%s" in the WMS GetCap' %
                             layer_name)

    def test_mosaic_granule_update(self):
        tmpdir = tempfile.mkdtemp()
        src = raster_file('relief_san_andres.tif')
        fmt = 'relief_san_andres_%s.tif'
        paths = []
        for year in range(2000, 2010):
            path = os.path.join(tmpdir, fmt % year)
            shutil.copy(src, path)
            paths.append(path)
        zip_file = _util.create_zip(paths)
        try:
            layer_name = self.run_single_upload(raster=zip_file, mosaic=True)
            # Avoid Layer DELETE on tearDown
            self.expected_layer = None
        finally:
            os.unlink(zip_file)

        lyr = gscat.get_layer(layer_name)
        self.assertTrue(lyr is not None,
                        msg='Expected to find layer "%s" in the catalog' %
                        layer_name)

        # Add a new granule to the existing mosaic
        path = os.path.join(tmpdir, fmt % 2011)
        shutil.copy(src, path)
        workspace = layer_name.split(":")[0]
        target_store = layer_name.split(":")[1]
        layer_updated_name = self.run_single_upload(raster=path,
                                                    mosaic=True,
                                                    workspace=workspace,
                                                    target_store=target_store,
                                                    update_mode='APPEND',
                                                    expected_layer=layer_name)

        self.assertTrue(layer_name == layer_updated_name,
                        msg='Expected layer "%s" to be updated' % layer_name)

    def test_csv(self):
        data = (
            ['lat', 'lon', 'date'],
            [1, 5, '2001'],
            [2, 5, '2002'],
            [3, 5, '2003'],
            [4, 5, '2004'],
            [5, 5, '2005'],
        )
        transforms = [{
            'type': 'AttributesToPointGeometryTransform',
            'latField': 'lat',
            'lngField': 'lon',
        }]
        atts = {'location': 'Point', 'date': 'Integer'}
        with csv_file(data) as f:
            self.run_single_upload(vector=f, expected_srs=None,
                                   target_srs='EPSG:4326',
                                   target_store=DB_DATASTORE_NAME,
                                   transforms=transforms,
                                   expected_atts=atts)


class ErrorTests(unittest.TestCase):

    def test_invalid_file(self):
        session = client.upload(bad_file('unsupported_ext.txt'))
        task, = session.tasks
        self.assertEqual('NO_FORMAT', task.state)
        self.assertEqual(None, task.data.format)
        self.assertEqual(None, task.layer)

    def test_invalid_target(self):
        ly_name = 'san_andres_y_providencia_poi.shp'
        session = client.upload(vector_file(ly_name))

        try:
            session.tasks[0].target.change_datastore('foobar')
            self.fail('Expected BadRequest')
        except BadRequest, br:
            self.assertEqual('Unable to find referenced store', str(br))
        except:
            self.fail('Expected BadRequest')


print 'using GEOSERVER_BASE_URL=%s' % GEOSERVER_BASE_URL

# Preflight connection testing
print 'testing access...',
client = Client(GEOSERVER_REST_URL)
gscat = catalog.Catalog(GEOSERVER_REST_URL)
try:
    sessions = client.get_sessions()
    print 'successfully listed imports...',
    ids = [s.id for s in sessions]
    gscat.get_layers()
    print 'successfully listed layers...'
except socket.error, ex:
    print 'error connecting to the server, check your GEOSERVER_BASE_URL'
    print ex
    sys.exit(1)

# handy while testing
if '--clean' in sys.argv:
    print 'cleaning'
    sys.argv.remove('--clean')
    for l in gscat.get_layers():
        res = l.resource
        store = res.store
        if store.workspace.name in (WORKSPACE, WORKSPACE2):
            print 'deleting layer', l.name
            gscat.delete(l)
            gscat.delete(res)
    for s in gscat.get_stores():
        if s.workspace.name in (WORKSPACE, WORKSPACE2):
            print 'deleting store', s.name
            gscat.delete(s)

# Preflight workspace setup
print 'checking for test workspaces...',


def create_ws(name):
    if not any([ws for ws in gscat.get_workspaces() if ws.name == name]):
        print 'creating workspace "%s"...' % name,
        gscat.create_workspace(name, 'http://geoserver.org/%s' % name)
create_ws(WORKSPACE)
create_ws(WORKSPACE2)
print 'done'
print 'setting default workspace to %s...' % WORKSPACE,
# @todo - put this into gsconfig and remove
xml = "<workspace><name>%s</name></workspace>" % WORKSPACE
headers = {"Content-Type": "application/xml"}
workspace_url = gscat.service_url + "/workspaces/default.xml"
headers, response = gscat.http.request(workspace_url, "PUT", xml, headers)
msg = "Tried to change default workspace but got "
assert 200 == headers.status, msg + str(headers.status) + ": " + response
print 'done'

# Preflight DB setup
print 'checking for test DB target datastore...',


def validate_datastore(ds):
    # force a reload to validate the datastore :(
    gscat.http.request('%s/reload' % gscat.service_url, 'POST')
    if not ds.enabled:
        print 'FAIL! Check your datastore settings, the store is not enabled:'
        pprint(DB_CONFIG)
        sys.exit(1)


def create_db_datastore(settings):
    # get or create datastore
    try:
        ds = gscat.get_store(settings['DB_DATASTORE_NAME'])
        validate_datastore(ds)
        return ds
    except catalog.FailedRequestError:
        pass
    print 'Creating target datastore %s ...' % settings['DB_DATASTORE_NAME'],
    ds = gscat.create_datastore(settings['DB_DATASTORE_NAME'])
    ds.connection_parameters.update(
        host=settings['DB_DATASTORE_HOST'],
        port=settings['DB_DATASTORE_PORT'],
        database=settings['DB_DATASTORE_DATABASE'],
        user=settings['DB_DATASTORE_USER'],
        passwd=settings['DB_DATASTORE_PASSWORD'],
        dbtype=settings['DB_DATASTORE_TYPE'])
    gscat.save(ds)
    ds = gscat.get_store(settings['DB_DATASTORE_NAME'])
    validate_datastore(ds)
    return ds
create_db_datastore(DB_CONFIG)
print 'done'
print

if __name__ == '__main__':
    unittest.main()
