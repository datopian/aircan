import unittest

from sqlalchemy import create_engine

import tests.data_loader_config as config
import tests.data_loader_load as load


@unittest.skip("Need Environment to Run, these tests are mocked in other tests")
class LoadTest(unittest.TestCase):

    def setUp(self):
        self.data_resource = {
            'path': '100kb.csv',
            'ckan_resource_id': '5d1af90f-cc57-4271-a990-1116dd1a400e',
            'schema': {
               'fields': [
                    {
                        'name': 'first_name',
                        'type': 'string'
                    },
                    {
                        'name': 'last_name',
                        'type': 'string'
                    },
                    {
                        'name': 'email',
                        'type': 'string'
                    },
                    {
                        'name': 'gender',
                        'type': 'string'
                    },
                    {
                        'name': 'ip_address',
                        'type': 'string'
                    },
                    {
                        'name': 'date',
                        'type': 'string'
                    }
                ]
            }
        }
        self.config = config.get_config()

        engine = create_engine(self.config['CKAN_DATASTORE_WRITE_URL'])
        self.conn = engine.raw_connection()
        columns_string = ', '.join(
            ['%s TEXT' % (f['name'])
             for f in self.data_resource['schema']['fields']]
        )
        self.create_cmd = '''CREATE TABLE IF NOT EXISTS "%s" (%s);
        ''' % (self.data_resource['ckan_resource_id'], columns_string)

    def test___delete_datastore_table(self):
        res = load.delete_datastore_table(self.data_resource, self.config)
        self.assertTrue(res['success'])

    def test___delete_datastore_table_returns_success_false_if_not_authorized(self):
        self.config['CKAN_SYSADMIN_API_KEY'] = 'invalid'
        res = load.delete_datastore_table(self.data_resource, self.config)
        self.assertFalse(res['success'])

    def test___delete_datastore_table_returns_success_false_if_smth_goes_wrong(self):
        self.config['CKAN_SITE_URL'] = 'invalid'
        res = load.delete_datastore_table(self.data_resource, self.config)
        self.assertFalse(res['success'])

    def test___create_datastore_table(self):
        res = load.create_datastore_table(self.data_resource, self.config)
        self.assertTrue(res['success'])

    def test___create_datastore_table_returns_success_false_if_not_authorized(self):
        self.config['CKAN_SYSADMIN_API_KEY'] = 'invalid'
        res = load.create_datastore_table(self.data_resource, self.config)
        self.assertFalse(res['success'])

    def test___create_datastore_table_returns_success_false_if_smth_goes_wrong(self):
        self.config['CKAN_SITE_URL'] = 'invalid'
        res = load.create_datastore_table(self.data_resource, self.config)
        self.assertFalse(res['success'])

    @unittest.skip('Postgres not supportet yet')
    def test___delete_index(self):
        cur = self.conn.cursor()
        cur.execute(self.create_cmd)
        res = load.delete_index(self.data_resource, self.config, self.conn)
        self.assertTrue(res['success'])

    @unittest.skip('Postgres not supportet yet')
    def test___restore_indexes_and_set_datastore_active(self):
        cur = self.conn.cursor()
        cur.execute(self.create_cmd)
        res = load.restore_indexes_and_set_datastore_active(
            self.data_resource, self.config, self.conn)
        self.assertTrue(res['success'])

    @unittest.skip('Postgres not supportet yet')
    def test___load_csv_to_postgres_via_copy(self):
        cur = self.conn.cursor()
        cur.execute(self.create_cmd)
        res = load.load_csv_to_postgres_via_copy(
            self.data_resource, self.config, self.conn)
        self.assertTrue(res['success'])

    @unittest.skip('Postgres not supportet yet')
    def test___load_csv_to_postgres_via_copy_data_is_in_db(self):
        cur = self.conn.cursor()
        cur.execute(self.create_cmd)
        # Check Table is empty
        cur.execute('SELECT COUNT(*) FROM "%s"' % self.data_resource['ckan_resource_id'])
        res = cur.fetchall()
        self.assertEqual(res[0][0], 0)

        load.load_csv_to_postgres_via_copy(self.data_resource, self.config, self.conn)
        cur.execute('SELECT COUNT(*) FROM "%s"' % self.data_resource['ckan_resource_id'])
        res = cur.fetchall()
        self.assertGreater(res[0][0], 0)

    def tearDown(self):
        cur = self.conn.cursor()
        cur.execute('DROP TABLE IF EXISTS "%s"' % self.data_resource['ckan_resource_id'])
        cur.close()
        self.conn.commit()
