import pymongo
from pymongo import DESCENDING, MongoClient, ASCENDING, InsertOne, UpdateOne
from pymongo.errors import BulkWriteError

from miniagro.db.gdb_plugin import GDBPlugin


class AtomInfoPlugin(GDBPlugin):
    def __init__(self, host=None, port=None):
        db_name = 'atom_info_db'
        super().__init__(host, port, db_name)
        self.col_names = ['atom_source_info', 'atom_units', 'atom_business_units', 'atom_users', 
                          'atom_gw_data', 'atom_events', 'atom_source_web', 'atom_source_raw',
                          'atom_gw_data', 'atom_sensor_raw', 'atom_expected_monitor'
                          ]
        self.prepare_collections()
    
    def get_indexes(self, key):
        if key == 'atom_source_info' or key == 'atom_source_web' or key == 'atom_source_raw' or key == 'atom_gw_data' or key == 'atom_sensor_raw' or key == 'atom_events':
            return [dict(name='source_idx', fields=[('source_id', ASCENDING)], options={}),
                    dict(name='datetime_idx', fields=[('datetime', ASCENDING)], options={}),
                    ]
        if key == 'atom_units' or key == 'atom_business_units' or key == 'atom_users':
            return [dict(name='source_idx', fields=[('source_id', ASCENDING)], options={}),
                    ]
        if key == 'atom_expected_monitor':
            return [dict(name='source_idx', fields=[('source_id', ASCENDING)], options={}),
                    dict(name='monitor_idx', fields=[('monitor_id', ASCENDING)], options={}),
                    ]


class DMInfoPlugin(GDBPlugin):

    def __init__(self, host=None, port=None, db_name=None, config=None):
        db_name = 'grofit_info_db'

        super().__init__(host, port, db_name, config)
        self.col_names = [ 'capsule_data','locations', 'report_views', 'dynamic_capsule_info', 'grofit_capsule_node', 'daily_summary_info'
                          ]
        self.prepare_collections()

   
    def get_indexes(self, key):
       
        if key == 'locations':
            return [dict(name='location_idx', fields=[("location.polygon", pymongo.GEOSPHERE)], options={}),
                    ]
        if key == 'report_views':
            return [dict(name='user_id_idx', fields=[("user_id", ASCENDING)], options={}),
                    dict(name='report_idx', fields=[("report_id", ASCENDING)], options={}),
                    ]
        if key == 'grofit_capsule_node':
            return [dict(name='source_idx', fields=[("source_id", ASCENDING)], options={}),
                    dict(name='gw_idx', fields=[("gw_id", ASCENDING)], options={}),
                    ]
        if key == 'dynamic_capsule_info':
            return [dict(name='source_idx', fields=[("source_id", ASCENDING)], options={}),
                    dict(name='gw_idx', fields=[("gw_id", ASCENDING)], options={}),

                    ]
        if key == 'daily_summary_info':
            return [dict(name='source_idx', fields=[("source_id", ASCENDING)], options={}),
                    dict(name='datetime_idx', fields=[("datetime", ASCENDING)], options={}),
                    ]
        else:
            return []
