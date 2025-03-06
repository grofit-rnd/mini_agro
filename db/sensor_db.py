from pymongo import MongoClient, ASCENDING, DESCENDING

from miniagro.db.gdb_plugin import GDBPlugin


class SensorDBPlugin(GDBPlugin):

    def __init__(self, host=None, port=None, db_name=None, config=None, col_extra=None):
        db_name = 'grofit_sensor_db'
        super().__init__(host, port, db_name, config)


    def add_dynamic_collection(self, name, index_name):
        if not index_name or not name:
            raise Exception('no index name or name')
        full_name = f'{name}_{index_name}'
        if full_name in self.collections:
            return self.collections[full_name]
        return self.add_collection(full_name, self.get_indexes(index_name))

    def get_source_collection(self, source_id, index_name):
        return self.add_dynamic_collection(source_id, index_name)

    
    def get_indexes(self, key):
        if 'sensor_raw' in key: 

            return [dict(name='source_idx', fields=[('source_id', ASCENDING)], options={}),
                    dict(name='datetime_idx', fields=[('datetime', ASCENDING)], options={}),
                    ]
        if 'sensor_stream' in key:
            return [dict(name='source_idx', fields=[('source_id', ASCENDING)], options={}),
                    dict(name='datetime_idx', fields=[('datetime', ASCENDING)], options={}),
                    ]
        return []