from pymongo import ASCENDING, MongoClient

from miniagro.utils.mongo_index_utils import MongoIndexUtils



class MultiDBPlugin:
    def __init__(self, host=None ,port=None):
        self.databases = {}
        self.collections = {}
        self.host = host 
        self.port = port
        print('host', self.host, 'port', self.port)
        # self.db_name = db_name or DictUtils.get_value(config, 'db_name', None)
        # if not self.db_name:
        #     raise Exception('db name not provided for plugin', self.__class__.__name__)
        # self.db = MongoClient(self.host, self.port)[self.db_name]
        self.prepare_collections()

    def prepare_collections(self):
       pass
    
    def add_db(self, db_name):
        if db_name not in self.databases:
            self.databases[db_name] = {'db': MongoClient(self.host, self.port)[db_name], 'collections': {}}
        return self.databases[db_name]
    
    def add_collection(self,name, db_name, indexes=None):
        db = self.databases[db_name]
        if name in db['collections'] and db['collections'][name] is not None:
            return db['collections'][name]
        if indexes:
            MongoIndexUtils.create_indexes_if_needed(db['db'], name, indexes)
        db['collections'][name] = db['db'][name]
        # print('added collection', name, 'to', self.db_name, 'for', self.__class__.__name__)
        return db['collections'][name]

    def get_collection(self, name, db_name):
        db = self.databases[db_name]
        if name in db['collections']:
            return db['collections'][name]

        col = self.add_dynamic_collection(name, db_name)
        if col is None:

            raise Exception('collection not found', name, db_name, 'for', self.__class__.__name__)
        return col

    def add_dynamic_collection(self, name, db_name):
        if not db_name or not name:
            raise Exception('no db name or name')
        if db_name not in self.databases:
           print('db not found', db_name)
           print('available dbs', self.databases.keys())
           raise Exception('db not found', db_name)

        dbi = self.databases[db_name]
        if name not in dbi['collections']:
            dbi['collections'][name] = dbi['db'][name]
            indexes = self.get_indexes(db_name, name)
            if indexes:
                MongoIndexUtils.create_indexes_if_needed(dbi['db'], name, indexes)
            dbi['collections'][name] = dbi['db'][name] 
        return dbi['collections'][name]
   

    def get_source_collection(self, source_id, db_name):
        return self.add_dynamic_collection(source_id, db_name)

    
    def get_indexes(self, db_name, key):
        if db_name == 'atom_source_web' or db_name == 'atom_source_raw' or db_name == 'atom_source_info':
            return [dict(name='source_idx', fields=[('source_id', ASCENDING)], options={}),
                    dict(name='datetime_idx', fields=[('datetime', ASCENDING)], options={}),
                    ]
        if db_name == 'atom_sensor_raw':
            return [dict(name='source_idx', fields=[('source_id', ASCENDING)], options={}),
                    dict(name='datetime_idx', fields=[('datetime', ASCENDING)], options={}),
                    ]
      


class AtomSourceRecordPlugin(MultiDBPlugin):
    def __init__(self, host=None, port=None):
        super().__init__(host, port)
        self.add_db('atom_source_web')
        self.add_db('atom_source_raw')
        self.add_db('atom_source_info')
        self.add_db('atom_gw_data')
        self.add_db('atom_sensor_raw')



