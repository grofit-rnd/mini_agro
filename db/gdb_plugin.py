from pymongo import MongoClient

from miniagro.utils.mongo_index_utils import MongoIndexUtils
from miniagro.utils.param_utils import DictUtils



class GDBPlugin:
    def __init__(self, host=None ,port=None, db_name=None, config=None):
        self.col_names = []
        self.collections = {}
        self.host = host or DictUtils.get_value(config, 'host', None)
        if not port:
            port = DictUtils.get_value(config, 'port_override', None)
        print(config)
        self.port = port or DictUtils.get_value(config, 'port', None)
        print('host', self.host, 'port', self.port)
        self.db_name = db_name or DictUtils.get_value(config, 'db_name', None)
        if not self.db_name:
            raise Exception('db name not provided for plugin', self.__class__.__name__)
        self.db = MongoClient(self.host, self.port)[self.db_name]
        self.prepare_collections()

    def prepare_collections(self, col_names = []):
        col_names = col_names or self.col_names
        if not col_names:
            return
        for k in col_names:
            self.add_collection(k, self.get_indexes(k))


    def add_collection(self, name, indexes=None):
        if name in self.collections and self.collections[name] is not None:
            return self.collections[name]
        if indexes:
            MongoIndexUtils.create_indexes_if_needed(self.db, name, indexes)
        self.collections[name] = self.db[name]
        return self.collections[name]

    def get_collection(self, name):

        if name in self.collections:
            return self.collections[name]

        col = self.add_dynamic_collection(name)
        if col is None:

            raise Exception('collection not found', name, self.db_name, 'for', self.__class__.__name__)
        return col

    def get_indexes(self, key):
        return []

    def add_dynamic_collection(self, name):
        return None
