from datetime import datetime, timedelta
import pprint
from tqdm import tqdm
from miniagro.config.server_config import ServerConfig
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.ext_api.atom_api import AtomApi
from miniagro.utils.db_utils import DBUtils
from miniagro.utils.param_utils import DictUtils


class StreamRecord:
    def __init__(self, source_id=None, name=None, index_name=None, datetime=None, atom_id=None, data=None):
        self.source_id = source_id
        self.index_name = index_name
        self.datetime = datetime
        self.atom_id = atom_id
        self.data = data
        self.name = name
        

    def to_dict(self):
        return {
            '_id': self.get_id(),
            'source_id': self.source_id,
            'index_name': self.index_name,
            'datetime': self.datetime,
            'atom_id': self.atom_id,
            'data': self.data,
            'name': self.name
        }
    def get_id(self):
        return self.source_id + '_' + self.datetime.strftime('%Y%m%d%H%M%S')
   
    def save_to_db(self, col):
        col.update_one({'_id': self.get_id()}, {'$set': self.to_dict()}, upsert=True)

class StaticRecord(StreamRecord):

    @classmethod
    def create_from_stream_rec(cls, stream_rec):
        return cls(source_id=stream_rec.source_id, 
                   name=stream_rec.name, index_name=stream_rec.index_name, 
                   datetime=stream_rec.datetime,
                     atom_id=stream_rec.atom_id, 
                     data=stream_rec.data)
    
    def __init__(self, source_id=None, name=None, index_name=None, datetime=None, atom_id=None, data=None):
        super().__init__(source_id, name, index_name, datetime, atom_id, data)
    
    def get_id(self):
        return self.source_id

class AtomDownloader:
    def __init__(self, supports_stream=False, supports_static=False):
        self.atom_api = AtomApi()
        self.server = ServerConfig.get_self()
        self.is_admin = self.server.is_admin
        self.supports_stream = supports_stream
        self.supports_static = supports_static
        self.source_map = {}
        self.force_save = False
    def get_server_source_ids(self):
        return self.server.get_source_ids()
    
    def download_data(self, source_ids=None, save=True, full=False):
        if not source_ids:
            source_ids = self.get_server_source_ids()
        else:
            source_ids = self.server.filter_ids(source_ids)
        data = self.download_atom_data(source_ids, full=full)
        stream_recs = None
        static_recs = None
        if self.supports_stream:
            stream_recs = self.prepare_stream_recs(data)
            if save and stream_recs:
                self.save_recs_to_stream(stream_recs)
        if self.supports_static:
            static_recs = self.prepare_static_recs(data, stream_recs)
            if save and static_recs:
                self.save_recs_to_static(static_recs)
          
        return stream_recs, static_recs

    def download_atom_data(self, source_ids=None, save=True, full=False):
        raise NotImplementedError('download_data not implemented')
    
    def add_sources_to_server(self, source_ids):
        self.server.add_ids(source_ids)

    def get_atom_ids(self, source_ids):
        if not self.source_map:
            col = DMSDK().atom_info_db.get_collection('atom_source_raw')
            di = col.find({})
            self.source_map = {r.get('source_id', None): r.get('atom_id', None) for r in di}
        return [self.source_map.get(source_id, None) for source_id in source_ids]
    
    def prepare_static_recs(self, data, stream_recs):
        if not stream_recs:
            stream_recs = self.prepare_stream_recs(data)
        if not stream_recs:
            return None
        final_recs = []
        for source_id, recs in stream_recs.items():
            for rec in recs:
                final_recs.append(StaticRecord.create_from_stream_rec(rec))

        return final_recs
    
    def save_recs_to_stream(self, stream_recs):
        for source_id, recs in stream_recs.items():
            col = self.get_stream_collection(source_id)
            DBUtils.update_bulk_records(col, recs)

        return stream_recs
    
    def save_recs_to_static(self, static_recs, force=False):
        force=force or self.force_save
        col = self.get_static_collection()
        if not force:
            old_recs = list(col.find({}))
            old_recs = {r['source_id']: DictUtils.get_datetime(r, 'datetime', None) for r in old_recs}
            static_recs = [r for r in static_recs if r.datetime > old_recs.get(r.source_id, datetime.min)]
        DBUtils.update_bulk_records(col, static_recs)
        return static_recs

    def get_static_collection(self):
        raise NotImplementedError('get_static_collection not implemented')
    
    def get_stream_collection(self, name):
        raise NotImplementedError('get_stream_collection not implemented')