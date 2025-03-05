from dm_server.records.base.base_record import BaseRecord
from dm_server.utils.param_utils import DictUtils


class ProviderRecord:
    def __init__(self, provider_id=None, dt=None, data=None, source_id=None,_id=None, record_type=None):  
        self.provider_id = provider_id
        self._id = _id
        self.source_id = source_id
        self.upload_time = None
        self.data = data or {}
        self.datetime = dt
        self.record_type = record_type

    def populate_from_dict(self, di):
        super().populate_from_dict(di)
        self.provider_id = DictUtils.get_value(di, 'provider_id', self.provider_id)
        self._id = DictUtils.get_value(di, '_id', self._id)
        self.source_id = DictUtils.get_value(di, 'source_id', self.source_id)
        self.data = DictUtils.get_value(di, 'data', self.data)
        self.datetime = DictUtils.get_datetime(di, 'datetime', self.datetime)
        self.record_type = DictUtils.get_value(di, 'record_type', self.record_type)
        return self

    def to_dict(self):
        ret = {}
        DictUtils.set_value(ret, '_id', self._id)

        DictUtils.set_value(ret, 'record_type', self.record_type)
        DictUtils.set_value(ret, 'provider_id', self.provider_id)
        DictUtils.set_value(ret, 'source_id', self.source_id)
        DictUtils.set_value(ret, 'data', self.data)
        DictUtils.set_datetime(ret, 'datetime', self.datetime)
        return ret

    def get_value(self):
        return self.data

  

    def save_to_db(self):
        self.populate_meta()
        if not self._id:
            self._id = self.get_id()
        col = self.get_record_collection()
        col.update_one({'_id': self._id}, {'$set': self.to_dict()}, upsert=True)

    def get_id(self):
        raise NotImplementedError
    
    def get_record_collection(self):
        raise NotImplementedError
    
    def populate_meta(self):
        pass
