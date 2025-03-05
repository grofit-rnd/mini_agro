


from miniagro.db.data_manager_sdk import DMSDK
from miniagro.downloaders.records.provider_record import ProviderRecord
from miniagro.utils.param_utils import DictUtils


class ProviderSourceRecord(ProviderRecord):
    @classmethod
    def get_record_collection(self):
        return DMSDK().info_db.get_collection('provider_source_info')
  
    @classmethod
    def get_from_db(cls, source_id):
        col = cls.get_record_collection()
        di = col.find_one({'_id': source_id})
        if not di:
            return None
        return cls().populate_from_dict(di)

    @classmethod
    def create_from_dict(cls, di):
        record_type = DictUtils.get_value(di, 'record_type', None)
        record_sub_type = DictUtils.get_value(di, 'record_sub_type', None)

        if record_type == 'provider_source_record':
            if record_sub_type:
                if 'atomation' in record_sub_type:
                    from dm_server.downloaders.providers.atomation.records.atom_source_record import AtomSourceRecord
                    return AtomSourceRecord().populate_from_dict(di)
                if 'ims' in record_sub_type:
                    from dm_server.downloaders.providers.ims.records.ims_source_record import IMSSourceRecord
                    return IMSSourceRecord().populate_from_dict(di)
        raise Exception(f'Unknown record type {record_type} {record_sub_type} {di}')

    def __init__(self, source_id=None,  provider_id=None, stream_id=None, dt=None, data=None, record_sub_type=None, name=None):
        super().__init__(_id=source_id, record_type='provider_source_record', record_sub_type=record_sub_type,
                                                   stream_id=stream_id, dt=dt, data=data,
                                                    source_id=source_id,
                         provider_id=provider_id)
        self.name = name
        self.mac = None
        self.location = Location()

    def populate_from_dict(self, di):
        super().populate_from_dict(di)
        self.name = DictUtils.get_value(di, 'name', self.name)
        self.mac = DictUtils.get_value(di, 'mac', self.mac)
        self.location = self.location.populate_from_dict(DictUtils.get_value(di, 'location', {}))
        return self

    def to_dict(self):
        ret = super().to_dict()
        DictUtils.set_value(ret, '_id', self.source_id)
        DictUtils.set_value(ret, 'name', self.name)
        DictUtils.set_value(ret, 'mac', self.mac)
        DictUtils.set_value(ret, 'location', self.location.to_dict())
        return ret

    def to_ds_record(self):
        raise NotImplementedError


    def save_to_db(self):
        col = self.get_record_collection()
        col.update_one({'_id': self.source_id}, {'$set': self.to_dict()}, upsert=True)

