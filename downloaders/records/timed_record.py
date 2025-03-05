    
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.utils.db_utils import DBUtils


class AtomTimedRecord:
    def __init__(self, source_id, index_name, datetime=None, atom_id=None, mac=None, data=None):
        self._id = None
        self.source_id = source_id
        self.index_name = index_name
        self.datetime = datetime
        self.atom_id = atom_id
        self.data = data
        self.mac = mac
    
    def get_id(self):
        if not self.source_id or not self.datetime:
            return None
        return self.source_id + self.datetime.strftime('%Y%m%d%H%M%S')
    
    def to_dict(self):
        if not self._id:
            self._id = self.get_id()

        return {
            '_id': self._id,
            'source_id': self.source_id,
            'index_name': self.index_name,
            'datetime': self.datetime,
            'atom_id': self.atom_id,
            'mac': self.mac,
            'data': self.data,
        }


    def save_to_db(self, col):
        if not self.datetime or not self.source_id:
            return
        if not self._id:
            self._id = self.get_id()
        col.update_one({'_id': self._id}, {'$set': self.to_dict()}, upsert=True)

class AtomSourceRecord:
    def __init__(self, source_id, index_name, atom_id=None, mac=None, data=None):
        self.source_id = source_id
        self.index_name = index_name
        self.atom_id = atom_id
        self.mac = mac
        self.data = data

    
    def get_id(self):
        return self.source_id
    
    def save_to_db(self, col):
        if not self.source_id:
            return
        col.update_one({'_id': self._id}, {'$set': self.to_dict()}, upsert=True)

class SensorRecord:
    def __init__(self, source_id, datetime, upload_time, data):
        self.source_id = source_id
        self.datetime = datetime
        self.upload_time = upload_time
        self.data = data

    def get_id(self):
        return self.source_id +'_'+ self.datetime.strftime('%Y%m%d%H%M%S')
    
    def to_dict(self):
        return {
            '_id': self.get_id(),
            'source_id': self.source_id,
            'datetime': self.datetime,
            'upload_time': self.upload_time,
            'data': self.data
        }
    
    def save_to_db(self, col):
        if not self.datetime or not self.source_id:
            return
        if not self._id:
            self._id = self.get_id()
        col.update_one({'_id': self._id}, {'$set': self.to_dict()}, upsert=True)

class LastRecord:
    @staticmethod
    def create_from_config(index_name, config):
        return LastRecord(source_id=config['source_id'], index_name=index_name, datetime=config['datetime'], atom_id=config['atom_id'], data=config['data'])
    
    @classmethod
    def get_collection(cls, index_name):
        return DMSDK().info_db.get_collection(index_name)
    
    @classmethod
    def save_bulk(cls, index_name, records):
        col = cls.get_collection(index_name)
        old_records = list(col.find({}))
        old_records = {rec['source_id']: rec for rec in old_records}
        new_records = []
        for rec in records:
            if rec['source_id'] in old_records:
                if old_records[rec['source_id']]['datetime'] > rec['datetime']:
                    continue
            else:
                new_records.append(rec) 
        DBUtils.update_bulk_records(col, new_records)

    def __init__(self, source_id, index_name, datetime, atom_id, data):
        self.source_id = source_id
        self.index_name = index_name
        self.datetime = datetime
        self.atom_id = atom_id
        self.data = data

    def to_dict(self):
        return {
            '_id': self.source_id,
            'source_id': self.source_id,
            'datetime': self.datetime,
            'atom_id': self.atom_id,
            'data': self.data
        }
    
    def save_to_db(self, force=False):
        if not self.datetime or not self.source_id:
            return
        col = self.get_collection(self.index_name)
        if force:
            col.update_one({'_id': self.source_id}, {'$set': self.to_dict()}, upsert=True)
        else:
            record = col.find_one({'_id': self.source_id})
            if not record or record['datetime'] < self.datetime :  
                col.update_one({'_id': self.source_id}, {'$set': self.to_dict()}, upsert=True)

class CapsuleDataRecord:
    def __init__(self, source_id=None, name=None, mac=None, atom_id=None, source_type=None, location=None):
        self.source_id = source_id
        self.name = name
        self.mac = mac
        self.atom_id = atom_id
        self.source_type = source_type if source_type else 'capsule'
        self.location = location
        self.datetime = None
        self.times = {
            'source_info': None,
            'sensor_time': None,
            'gw_time': None,
            'raw_time': None,
            'web_time': None,
            'info_time': None,
            'event_time': None,
            'upload_time': None,
            'keep_alive_time': None
        }


    def get_id(self):
        return self.source_id
 
    def to_dict(self):
        return {
            'source_id': self.source_id,
            'name': self.name,
            'mac': self.mac,
            'atom_id': self.atom_id,
            'source_type': self.source_type,
            'location': self.location,
            'last_updated_time': self.last_updated_time,
            'last_sensor_time': self.last_sensor_time,
            'last_gw_time': self.last_gw_time,
            'last_raw_time': self.last_raw_time,
            'last_web_time': self.last_web_time,
            'last_info_time': self.last_info_time,
            'last_event_time': self.last_event_time,
            'last_upload_time': self.last_upload_time
        }

    def save_to_db(self):
        DMSDK().info_db.get_collection('capsule_data').update_one({'_id': self.source_id}, {'$set': self.to_dict()}, upsert=True)
