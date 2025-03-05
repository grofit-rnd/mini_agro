from miniagro.db.data_manager_sdk import DMSDK
from miniagro.downloaders.records.provider_record import ProviderRecord
from miniagro.utils.param_utils import DictUtils


class AtomSourceRecord(ProviderRecord):
    @classmethod
    def get_record_collection(self):
        return DMSDK().info_db.get_collection('atom_source_info')
    
    def __init__(self, source_id=None, data=None):
        super().__init__(
            _id=source_id,
            source_id=source_id, provider_id='atomation', data=data, record_type='atom_source_record')
        self.name = None
        self.mac = None
        self.atom_id = None
        self.info = None
        self.raw = None
        self.web = None
        self.dts = {}
   
    def get_id(self):
        return self.source_id
    
    def populate_meta(self):
        dts = {
            'info': DictUtils.get_datetime(self.info, 'last_sample_time_utc', None) if self.info else None,
            'raw': DictUtils.get_datetime(self.raw, 'updatedAt', None) if self.raw else None,
            'web': DictUtils.get_datetime(self.web, 'updatedAt', None) if self.web else None
        }

        # Determine the latest datetime source
        self.datetime, latest_source = max(((dt, key) for key, dt in dts.items() if dt is not None), default=(None, None))
        self.dts = dts
        # Use the latest source to populate primary attributes
        source = getattr(self, latest_source, None) if latest_source else None

        if source:
            self.name = DictUtils.get_value(source, 'name', None)
            self.mac = DictUtils.get_value(source, 'mac', None)

        # Fill missing values from other sources in priority order: info -> raw -> web
        for src in ['info', 'raw', 'web']:
            if getattr(self, src, None):
                data = getattr(self, src)
                self.name = self.name or DictUtils.get_value(data, 'name', None)
                self.mac = self.mac or DictUtils.get_value(data, 'mac', None)
                if src == 'raw' or src == 'web':  # Only `raw` has `atom_id`
                    self.atom_id = self.atom_id or DictUtils.get_value(data, '_id', None)

        return self

    
    def add_info(self, di):
        self.info = di
        return self
    
    def add_raw(self, di):
        self.raw = di
        return self
    
    def add_web(self, di):
        self.web = di
        return self
    
    def populate_from_dict(self, di):
        super().populate_from_dict(di)
        self.add_info(di['info'])
        self.add_raw(di['raw'])
        self.add_web(di['web'])
        self.populate_meta()
        return self

    def to_dict(self):
        ret = super().to_dict()
        DictUtils.set_value(ret, 'info', self.info)
        DictUtils.set_value(ret, 'raw', self.raw)
        DictUtils.set_value(ret, 'web', self.web)
        DictUtils.set_value(ret, 'dts', self.dts)
        DictUtils.set_value(ret, 'datetime', self.datetime)
        DictUtils.set_value(ret, 'name', self.name)
        DictUtils.set_value(ret, 'mac', self.mac)
        DictUtils.set_value(ret, 'atom_id', self.atom_id)
        return ret
    