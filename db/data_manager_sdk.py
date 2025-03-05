import os

from miniagro.db.admin_db import AdminPlugin

from miniagro.db.info_db import AtomInfoPlugin, DMInfoPlugin
from miniagro.db.multi_db_source_plugin import AtomSourceRecordPlugin
from miniagro.db.record_db import RecordDBPlugin
from miniagro.db.sensor_db import SensorDBPlugin
# from miniagro.db.source_meta_db import DMSourceRecordPlugin


class DMSDK:
    _instance = None

    def __new__(cls,):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init()
        return cls._instance

    def _init(self, port_override=None):
        self.admin_db = AdminPlugin(port=port_override)
        self.info_db = DMInfoPlugin(port=port_override)
        self.record_db = RecordDBPlugin(port=port_override)
        self.sensor_db = SensorDBPlugin(port=port_override)
        self.atom_source_db = AtomSourceRecordPlugin(port=port_override)
        self.atom_info_db = AtomInfoPlugin(port=port_override)

if __name__ == '__main__':
    DMSDK()
