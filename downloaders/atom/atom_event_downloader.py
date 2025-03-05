    
from datetime import datetime, timedelta

import tqdm
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.downloaders.atom.atom_source_raw_downloader import AtomDownloader, StaticRecord, StreamRecord
from miniagro.utils.db_utils import DBUtils
from miniagro.utils.grofit_id_utils import IDUtils
from miniagro.utils.param_utils import DictUtils




class AtomEventDownloader(AtomDownloader):
    def __init__(self, start=None, end=None):
        super().__init__(supports_stream=False, supports_static=True)
        self.start = start
        self.end = end
        if not self.start:
            self.start = datetime.utcnow() - timedelta(days=10)
        if not self.end:
            self.end = datetime.utcnow()
        if isinstance(self.start, str):
            self.start = datetime.strptime(self.start, '%Y-%m-%d %H:%M:%S')
        if isinstance(self.end, str):
            self.end = datetime.strptime(self.end, '%Y-%m-%d %H:%M:%S')
        if self.start > self.end:
            raise Exception('start date is greater than end date')

    def get_static_collection(self):
        return DMSDK().atom_info_db.get_collection('atom_events')

    def save_recs_to_static(self, static_recs, force=False):
        col = self.get_static_collection()
        DBUtils.update_bulk_records(col, static_recs)
        return static_recs
    
    def prepare_static_recs(self, data, stream_recs):
        recs = []
        for r in data:
            source_id = IDUtils.gd_mac_to_source_id(r.get('mac', None))
            atom_id = r.get('_id', None)
            name = r.get('name', None)
            dt = DictUtils.get_datetime(r, 'event_time_utc', None)
            if not source_id or not dt:
                continue
            recs.append(StreamRecord(source_id=source_id, name=name, index_name='atom_events', datetime=dt, atom_id=atom_id, data=r))
        return recs
    
    def download_data(self, source_ids=None, save=True):
        if not source_ids:
            source_ids = self.get_server_source_ids()
        else:
            source_ids = self.server.filter_ids(source_ids)
        data = self.download_atom_data(source_ids)
        static_recs = self.prepare_static_recs(data, None)
        if save and static_recs:
            self.save_recs_to_static(static_recs)
          
        return None, static_recs

    def download_atom_data(self, source_ids=None):
        records = []
        source_ids = source_ids[:100]
        for i in tqdm(range(0, len(source_ids), 100), desc='getting atom events', total=len(source_ids)//100):
            recs = self.atom_api.download_events(macs=source_ids[i:i+100], start=self.start, end=self.end)
            records.extend(recs)
        return records