import pprint
from tqdm import tqdm
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.downloaders.atom.atom_base_downloader import AtomDownloader, StreamRecord
from miniagro.utils.db_utils import DBUtils
from miniagro.utils.grofit_id_utils import IDUtils
from miniagro.utils.param_utils import DictUtils



class AtomSourceRawDownloader(AtomDownloader):
    def __init__(self):
        super().__init__(supports_stream=True, supports_static=True)
    
    def get_stream_collection(self, name):
        return DMSDK().atom_source_db.get_collection(name, 'atom_source_raw')
    
    def get_static_collection(self):
        return DMSDK().atom_info_db.get_collection('atom_source_raw')
    
    def prepare_stream_recs(self, data):
        recs = {}
        for r in data:
            source_id = IDUtils.gd_mac_to_source_id(r.get('mac', None))
            atom_id = r.get('_id', None)
            name = r.get('name', None)
            dt = DictUtils.get_datetime(r, 'updatedAt', None)
            if not source_id or not dt:
                continue
            if source_id not in recs:
                recs[source_id] = []
            recs[source_id].append(StreamRecord(source_id=source_id, name=name, index_name='atom_source_raw', datetime=dt, atom_id=atom_id, data=r))
        return recs
    
   
    
    def download_atom_data(self, source_ids=None, full=False):
        sync_di_list = []
        page = 1
        while True:
            sl = self.atom_api.get_source_info_from_sync_packet(page=page)
            if not sl:
                break
            sync_di_list.extend(sl)
            page += 1
            if len(sl) < 1000:
                break

        if self.is_admin:
            source_ids = [IDUtils.gd_mac_to_source_id(r['mac']) for r in sync_di_list]
            source_ids = list(set(source_ids))
            self.add_sources_to_server(source_ids)
        return sync_di_list


class AtomInfoDownloader(AtomDownloader):
    def __init__(self):
        super().__init__(supports_stream=True, supports_static=True)

    def get_static_collection(self):
        return DMSDK().atom_info_db.get_collection('atom_source_info')
    
    def get_stream_collection(self, name):
        return DMSDK().atom_source_db.get_collection(name, 'atom_source_info')
    

    def prepare_stream_recs(self, data):
        recs = {}
        for r in data:
            source_id = IDUtils.gd_mac_to_source_id(r.get('mac', None))
            name = r.get('name', None)
            dt = DictUtils.get_datetime(r, 'last_sample_time_utc', None)
            if not source_id or not dt:
                continue
            if source_id not in recs:
                recs[source_id] = []
            recs[source_id].append(StreamRecord(source_id=source_id, name=name, index_name='atom_source_raw', datetime=dt, atom_id=None, data=r))
        return recs
    
  
    def download_atom_data(self, source_ids=None, full=False):
        ret = self.atom_api.get_atom_info(source_ids)
        return ret
    
class AtomSourceWebDownloader(AtomDownloader):
    def __init__(self):
        super().__init__(supports_stream=True, supports_static=True)

    def get_stream_collection(self, name):
        return DMSDK().atom_source_db.get_collection(name, 'atom_source_web')
    
    def get_static_collection(self):
        return DMSDK().atom_info_db.get_collection('atom_source_web')

    def prepare_stream_recs(self, data):
        recs = {}
        for r in data:
            source_id = IDUtils.gd_mac_to_source_id(r.get('mac', None))
            name = r.get('name', None)
            dt = DictUtils.get_datetime(r, 'updatedAt', None)
            atom_id = r.get('_id', None)
            rec = StreamRecord(source_id=source_id, name=name, index_name='atom_source_web', datetime=dt, atom_id=atom_id, data=r)
            if source_id not in recs:
                recs[source_id] = []
            recs[source_id].append(rec)
        return recs
    
    def download_atom_data(self, source_ids=None, full=False):
        atom_ids = self.get_atom_ids(source_ids)
        data = []
        for i, atom_id in tqdm(enumerate(atom_ids), desc='downloading source web info', total=len(atom_ids)):
            record = self.atom_api.get_source_web_info(atom_id)
            if not record:
                continue 
            data.append(record)
        
        return data
 
    def save_recs_to_stream(self, stream_recs):
        for source_id, recs in stream_recs.items():
            col = self.get_stream_collection(source_id)
            DBUtils.update_bulk_records(col, recs)
        return stream_recs
    


# class StaticOnlyDownloader(AtomDownloader):
#     def __init__(self, is_manager=False):
#         super().__init__(supports_stream=False, supports_static=True)
#         self.is_manager = is_manager
    
#     def get_new_source_ids(self, data):
#         raise NotImplementedError('get_new_source_ids not implemented')
    
#     def download_data(self, source_ids=None, save=True):
#         if not source_ids:
#             src_ids = self.get_server_source_ids()
#         else:
#             src_ids = self.server.filter_ids(source_ids)
        
#         data = self.download_atom_data(source_ids)
#         if self.is_manager and self.is_admin:
#             n_source_ids = self.get_new_source_ids(data)
#             self.add_sources_to_server(n_source_ids)
#             source_ids = self.server.get_server_source_ids()

#         sv_recs = []
#         static_recs = self.prepare_static_recs(data, None)
#         for rec in static_recs:
#             if rec.source_id in source_ids:
#                 sv_recs.append(rec)
#         if save and sv_recs:
#             self.save_recs_to_static(sv_recs)
        
#         return None, sv_recs

#         return super().download_data(source_ids, save)
#     def download_atom_data(self, source_ids=None):
#         recs = self.atom_api.get_units()
#         return recs



    
if __name__ == '__main__':
    # AtomSourceRawDownloader().download_data()
    # AtomInfoDownloader().download_data()
    # AtomSourceWebDownloader().download_data()
    # AtomGatewayDownloader().download_data(source_ids=['gw_E1_8D_DE_B5_D2_8D'])
    # AtomEventDownloader(start='2025-02-12 00:00:00', end='2025-02-22 23:59:59').download_data()
    # AtomUnitDownloader().download_data()
    # AtomBusinessUnitDownloader().download_data()
    # AtomUserDownloader().download_data()
    pass