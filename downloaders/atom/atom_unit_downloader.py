    
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.downloaders.atom.atom_base_downloader import AtomDownloader, StaticRecord
from miniagro.utils.grofit_id_utils import IDUtils
from miniagro.utils.param_utils import DictUtils



class AtomUnitDownloader(AtomDownloader):
    def __init__(self):
        super().__init__(supports_stream=False, supports_static=True)

    def get_static_collection(self):
        return DMSDK().atom_info_db.get_collection('atom_units')
    
    def get_server_source_ids(self):
        return self.server.get_unit_ids()
    
    def prepare_static_recs(self, data, stream_recs):
        recs = []
        for r in data:
            source_id = IDUtils.unit_id_to_source_id(r.get('_id', None))
            atom_id = r.get('_id', None)
            name = r.get('name', None)
            dt = DictUtils.get_datetime(r, 'updatedAt', None)
            if not source_id or not dt:
                continue
            recs.append(StaticRecord(source_id=source_id, name=name, index_name='atom_units', datetime=dt, atom_id=atom_id, data=r))
        return recs
    
    # def get_stream_collection(self, name):
    #     return DMSDK().atom_source_db.get_collection(name, 'atom_units')
    
    def get_new_source_ids(self, data):
        return [IDUtils.unit_id_to_source_id(r['_id']) for r in data]
    
    def download_atom_data(self, source_ids=None):
        if not source_ids and not self.is_admin:
            return None
        
        recs = self.atom_api.get_units()
        if self.is_admin:
            n_source_ids = [IDUtils.unit_id_to_source_id(r['_id']) for r in recs]
            n_source_ids = list(set(n_source_ids))
            self.add_sources_to_server(n_source_ids)
        else:
            source_ids = self.server.filter_ids(source_ids)
            recs = [r for r in recs if r.get('mac', None) in source_ids]
        return recs
    