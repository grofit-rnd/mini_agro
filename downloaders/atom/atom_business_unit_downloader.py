    
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.downloaders.atom.atom_base_downloader import AtomDownloader, StaticRecord
from miniagro.utils.grofit_id_utils import IDUtils


class AtomBusinessUnitDownloader(AtomDownloader):
    def __init__(self):
        super().__init__(supports_stream=False, supports_static=True)
        self.force_save = True
    def get_server_source_ids(self):
        return self.server.get_bu_ids()  
    
    def get_static_collection(self):
        return DMSDK().atom_info_db.get_collection('atom_business_units')
    
    def prepare_static_recs(self, data, stream_recs):
        recs = []
        for r in data:
            source_id = IDUtils.bu_id_to_source_id(r.get('_id', None))
            atom_id = r.get('_id', None)
            name = r.get('name', None)
            if not source_id:
                continue
            recs.append(StaticRecord(source_id=source_id, name=name, index_name='atom_business_units', datetime=None, atom_id=atom_id, data=r))
        return recs
    
    def download_atom_data(self, source_ids=None):
        business_units = self.atom_api.get_business_units()
        if self.is_admin:
            n_source_ids = [IDUtils.bu_id_to_source_id(r['_id']) for r in business_units]
            n_source_ids = list(set(n_source_ids))
            self.add_sources_to_server(n_source_ids)
        else:
            source_ids = self.server.filter_ids(source_ids)
            business_units = [r for r in business_units if r.get('mac', None) in source_ids]
        return business_units