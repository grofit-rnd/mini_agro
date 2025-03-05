    
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.downloaders.atom.atom_base_downloader import AtomDownloader, StaticRecord
from miniagro.utils.grofit_id_utils import IDUtils
from miniagro.utils.param_utils import DictUtils


class AtomUserDownloader(AtomDownloader):
    def __init__(self):
        super().__init__(supports_stream=False, supports_static=True)   

    def get_static_collection(self):
        return DMSDK().atom_info_db.get_collection('atom_users')
    
    def download_atom_data(self, source_ids=None):
        users = self.atom_api.get_users()
        return users
    
    def get_server_source_ids(self):
        return self.server.get_ids('au')

    def get_new_source_ids(self, data):
        return [IDUtils.user_id_to_source_id(r['_id']) for r in data]
    
    def download_atom_data(self, source_ids=None):
        users = self.atom_api.get_users()
        if self.is_admin:
            n_source_ids = [IDUtils.user_id_to_source_id(r['_id']) for r in users]
            n_source_ids = list(set(n_source_ids))
            self.add_sources_to_server(n_source_ids)
        else:
            source_ids = self.server.filter_ids(source_ids)
            users = [r for r in users if r.get('mac', None) in source_ids]
       
        return users
   
    def prepare_static_recs(self, data, stream_recs):
        recs = []
        for r in data:
            source_id = IDUtils.user_id_to_source_id(r.get('_id', None))
            atom_id = r.get('_id', None)
            name = r.get('name', None)
            dt = DictUtils.get_datetime(r, 'updatedAt', None)
            if not source_id:
                continue
            recs.append(StaticRecord(source_id=source_id, name=name, index_name='atom_users', datetime=dt, atom_id=atom_id, data=r))
        return recs
    
if __name__ == '__main__':
    AtomUserDownloader().download_data()
    exit(1)