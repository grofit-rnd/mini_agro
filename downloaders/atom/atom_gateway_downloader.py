    
import pprint
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.downloaders.atom.atom_base_downloader import AtomDownloader, StaticRecord, StreamRecord
from miniagro.utils.db_utils import DBUtils
from miniagro.utils.grofit_id_utils import IDUtils
from miniagro.utils.param_utils import DictUtils



        
class AtomGatewayDownloader(AtomDownloader):
    def __init__(self):
        super().__init__(supports_stream=True, supports_static=True)   

    def get_server_source_ids(self):
        return self.server.get_gw_ids()
    
    def get_static_collection(self):
        return DMSDK().atom_info_db.get_collection('atom_gw_data')
    
    def get_stream_collection(self, name):
        return DMSDK().atom_source_db.get_collection(name, 'atom_gw_data')
    
    def prepare_stream_recs(self, data):
        recs = {}
        for r in data:
            source_id = IDUtils.gw_uniq_id_to_source_id(r.get('uniq_id', None))
            rec = StreamRecord(source_id=source_id, index_name='atom_gateway', datetime=DictUtils.get_datetime(r, 'updatedAt', None), atom_id=r.get('_id', None), data=r)
            if source_id not in recs:
                recs[source_id] = []
            recs[source_id].append(rec)
        return recs
   
    def download_full_data(self, recs):
        for r in recs:
            gw_id = IDUtils.gw_uniq_id_to_source_id(r['uniq_id'])
            r['gw_id'] = gw_id
        gw_uniq_ids = [r['uniq_id'] for r in recs]
        frecs = self.atom_api.get_gw_data_full(gw_uniq_ids)
        frecs = {IDUtils.gw_uniq_id_to_source_id(r['uniq_id']): r for r in frecs.values()}
        for r in recs:
            frec = frecs.get(r['gw_id'], {})
            r.update(frec)
        return recs
    
    def prepare_static_recs(self, data, stream_recs):
        if not stream_recs:
            stream_recs = self.prepare_stream_recs(data)
        if not stream_recs:
            return None
        final_recs = []
        for recs in stream_recs.values():
            for rec in recs:
                final_recs.append(StaticRecord.create_from_stream_rec(rec))
        return final_recs
    
    def save_recs_to_stream(self, stream_recs):
        for source_id, recs in stream_recs.items():
            col = self.get_stream_collection(source_id)
            DBUtils.update_bulk_records(col, recs)
            

        return stream_recs
    def download_atom_data(self, source_ids=None, full=True):
        recs = self.atom_api.get_gw_data()
        if self.is_admin:
            n_source_ids = [IDUtils.gw_uniq_id_to_source_id(r['uniq_id']) for r in recs]
            n_source_ids = list(set(n_source_ids))
            self.add_sources_to_server(n_source_ids)
        nrecs = []
        for r in recs:
            gw_id = IDUtils.gw_uniq_id_to_source_id(r['uniq_id'])
            if gw_id not in self.server.get_gw_ids() or (source_ids and gw_id not in source_ids):
                continue
            nrecs.append(r)
        # if not full:
        #     return nrecs
        # source_uniq_id_map ={IDUtils.gw_uniq_id_to_source_id(r['uniq_id']): r['uniq_id'] for r in recs}
        # if source_ids:
        #     gw_uniq_ids = [source_uniq_id_map.get(source_id, None) for source_id in source_ids]
        #     gw_uniq_ids = list(set(gw_uniq_ids))
        # else:
        #     gw_uniq_ids = [r['uniq_id'] for r in recs]

        # if full:
        full_data = self.download_full_data(nrecs)
        # else:
        #    return nrecs

       
        return list(full_data)


if __name__ == '__main__':
    from miniagro.data_utils.source_record_utils import SourceRecordUtils

    gws = SourceRecordUtils().get_last_gw_records()
    print(len(gws))
    gwl = AtomGatewayDownloader()
    for gw in gws:
        source_id = gw['source_id']
        stcol = gwl.get_stream_collection(source_id)
        recs = stcol.find_one({}, sort=[('datetime', -1)])
        pprint.pp(recs)
        pprint.pp(gw)
        exit(1)
        
