import pprint
from miniagro.config.server_config import ServerConfig
from miniagro.data_utils.source_record_utils import SourceRecordUtils
from miniagro.downloaders.atom.atom_business_unit_downloader import AtomBusinessUnitDownloader
from miniagro.downloaders.atom.atom_gateway_downloader import AtomGatewayDownloader
from miniagro.downloaders.atom.atom_source_raw_downloader import AtomSourceRawDownloader, AtomSourceWebDownloader
from miniagro.downloaders.atom.atom_unit_downloader import AtomUnitDownloader
from miniagro.downloaders.atom.atom_user_downloader import AtomUserDownloader
from miniagro.utils.grofit_id_utils import IDUtils


class AtomDownloaderUtils:
    @classmethod
    def fill_missing_atom_data(cls):
        AtomBusinessUnitDownloader().download_data()
        AtomUnitDownloader().download_data()
        AtomUserDownloader().download_data()
        AtomSourceRawDownloader().download_data()
        AtomGatewayDownloader().download_data()

    @classmethod
    def fill_missing_source_web_data(cls):
        source_ids = ServerConfig.get_self().get_source_ids()
        last_recs = SourceRecordUtils().get_last_source_records(index_name='atom_source_web')
        last_recs = {rec['source_id']: rec for rec in last_recs}
        # recs = {IDUtils.gw_uniq_id_to_source_id(r['uniq_id']): r for r in recs}
        n_source_ids = []
        source_ids = set(source_ids)
        for src_id in source_ids:
            if src_id not in last_recs:  
                n_source_ids.append(src_id)
        if n_source_ids:
            for src_id in n_source_ids:
                AtomSourceWebDownloader().download_data(source_ids=[src_id])
        return n_source_ids
    
    @classmethod
    def fill_missing_gw_data(cls):

        AtomGatewayDownloader().download_data(full=False)
        exit()
        source_ids = ServerConfig.get_self().get_gw_ids()
      
        last_recs = SourceRecordUtils().get_last_gw_records()
        last_recs = {rec['source_id']: rec for rec in last_recs}
        # recs = {IDUtils.gw_uniq_id_to_source_id(r['uniq_id']): r for r in recs}
        n_source_ids = []
        source_ids = set(source_ids)
        for src_id in source_ids:
            if src_id not in last_recs:  
                n_source_ids.append(src_id)
        if n_source_ids:
            pprint.pprint(n_source_ids)
            for src_id in n_source_ids:
                AtomGatewayDownloader().download_data(source_ids=[src_id], full=True)
          

if __name__ == '__main__':
    AtomDownloaderUtils.fill_missing_gw_data()
