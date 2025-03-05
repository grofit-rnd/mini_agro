import copy
import json
import pprint
from datetime import datetime, timedelta
from random import random

from dm_server.admin.server.server_config import ServerConfig
from dm_server.data_utils.record_utils import RecordUtils
from dm_server.downloaders.providers.atomation.atom_api import AtomApi
from dm_server.providers.atomation.atom_sync_utils import AtomSyncUtils
from dm_server.records.base.ds_info_record import DSInfoRecord
from dm_server.sync.provider_sensor_record_sync import ProviderSensorRecordSync
from dm_server.utils.grofit_id_utils import IDUtils
from grofit_servers.agro_mng_app.app_data.source.app_ds import AppDS
from grofit_servers.clients.client_info import ClientSources
from dm_server.downloaders.providers.atomation.atom_record_downloader import AtomRecordDownloader
from dm_server.downloaders.providers.atomation.records.atom_source_record import AtomSourceRecord
from dm_server.sdk.data_manager_sdk import DMSDK
from dm_server.sdk.db.db_utils import DBUtils
from dm_server.sync.provider_raw_record_sync import ProviderRawRecordSync
from dm_server.utils.param_utils import DictUtils


class AtomSourceGroupBuilder(AtomRecordDownloader):
  
    def __init__(self, max_devices=None):
        self.group_id = 'dynamic_group'
        self.source_ids = []
        self.max_devices = max_devices or 100
        self.update_sources = []
        self.sources = {}
        super().__init__(sync_id=self.group_id, sync_type='atom_dynamic_record_downloader')

    def to_dict(self):
        ret = super().to_dict()
        DictUtils.set_value(ret, '_id', self.group_id)
        DictUtils.set_value(ret, 'source_ids', list(set(self.source_ids)))
        DictUtils.set_value(ret, 'max_devices', self.max_devices)
        return ret

    def populate_from_dict(self, di):
        super().populate_from_dict(di)
        self.group_id = DictUtils.get_value(di, '_id', self.group_id)
        self.source_ids = DictUtils.get_value(di, 'source_ids', self.source_ids)
        self.max_devices = DictUtils.get_value(di, 'max_devices', self.max_devices)

        return self

    def sync_sources(self, sync_sources, st, end, cdt):
        st = max(sync_sources[-1]['last_provider_check'], st)
        src_ids = [s['source_id'] for s in sync_sources]
        # st = datetime.utcnow() - timedelta(days=4)
        print('syncing', len(src_ids), 'sources', st, end)
        
        pr = self.download(src_ids, st, end, use_gw_dt=True)
        processed_records = []
        if pr:
            ps = ProviderRawRecordSync(provider_id='atomation')
            processed_records = ps.sync_records(raw_records=pr)

        self.last_sync_time = end
        self.data['stats'] = {'start': st, 'end': end, 'raw': len(pr),
                              'processed': len(processed_records)}
        self.save_to_db()
        pss = ProviderSensorRecordSync()
        pss.sync_records(processed_records)
        update_sources = []

        srcs = DSInfoRecord.get_ds_info_records(src_ids)
        srcs = {s.source_id: s for s in srcs}
        for src_id in src_ids:
            if src_id not in srcs:
                print('source not found', src_id)
                continue
            srcs[src_id].last_provider_check = cdt - timedelta(minutes=1)
            update_sources.append(srcs[src_id])
        DBUtils.update_bulk_records(DSInfoRecord.get_collection(), update_sources)
        ProviderRawRecordSync.update_provider_last_record(src_ids)
        # provider_sources = DMSDK().info_db.get_collection('provider_source_info').find({})
        # provider_sources = {s['source_id']: s for s in provider_sources}
        # comb_recs = []
        # for rec in processed_records:
        #     nr = rec.to_dict()
        #     src = provider_sources.get(rec.source_id, None)
        #     new_rec = {}
        #     new_rec['_id'] = nr['source_id']
        #     new_rec['source_id'] = nr['source_id']
        #     new_rec['provider_id'] = 'atomation'
        #     new_rec['updated'] = datetime.utcnow()
        #     if src:
        #         new_rec['source'] = DictUtils.get_path(src, 'data', {})
        #     new_rec['data'] = nr.get('data', {})
        #     new_rec['datetime'] = DictUtils.get_path(nr, 'time_info.gw_time', None)
        #     new_rec['atom_id'] = DictUtils.get_path(new_rec.get('source', {}), 'raw._id', None)
        #     comb_recs.append(new_rec)
        # DBUtils.update_bulk_records(
        #     DMSDK().info_db.get_collection('provider_last_record'),
        #     comb_recs
        # )
        return processed_records

    def sync(self):
        tot_recs = []
        if not self.update_sources:
            print('no sources to update')
            return []

        sorted_sources = sorted(self.update_sources, key=lambda x: x['last_provider_check'], reverse=True)
        print('sources to sync', len(sorted_sources))
        cdt = datetime.utcnow()
        end = datetime.utcnow()
        first = None
        tot_srcs = len(sorted_sources)
        sync_sources = []

        for i in range(0, tot_srcs):
            st = cdt - timedelta(days=2)

            # print('syncing', len(sorted_sources), 'sources')
            first = first or sorted_sources[i]['last_provider_check']
            s = sorted_sources[i]
            n_sync = False
            # print('syncing', s['last_provider_check'])

            if len(sync_sources) >= self.max_devices:
                print('max devices reached')
                n_sync = True
            elif s['last_provider_check'] > first - timedelta(hours=2):
                sync_sources.append(s)
                # print('adding', len(sorted_sources), len(sync_sources))
            else:
                n_sync = True
                print('time reached', s['last_provider_check'], first - timedelta(hours=2))
            # print('syncing', len(sync_sources), 'sources')
            if i == tot_srcs - 1:
                n_sync = True
            if n_sync and sync_sources:
                st = max(sync_sources[-1]['last_provider_check'], st)
                print('syncing', len(sync_sources), 'sources', st, end)
                tot_recs.extend(self.sync_sources(sync_sources, st, end, cdt))
                print('synced', len(tot_recs), 'records')
                first = None
                sync_sources = []

        return tot_recs


    def init_groups(self):
        sources = DSInfoRecord.get_ds_info_for_provider('atomation')
        server_config = ServerConfig().load_from_db('self')
        if server_config and not server_config.is_admin_server:
            # pprint.pp(server_config.to_dict())
            sources = [s for s in sources if s.source_id in server_config.nodes['grofit_capsule']]
        print(f'Active devices: {len(sources)}')
        # pprint.pp(sources)
        sorted_sources = sorted(sources, key=lambda x: x.upload_frequency or 1000000)
        dt = datetime.utcnow() - timedelta(days=2)
        groups = {}
        nw = datetime.utcnow()
        update_sources = []
        for s in sources:
            status = 'expecting'
            if (not s.last_provider_check or s.last_provider_check < nw - timedelta(hours=21) +
                    timedelta(minutes=int(random() * 180))):
                update_sources.append(s)
                continue
            elif not s.upload_frequency and s.last_provider_check < nw - timedelta(hours=2):
                update_sources.append(s)
                continue

            if not s.last_record_dt or not s.upload_frequency:
                continue
            #early or late once a hour

            if ((s.last_record_dt + (min(timedelta(seconds=s.upload_frequency * 3), timedelta(hours=4))) < nw) or
                    (s.last_record_dt + timedelta(seconds=s.upload_frequency) > nw - timedelta(minutes=10))):
                if s.last_provider_check + timedelta(hours=1) < nw:
                    update_sources.append(s)

            else:  #expecting
                update_sources.append(s)

        for s in update_sources:
            exp = s.last_record_dt + timedelta(seconds=s.upload_frequency) if s.upload_frequency else None
            groups[s.source_id] = {
                'source_id': s.source_id,
                'upload_frequency': s.upload_frequency,
                'last_record_dt': s.last_record_dt or datetime(2021, 1, 1),
                'expected': exp,
                'last_provider_check': s.last_provider_check or datetime(2021, 1, 1)
            }
        self.sources = {s.source_id: s for s in update_sources}
        self.source_ids = list(groups.keys())
        self.update_sources = list(groups.values())
        print(f'Active devices: {len(self.sources)}')


  
def fix_records(source_id, collection=None):
    """
    Fixes corrupted date/time fields in MongoDB documents by converting string representations
    back to datetime objects.

    Parameters:
    - source_id (str): The source_id to filter documents.
    - collection (pymongo.collection.Collection): The MongoDB collection instance.

    Returns:
    - None
    """
    # Define the fields to be checked and corrected
    date_fields = [
        'datetime',
        'time_info.sample_time',
        'time_info.gw_time',
        'time_info.server_time',
        'upload_time'
    ]
    collection = DMSDK().provider_source_db.db[f'{source_id}_provider_source_records']
    # if not collection:
    #     collection = DMSDK().provider_source_db.get_source_collection(source_id)
    pprint.pp(collection)
    # Query to select documents with the specified source_id
    query = {}

    # Counter for tracking
    total_docs = collection.count_documents(query)
    print(f"Starting to fix {total_docs} documents with source_id: {source_id}")
    recs = collection.find(query)
    # Iterate through each document
    update_records = {}
    i = 0
    for rec in recs:
        i += 1
        tmp = copy.deepcopy(rec)
        tmp['datetime'] = DictUtils.get_datetime(tmp, 'datetime', None)
        tmp['time_info']['gw_time'] = DictUtils.get_datetime(tmp, 'time_info.gw_time', None)
        tmp['time_info']['server_time'] = DictUtils.get_datetime(tmp, 'time_info.server_time', None)
        tmp['upload_time'] = DictUtils.get_datetime(tmp, 'upload_time', None)
        tmp['time_info']['sample_time'] = DictUtils.get_datetime(tmp, 'time_info.sample_time', None)
        update_records[int(tmp['datetime'].timestamp())] = tmp
            # pprint.pp(rec)
            # pprint.pp(tmp)
        # else:
        #     print('no update needed', rec['_id'])
        # if len(update_records) > 5000:
        #     pprint.pp(update_records[0])
        #     pprint.pp(update_records[-1])
        #     DBUtils.update_bulk_records(collection, update_records)
        #     update_records = []
    # file_name = f'current.json'
    # with open(file_name, 'w') as f:
    #     js = json.dumps(update_records, default=str)
    #     f.write(js)
    
    print(len(update_records))
    # DMSDK().provider_source_db.db.drop_collection(collection)
    # col = DMSDK().provider_source_db.get_source_collection(source_id)
    collection.delete_many({})
    DBUtils.update_bulk_records(collection, list(update_records.values()))
    print(i)
    # DBUtils.update_bulk_records(collection, update_records)

    print("Completed fixing records.")


if __name__ == '__main__':
    # fix_records('gd_DE_0E_45_A6_84_79')
    agb = AtomGroupBuilder()
    agb.init_groups()
    agb.sync()
    AppDS.update_all()
