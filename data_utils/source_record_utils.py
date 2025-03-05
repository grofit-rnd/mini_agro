from datetime import datetime, timedelta
import pandas as pd
from miniagro.config.server_config import ServerConfig
from miniagro.data_utils.source_utils import SourceUtils
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.utils.grofit_id_utils import IDUtils


class SourceRecordUtils:
    def __init__(self):
        pass
    
    def get_source_records(self, source_id, index_name, start_date, end_date, limit=10000, skip=0, full=False, as_df=False):
        if not start_date:
            start_date = datetime.now() - timedelta(days=10)
        if not end_date:
            end_date = datetime.now()
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        pipeline = [
            {'$match': {'datetime': {'$gte': start_date, '$lte': end_date}}},
            {'$limit': limit},
            {'$skip': skip}
        ]
       
        ret = []

        col = DMSDK().atom_source_db.get_collection(source_id, index_name)
        ret.extend(list(col.aggregate(pipeline)))
        return ret
        # if not as_df or index_name in ['source_web', 'source_raw']:
        #     return ret
        # print(ret)
        # df = pd.DataFrame([entry['data'] for entry in ret])
        # if df is None or df.empty:
        #     return None
        # print(df.head())
        # print(df.columns)
        # if not full :
        #     df.drop(columns=['_id', 'sensors', 'timers', 'config_synced', 'config_create_time_utc'], inplace=True, errors='ignore')
        #     df['source_id'] = [IDUtils.gd_mac_to_source_id(mac) for mac in df['mac']]
        # # df['datetime'] = pd.to_datetime(df['datetime'])
        # # df.set_index('datetime', inplace=True)
        # # df.sort_index(inplace=True)
        # return df

    def get_last_source_records(self,  index_name, source_ids=None,):
        if not source_ids:
            source_ids = ServerConfig.get_self().get_source_ids()
        col = DMSDK().atom_info_db.get_collection(index_name)
        return list(col.find({'source_id': {'$in': source_ids}}, sort=[('datetime', -1)]))
    
    def get_last_gw_records(self, source_ids=None):
        if not source_ids:
            source_ids = ServerConfig.get_self().get_gw_ids()
        col = DMSDK().atom_info_db.get_collection('atom_gw_data')
        return list(col.find({'source_id': {'$in': source_ids}}, sort=[('datetime', -1)]))
    
    # def update_last_records(self):
    #     source_ids = ServerConfig.get_self().get_source_ids()
    #     for source_id in source_ids:
    #         self.update_source_records(source_id)
    #     gw_ids = ServerConfig.get_self().get_gw_ids()
    #     for gw_id in gw_ids:
    #         self.update_gw_records(gw_id)
if __name__ == '__main__':
    sru = SourceRecordUtils()
    ret = sru.get_last_source_records(None, 'atom_source_web')
    print(ret)
