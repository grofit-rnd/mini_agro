from miniagro.db.gdb_plugin import GDBPlugin
from pymongo import MongoClient, ASCENDING, DESCENDING



class RecordDBPlugin(GDBPlugin):

    def __init__(self, host=None, port=None, db_name=None, config=None, col_extra=None):
        db_name = 'grofit_record_db'
        super().__init__(host, port, db_name, config)


    def add_dynamic_collection(self, name, index_name):
        if not index_name or not name:
            raise Exception('no index name or name')
        full_name = f'{name}_{index_name}'
        if full_name in self.collections:
            return self.collections[full_name]
        return self.add_collection(full_name, self.get_indexes(index_name))

    def get_source_collection(self, source_id, index_name):
        return self.add_dynamic_collection(source_id, index_name)

    
    def get_indexes(self, key):
        if 'source_web' in key or 'source_raw' in key or 'source_info' in key:
            return [dict(name='source_idx', fields=[('source_id', ASCENDING)], options={}),
                    dict(name='datetime_idx', fields=[('datetime', ASCENDING)], options={}),
                    ]
        
        
        # if 'latent' in key:
        #     return [dict(name='source_idx', fields=[('source_id', ASCENDING)], options={}),
        #             dict(name='latent_idx', fields=[('lat_id', ASCENDING)], options={}),
        #             dict(name='datetime_idx', fields=[('datetime', ASCENDING)], options={}),
        #             ]
        # if 'source_info' in key:
        #     return [dict(name='source_idx', fields=[('source_id', ASCENDING)], options={}),
        #             dict(name='datetime_idx', fields=[('datetime', ASCENDING)], options={}),
        #             ]
        # if 'latent' in key:
        #     return [dict(name='source_idx', fields=[('source_id', ASCENDING)], options={}),
        #             dict(name='latent_idx', fields=[('lat_id', ASCENDING)], options={}),
        #             dict(name='datetime_idx', fields=[('datetime', ASCENDING)], options={}),
        #             ]
        # if 'pred' in key:
        #     return [dict(name='pred_id_idx', fields=[('pred_id', ASCENDING)], options={}),
        #             dict(name='pipeline_idx', fields=[('pipeline_id', ASCENDING)], options={}),
        #             dict(name='start_date_idx', fields=[('start_date', ASCENDING)], options={}),
        #             dict(name='end_date_idx', fields=[('end_date', ASCENDING)], options={}),
        #             ]
        # if 'provider_source_records' in key:
        #     return [dict(name='datetime_1', fields=[('time_info.sample_time', ASCENDING)], options={}),
        #             dict(name='un_index', fields=[('stream_id', ASCENDING), ('time_info.sample_time', ASCENDING)],
        #                  options={"unique": True}),
        #             dict(name='upload_time_idx', fields=[('time_info.server_time', ASCENDING)], options={}),
        #             dict(name='source_id_idx', fields=[('source_id', ASCENDING), ('time_info.sample_time', ASCENDING)],
        #                  options={}),
        #             ]
        # if 'provider_gw_records' in key:
        #     return [dict(name='datetime_idx', fields=[('datetime', ASCENDING)], options={}),
        #              dict(name='un_index', fields=[('source_id', ASCENDING), ('last_keep_alive_time', DESCENDING)],
        #                  options={"unique": True}),
        #             ]
        # if 'source_records' in key:
        #     return [dict(name='datetime_idx', fields=[('datetime', ASCENDING)], options={}),
        #             dict(name='un_idx', fields=[('stream_id', ASCENDING), ('datetime', ASCENDING)],
        #                  options={"unique": True}),
        #             dict(name='upload_time_idx', fields=[('upload_time', ASCENDING)], options={}),
        #             dict(name='source_id_idx', fields=[('source_id', ASCENDING), ('datetime', ASCENDING)], options={}),
        #             dict(name='sensor_type_idx', fields=[('sensor_type', ASCENDING), ('datetime', ASCENDING)], options={}),
        #             ]
        return None


    def save_source_records(self, records, collection_name=None):
        collection_name = collection_name if collection_name else self.col_extra
        if not records:
            return
        if isinstance(records, dict):
            records = list(records.values())
        
        if not isinstance(records[0], dict):
            records = [r.to_dict() for r in records]

        source_recs = {}
        for sr in records:
            if sr['source_id'] not in source_recs:
                source_recs[sr['source_id']] = []
            source_recs[sr['source_id']].append(sr)
        for source_id, recs in source_recs.items():
            col = self.get_collection(f'{source_id}_{collection_name}')
            DBUtils.update_bulk_records(col, recs)
