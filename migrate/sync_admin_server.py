from datetime import datetime
import pprint
from pymongo import MongoClient
from tqdm import tqdm

from miniagro.config.server_config import ServerConfig
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.downloaders.atom.atom_base_downloader import StreamRecord
from miniagro.downloaders.atom.atom_sensor_downloader import AtomSensorStaticRecord, AtomSensorStreamRecord, GrofitSensorStreamRecord
from miniagro.utils.db_utils import DBUtils
from miniagro.utils.grofit_id_utils import IDUtils
from miniagro.utils.param_utils import DictUtils

class SyncAdminServer:
    def __init__(self):
        self.base_ip = "mongodb://localhost:8012"
        self.client = None  # MongoDB client
        self.admin_db = None  # Reference to the admin database
        self.sensor_col = None

        self.connect_to_admin_mongo_db()
   
    def get_stream_collection(self, name):
        return DMSDK().atom_source_db.get_collection(name, 'atom_sensor_raw')
   
    def get_sensor_stream_collection(self, name):
        return DMSDK().sensor_db.get_source_collection(name, 'sensor_stream')
    
    def connect_to_admin_mongo_db(self, mongo_uri="mongodb://localhost:27017", admin_db_name="admin"):
        """Connects to the MongoDB server and selects the admin database."""
        try:
            
            self.client = MongoClient(self.base_ip)
            self.admin_db = self.client[admin_db_name]
            print(f"Connected to MongoDB at {mongo_uri}, using '{admin_db_name}' database.")
        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}")

    def db_from_connection(self, db_name):
        """Returns a reference to a specific database if the connection exists."""
        if self.client is None:
            raise ValueError("Not connected to MongoDB. Call connect_to_admin_mongo_db first.")
        return self.client[db_name]

    def get_records(self, db_name, collection_name, query={}, page=0):
        """Returns all records from a collection."""
        if self.client is None:
            raise ValueError("Not connected to MongoDB. Call connect_to_admin_mongo_db first.")
        # print(self.client[db_name][collection_name])
        return self.client[db_name][collection_name].find(query, 
                                                          limit=10000, 
                                                          skip=page*10000,
                                                          sort=[('datetime', 1)])
    
    def prepare_stream_recs(self, data):
        recs = {}

        for old in data:
            r = old.get('data', None)
            if not r:
                print(f"No data for {old['_id']}")
                continue
            source_id = IDUtils.gd_mac_to_source_id(r['mac'])
            ut = DictUtils.get_datetime(r, 'gw_read_time_utc', None)
            dt = DictUtils.get_datetime(r, 'sample_time_utc', None)
            name = r.get('device_name', None)
            if not dt or not source_id:
                continue
            if source_id not in recs:   
                recs[source_id] = []
            recs[source_id].append(AtomSensorStreamRecord(source_id=source_id, dt=dt, ut=ut, data=r, name=name))
        return recs
    
    def save_recs_to_stream(self, stream_recs):
        for source_id, recs in stream_recs.items():
            col = self.get_stream_collection(source_id)
            DBUtils.update_bulk_records(col, recs, silent=True)
        return stream_recs
    
    def handle_sensor_records(self, source_id, page=0):
        recs = self.get_records("dm_provider_source_db", f"{source_id}_provider_source_records", page=page)
        if not recs:
            return
        stream_recs = self.prepare_stream_recs(recs)
        self.save_recs_to_stream(stream_recs)
        return stream_recs
                
    # def prepare_app_data(self, stream_recs):
    #     app_data_builder = AppDailySummaryBuilder()
    #     nodes = AppNodeBuilder.load_nodes()
    #     nodes = {node['source_id']: node for node in nodes}
    #     for source_id, recs in stream_recs.items():
    #         app_node = nodes.get(source_id, None)
    #         if not app_node:
    #             continue
    #         app_data_builder.update_nodes(source_id, recs[0].datetime, recs[-1].upload_time, app_node)
    #     pass


    def prepare_sensor_records(self, stream_recs):
        for source_id, recs in stream_recs.items():
            source_sensors = GrofitSensorStreamRecord.get_grofit_sensors(recs)
            if source_sensors:
                col = self.get_sensor_stream_collection(source_id)
                DBUtils.update_bulk_records(col, source_sensors, silent=True)
        return stream_recs
    
    def update_sensor_records(self, source_id, page=0):

        stream_recs = self.handle_sensor_records(source_id, page)
        if not stream_recs:
            return
        self.prepare_sensor_records(stream_recs)
        # print(stream_recs.values())
        # print(len(list(stream_recs.values())[0]))
        if len(list(stream_recs.values())[0]) < 10000:
            return stream_recs.get(source_id, [])
        return self.update_sensor_records(source_id, page+1)
    
    def update_gw_records(self, gateway_id, page=0):
        recs = self.get_records("dm_provider_gw_db", f"{gateway_id}_provider_gw_records", page=page)
        if not recs:
            return 
        stream_recs = []
        for r in recs:
            base_data = r.get('data', {})
            full_data = r.get('full_data', {})
            data = {**base_data, **full_data}
            source_id = IDUtils.gw_uniq_id_to_source_id(base_data.get('uniq_id', None))
            rec = StreamRecord(source_id=source_id, index_name='atom_gateway', 
                               datetime=DictUtils.get_datetime(base_data, 'updatedAt', None), 
                               atom_id=r.get('_id', None), 
                               data=data)
            stream_recs.append(rec.to_dict())
        col = DMSDK().atom_source_db.get_collection(source_id, 'atom_gw_data')
        DBUtils.update_bulk_records(col, stream_recs, silent=True)

        return stream_recs
    
    def save_recs_to_static(self, recs, force=False):
        if not recs:
            return
        if not isinstance(recs, list):
            recs = [recs]
        st_recs = []
        for r in recs:
            st_rec = AtomSensorStaticRecord.create_from_stream_rec(r)
            st_recs.append(st_rec)
        col = DMSDK().atom_info_db.get_collection('atom_sensor_raw')
        if not force:
            old_recs = list(col.find({}))
            old_recs = {r['source_id']: DictUtils.get_datetime(r, 'datetime', None) for r in old_recs}

            ns = []
            for r in static_recs:
                if r.datetime > old_recs.get(r.source_id, datetime.min):
                    ns.append(r)
            static_recs = ns
        else:
            static_recs = recs
        if static_recs:
            DBUtils.update_bulk_records(col, static_recs, silent=True)
        return static_recs
    
    def update_source_records(self):
        source_ids = ServerConfig.get_self().get_source_ids()[10:13]
        st_recs = []
        current_source_id = source_ids[0]
        for i, source_id in tqdm(enumerate(source_ids), total=len(source_ids), desc=f"Updating source records"):
            current_source_id = source_id
            last_recs = self.update_sensor_records(source_id)
            if last_recs:
                st_recs.append(last_recs[-1])
            # if i % 5 == 1:
            self.save_recs_to_static(st_recs, force=True)
            st_recs = []
            if i > 10:
                break
        
        gw_recs = []
        
        for i, gateway_id in enumerate(ServerConfig.get_self().get_gw_ids()):
            print(f"Updating gateway {i} of {len(ServerConfig.get_self().get_gw_ids())}: {gateway_id}")
            last_recs = self.update_gw_records(gateway_id)
            if last_recs:
                gw_recs.append(last_recs[-1])
            if i > 10:
                break
        # col = DMSDK().atom_info_db.get_collection('atom_gw_data')
        # DBUtils.update_bulk_records(col, gw_recs)
    
# Example usage
if __name__ == "__main__":
    server = SyncAdminServer()
    server.update_source_records()
    # server.connect_to_admin_mongo_db()  # Connect to default MongoDB instance
    # db = server.db_from_connection("dm_provider_source_db")  # Get a reference to 'test_db'

    # print("Database retrieved:", db)

    # records = list(server.get_records("dm_provider_source_db", "gd_C0_1C_F8_AA_3B_50_provider_source_records"))

    # rec = records[0]
    # pprint.pprint(rec)
    # # server.update_sensor_records("gd_C1_D9_0C_6A_44_08")    
    # server.update_gw_records("gw_C0_95_AD_1C_B8_4F")