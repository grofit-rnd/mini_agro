from datetime import datetime, timedelta
import pprint

import numpy as np
from tqdm import tqdm
from miniagro.app_data.app_daily_summary import AppDailySummaryBuilder
from miniagro.app_data.app_dynamic_nodes import AppDynamicNodeBuilder
from miniagro.app_data.app_nodes import AppNodeBuilder
from miniagro.config.server_config import ServerConfig
from miniagro.data_utils.sensor_utils import SensorUtils
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.downloaders.atom.atom_base_downloader import AtomDownloader, StreamRecord, StaticRecord
from miniagro.downloaders.atom.atom_gateway_downloader import AtomGatewayDownloader
from miniagro.downloaders.atom.atom_source_raw_downloader import AtomSourceWebDownloader
from miniagro.ext_api.atom_api import AtomApi
from miniagro.utils.db_utils import DBUtils
from miniagro.utils.grofit_id_utils import IDUtils
from miniagro.utils.param_utils import DictUtils

class AtomSensorStreamRecord(StreamRecord):
    def __init__(self, source_id, dt, ut, data, name=None):
        super().__init__(source_id=source_id, index_name='atom_sensors', datetime=dt, data=data, name=name)
        self.upload_time = ut

    def to_dict(self):
        d = super().to_dict()
        d['upload_time'] = self.upload_time
        return d

class GrofitSensorStreamRecord(StreamRecord):
    def __init__(self, source_id, dt, ut, data, name):
        super().__init__(source_id=source_id, index_name='sensor_stream', datetime=dt, data=data, name=name)
        self.upload_time = ut

    def to_dict(self):
        d = super().to_dict()
        d['upload_time'] = self.upload_time
        return d
    @classmethod
    def calc_vpd(cls, rh, t):
        vpd = 0.6108*np.exp(17.27*t/(t+237.3))*(1.0-rh/100.0)
        return vpd
    
    @classmethod
    def calc_dew_point(cls, rh, t):
        a = 17.27
        b = 237.3
        gamma = np.log(rh/100.0)*(a*b)/(t+237.3)**2
        return b*gamma/(a-gamma)
    
    @classmethod
    def get_grofit_sensors(cls, atom_records):

        if not isinstance(atom_records, list):
            atom_records = [atom_records]
        records = []
        for atom_record in atom_records:
            if not isinstance(atom_record, dict):
                atom_record = atom_record.to_dict()
            data ={}
            for k, v in atom_record['data'].items():
                if k in SensorUtils.atom_sensor_to_sensor_dict:
                    data[SensorUtils.atom_sensor_to_sensor_dict[k]] = v 
            if 'datetime' in data:
                del data['datetime']
            if 'upload_time' in data:
                del data['upload_time']
            if 'temperature' in data and 'humidity' in data:
                data['vpd'] = GrofitSensorStreamRecord.calc_vpd(data['humidity'], data['temperature'])
                data['dew_point'] = GrofitSensorStreamRecord.calc_dew_point(data['humidity'], data['temperature'])
            name = DictUtils.get_value(atom_record, 'name', DictUtils.get_path(atom_record, 'data.device_name'))
            dt = DictUtils.get_datetime(atom_record, 'datetime', None)
            ut = DictUtils.get_datetime(atom_record, 'upload_time', None)
            source_id = atom_record['source_id']
            records.append(GrofitSensorStreamRecord(source_id, dt, ut, data, name))

        return records
    
class AtomSensorStaticRecord(AtomSensorStreamRecord):
    @classmethod
    def create_from_stream_rec(cls, rec):
        return cls(source_id=rec.source_id, dt=rec.datetime, ut=rec.upload_time, data=rec.data)
    
    def __init__(self, source_id, dt, ut, data):
        super().__init__(source_id, dt, ut, data)
        self.index_name = 'atom_sensor_raw'

    def get_id(self):
        return self.source_id
    

class AtomExpectedMonitor:
    @classmethod
    def get_collection(cls):
        return DMSDK().atom_info_db.get_collection('atom_expected_monitor')
   
    def __init__(self, monitor_id):
        self.monitor_id = monitor_id
        # self.get_collection().delete_many({})
    
    def get_records(self, source_ids=None):
        recs = list(self.get_collection().find({'monitor_id': self.monitor_id}))
        recs = sorted(recs, key=lambda x: x.get('expected', datetime.min))
        # if source_ids:
        #     rmap = {r['source_id']: r for r in recs}
        #     for source_id in source_ids:
        #         if source_id not in rmap:
        #             recs.append({
        #                 '_id': self.monitor_id+'_'+source_id,
        #                 'monitor_id': self.monitor_id,
        #                 'source_id': source_id,
        #                 'last_dt': datetime.utcnow()-timedelta(days=1),
        #                 'diff_10': [60]*10,
        #                 'last_attempt': datetime.utcnow()-timedelta(days=1),
        #                 'expected': datetime.min
        #             })
        return recs
    

    

    def update_records(self, recs, source_ids=None):
        new_recs =[]
        source_recs = {}
        if isinstance(recs, list):
            for r in recs:
                if r['source_id'] not in source_recs:
                    source_recs[r['source_id']] = []
                source_recs[r['source_id']].append(r)
        else:
            source_recs = recs
           
        if not source_recs:
            return None
        nw = datetime.utcnow()
        old_recs = self.get_collection().find({'monitor_id': self.monitor_id, 'source_id': {'$in': list(source_recs.keys())}})
        old_recs = {r['source_id']: r for r in old_recs}
        for source_id, srecs in source_recs.items():
            old_rec = old_recs.get(source_id, None) 
            if not old_rec:
                old_rec = {
                    '_id': self.monitor_id+'_'+source_id,
                    'monitor_id': self.monitor_id,
                    'source_id': source_id,
                    'last_dt': nw-timedelta(days=1),
                    'diff_10': [60]*10,
                    'last_attempt': nw
                }
            srecs = [r.to_dict() if not isinstance(r, dict) else r for r in srecs]
            srecs = sorted(srecs, key=lambda x: x['upload_time'])         
            for r in srecs:
                if not isinstance(r, dict):
                    r = r.to_dict()
                passed_time = r['upload_time'] - old_rec['last_dt']
                if passed_time.total_seconds() < 60:
                    continue
                old_rec['diff_10'].pop(0)
                old_rec['diff_10'].append(passed_time.total_seconds()/60)
                old_rec['last_dt'] = r['upload_time']
            old_rec['last_attempt'] = nw
            old_rec['expected'] = old_rec['last_dt']+timedelta(minutes=np.median(old_rec['diff_10']))
            new_recs.append(old_rec)
        for src_id in source_ids:
            if src_id not in source_recs or src_id not in old_recs:
                continue
            nr = old_recs[src_id]
            nr['last_attempt'] = nw
            new_recs.append(nr)
        if new_recs:
            DBUtils.update_bulk_records(self.get_collection(), new_recs)
        return new_recs

class AtomSensorDownloader:
    def __init__(self):
        self.atom_api = AtomApi()
        self.server = ServerConfig.get_self()
        self.is_admin = self.server.is_admin
        self.extended_download = True
        self.expected_monitor = AtomExpectedMonitor(monitor_id='atom_sensor_downloader')

    def get_stream_collection(self, name):
        return DMSDK().atom_source_db.get_collection(name, 'atom_sensor_raw')
    
    def get_static_collection(self):
        return DMSDK().atom_info_db.get_collection('atom_sensor_raw')
    
    def get_sensor_stream_collection(self, name):
        return DMSDK().sensor_db.get_source_collection(name, 'sensor_stream')
    
    def adjust_dates(self, start, end, use_gw_dt):
        if start and isinstance(start, str):
            start = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        if end and isinstance(end, str):
            end = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
        if not start and not end:
            if use_gw_dt:
                start = datetime.utcnow() - timedelta(hours=40)
            else:
                start = datetime.utcnow() - timedelta(days=13)
            end = datetime.utcnow()
            return start, end
        if use_gw_dt:
            end = datetime.utcnow()
            if start and start < end - timedelta(hours=40):
                start = end - timedelta(hours=40)
            return start, end
        if start and not end:
            end = start+timedelta(days=13)
        if end and not start:
            start = end-timedelta(days=13)
        return start, end
    

    def prepare_groups(self, source_ids):
        added = []
        recs = self.expected_monitor.get_records()
        rmap = {r['source_id']: r for r in recs}
        for src_id in source_ids:
            if src_id not in rmap:

                added.append({
                    '_id': self.expected_monitor.monitor_id+'_'+src_id,
                    'source_id': src_id,
                    'monitor_id': self.expected_monitor.monitor_id,
                    'last_dt': datetime.utcnow()-timedelta(hours=1),
                    'diff_10': [60]*10,
                    'last_attempt': datetime.utcnow()-timedelta(hours=1),
                    'expected': datetime.utcnow()-timedelta(hours=1),
                })
        if added:
            DBUtils.update_bulk_records(self.expected_monitor.get_collection(), added)
        diff_times = {}
        nw = datetime.utcnow()
        groups = {}
        updates = []

        for r in recs:
            expected = r['expected']
            df_time = (expected-nw).total_seconds()/60
            last_attempt = r.get('last_attempt', r['last_dt'])
            ldiff = (nw-last_attempt).total_seconds()/60        
            if ldiff < 10:
                continue
            if df_time > 10 or df_time < -60*24:
                if ldiff < 60:
                    continue
            elif df_time < -60*6:
                if ldiff < 30:
                    continue
            updates.append(r)
        added = False
        # for src_id in source_ids:
        #     if src_id not in rmap:
        #         print('adding new source', src_id)
        #         added = True
        #         updates.append({
        #             '_id': self.expected_monitor.monitor_id+'_'+src_id,
        #             'source_id': src_id,
        #             'last_dt': datetime.utcnow()-timedelta(days=1),
        #             'diff_10': [60]*10,
        #             'last_attempt': nw,
        #             'expected': datetime.min
        #         })
        #     else:
        #         r = rmap[src_id]
        #         r['last_attempt'] = nw
        #         updates.append(r)
        #         print('updating source', src_id)
        # DBUtils.update_bulk_records(self.expected_monitor.get_collection(), updates)
        if updates:
            groups = {'2h': [], '6h': [], '12h': [], '24h': [], '46h': [], 'late_13': [], 'late_10': [], 'late_7': [], 'late_4': [], 'late_3': []}
            updates = sorted(updates, key=lambda x: x['last_dt'])
            for r in updates:
                diff = (nw-r['last_dt']).total_seconds()/60
                ldiff = (nw-r['last_attempt']).total_seconds()/60
                if diff > 60*24*10:
                    groups['late_13'].append(r['source_id'])
                elif diff > 60*24*7:
                    groups['late_10'].append(r['source_id'])
                elif diff > 60*24*4:
                    groups['late_7'].append(r['source_id'])
                elif diff > 60*24*3:
                    groups['late_4'].append(r['source_id'])
                elif diff > 60*24*2:
                    groups['late_3'].append(r['source_id'])
                elif ldiff < 60*2:
                    groups['2h'].append(r['source_id'])
                elif ldiff < 60*6:
                    groups['6h'].append(r['source_id'])
                elif diff < 60*12:
                    groups['12h'].append(r['source_id'])
                elif diff < 60*24:
                    groups['24h'].append(r['source_id'])
                elif diff < 60*46:
                    groups['46h'].append(r['source_id'])
                # elif diff < 60*72:
                #     if not '72h' in groups:
                #         groups['72h'] = []
                #     groups['72h'].append(r['source_id'])
                # elif diff < 60*144:
                #     if not '144h' in groups:
                #         groups['144h'] = []
                #     groups['144h'].append(r['source_id'])
                # else:
                #     if not 'late' in groups:
                #         groups['late'] = []
                #     groups['late'].append(r['source_id'])
       
        return groups
    
    def download_data(self, source_ids=None, start=None, end=None, use_gw_dt=True, app_data=False):
        if not source_ids:
            source_ids = self.server.get_source_ids()
        else:
            source_ids = self.server.filter_ids(source_ids)
        # start, end = self.adjust_dates(start, end, use_gw_dt)
        groups = self.prepare_groups(source_ids)
        # for group, ids in groups.items():
        #     print(group, len(ids))
        saved_source_ids = set()
        nw = datetime.utcnow()
        gw_recs = []
        web_recs = []
        source_min = {}
        for group, ids in groups.items():
            for i in tqdm(range(0, len(ids), 100), desc=f'getting atom info for {group}', total=len(ids)//100):
                self.min_rec = None

                if group.startswith('late'):
                    days = int(group.split('_')[1])
                    start = nw-timedelta(days=days)
                    end = nw
                    data = self.download_sensor_records(source_ids=ids[i:i+100], start=start, end=end, use_gw_dt=False)
                else:
                    start = nw-timedelta(hours=int(group.split('h')[0]))
                    end = nw
                    data = self.download_sensor_records(source_ids=ids[i:i+100], start=start, end=end, use_gw_dt=True)
                if not data:
                    continue
                stream_recs = self.prepare_stream_recs(data)
                self.expected_monitor.update_records(stream_recs, ids)
                if stream_recs:
                    self.save_recs_to_stream(stream_recs)
                # if stream_recs and app_data:
                    # self.prepare_app_data(stream_recs)
                static_recs = self.prepare_static_recs(data, stream_recs)
                if static_recs:
                    n_static_recs = self.save_recs_to_static(static_recs)
                    for n in n_static_recs:
                        saved_source_ids.add(n.source_id)
                        source_min[n.source_id] = self.min_rec
                    if self.extended_download and n_static_recs:
                        update_gw_info, gw_recs = self.download_gw_data(n_static_recs)
                        update_web_info, web_recs = self.download_web_data(n_static_recs)
                  
                if stream_recs:
                    sens_recs = self.prepare_sensor_records(stream_recs, gw_recs, web_recs)
        if app_data:
            try:
                dnd,snd = AppDynamicNodeBuilder().build_nodes()
        
            
                for source_id, min_rec in tqdm(source_min.items(), desc='Updating daily summary records', total=len(source_min.keys())):
                    di = {
                        'start': min_rec,
                        'end': nw
                    }
                    start = DictUtils.get_datetime(di, 'start', None)
                    end = DictUtils.get_datetime(di, 'end', None)
                    if not start or not end:
                        continue
                    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
                    end = end.replace(hour=0, minute=0, second=0, microsecond=0)
                    end = end + timedelta(days=1)
                
                    AppDailySummaryBuilder().update_nodes(source_id, start, end, snd.get(source_id, None))
            except Exception as e:
                pprint.pp(e)
        print('all done with downloader')        
        return saved_source_ids
    
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


    def prepare_sensor_records(self, stream_recs, gw_recs, web_recs):
        for source_id, recs in stream_recs.items():
            source_sensors = GrofitSensorStreamRecord.get_grofit_sensors(recs)
            if source_sensors:
                col = self.get_sensor_stream_collection(source_id)
                DBUtils.update_bulk_records(col, source_sensors)
        return stream_recs
    
    def download_gw_data(self, static_recs):
        update_gw_info = []
        atom_gw_downloader = AtomGatewayDownloader()
        existing_data = atom_gw_downloader.get_static_collection().find({})
        existing_data = {r['source_id']: DictUtils.get_datetime(r, 'datetime', None) for r in existing_data}
        for r in static_recs:
            gw = r.data.get('gw_id', None)
            if not gw:
                continue
            gw_source_id = IDUtils.gw_uniq_id_to_source_id(gw)
            gw_dt = DictUtils.get_datetime(r.data, 'gw_read_time_utc', None)
            if existing_data.get(gw_source_id, None) and existing_data[gw_source_id] >= gw_dt:
                continue
            update_gw_info.append(gw_source_id)  
        if update_gw_info:
            recs =atom_gw_downloader.download_data(update_gw_info)
        return update_gw_info, recs
    
    def download_web_data(self, static_recs):
        atom_web_downloader = AtomSourceWebDownloader()
        source_ids = [r.source_id for r in static_recs]
        # atom_ids = atom_web_downloader.get_atom_ids(source_ids)
        existing_data = list(atom_web_downloader.get_static_collection().find({'source_id': {'$in': source_ids}}))
        existing_data = {r['source_id']: DictUtils.get_datetime(r, 'datetime', None) for r in existing_data}
        w_source_ids = []
        for r in static_recs:
            if existing_data.get(r.source_id, None) and existing_data[r.source_id] >= r.datetime:
                continue
            w_source_ids.append(r.source_id)
        if w_source_ids:
            recs = atom_web_downloader.download_data(w_source_ids)
        return w_source_ids, recs

        
        # existing_data = {r['source_id']: DictUtils.get_datetime(r, 'datetime', None) for r in existing_data}
        

    def prepare_stream_recs(self, data):
        recs = {}
        for r in data:
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
            DBUtils.update_bulk_records(col, recs)
        return stream_recs
   
    
    def prepare_static_recs(self, data, stream_recs):
        if not stream_recs:
            stream_recs = self.prepare_stream_recs(data)
        if not stream_recs:
            return None
        final_recs = []
        for source_id, recs in stream_recs.items():
            sr = sorted(recs, key=lambda x: x.datetime)
            final_recs.append(AtomSensorStaticRecord.create_from_stream_rec(sr[-1]))
        return final_recs
    
   
    
    def save_recs_to_static(self, static_recs, force=False):
        col = self.get_static_collection()
        if not force:
            old_recs = list(col.find({}))
            old_recs = {r['source_id']: DictUtils.get_datetime(r, 'datetime', None) for r in old_recs}

            ns = []
            for r in static_recs:
                if r.datetime > old_recs.get(r.source_id, datetime.min):
                    ns.append(r)
            static_recs = ns
           
        if static_recs:
            DBUtils.update_bulk_records(col, static_recs)
        return static_recs

    def download_sensor_records(self, source_ids, start, end, use_gw_dt=True):
        records = self.atom_api.get_atom_readings(source_ids, start, end, use_gw_dt=use_gw_dt)
        for r in records:
            if self.min_rec and r['sample_time_utc'] >self.min_rec:
                continue
            self.min_rec = r['sample_time_utc']
        return records
 

if __name__ == '__main__':
    downloader = AtomSensorDownloader()
    downloader.download_data(start='2025-02-24 00:00:00', app_data=True)