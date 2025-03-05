
from datetime import datetime, timedelta
import pprint
import pandas as pd

from miniagro.data_utils.source_record_utils import SourceRecordUtils
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.utils.db_utils import DBUtils
from miniagro.utils.param_utils import DictUtils



# def get_sensor_type_and_class(cls, sensor_key):

#     if sensor_key == 'Temperature':
#         return 'temperature', 'temperature'
#     if sensor_key == 'Humidity':
#         return 'humidity', 'humidity'
#     if sensor_key == 'Radiation':
#         return 'radiation', 'radiation'
#     if sensor_key == 'Tension T' or sensor_key == 'tension_t':
#         return 'tension_top', 'tension'
#     if sensor_key == 'Tension D' or sensor_key == 'Tension B' \
#             or sensor_key == 'tension_d' or sensor_key == 'tension_b':
#         return 'tension_deep', 'tension'
#     if sensor_key == 'EC':
#         return 'ec', 'ec'
#     if sensor_key == 'Soil Temperature':
#         return 'soil_temperature', 'temperature'
#     if sensor_key == 'Vibration Raw Data':
#         return 'vibration', 'vibration'
#     if sensor_key == 'Battery Level':
#         return 'battery_level', 'battery_level'

#     if sensor_key == 'Flow':
#         return 'flow', 'flow'
#     if sensor_key == 'Soil Moisture':
#         return 'soil_moisture', 'soil_moisture'
#     if sensor_key == 'Watermark top':
#         return 'watermark_top', 'watermark'
#     if sensor_key == 'Watermark deep':
#         return 'watermark_deep', 'watermark'
#     if sensor_key == 'gw_read_time_utc':
#         return 'upload_time', 'upload_time'
#     if sensor_key == 'vpd':
#         return 'vpd', 'vpd'
#     if sensor_key == 'dew_point':
#         return 'dew_point', 'dew_point'
#     return None, None



class SensorUtils:

    sensor_to_atom_sensor_dict = {
        'temperature': 'Temperature',
        'humidity': 'Humidity',
        'radiation': 'Radiation',
        'tension_top': 'Tension T',
        'tension_deep': 'Tension D',
        'ec': 'EC',
        'soil_temperature': 'Soil Temperature',
        'vibration': 'Vibration Raw Data',
        'battery_level': 'Battery Level',
        'flow': 'Flow',
        'soil_moisture': 'Soil Moisture',
        'watermark_top': 'Watermark top',
        'watermark_deep': 'Watermark deep',
        'upload_time': 'gw_read_time_utc',
        'vpd': 'vpd',
        'dew_point': 'dew_point',
        'datetime': 'sample_time_utc',
    }
    atom_sensor_to_sensor_dict = {
        'Temperature': 'temperature',

        'Humidity': 'humidity',
        'Radiation': 'radiation',
        'Tension T': 'tension_top',
        'Tension D': 'tension_deep',
        'EC': 'ec',
        'Soil Temperature': 'soil_temperature',
        'Vibration Raw Data': 'vibration',
        'Battery Level': 'battery_level',
        'Flow': 'flow',
        'Soil Moisture': 'soil_moisture',
        'Watermark top': 'watermark_top',
        'Watermark deep': 'watermark_deep',
        'gw_read_time_utc': 'upload_time',
        'vpd': 'vpd',
        'dew_point': 'dew_point',
        'sample_time_utc': 'datetime',
        'temperature': 'temperature',
        'humidity': 'humidity',
        'radiation': 'radiation',
        'tension_t': 'tension_top',
        'tension_d': 'tension_deep',
        'tension_b': 'tension_deep',
        'ec': 'ec',
        'soil_temperature': 'soil_temperature',
        'vibration': 'vibration',
        'battery_level': 'battery_level',
        'flow': 'flow',
        'soil_moisture': 'soil_moisture',
        'watermark_top': 'watermark_top',
        'watermark_deep': 'watermark_deep',
    }

    # sensors come in from atomation with different names this maps all known names to a single name
   
    @classmethod
    def get_sensor_name(cls, atom_record):
        
        return cls.atom_sensor_to_sensor_dict.get(atom_record['sensor_name'], None)
    
    @classmethod
    
    def get_grofit_sensors(cls, atom_records):
        from miniagro.downloaders.atom.atom_sensor_downloader import GrofitSensorStreamRecord

        if not isinstance(atom_records, list):
            atom_records = [atom_records]
        records = []
        for atom_record in atom_records:
            data ={}
            if not isinstance(atom_record, dict):
                atom_record = atom_record.to_dict()
            pprint.pprint(atom_record)
            for k, v in atom_record['data'].items():

                if k in SensorUtils.atom_sensor_to_sensor_dict:
                    data    [SensorUtils.atom_sensor_to_sensor_dict[k]] = v 
            if 'datetime' in data:
                del data['datetime']
            if 'upload_time' in data:
                del data['upload_time']
            name = DictUtils.get_value(atom_record, 'name', DictUtils.get_path(atom_record, 'data.device_name'))
            dt = DictUtils.get_datetime(atom_record, 'datetime', None)
            ut = DictUtils.get_datetime(atom_record, 'upload_time', None)
            source_id = atom_record['source_id']
            records.append(GrofitSensorStreamRecord(source_id, dt, ut, data, name))
        return records
    



    def __init__(self, source_id, sensors=None):
        self.source_id = source_id
        self.sensors = sensors
    def prepare_time_range(self, start, end):
        if not start:
            start = datetime.utcnow() - timedelta(days=10)
        if isinstance(start, str):
            start = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        if not end:
            end = datetime.utcnow() 
        if isinstance(end, str):
            end = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
        return start, end
   
    def get_raw_data(self, start, end, limit=10000, include_upload_time=False, as_df=False):
        start, end = self.prepare_time_range(start, end)
        pipeline = [
            {'$match': {'datetime': {'$gte': start, '$lte': end}}},
            {'$limit': limit}
        ]
        records = list(DMSDK().sensor_db.get_source_collection(self.source_id, 'sensor_raw').aggregate(pipeline))
        return records
    
    def get_sensor_records(self, start, end, limit=10000, include_upload_time=False, as_df=False, full_data=False):
        start, end = self.prepare_time_range(start, end)
        sensor_projection =[]
        if not self.sensors:
            self.sensors = list(SensorUtils.atom_sensor_to_sensor_dict.values())
        if self.sensors:
            for sensor in self.sensors:
                sensor_projection.append(SensorUtils.sensor_to_atom_sensor_dict[sensor])
            sensor_projection.append('sample_time_utc')
            if include_upload_time:
                sensor_projection.append('gw_read_time_utc')
        # else:
            
        #     sensor_projection = list(atom_sensor_to_sensor_dict.keys())
        projection = {f'data.{k}': 1 for k in sensor_projection}
        projection['_id'] = 0
       
        pipeline = [
            {'$match': {'source_id': self.source_id, 'datetime': {'$gte': start, '$lte': end}}},
        ]
        if not full_data:
            pipeline.append({'$project': projection})
        pipeline.append({'$limit': limit})
        # print(pipeline)
        records = list(DMSDK().atom_source_db.get_source_collection(self.source_id, 'atom_sensor_raw').aggregate(pipeline))
        if not records:
            return None
        # print('got records', len(records))
        # print(records[0])
        if not as_df:
            return records
        df = pd.DataFrame([entry['data'] for entry in records])
        df['datetime'] = pd.to_datetime(df['sample_time_utc'])
        if include_upload_time:
            df['upload_time'] = pd.to_datetime(df['gw_read_time_utc'])
        df.drop(columns=['sample_time_utc', 'gw_read_time_utc'], inplace=True, errors='ignore')
        df.rename(columns=SensorUtils.atom_sensor_to_sensor_dict, inplace=True)
        df.set_index('datetime', inplace=True)
        df.sort_index(inplace=True)
        # print(df.head())
        return df
    
    def get_grofit_sensor_records(self, start, end, limit=10000, include_upload_time=False, as_df=False, full_data=False):
        start, end = self.prepare_time_range(start, end)
        sensor_projection =[]
        if not self.sensors:
            self.sensors = list(SensorUtils.atom_sensor_to_sensor_dict.values())
        # if self.sensors:
            
        #     sensor_projection.append('sample_time_utc')
        #     if include_upload_time:
        #         sensor_projection.append('gw_read_time_utc')
        # else:
            
        #     sensor_projection = list(atom_sensor_to_sensor_dict.keys())
        if self.sensors:
            projection = {k:f'$data.{k}' for k in self.sensors}
        projection['_id'] = 0
        projection['datetime'] = 1
        if include_upload_time:
            projection['upload_time'] = 1
        pipeline = [
            {'$match': {'source_id': self.source_id, 'datetime': {'$gte': start, '$lte': end}}},
        ]
        if not full_data:
            pipeline.append({'$project': projection})
        pipeline.append({'$limit': limit})
        print(pipeline)
        records = list(DMSDK().sensor_db.get_source_collection(self.source_id, 'sensor_stream').aggregate(pipeline))
        nr = []

        print('got records', len(records))
        print(records[0])
        if not as_df:
            return records
        df = pd.DataFrame([entry for entry in records])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        # if include_upload_time:
        #     df['upload_time'] = pd.to_datetime(df['upload_time'])
        # df.rename(columns=SensorUtils.atom_sensor_to_sensor_dict, inplace=True)
        # df.set_index('datetime', inplace=True)
        df.sort_index(inplace=True)
        print(df.head())
        return df
    
    def get_advanced_sensor_records(self, start, end, limit=10000, include_upload_time=False, options=None, as_df=False):
        start, end = self.prepare_time_range(start, end)
        start = start - timedelta(days=1)
        records_df = self.get_grofit_sensor_records(start, end, limit, include_upload_time, True)
   
        sensors = [s for s in self.sensors if s in records_df.columns]
        if options:
            if 'agg' in options:
                agg_fn = options.get('agg_fn', 'mean')
                agg_res = options.get('agg', '1h')
                records_df = records_df.resample(agg_res).agg(agg_fn)
            if DictUtils.get_value(options, 'diff', False):
                for sensor in sensors:
                    records_df[f'{sensor}_diff'] = records_df[sensor].diff()
            if DictUtils.get_value(options, 'sec_diff', False):
                for sensor in sensors:
                    records_df[f'{sensor}_sec_diff'] = records_df[sensor].diff().diff()
            records_df.dropna(inplace=True)
            if start:
                records_df = records_df.loc[start:]

        if as_df:
            return records_df
        # print(records_df.head())
        records = records_df.reset_index().to_dict(orient="records")
        return records

    def get_app_records(self, start, end, limit=10000, options=None, sensors=None):
        start, end = self.prepare_time_range(start, end)
        records = self.get_advanced_sensor_records(start, end, limit, options=options, as_df=False)
        sensor_records = {}
     
        for rec in records:
            for k, v in rec.items():
                if k == 'datetime':
                    continue
                if sensors and k not in sensors:
                    continue
                dt = rec['datetime']
                if k not in sensor_records:
                    sensor_records[k] = []
                sensor_records[k].append([dt, v])
        final_records = {}
        for k, v in sensor_records.items():
            final_records[k] = {}
            final_records[k]['info'] = {}
            final_records[k]['info']['aggEvery'] = options.get('agg', '1h')
            final_records[k]['info']['aggFn'] = options.get('agg_fn', 'mean')
            final_records[k]['info']['sensorType'] = k
            final_records[k]['info']['name'] = k.replace('_', ' ').title()
            if k == 'vpd' or k == 'ec':
                final_records[k]['info']['name'] = k.upper()
            final_records[k]['records'] = v
        return final_records
    
    def get_track_records(self, start, end, options=None, limit=10000):
        from miniagro.app_data.app_daily_summary import DailySummaryRecord
        from miniagro.app_data.app_dynamic_nodes import GrofitCapsuleDynamicNode
        from miniagro.app_data.app_nodes import GrofitCapsuleNode
        source = GrofitCapsuleNode.get_collection().find_one({'source_id': self.source_id})
        gw_id = source['gw_id']
        start, end = self.prepare_time_range(start, end)
        # records = self.get_advanced_sensor_records(start, end, limit, options=options, include_upload_time=True)
        daily_col = DailySummaryRecord.get_collection()
        source_recs = SourceRecordUtils().get_source_records(gw_id, 'atom_gw_data', start, end)
        daily_recs = list(daily_col.find({ 'source_id': self.source_id, 'datetime': {'$gte': start, '$lte': end}}))
        pprint.pp(source_recs)

      
        dt_recs = {}
        ut_recs = {}
        ret = {
            'daily_track': {},
            'last_track': {}
        }
        # cell = status_rec['cell_signal']
        # ret['last_track']['signal'] = {'RSRP': cell['last_10_RSRP'], 'RSRQ': cell['last_10_RSRQ'], 'RSSI': cell['last_10_RSSI']}
        # ka = status_rec['keep_alive']
        # ret['last_track']['keep_alive'] = {'keep_alive': ka['last_10']}

        stream_recs = {
            'record': {'count': {}, 'expected': {}, 'pct': {}},
            'upload': {'count': {}, 'expected': {}, 'pct': {}},
            'cell_signal': {'RSRP': {}, 'RSRQ': {}, 'RSSI': {}}
        }
        st = start
        while st < end:
            dst = st.strftime('%Y-%m-%d')
            stream_recs['record']['count'][dst] = 0
            stream_recs['record']['expected'][dst] = 0
            stream_recs['record']['pct'][dst] = 0
            stream_recs['upload']['count'][dst] = 0
            stream_recs['upload']['expected'][dst] = 0
            stream_recs['upload']['pct'][dst] = 0
            stream_recs['cell_signal']['RSRP'][dst] = 0
            stream_recs['cell_signal']['RSRQ'][dst] = 0
            stream_recs['cell_signal']['RSSI'][dst] = 0
            st += timedelta(days=1)

        for rec in daily_recs:
            dt = rec['datetime'].strftime('%Y-%m-%d')
            stream_recs['record']['count'][dt] = rec['record']['count']
            stream_recs['record']['expected'][dt] = rec['record']['expected_count']
            stream_recs['record']['pct'][dt] = rec['record']['pct']
            stream_recs['upload']['count'][dt] = rec['upload']['count']
            stream_recs['upload']['expected'][dt] = rec['upload']['expected_count']
            stream_recs['upload']['pct'][dt] = rec['upload']['pct']
        
        has_cell_signal = False
        has_rssi = False
        has_rsrp = False
        has_rsrq = False
        for rec in source_recs:
            dt = rec['datetime'].strftime('%Y-%m-%d')
            cell = DictUtils.get_path(rec, 'data.device_event.data.cellular')
            if not cell:
                continue    
            stream_recs['cell_signal']['RSRP'][dt] = cell['RSRP']
            stream_recs['cell_signal']['RSRQ'][dt] = cell['RSRQ']
            stream_recs['cell_signal']['RSSI'][dt] = cell['RSSI']
            has_cell_signal = True
            has_rssi = has_rssi or cell['RSSI']
            has_rsrp = has_rsrp or cell['RSRP']
            has_rsrq = has_rsrq or cell['RSRQ']

        if not has_cell_signal:
            del stream_recs['cell_signal']

        else:
            if not has_rssi:
                del stream_recs['cell_signal']['RSSI']
            if not has_rsrp:
                del stream_recs['cell_signal']['RSRP']
            if not has_rsrq:
                del stream_recs['cell_signal']['RSRQ']
        pprint.pp(stream_recs)
        for k, v in stream_recs.items():
            # if k.startswith('cell_signal'):
            #     continue
            for k2, v2 in v.items():
                lr = []
                for k3, v3 in v2.items():
                    lr.append([k3, v3])
                stream_recs[k][k2]= lr
        pprint.pp(stream_recs)
      
          
     
     
        # pprint.pp(stream_recs)
        # for rec in source_recs:
        #     cell = DictUtils.get_path(rec, 'data.device_event.data.cellular')
        #     if not cell:
        #         continue
        #     dt = rec['datetime']
        #     if dt < start or dt > end:
        #         continue
        #     rssi = cell.get('RSSI', 0)
        #     rsrp = cell.get('RSRP', 0)
        #     rsrq = cell.get('RSRQ', 0)
        #     stream_recs['cell_signal']['RSSI'].append([dt, rssi])
        #     stream_recs['cell_signal']['RSRP'].append([dt, rsrp])
        #     stream_recs['cell_signal']['RSRQ'].append([dt, rsrq])
        pprint.pp(stream_recs)
        return stream_recs
    
    def get_app_track_records(self, start, end, limit=10000):
        start, end = self.prepare_time_range(start, end)
        ret = self.get_track_records(start, end, limit=limit)
        ret = DictUtils.to_camel_case(ret)
        ret = DictUtils.flatten_dict(ret)
        return ret
if __name__ == '__main__':
    su = SensorUtils('gd_D0_E3_D7_CA_F2_94')
    # recs = su.get_sensor_records(start='2025-02-21 00:00:00', end='2025-02-24 00:00:00', limit=10000, as_df=False, full_data=True)
    # srecs = SensorUtils.get_grofit_sensors(recs)
    # exit(1)
    options = {
        'agg': '1h',
        'agg_fn': 'max',
        'diff': True,
        'sec_diff': True
    }
    df = su.get_track_records(start='2025-02-21 00:00:00', end=datetime.utcnow(), limit=10000, options=options)
