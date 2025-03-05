from datetime import datetime, timedelta
import pprint
import time

import numpy as np
import pandas as pd
from tqdm import tqdm
from miniagro.config.server_config import ServerConfig
from miniagro.data_utils.sensor_utils import SensorUtils
from miniagro.data_utils.source_record_utils import SourceRecordUtils
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.downloaders.atom.atom_gateway_downloader import AtomGatewayDownloader
from miniagro.utils.db_utils import DBUtils
from miniagro.utils.grofit_id_utils import IDUtils
from miniagro.utils.param_utils import DictUtils
from miniagro.app_data.app_nodes import AppNodeBuilder
#  {
#             "_id": "dd-gd_D3_C0_2A_65_CE_E8",
#             "battery": {
#                 "level": 98,
#                 "voltage": 0
#             },
#             "data": {
#                 "record": {
#                     "expected": "2025-02-25 02:38:51",
#                     "last": "2025-02-25T02:28:51.000Z",
#                     "timer": 600
#                 },
#                 "upload": {
#                     "expected": "2025-02-25 03:39:33",
#                     "last": "2025-02-25 02:39:33",
#                     "timer": 3600
#                 }
#             },
#             "last_updated": "2025-02-25 02:51:55.540739",
#             "name": "IT 24SIW 30 cm",
#             "node_id": "dd-cap-gd_D3_C0_2A_65_CE_E8",
#             "node_type": "dd_grofit_capsule",
#             "source_id": "gd_D3_C0_2A_65_CE_E8"
#         },

class SummaryModule:
    def __init__(self):
        self.expected = 0
        self.count = 0
        self.status = 'unknown'
        self.last_update = datetime(1970, 1, 1)

    def to_dict(self):
        return {
            'expected': self.expected,
            'count': self.count,
            'status': self.status,
            'last_update': self.last_update,
        }

    def populate_from_dict(self, di):
        self.expected = di.get('expected', self.expected)
        self.count = di.get('count', self.count)
        self.status = di.get('status', self.status)
        self.last_update = di.get('last_update', self.last_update)
        return self
    
    def populate_records(self, dt, count, expected=None):
        
        if expected:
            self.expected = expected
        if dt < self.last_update:
            return
        self.count = count
        if self.count >= self.expected:
            self.status = 'good'
        elif self.count > self.expected - self.expected * 0.1:
            self.status = 'warning'
        else:
            self.status = 'missing'
        return self
       

    def update_status(self, full_record):
        pass

class UploadSummaryModule(SummaryModule):   
    def __init__(self):
        super().__init__()
        self.status = 'unknown'

    
    def update_status(self, full_record):
        if self.last > self.expected:
            self.status = 'unknown'
        elif self.last > self.expected - timedelta(seconds=self.timer+upload_leway):
            self.status = 'good'
        elif self.last > self.expected - timedelta(seconds=self.timer*10):
            self.status = 'late'
        elif self.last > self.expected - timedelta(days=14) + timedelta(seconds=self.timer*10):
            self.status = 'error'
        else:
            self.status = 'offline'


class DailySensorModule:
    def __init__(self, dt):
        self.sensors = {}
        self.dt = dt
        if self.dt:
            self.dt = self.dt.replace(hour=0, minute=0, second=0, microsecond=0)

    def record_to_sensor_error(self, record):
        errors = {}
        humidity = DictUtils.get_value(record, 'Humidity', -100)
        if humidity > -100 and (humidity < 0 or humidity > 100):
            errors['humidity'] = 1
        temperature = DictUtils.get_value(record, 'Temperature', -100)
        if temperature > -100 and (temperature < -10 or temperature > 70):
            errors['temperature'] = 1
        soil_temp = DictUtils.get_value(record, 'Soil Temperature', -100)
        if soil_temp > -100 and (soil_temp < -10 or soil_temp > 70):
            errors['soil_temperature'] = 1
        soil_moisture = DictUtils.get_value(record, 'Soil Moisture', -100)
        if soil_moisture > -100 and (soil_moisture < 0 or soil_moisture > 100):
            errors['soil_moisture'] = 1
        radiation = DictUtils.get_value(record, 'Radiation', -100)
        if radiation > -100 and (radiation < 0 or radiation > 20000):
            errors['radiation'] = 1
        ec = DictUtils.get_value(record, 'EC', -100)
        if ec > -100 and (ec < -10 or ec > 10):
            errors['ec'] = 1
        tension_top = DictUtils.get_value(record, 'Tension Top', -100)
        if tension_top > -100 and (tension_top < 0.5 or tension_top > 90):
            errors['tension_top'] = 1

        tension_deep = DictUtils.get_value(record, 'Tension D', -100)
        if tension_deep > -100 and (tension_deep < 0.5 or tension_deep > 90):
            errors['tension_deep'] = 1
       
        return errors
    
    def populate_from_record(self, record):
        rdt = DictUtils.get_datetime(record, 'datetime', None)
        if not rdt or rdt.replace(hour=0, minute=0, second=0, microsecond=0) != self.dt:
            return
        
        
        
        self.sensors = record.get('sensors', {})
        self.dt = rdt
        return self

    def to_dict(self):
        return {
            'sensors': self.sensors,
            'dt': self.dt,
        }

class DailySummaryRecord:
    @classmethod
    def get_collection(cls):
        return DMSDK().info_db.get_collection('daily_summary_info')
    
    def __init__(self, source_id, dt, record_count, upload_count, sensors, expected_record_count, expected_upload_count, status):
        self.source_id = source_id
        self.dt = dt
        self.record_count = record_count
        self.upload_count = upload_count
        self.sensors = sensors
        self.expected_record_count = expected_record_count
        self.expected_upload_count = expected_upload_count
        self.record_pct = int(self.record_count/self.expected_record_count*100) if self.expected_record_count > 0 else 0
        self.upload_pct = int(self.upload_count/self.expected_upload_count*100) if self.expected_upload_count > 0 else 0
        self.upload_status = self.get_upload_status()
        self.record_status = self.get_record_status()

    def get_upload_status(self):
        if self.expected_upload_count == 0:
            return 'unknown'
        if self.upload_pct >= 90:
            return 'good'
        elif self.upload_pct >= 70:
            return 'warning'
        elif self.upload_count > 1:
            return 'missing'
        else:
            return 'no_data'
        

    def get_record_status(self):
        if self.expected_record_count == 0:
            return 'unknown'
        if self.record_pct >= 90:
            return 'good'
        elif self.record_pct >= 70:
            return 'warning'
        elif self.record_count > 1:
            return 'missing'
        else:
            return 'no_data'

    def to_dict(self):
        return {
            '_id': f'{self.source_id}_{self.dt}',
            'source_id': self.source_id,
            'datetime': self.dt,
            'record_count': self.record_count,
            'upload_count': self.upload_count,
            'expected_record_count': self.expected_record_count,
            'expected_upload_count': self.expected_upload_count,
            'record_pct': self.record_pct,
            'upload_pct': self.upload_pct,
            'upload_status': self.upload_status,
            'record_status': self.record_status,
            'sensors': self.sensors,
        }


class AppDailySummaryBuilder1:
    @classmethod
    def get_collection(cls):
        return DMSDK().info_db.get_collection('daily_summary_info')
    
    def __init__(self):
        self.records = []

    def record_to_sensor_error(self, record):
        errors = {}
        humidity = DictUtils.get_value(record, 'Humidity', -100)
        if humidity > -100 and (humidity < 0 or humidity > 100):
            errors['humidity'] = 1
        temperature = DictUtils.get_value(record, 'Temperature', -100)
        if temperature > -100 and (temperature < -10 or temperature > 70):
            errors['temperature'] = 1
        soil_temp = DictUtils.get_value(record, 'Soil Temperature', -100)
        if soil_temp > -100 and (soil_temp < -10 or soil_temp > 70):
            errors['soil_temperature'] = 1
        soil_moisture = DictUtils.get_value(record, 'Soil Moisture', -100)
        if soil_moisture > -100 and (soil_moisture < 0 or soil_moisture > 100):
            errors['soil_moisture'] = 1
        radiation = DictUtils.get_value(record, 'Radiation', -100)
        if radiation > -100 and (radiation < 0 or radiation > 20000):
            errors['radiation'] = 1
        ec = DictUtils.get_value(record, 'EC', -100)
        if ec > -100 and (ec < -10 or ec > 10):
            errors['ec'] = 1
        tension_top = DictUtils.get_value(record, 'Tension Top', -100)
        if tension_top > -100 and (tension_top < 0.5 or tension_top > 90):
            errors['tension_top'] = 1

        tension_deep = DictUtils.get_value(record, 'Tension D', -100)
        if tension_deep > -100 and (tension_deep < 0.5 or tension_deep > 90):
            errors['tension_deep'] = 1
       
        return errors
  
    def update_nodes(self, source_id, start, end, app_node):
        if not start or not end:
            return
        
        if isinstance(start, str):
            start = datetime.strptime(start, '%Y-%m-%d')
        if isinstance(end, str):
            end = datetime.strptime(end, '%Y-%m-%d')
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = (end + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    
        pipeline = []
        #calculate the real first record we need which is the first record that datetime = start but make sure we dont miss any records 
        pipeline.append(
            {
                '$match': {
                    'datetime': {'$gte': start},
                    'datetime': {'$lte': end}
                }
            }
        )
        records = list(DMSDK().atom_source_db.get_collection(source_id, 'atom_sensor_raw').aggregate(pipeline))
        records = sorted(records, key=lambda x: x['datetime'], reverse=False)
        dt_sums = {}
        ut_sums = {}
        daily_recs = {}
        daily_ut_recs = {}
        daily_rec_errors = {}
        for rec in records:
            ut = DictUtils.get_datetime(rec, 'upload_time', None)
            dt = DictUtils.get_datetime(rec, 'datetime', None)
            if not ut or not dt:
                continue
            dts = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            uts = ut.replace(hour=0, minute=0, second=0, microsecond=0)
            if dts not in daily_recs:
                daily_recs[dts] = []
            if uts not in daily_ut_recs:
                daily_ut_recs[uts] = set()
            daily_recs[dts].append(rec)
            daily_ut_recs[uts].add(ut)
        
        for dts, recs in daily_recs.items():
            if dts < start or dts > end:
                continue
            dt_sums[dts] = len(recs)
        for uts, recs in daily_ut_recs.items():
            if uts < start or uts > end:
                continue
            ut_sums[uts] = len(recs)
        # pprint.pp(ut_sums)
        
        for dts, recs in daily_recs.items():
            if dts < start or dts > end:
                continue
            for rec in recs:
                errors = self.record_to_sensor_error(rec)
                if errors and len(errors) > 0:
                    pprint.pp(errors)
                daily_rec_errors[dts] = errors
        
        # pprint.pp(daily_rec_errors)
        ds_recs = []
        st = start
        expected_daily_records = 24*60*60//app_node.timers.get('sampling_timer', 0) if app_node.timers.get('sampling_timer', 0) > 0 else 0
        expected_daily_uploads = 24*60*60//app_node.timers.get('uploading_timer', 0) if app_node.timers.get('uploading_timer', 0) > 0 else 0
        while st < end:
            st_r = st.strftime('%Y-%m-%d')
            ds_recs.append(DailySummaryRecord(source_id, 
                                           st_r, 
                                           dt_sums.get(st, 0), 
                                           ut_sums.get(st, 0), 
                                           daily_rec_errors.get(st, {}),
                                           expected_daily_records, 
                                           expected_daily_uploads, 
                                           'unknown'))
           
            # pprint.pp(recs[-1].to_dict())
            st += timedelta(days=1)
        self.records.extend(recs)
        return recs

    def save_to_db(self):
        col = DailySummaryRecord.get_collection()
        DBUtils.update_bulk_records(col, self.records)
        print(f'Saved {len(self.records)} records to db')
    # def update_nodes(self, records):
    #     records = sorted(records, key=lambda x: x['datetime'], reverse=False)
        
        # last_record = None
        # for rec in records:
        #     if record_type == 'gw':




class AppDailySummaryBuilder:
    
    @classmethod
    def get_collection(cls):
        return DMSDK().info_db.get_collection('daily_summary_info')
    
    @classmethod
    def load_records(cls, source_id, start, end):
        col = cls.get_collection()
        return list(col.find({
            'source_id': source_id,
            'datetime': {'$gte': start, '$lte': end}
        }))
        
    @classmethod
    def load_multiple_sources(cls, source_ids, start, end):
        col = cls.get_collection()
        if not start or not end:
            return []
        if isinstance(start, str):
            start = datetime.strptime(start, '%Y-%m-%d')
        if isinstance(end, str):
            end = datetime.strptime(end, '%Y-%m-%d')
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = end.replace(hour=0, minute=0, second=0, microsecond=0)
        return list(col.find({
            'source_id': {'$in': source_ids},
            'datetime': {'$gte': start, '$lte': end}
        }))
    def __init__(self):
        self.records = []
        self.accepted = {
            'temperature': [-10, 70],
            'soil_temperature': [-10, 70],
            'humidity': [0, 100],
            'soil_moisture': [0, 100],
            'radiation': [0, 20000],
            'ec': [-10, 10],
            'tension_top': [0.5, 90],
            'tension_deep': [0.5, 90],
            'battery_level': [0, 100],
        }
    def get_status(self, count, expected_count):
        if not count or not expected_count:
            return 'unknown'
        if count >= expected_count:
            return 'good'
        elif count >= expected_count - expected_count * 0.1:
            return 'warning'
        else:
            return 'missing'
        
   
    def update_nodes(self, source_id, start, end, app_node):
        if not start or not end or not app_node:
            return []
        if isinstance(start, str):
            start = datetime.strptime(start, '%Y-%m-%d')
        if isinstance(end, str):
            end = datetime.strptime(end, '%Y-%m-%d')
        start = (start-timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = (end + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        sensor_info = SensorUtils(source_id).get_sensor_records(start, end, as_df=True, include_upload_time=True)
        if sensor_info is None or sensor_info.empty:
            return []
        unique_dates = pd.to_datetime(sensor_info.index.date).unique()  
        rec_records = {}
        for date in unique_dates:
            if not date or not isinstance(date, datetime) or pd.isna(date):
                continue
            # Convert the date to a string in 'YYYY-MM-DD' format
            date_str = date.strftime('%Y-%m-%d')
            count = len(sensor_info.loc[date_str])
            expected_count = 24*60*60//app_node.timers.get('sampling_timer', 0) if app_node.timers.get('sampling_timer', 0) > 0 else 0
            status = self.get_status(count, expected_count)
            pct = int(count/expected_count*100) if expected_count > 0 else 0
            # Slice the DataFrame using .loc with the date string
            rec_records[date_str] = {
                'count': count,
                'expected_count': expected_count,
                'status': status,
                'pct': pct
            }
        # pprint.pp(rec_records)
  
        ut_info = sensor_info[['upload_time']]
        ut_unique = list(set(ut_info['upload_time']))
        ut_unique.sort()
        # pprint.pp(ut_unique)
        ut_records = {}
        expected_upload_count = 24*60*60//app_node.timers.get('uploading_timer', 0) if app_node.timers.get('uploading_timer', 0) > 0 else 0
        for date in ut_unique:
            if not date or not isinstance(date, datetime) or pd.isna(date):
                continue
            date_str = date.strftime('%Y-%m-%d')
# Convert the index to strings for the check
            if date_str not in ut_records:

                utr = {
                    'count': 0,
                    'expected_count': expected_upload_count,
                    'status': 'unknown',
                    'pct': 0,
                }
                ut_records[date_str] = utr
            utr['count'] += 1
        for date, utr in ut_records.items(): 
            utr['pct'] = int(utr['count']/expected_upload_count*100) if expected_upload_count > 0 else 0
            utr['status'] = self.get_status(utr['count'], expected_upload_count)

        # pprint.pp(ut_records)
        grouped_df = sensor_info.groupby(sensor_info.index.date).agg(['max', 'min'])
        for sensor, (low, high) in self.accepted.items():
            if sensor in sensor_info.columns:
                grouped_df[(sensor, 'error')] = (grouped_df[(sensor, 'min')] < low) | (grouped_df[(sensor, 'max')] > high)

        # pprint.pp(ut_records)
        for sensor in self.accepted.keys():
            if (sensor, 'max') in grouped_df.columns and (sensor, 'min') in grouped_df.columns:
                grouped_df[(sensor, 'max_trend_3')] = (grouped_df[(sensor, 'max')] - grouped_df[(sensor, 'max')].shift(3))
                grouped_df[(sensor, 'min_trend_3')] = grouped_df[(sensor, 'min')] - grouped_df[(sensor, 'min')].shift(3)

    # Convert the grouped DataFrame into a dictionary with a structured format
        records_dict = {
        str(date): {

                sensor: {
                    metric: round(value, 3) if isinstance(value, (int, float)) and not pd.isna(value) else value
                    for metric, value in {
                        metric: row[(sensor, metric)] for metric in ['max', 'min', 'error', 'max_trend_3', 'min_trend_3']
                        if (sensor, metric) in row
                    }.items()
                }
                for sensor in self.accepted.keys()
                if any((sensor, metric) in row for metric in ['max', 'min', 'error', 'max_trend_3', 'min_trend_3'])
            
        }
        for date, row in grouped_df.iterrows()
    }   
        
    # Display the dictionary
        # app_node = nodes.get(source_id)
        has_sensor_error = False
        for dt, rec in records_dict.items():
           
          
            rec['record'] = rec_records.get(dt, {})
            rec['upload'] = ut_records.get(dt, {})
            rec['source_id'] = source_id
            rec['datetime'] = datetime.strptime(dt, '%Y-%m-%d')
            rec['_id'] = f'{source_id}_{dt}'
            rec['name'] = app_node.name
            for sensor in self.accepted.keys():
                if sensor not in rec:
                    continue
                if np.isnan(rec[sensor]['max_trend_3']):
                    rec[sensor]['max_trend_3'] = 0
                if np.isnan(rec[sensor]['min_trend_3']):
                    rec[sensor]['min_trend_3'] = 0
                if rec[sensor]['error']:
                    has_sensor_error = True
            # rec['record_count'] = 
            rec['sensor_error_status'] = 'good' if not has_sensor_error else 'error'
        # pprint.pp(records_dict)
        self.records.extend(records_dict.values())

        return self.records

    def save_to_db(self):
        col = AppDailySummaryBuilder.get_collection()
        DBUtils.update_bulk_records(col, self.records)
        print(f'Saved {len(self.records)} records to db')

class RangeSummaryRecordBuilder:
    def __init__(self):
        self.records = []
        self.accepted = {
            'temperature': [-10, 70],
            'soil_temperature': [-10, 70],
            'humidity': [0, 100],
            'soil_moisture': [0, 100],
            'radiation': [0, 20000],
            'ec': [-10, 10],
            'tension_top': [0.5, 90],
            'tension_deep': [0.5, 90],
            'battery_level': [0, 100],
        }
    def get_status(self, count, expected_count, is_last_record=False):
        if is_last_record:
            return 'incomplete'
        if not count or not expected_count:
            return 'unknown'
        if count >= expected_count:
            return 'good'
        elif count >= expected_count - expected_count * 0.1:
            return 'warning'
        else:
            return 'missing'
        

    def create_range_summary_records(self, source_ids, start, end):
        app_nodes = AppNodeBuilder().load_nodes(source_ids)
        app_nodes = {node['source_id']: node for node in app_nodes}
        records = AppDailySummaryBuilder.load_multiple_sources(source_ids, start, end)
        source_recs = {}
        for rec in records:
            if rec['source_id'] not in source_recs:
                source_recs[rec['source_id']] = []
            source_recs[rec['source_id']].append(rec)
        summary_recs = []
        for source_id, recs in source_recs.items():
            summary_recs.append(self.create_summary_record(source_id, start, end, True))
        return summary_recs
   
    def create_summary_record(self, source_id, start, end, is_last_record=False, records=[]):
        if isinstance(start, str):
            start = datetime.strptime(start, '%Y-%m-%d')
        if isinstance(end, str):
            end = datetime.strptime(end, '%Y-%m-%d')
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = end.replace(hour=0, minute=0, second=0, microsecond=0)
        if not records:
            records = AppDailySummaryBuilder.load_records(source_id, start, end)
        if not records:
            return None
        self.records = records
        sum_rec ={
            'date_range': [start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")],
            'id': f'{source_id}_{start.strftime("%Y-%m-%d")}_{end.strftime("%Y-%m-%d")}',
            'has_full_data': False,
            'actual_date_range': ['',''],
            'source_id': source_id,
            'name': records[0].get('name', ''),
            'record': {
                'count': 0,
                'expected_count': 0,
                'status': 'unknown',
                'pct': 0,
            },
            'upload': {
                'count': 0,
                'expected_count': 0,
                'status': 'unknown',
                'pct': 0,
            },
        }
        if not self.records:
            return sum_rec
        self.records.sort(key=lambda x: x['datetime'])
        first_rec_dt = DictUtils.get_datetime(self.records[0], "datetime", None)
        last_rec_dt = DictUtils.get_datetime(self.records[-1], "datetime", None)
        if first_rec_dt and last_rec_dt:
            sum_rec['actual_date_range'] = [first_rec_dt.strftime("%Y-%m-%d"), last_rec_dt.strftime("%Y-%m-%d")]
        sum_rec['has_full_data'] = first_rec_dt == start and last_rec_dt == end
          
        for rec in self.records:
            sum_rec['record']['count'] += DictUtils.get_path(rec, 'record.count', 0)
            sum_rec['upload']['count'] += DictUtils.get_path(rec, 'upload.count', 0)
            sum_rec['record']['expected_count'] += DictUtils.get_path(rec, 'record.expected_count', 0)
            sum_rec['upload']['expected_count'] += DictUtils.get_path(rec, 'upload.expected_count', 0)
            sum_rec['record']['pct'] = int(sum_rec['record']['count']/sum_rec['record']['expected_count']*100) if sum_rec['record']['expected_count'] > 0 else 0
            sum_rec['upload']['pct'] = int(sum_rec['upload']['count']/sum_rec['upload']['expected_count']*100) if sum_rec['upload']['expected_count'] > 0 else 0
        sum_rec['record']['status'] = self.get_status(sum_rec['record']['count'], sum_rec['record']['expected_count'], is_last_record=is_last_record)
        sum_rec['upload']['status'] = self.get_status(sum_rec['upload']['count'], sum_rec['upload']['expected_count'], is_last_record=is_last_record )

        has_sensor_error = False
        for sensor in self.accepted.keys():

            if sensor not in self.records[0]:
                continue
            mt = self.records[-1][sensor]['max'] - self.records[0][sensor]['max']
            lt = self.records[-1][sensor]['min'] - self.records[0][sensor]['min']
            sum_rec[sensor] = {
                'max': 0,
                'min': 100000,
                'error': False,
                'max_trend': mt if not np.isnan(mt) else 0,
                'min_trend': lt if not np.isnan(lt) else 0,
            }
            
            for rec in self.records:
                if sensor in rec:
                    sum_rec[sensor]['max'] = max(sum_rec[sensor]['max'], rec[sensor]['max'])
                    sum_rec[sensor]['min'] = min(sum_rec[sensor]['min'], rec[sensor]['min'])
                    sum_rec[sensor]['error'] = sum_rec[sensor]['error'] or rec[sensor]['error']
                if rec[sensor]['error']:
                    has_sensor_error = True
        sum_rec['sensor_error_status'] = 'good' if not has_sensor_error else 'error'
        return sum_rec
    

if __name__ == '__main__':
    from miniagro.app_data.app_nodes import AppNodeBuilder
    col = DailySummaryRecord.get_collection()
    # col.delete_many({})

    nodes = AppNodeBuilder().build_nodes()
    nodes = {node.source_id: node for node in nodes.values()}


    builder = AppDailySummaryBuilder()
    builder.update_nodes('gd_EF_64_A0_C5_23_B8', datetime(2025, 2, 19), datetime(2025, 3, 1), nodes['gd_EF_64_A0_C5_23_B8'])
    builder.save_to_db()
    tm = time.time()
    sum_recs = []
    d_recs = []
    rb = RangeSummaryRecordBuilder()
    for node in tqdm(nodes.values(), desc='Updating daily summary records', total=len(nodes)):
        builder.update_nodes(node.source_id, datetime(2025, 2, 19), datetime(2025, 3, 1), node)
        # builder.save_to_db()
    d_recs = builder.records
        # sum_recs.append(rb.create_summary_record(node.source_id, datetime(2025, 2, 19), datetime(2025, 3, 1)))
    print(f'Saving {len(d_recs)} records to db')
    DBUtils.update_bulk_records(DailySummaryRecord.get_collection(), d_recs)
    sum_recs = rb.create_range_summary_records(list(nodes.keys()), datetime(2025, 2, 19), datetime(2025, 3, 1))

    tm1 = time.time()

    if sum_recs:
        pprint.pp(sum_recs[0])
    print(f'Time taken: {tm1 - tm}')

    exit(1)
    accepted = {
        'temperature': [-10, 70],
        'soil_temperature': [-10, 70],
        'humidity': [0, 100],
        'soil_moisture': [0, 100],
        'radiation': [0, 20000],
        'ec': [-10, 10],
        'tension_top': [0.5, 90],
        'tension_deep': [0.5, 90],
        'battery_level': [0, 100],
    }
    source_id = 'gd_EF_64_A0_C5_23_B8'
    sensor_info = SensorUtils('gd_EF_64_A0_C5_23_B8').get_sensor_records(datetime(2025, 2, 19), datetime(2025, 3, 1), as_df=True, include_upload_time=True)
    df = sensor_info
    unique_dates = pd.to_datetime(df.index.date).unique()

# Create a dictionary to store the sub-DataFrames for each date
    dfs = {}
    for date in unique_dates:
        # Convert the date to a string in 'YYYY-MM-DD' format
        date_str = date.strftime('%Y-%m-%d')
        # Slice the DataFrame using .loc with the date string
        dfs[date_str] = len(df.loc[date_str])
    pprint.pp(dfs)
    # new_dfs = {}
    # for date, sub_df in dfs.items():
    #     for column in sub_df.columns:
    #         new_dfs[date][column] = sub_df[column].agg(['max', 'min'])

    # pprint.pp(new_dfs)
    # Example: Print the DataFrame for a specific date
    # print("Data for 2023-03-01:")
    # print(dfs['2023-03-01'])
# Add error columns for each sensor
    grouped_df = sensor_info.groupby(sensor_info.index.date).agg(['max', 'min'])
    for sensor, (low, high) in accepted.items():
        if sensor in sensor_info.columns:
            grouped_df[(sensor, 'error')] = (grouped_df[(sensor, 'min')] < low) | (grouped_df[(sensor, 'max')] > high)

    pprint.pp(grouped_df)
    ut_info = sensor_info[['upload_time']]
    ut_grouped_df = ut_info.groupby(ut_info['upload_time'].dt.date).agg(lambda x: x.nunique())
    pprint.pp(ut_grouped_df)
    ut_records = {}
    for date, row in ut_grouped_df.iterrows():
        ut_records[date.strftime('%Y-%m-%d')] = float(row.get('upload_time', 0))
        
    pprint.pp(ut_records)
    # flattened_df = grouped_df.reset_index()

    # Rename columns to make them more structured (flatten multi-index)
    # flattened_df.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in flattened_df.columns]
    
    # pprint.pp(flattened_df)
    # records = flattened_df.to_dict(orient="records")
    # pprint.pp(records)
    
    # structured_dict = {
    # str(row['index_']): {
    #     sensor: {
    #         "max": row.get(f"{sensor}_max", None),
    #         "min": row.get(f"{sensor}_min", None),
    #         "error": row.get(f"{sensor}_error", None)
    #     }
    #     for sensor in accepted.keys() if f"{sensor}_max" in row and f"{sensor}_min" in row
    #     }
    #     for row in flattened_df.to_dict(orient="records")
    # }
    # pprint.pp(structured_dict)


    for sensor in accepted.keys():
        if (sensor, 'max') in grouped_df.columns and (sensor, 'min') in grouped_df.columns:
            grouped_df[(sensor, 'max_trend_3')] = (grouped_df[(sensor, 'max')] - grouped_df[(sensor, 'max')].shift(3))
            grouped_df[(sensor, 'min_trend_3')] = grouped_df[(sensor, 'min')] - grouped_df[(sensor, 'min')].shift(3)
    pprint.pp(grouped_df)


    records = grouped_df.to_dict(orient="records")
  # Convert the grouped DataFrame into a dictionary with a structured format
    records_dict = {
    str(date): {

            sensor: {
                metric: round(value, 3) if isinstance(value, (int, float)) and not pd.isna(value) else value
                for metric, value in {
                    metric: row[(sensor, metric)] for metric in ['max', 'min', 'error', 'max_trend_3', 'min_trend_3']
                    if (sensor, metric) in row
                }.items()
            }
            for sensor in accepted.keys()
            if any((sensor, metric) in row for metric in ['max', 'min', 'error', 'max_trend_3', 'min_trend_3'])
        
    }
    for date, row in grouped_df.iterrows()
}   
    
# Display the dictionary
    app_node = nodes.get(source_id)
    pprint.pp(records_dict) 
    pprint.pp(grouped_df.keys())
    for dt, rec in records_dict.items():
        rs = {}
        ul = {}
        rs['count'] = dfs.get(dt, 0)
        rs['expected_count'] = 24*60*60//app_node.timers.get('sampling_timer', 0) if app_node.timers.get('sampling_timer', 0) > 0 else 0
        rec['upload_expected_count'] = 24*60*60//app_node.timers.get('uploading_timer', 0) if app_node.timers.get('uploading_timer', 0) > 0 else 0
        rs['status'] = get_status(rs['count'], rs['expected_count'])
        ul['count'] = ut_records.get(dt, 0)
        ul['expected_count'] = 24*60*60//app_node.timers.get('uploading_timer', 0) if app_node.timers.get('uploading_timer', 0) > 0 else 0
        ul['status'] = get_status(ul['count'], ul['expected_count'])
        rs['pct'] = int(rs['count']/rs['expected_count']*100) if rs['expected_count'] > 0 else 0
        ul['pct'] = int(ul['count']/ul['expected_count']*100) if ul['expected_count'] > 0 else 0
        rec['record'] = rs
        rec['upload'] = ul
        rec['source_id'] = source_id
        rec['datetime'] = dt
        rec['_id'] = f'{source_id}_{dt}'
        rec['name'] = app_node.name
        
        # rec['record_count'] = 
    pprint.pp(records_dict)
    # pprint.pp(structured_dict)
    exit(1)
    for node in nodes.values():
       

        break
        builder = AppDailySummaryBuilder()
        builder.update_nodes(node.source_id, datetime(2025, 2, 19), datetime(2025, 3, 1), node)
        builder.save_to_db()