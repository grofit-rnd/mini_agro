from datetime import datetime, timedelta
import pprint
import time
from miniagro.config.server_config import ServerConfig
from miniagro.data_utils.sensor_utils import SensorUtils
from miniagro.data_utils.source_record_utils import SourceRecordUtils
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.downloaders.atom.atom_gateway_downloader import AtomGatewayDownloader
from miniagro.utils.db_utils import DBUtils
from miniagro.utils.grofit_id_utils import IDUtils
from miniagro.utils.param_utils import DictUtils
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

class TimerModule:
    def __init__(self):
        self.timer = 0
        self.last = datetime(1970, 1, 1)
        self.expected = datetime(1970, 1, 1)
        self.last_10 = []
        self.status = 'none'
    def to_dict(self):
        return {
            'timer': self.timer,
            'last': self.last,
            'expected': self.expected,
            'last_10': self.last_10,
            'status': self.status,
        }
    def populate_from_dict(self, di):
        self.timer = di.get('timer', self.timer)
        self.last = di.get('last', self.last)
        self.expected = di.get('expected', self.expected)
        self.last_10 = di.get('last_10', self.last_10)
        self.status = di.get('status', self.status)
        return self
    
    def populate_timer_data(self, new_time, timer_value=None, full_record=None):
        if new_time < self.last:
            return
        if timer_value:
            self.timer = timer_value
        self.last = new_time
        self.expected = new_time + timedelta(seconds=self.timer)
        self.last_10.append(new_time)
        if len(self.last_10) > 10:
            self.last_10.pop(0)
        self.update_status(full_record)
        return self

    def update_status(self, full_record):
        pass

class UploadTimerModule(TimerModule):
    def __init__(self):
        super().__init__()
        self.status = 'unknown'

    
    def update_status(self, full_record):
        upload_leway = self.timer* 0.9
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


class RecordTimerModule(TimerModule):
    def update_status(self, full_record):
        upload = full_record.upload
        
        if self.expected < upload.last:
            self.status = upload.status
        elif self.last > self.expected - timedelta(seconds=self.timer):
            self.status = 'good'
        else:
            self.status = 'error'

class KeepaliveTimerModule(TimerModule):
    
    def populate_timer_data(self, new_time, timer_value=None, full_record=None):
        self.last = new_time
        self.last_10.append(new_time)
        if len(self.last_10) > 10:
            self.last_10.pop(0)
        return self
      
      
class BatteryModule:
    def __init__(self):
        self.level = 0
        self.voltage = 0
        self.status = 'unknown'
        self.last_low = 0
        self.last_low_time = datetime(1970, 1, 1)
        self.weakly_low = False
        self.last_update = datetime(1970, 1, 1)
    
    def populate_from_dict(self, di):
        self.level = di.get('level', 0)
        self.voltage = di.get('voltage', 0)
        self.status = di.get('status', 'unknown')
        self.last_update = di.get('last_update', datetime(1970, 1, 1))
        self.last_low = di.get('last_low', 0)
        self.last_low_time = di.get('last_low_time', datetime(1970, 1, 1))
        self.weakly_low = di.get('weakly_low', False)
        return self
    
    def to_dict(self):
        return {
            'level': self.level,
            'voltage': self.voltage,
            'status': self.status,
            'last_update': self.last_update,
            'last_low': self.last_low,
            'last_low_time': self.last_low_time,
            'weakly_low': self.weakly_low,
        }

    def populate_from_record(self, new_time, level=None, voltage=None):
        if new_time < self.last_update:
            return
        if level:
            self.level = level
        if voltage:
            self.voltage = voltage
        if self.level > 90:
            self.status = 'good'
        elif self.level > 80:
            self.status = 'used'
        elif self.level > 40:
            self.status = 'warning'
        elif self.level > 10:
            self.status = 'low'
        else:
            self.status = 'critical'
        
        if self.level <= 40:
            self.last_low = self.level
            self.last_low_time = datetime.utcnow()
        elif self.last_low_time < datetime.utcnow() - timedelta(days=7):
            self.weakly_low = True
        else:
            self.weakly_low = False
        return self

class CellSignalModule:
    def __init__(self):
        self.last_update = datetime(1970, 1, 1)
        self.RSRP = 0
        self.RSRQ = 0
        self.RSSI = 0
        self.last_10_RSRP = []
        self.last_10_RSRQ = []
        self.last_10_RSSI = []
        self.isWeak = False
        self.last_weak = datetime(1970, 1, 1)
        self.network_type = 'unknown'
        self.status = 'unknown'

    def populate_from_dict(self,  di):
       
        self.last_update = DictUtils.get_datetime(di, 'last_time', datetime(1970, 1, 1))
        self.RSRP = di.get('RSRP', 0)
        self.RSRQ = di.get('RSRQ', 0)
        self.RSSI = di.get('RSSI', 0)
        self.last_10_RSRP = di.get('last_10_RSRP', [])
        self.last_10_RSRQ = di.get('last_10_RSRQ', [])
        self.last_10_RSSI = di.get('last_10_RSSI', [])
        self.isWeak = di.get('isWeak', False)
        self.last_weak = di.get('last_weak', datetime(1970, 1, 1))
        self.network_type = di.get('network_type', 'unknown')
        self.status = di.get('status', 'unknown')
        return self
    
    def to_dict(self):
        return {
            'last_time': self.last_update,
            'RSRP': self.RSRP,
            'RSRQ': self.RSRQ,
            'RSSI': self.RSSI,
            'last_10_RSRP': self.last_10_RSRP,
            'last_10_RSRQ': self.last_10_RSRQ,
            'last_10_RSSI': self.last_10_RSSI,
            'isWeak': self.isWeak,
            'last_weak': self.last_weak,
            'network_type': self.network_type,
            'status': self.status,
        }
    
    def populate_from_record(self, new_time, rsrp=None, rsrq=None, rssi=None, signal_quality=None, network_type=None):
        if new_time < self.last_update:
            return
        if rsrp != None:
            self.RSRP = rsrp
            self.last_10_RSRP.append(rsrp)
            if len(self.last_10_RSRP) > 10:
                self.last_10_RSRP.pop(0)
            if self.RSRP < -100:
                self.isWeak = True
                self.last_weak = datetime.utcnow()
            else:
                self.isWeak = False
        if rsrq != None :
            self.RSRQ = rsrq
            self.last_10_RSRQ.append(rsrq)
            if len(self.last_10_RSRQ) > 10:
                self.last_10_RSRQ.pop(0)
        if rssi != None:
            self.RSSI = rssi
            self.last_10_RSSI.append(rssi)
            if len(self.last_10_RSSI) > 10:
                self.last_10_RSSI.pop(0)
       
        if signal_quality != None:
            self.signal_quality = signal_quality
        if network_type != None:
            self.network_type = network_type
        return self
    
    def update_status(self, full_record):
        if self.isWeak:
            self.status = 'weak'
        else:
            self.status = 'good'
        
class SystemModule:
    def __init__(self):
        self.reboots = 0
        self.run_time_sec = 0
        self.status = 'unknown'
        self.last_reboot = datetime(1970, 1, 1)
        self.last_update = datetime(1970, 1, 1)

    def populate_from_dict(self, di):
        self.reboots = di.get('reboots', 0)
        self.run_time_sec = di.get('run_time_sec', 0)
        self.status = di.get('status', 'unknown')
        self.last_reboot = di.get('last_reboot', datetime(1970, 1, 1))
        self.last_update = di.get('last_update', datetime(1970, 1, 1))
        return self
    def to_dict(self):
        return {
            'reboots': self.reboots,
            'run_time_sec': self.run_time_sec,
            'status': self.status,
            'last_reboot': self.last_reboot,
            'last_update': self.last_update,
        }
    def populate_from_record(self,  new_time, reboots=None, run_time_sec=None, last_reset_reason=None):
        if new_time < self.last_update:
            return
        self.last_update = new_time
        if reboots != None:
            if self.reboots != reboots:
                self.reboots = reboots
                self.last_reboot = datetime.utcnow()
        if last_reset_reason != None:
            self.last_reset_reason = last_reset_reason
        if run_time_sec != None:
            self.run_time_sec = run_time_sec
        if self.last_reboot > datetime.utcnow() - timedelta(days=1):
            self.status = 'error'
        elif self.last_reboot > datetime.utcnow() - timedelta(days=3):
            self.status = 'warning'
        else:
            self.status = 'good'
        return self

class HttpCountersModule:
    def __init__(self):
        self.last_update = datetime(1970, 1, 1)
        self.get_failures = 0
        self.get_success = 0
        self.last_failure_rssi = 0
        self.last_failure_signal_quality = 0
        self.last_failure_timestamp = datetime(1970, 1, 1)
        self.post_failures = 0
        self.post_success = 0
        self.last_post_failure = datetime(1970, 1, 1)
        self.last_get_failure = datetime(1970, 1, 1)
        self.last_rssi_failure = datetime(1970, 1, 1)
        self.last_signal_quality_failure = datetime(1970, 1, 1)
        self.status = 'unknown'

    def populate_from_dict(self, di):
        self.last_update = DictUtils.get_datetime(di, 'last_update', datetime(1970, 1, 1))
        self.get_failures = di.get('get_failures', 0)
        self.get_success = di.get('get_success', 0)
        self.last_failure_rssi = di.get('last_failure_rssi', 0)
        self.last_failure_signal_quality = di.get('last_failure_signal_quality', 0)
        self.last_failure_timestamp = di.get('last_failure_timestamp', datetime(1970, 1, 1))
        self.post_failures = di.get('post_failures', 0)
        self.post_success = di.get('post_success', 0)
        self.last_post_failure = di.get('last_post_failure', datetime(1970, 1, 1))
        self.last_get_failure = di.get('last_get_failure', datetime(1970, 1, 1))
        self.status = di.get('status', 'unknown')
        return self

    def to_dict(self):
        return {
            'last_update': self.last_update,
            'get_failures': self.get_failures,
            'get_success': self.get_success,
            'last_get_failure': self.last_get_failure,
            'post_failures': self.post_failures,
            'post_success': self.post_success,
            'last_post_failure': self.last_post_failure,
            'status': self.status,
        }
    def populate_from_record(self, new_time, http_info):
        if new_time < self.last_update:
            return
        if http_info.get('get_failures', 0) != self.get_failures:
            self.get_failures = http_info.get('get_failures', 0)
            self.last_get_failure = datetime.utcnow()
            self.last_failure_timestamp = datetime.utcnow()
        if http_info.get('get_success', 0) != self.get_success:
            self.get_success = http_info.get('get_success', 0)
            self.last_get_success = datetime.utcnow()
            self.last_failure_timestamp = datetime.utcnow()
        
        if http_info.get('post_failures', 0) != self.post_failures:
            self.post_failures = http_info.get('post_failures', 0)
            self.last_post_failure = datetime.utcnow()
            self.last_failure_timestamp = datetime.utcnow()
        if http_info.get('post_success', 0) != self.post_success:
            self.post_success = http_info.get('post_success', 0)
    
        if http_info.get('last_failure_rssi', 0) != self.last_failure_rssi:
            self.last_failure_rssi = http_info.get('last_failure_rssi', 0)
            self.last_rssi_failure = datetime.utcnow()
            self.last_failure_timestamp = datetime.utcnow()
        if http_info.get('last_failure_signal_quality', 0) != self.last_failure_signal_quality:
            self.last_failure_signal_quality = http_info.get('last_failure_signal_quality', 0)
            self.last_signal_quality_failure = datetime.utcnow()
            self.last_failure_timestamp = datetime.utcnow()
        

        if self.last_get_failure < datetime.utcnow() - timedelta(days=1):
            self.status = 'error'
        elif self.last_get_failure < datetime.utcnow() - timedelta(days=3):
            self.status = 'warning'
        else:
            self.status = 'good'
        
        return self

class GrofitCapsuleDynamicNode:
    @classmethod
    def get_collection(cls):
        return DMSDK().info_db.get_collection('dynamic_capsule_info')
    

    def __init__(self, source_id):
        self.source_id = source_id
        self.node_type = 'grofit_dynamic_capsule'
        self.name = None
        self.node_id = source_id
        self.mac = None
        self.last_updated = datetime.utcnow()
        
        self.gw_id = None
        self.upload = UploadTimerModule()
        self.battery = BatteryModule()  
        self.record = RecordTimerModule()
        self.keep_alive = KeepaliveTimerModule()
        self.http_counters = HttpCountersModule()
        self.cell_signal = CellSignalModule()
        self.system = SystemModule()
        self.status = 'unknown'
        self.last_update = datetime(1970, 1, 1)

    def to_dict(self):
        return {
            '_id': self.source_id,
            'source_id': self.source_id,
            'mac': self.mac,
            'gw_id': self.gw_id,
            'node_type': self.node_type,
            'name': self.name,
            'node_id': self.node_id,
            'last_updated': self.last_updated,
            'battery': self.battery.to_dict(),
            'record': self.record.to_dict(),
            'upload': self.upload.to_dict(),
            'keep_alive': self.keep_alive.to_dict(),
            'http_counters': self.http_counters.to_dict(),
            'cell_signal': self.cell_signal.to_dict(),
            'system': self.system.to_dict(),
            'last_update': self.last_update,
        }
    
    def populate_from_dict(self, di):
        self.source_id = DictUtils.get_path(di, 'source_id', self.source_id)
        self.mac = DictUtils.get_path(di, 'mac', self.mac)
        self.gw_id = DictUtils.get_path(di, 'gw_id', self.gw_id)
        self.node_type = DictUtils.get_path(di, 'node_type', self.node_type)
        self.name = DictUtils.get_path(di, 'name', self.name)
        self.node_id = DictUtils.get_path(di, 'node_id', self.node_id)
        self.last_updated = DictUtils.get_datetime(di, 'last_updated', datetime.utcnow())
        self.battery.populate_from_dict(DictUtils.get_path(di, 'battery', {}))
        self.record.populate_from_dict(DictUtils.get_path(di, 'record', {}))
        self.upload.populate_from_dict(DictUtils.get_path(di, 'upload', {}))
        self.keep_alive.populate_from_dict(DictUtils.get_path(di, 'keep_alive', {}))
        self.http_counters.populate_from_dict(DictUtils.get_path(di, 'http_counters', {}))
        self.cell_signal.populate_from_dict(DictUtils.get_path(di, 'cell_signal', {}))
        self.system.populate_from_dict(DictUtils.get_path(di, 'system', {}))
        self.last_update = DictUtils.get_datetime(di, 'last_update', datetime(1970, 1, 1))
        return self

    def populate_source_record_data(self, record):
        record_dt = DictUtils.get_datetime(record, 'datetime', None)
        if self.last_update < record_dt or self.name == None:
            self.last_update = record_dt
            self.name = DictUtils.get_path(record, 'data.name', self.name)
            self.mac = DictUtils.get_path(record, 'data.mac', self.mac)
      
        
    def populate_sensor_record_data(self, record):
        record_dt = DictUtils.get_datetime(record, 'datetime', None)
        if record_dt > self.last_update:
            self.last_update = record_dt
            self.name = DictUtils.get_path(record, 'data.name', self.name)
            self.mac = DictUtils.get_path(record, 'data.mac', self.mac)
        upload_dt = DictUtils.get_datetime(record, 'upload_time', None)
       
        self.upload.populate_timer_data(new_time=upload_dt)
        self.record.populate_timer_data(new_time=record_dt, full_record=self)
        self.battery.populate_from_record(new_time=record_dt, level=DictUtils.get_path(record, 'data.Battery Level', None), voltage=None)
      
    def populate_gw_record_data(self, gw_record):
        record_dt = DictUtils.get_datetime(gw_record, 'datetime', None)

        self.gw_id = DictUtils.get_path(gw_record, 'source_id', None)
        device_event = DictUtils.get_path(gw_record, 'data.device_event', {})
        voltage = DictUtils.get_path(device_event, 'data.battery_voltage_V', 0)
        if voltage:
            self.battery.voltage = voltage
     
        cell_signal = DictUtils.get_path(device_event, 'data.cellular', {})
        if cell_signal:
            self.cell_signal.populate_from_record(new_time=record_dt, 
                                                  rsrp=cell_signal.get('RSRP', None), 
                                                  rsrq=cell_signal.get('RSRQ', None), 
                                                  rssi=cell_signal.get('RSSI', None), 
                                                  signal_quality=cell_signal.get('signal_quality', None),

                                                  network_type=cell_signal.get('network_type', None))
            http_counters = DictUtils.get_path(cell_signal, 'http_counters', {})
            if http_counters:
                self.http_counters.populate_from_record(new_time=record_dt, http_info=http_counters)
        reboots = DictUtils.get_path(device_event, 'data.reboots', None)
        last_reset_reason = DictUtils.get_path(device_event, 'data.last_reset_reason', None)
        system_run_time = DictUtils.get_path(device_event, 'data.system_runtime_sec', None)
        self.system.populate_from_record(new_time=record_dt, reboots=reboots, last_reset_reason=last_reset_reason, run_time_sec=system_run_time)
       
        timers = DictUtils.get_path(gw_record, 'data.configuration.configuration.timers', {})
        if timers:
            if timers.get('uploading_timer', 0) != self.upload.timer:
                self.upload.timer = timers.get('uploading_timer', 0)
            if timers.get('keep_alive', 0) != self.keep_alive.timer:
                self.keep_alive.timer = timers.get('keep_alive', 0)
        last_keepalive = DictUtils.get_datetime(gw_record, 'data.last_keep_alive_time', None)
        if last_keepalive:
            self.keep_alive.populate_timer_data(new_time=last_keepalive)

    def populate_web_record_data(self, record):
        record_dt = DictUtils.get_datetime(record, 'datetime', None)
        if record_dt > self.last_update:
            self.last_update = record_dt
            self.name = DictUtils.get_path(record, 'data.name', self.name)
            self.mac = DictUtils.get_path(record, 'data.mac', self.mac)
        
    def populate_raw_record_data(self, record):
        record_dt = DictUtils.get_datetime(record, 'datetime', None)
        if record_dt > self.last_update:
            self.last_update = record_dt
            self.name = DictUtils.get_path(record, 'data.name', self.name)
            self.mac = DictUtils.get_path(record, 'data.mac', self.mac)

    def populate_data(self, source_record=None, sensor_record=None, raw_record=None, gw_record=None, web_record=None):
        if source_record:
            self.populate_source_record_data(source_record)
        if sensor_record:
            self.populate_sensor_record_data(sensor_record)
        if raw_record:
            self.populate_raw_record_data(raw_record)
        if gw_record:
            self.populate_gw_record_data(gw_record)
        if web_record:
            self.populate_web_record_data(web_record)

class AppDynamicNodeBuilder:
    @classmethod
    def get_collection(cls):
        return GrofitCapsuleDynamicNode.get_collection()
    
    @classmethod
    def load_nodes(cls, source_ids=None):
        col = cls.get_collection()
        if not source_ids:
            source_ids = ServerConfig.get_self().get_source_ids()
        return list(col.find({'source_id': {'$in': source_ids}}))
    
    def __init__(self):
        self.nodes = {}
        # self.build_nodes()



    def update_nodes(self, record_type, records):
        records = sorted(records, key=lambda x: x['datetime'], reverse=False)
        
        # last_record = None
        # for rec in records:
        #     if record_type == 'gw':

    def build_nodes(self):
        from miniagro.app_data.app_nodes import AppNodeBuilder

        tm = time.time()
        app_node_records = AppNodeBuilder().build_nodes()
        source_records = SourceRecordUtils().get_last_source_records(index_name='atom_source_info')
        web_records = SourceRecordUtils().get_last_source_records(index_name='atom_source_web')
        raw_records = SourceRecordUtils().get_last_source_records(index_name='atom_source_raw')
        sensor_records = SourceRecordUtils().get_last_source_records(index_name='atom_sensor_raw')
        gw_records = SourceRecordUtils().get_last_gw_records()
        sensor_records = {rec['_id']: rec for rec in sensor_records}
        web_records = {rec['_id']: rec for rec in web_records}
        raw_records = {rec['_id']: rec for rec in raw_records}
        gw_records = {rec['_id']: rec for rec in gw_records}
        source_gw_records = {}
        for app_node in app_node_records.values():
            source_gw_records[app_node.source_id] = gw_records.get(app_node.gw_id, {})
        # pprint.pp(source_gw_records)
        # exit()
        for rec in source_records:
            node = GrofitCapsuleDynamicNode(rec['_id'])
            node.populate_data(source_record=rec, sensor_record=sensor_records.get(rec['_id'], {}), 
                               raw_record=raw_records.get(rec['_id'], {}),
                                 gw_record=source_gw_records.get(rec['_id'], {}),
                                   web_record=web_records.get(rec['_id'], {}))
            self.nodes[node.source_id] = node
        tm = time.time() - tm
        # pprint.pp(node.to_dict())   
        pprint.pp(f'Time to build nodes: {tm}')
        return self.nodes, app_node_records
   
    def save_to_db(self):

        col = GrofitCapsuleDynamicNode.get_collection()
        col.delete_many({})
        DBUtils.update_bulk_records(col, list(self.nodes.values()))
        print(f'Saved {len(self.nodes)} nodes to db')
if __name__ == '__main__':
    builder = AppDynamicNodeBuilder()
    builder.build_nodes()
    builder.save_to_db()
