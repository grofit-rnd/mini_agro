from datetime import datetime
import pprint
import time

from tqdm import tqdm
from miniagro.app_data.node_location import NodeLocation
from miniagro.config.server_config import ServerConfig
from miniagro.data_utils.sensor_utils import SensorUtils
from miniagro.data_utils.source_record_utils import SourceRecordUtils
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.downloaders.atom.atom_gateway_downloader import AtomGatewayDownloader
from miniagro.downloaders.atom.atom_source_raw_downloader import AtomSourceWebDownloader
from miniagro.utils.db_utils import DBUtils
from miniagro.utils.grofit_id_utils import IDUtils
from miniagro.utils.param_utils import DictUtils
#   {
#             "_id": "gd_FF_65_8B_F6_B2_E8",
#             "atom_ids": {
#                 "business_unit_id": "608a9b3b2c59a8006ee1e29a",
#                 "capsule_id": "64e4bb78f692900022e3fa89",
#                 "client_id": "5ecb825548d1b588feec8eeb",
#                 "gw_id": "65097b06856428002217122a",
#                 "unit_id": "66d6e7887f0a3b003c0e0439"
#             },
#             "data": {
#                 "config_create_time": "2024-11-06T21:09:50.158Z",
#                 "connection_type": "Bluetooth",
#                 "device_name": "IT 22SIW 15 cm",
#                 "device_type_name": "AGRI3",
#                 "fw_version": {
#                     "mcu": "2.9.00+001"
#                 },
#                 "gw_connection_type": "EGPRS",
#                 "gw_id": "gw_E1_8D_DE_B5_D2_8D",
#                 "gw_name": "IT GW 44",
#                 "gw_timers": {
#                     "collection_data": 1800,
#                     "keep_alive": 43200,
#                     "uploading_timer": 3600
#                 },
#                 "gw_type": "Gateway",
#                 "mac": "FF:65:8B:F6:B2:E8",
#                 "projectName": "Spring 2024",
#                 "sensors": [
#                     {
#                         "battery_level": true
#                     },
#                     {
#                         "soil_humidity": true
#                     },
#                     {
#                         "soil_temperature": true
#                     },
#                     {
#                         "temperature": true
#                     },
#                     {
#                         "humidity": true
#                     },
#                     {
#                         "radiation": true
#                     },
#                     {
#                         "ec": true
#                     }
#                 ],
#                 "timers": {
#                     "after_trigger_freq": 600,
#                     "after_trigger_number": 0,
#                     "event_timer": 600,
#                     "sampling_timer": 600
#                 }
#             },
#             "groups": {
#                 "business_unit_name": "syngenta spain",
#                 "unit_name": "24WNTI2HTT22SIW "
#             },
#             "hash": "nd-278cf9284d",
#             "last_updated": "2025-02-25 02:51:54.648511",
#             "location": {
#                 "_id": "37.0020065,14.3524131",
#                 "city": "",
#                 "city_district": "",
#                 "continent": "Europe",
#                 "country": "Italy",
#                 "country_code": "it",
#                 "county": "Ragusa",
#                 "district": null,
#                 "flag": "\ud83c\uddee\ud83c\uddf9",
#                 "formatted_address": "unnamed road, 97011 Acate RG, Italy",
#                 "lat": 37.0020065,
#                 "lng": 14.3524131,
#                 "region": null,
#                 "state": "Sicily",
#                 "state_district": "",
#                 "suburb": "",
#                 "timezone": {
#                     "name": "Europe/Rome",
#                     "now_in_dst": 0,
#                     "offset_sec": 3600,
#                     "offset_string": "+0100",
#                     "short_name": "CET"
#                 }
#             },
#             "name": "IT 22SIW 15 cm",
#             "node_id": "gd_FF_65_8B_F6_B2_E8",
#             "node_type": "grofit_capsule",
#             "source_id": "gd_FF_65_8B_F6_B2_E8",
#             "status": {
#                 "active_since": null,
#                 "disabled_at": "2025-01-13 10:21:21",
#                 "disabled_reason": "No data in the last 30 days",
#                 "state": "disabled"
#             }
#         },

#  {
#             "_id": "gw_F3_E7_92_BC_62_06",
#             "atom_ids": {
#                 "business_unit_id": "608a9b3b2c59a8006ee1e29a",
#                 "client_id": "5ecb825548d1b588feec8eeb",
#                 "gw_configuration_id": "64f72da728a4000010c7aa0a",
#                 "gw_id": "64f72da728a4000010c7aa0c"
#             },
#             "data": {
#                 "cellular": {
#                     "APN": null,
#                     "IMEI": "350457796970364",
#                     "IMSI": {
#                         "MCC": 214,
#                         "MNC": 3,
#                         "MSIN": 2597327247
#                     },
#                     "access_tech": "E_UTRAN(LTE)",
#                     "carrier": null,
#                     "cell_id": "00A7DD16",
#                     "network_type": "LTE"
#                 },
#                 "firmware_update_required": false,
#                 "gw_connection_type": "LTE",
#                 "gw_model_and_version": {
#                     "central_mcu": {
#                         "fw_ver": "2.9.00+001",
#                         "model": "nRF52840"
#                     },
#                     "modem": {
#                         "app_ver": "0.2.02+003",
#                         "fw_ver": "nrf9160_1.3.0",
#                         "model": "nRF9160-SICA"
#                     }
#                 },
#                 "gw_timers": {
#                     "collection_data": 1800,
#                     "keep_alive": 43200,
#                     "uploading_timer": 3600
#                 },
#                 "gw_type": "sa",
#                 "gw_version": "2.9.00+001",
#                 "mode": 1
#             },
#             "devices": [
#                 {
#                     "atom_id": "64f72da728a4000010c7aa01",
#                     "source_id": "gd_F3_E7_92_BC_62_06"
#                 }
#             ],
#             "groups": {
#                 "business_unit_name": "syngenta spain"
#             },
#             "gw_id": "gw_F3_E7_92_BC_62_06",
#             "hash": "nd-01cab1c2f9",
#             "last_updated": "2025-02-25 23:40:58.737950",
#             "location": {},
#             "name": "TA122 agri3sa prof",
#             "node_id": "gw_F3_E7_92_BC_62_06",
#             "node_type": "grofit_gateway",
#             "status": {
#                 "active_since": "2025-02-25 23:40:58.737926",
#                 "disabled_at": null,
#                 "disabled_reason": null,
#                 "state": "active"
#             }
#         },
class GrofitCapsuleNode:
    @classmethod
    def get_collection(cls):
        return DMSDK().info_db.get_collection('grofit_capsule_node')
    
    def __init__(self, source_id):
        self.source_id = source_id
        self.node_type = 'grofit_capsule'
        self.name = None
        self.node_id = source_id
        self.gw_id = None
        # self.status = {
        #     'state': 'active',
        #     'active_since': datetime.utcnow(),
        #     'disabled_at': None,
        #     'disabled_reason': None
        # }
        self.mac = None
        self.last_updated = datetime.utcnow()
        self.location = {}
        self.data = {}
        self.atom_ids = {}
        self.hash = None
        self.groups = {} 
        self.gw_info = {}
        self.timers = {}
        self.connection ={}
        self.sensors = []
        self.search_index = ''

    def populate_data(self, source_record=None, web_record=None, raw_record=None, gw_record=None):
        if not web_record:
                AtomSourceWebDownloader().download_data(source_ids=[self.source_id])
        self.data = {}
        self.last_updated = source_record.get('last_updated', None)
        self.location = source_record.get('location', None)
        self.name = source_record.get('name', None)
        self.atom_ids['business_unit_id'] = DictUtils.get_path(web_record, 'data.business_unit_id')
        self.atom_ids['unit_id'] = DictUtils.get_path(web_record, 'data.unit_id')
        self.atom_ids['capsule_id'] = DictUtils.get_path(web_record, 'atom_id')
        self.atom_ids['gw_id'] = DictUtils.get_path(web_record, 'data.gw_id')
        self.atom_ids['client_id'] = DictUtils.get_path(web_record, 'data.client_id')
        self.mac = DictUtils.get_path(source_record, 'data.mac')
        sensor_data = DictUtils.get_value(source_record, 'data.sensors', [])
        if sensor_data:
            for sens in sensor_data:
                # if not DictUtils.get_value(sens, 'active', False):
                    #     continue
                sens_name = DictUtils.get_path(sens, 'sensor_name', None)
                if not sens_name:
                    pprint.pp(f'Sensor {sens} no sensor_name')
                sens_name = SensorUtils.atom_sensor_to_sensor_dict.get(sens_name, None)
                if not sens_name:
                    # print(f'Sensor {sens} not found')
                    continue
                if 'et' in sens_name:
                    continue
                # sens_type, sens_class = AtomApi.get_sensor_type_and_class(sens_name)
                if sens.get('active', False):
                    self.sensors.append(sens_name)
        if 'humidity' in self.sensors and 'temperature' in self.sensors:
            self.sensors.append('vpd')
            self.sensors.append('dew_point')
        self.groups['business_unit_name'] = DictUtils.get_path(raw_record, 'data.business_unit_name')
        self.groups['unit_name'] = DictUtils.get_path(web_record, 'data.unit_name')
        gateways = DictUtils.get_path(web_record, 'data.gateways', [])
        self.connection['connection_type'] = DictUtils.get_path(raw_record, 'data.connection_type')
        self.data['device_type_name'] = DictUtils.get_path(source_record, 'data.device_type_name')
        self.data['device_type_id'] = DictUtils.get_path(source_record, 'data.device_type')
        self.data['fw_version'] = DictUtils.get_path(raw_record, 'data.fw_version')
        self.timers = DictUtils.get_path(source_record, 'data.timers')

        if gateways:
            gw = gateways[-1]
            # pprint.pp(gw)
            gw_source_id = IDUtils.gw_uniq_id_to_source_id(gw['uniq_id'])
            self.atom_ids['gw_id'] = gw['_id']
            self.atom_ids['client_id'] = gw['client_id']
            self.gw_info['gw_id'] = gw_source_id
            self.gw_info['gw_model_and_version'] = gw.get('gw_model_and_version', {})
            self.gw_info['gw_name'] = gw['name']
            self.gw_id = gw_source_id

            if gw_source_id.replace('gw_', 'gd_') == self.source_id:
                self.gw_info['gw_type'] = 'Stand Alone'
            else:
                self.gw_info['gw_type'] = 'Gateway'
            gw_connection_type = DictUtils.get_path(gw, 'last_cell_signal.network_type', '')
            self.connection['gw_connection_type'] = gw_connection_type
          
            # if gw_source_id not in gw_record:
            #     pprint.pp(f'GW {gw_source_id} not found')
            #     pprint.pp(self.to_dict())
            #     gwl = AtomGatewayDownloader()
            #     gwm = gwl.download_data(source_ids=[self.atom_ids['gw_id']])
            #     pprint.pp(gwm)
            #     gwm = gwm[0].get(gw_source_id, None)
            #     if gwm:
            #         gw_record[gw_source_id] = gwm

            if gw_source_id in gw_record:
                gw_record = gw_record[gw_source_id]
                # pprint.pp(gw_record[gw_source_id])
                gw_timers = DictUtils.get_path(gw_record, 'data.configuration.configuration.timers', {})

                if gw_timers:
                    self.timers.update(gw_timers)
                cell = DictUtils.get_path(gw_record, 'data.device_event.data.cellular', None)
                if cell:
                    self.gw_info['cellular'] = {}
                    self.gw_info['cellular']['APN'] = cell.get('APN', None)
                    self.gw_info['cellular']['IMEI'] = cell.get('IMEI', None)
                    self.gw_info['cellular']['IMSI'] = cell.get('IMSI', None)
                    self.gw_info['cellular']['access_tech'] = cell.get('access_tech', None)
                    self.gw_info['cellular']['carrier'] = cell.get('carrier', None)
                    self.gw_info['cellular']['cell_id'] = cell.get('cell_id', None)
                    self.gw_info['cellular']['network_type'] = cell.get('network_type', None)
                    # pprint.pp(f'GW {gw_source_id} timers')

                # else:
                #     pprint.pp(f'GW {gw_source_id} no timers')
                 
                        # pprint.pp(gw['device_event'])
        else:
            # pprint.pp(f'GW {self.source_id} not found')
            # pprint.pp(self.to_dict())
            self.connection['connection_type'] = 'Bluetooth'
        loc = DictUtils.get_path(source_record, 'data.location', {})
        if not loc or not DictUtils.get_path(loc, 'lat', None) or not DictUtils.get_path(loc, 'lng', None):
            loc = DictUtils.get_path(raw_record, 'location', {})
        if loc and DictUtils.get_path(loc, 'lat', None) and DictUtils.get_path(loc, 'lng', None):
            self.location = NodeLocation.load_or_get_location(loc['lat'], loc['lng'])
            if self.location and isinstance(self.location, NodeLocation):
                self.location = self.location.to_api_dict()

        self.create_search_index()

        # if self.source_id == 'gd_FF_65_8B_F6_B2_E8':
        #     print('--------------------------------')
        #     print(self.source_id)
        #     print('source_record')
        #     pprint.pp(source_record)
        #     print('web_record')
        #     pprint.pp(web_record)
        #     if not web_record:
        #         AtomSourceWebDownloader().download_data(source_ids=[self.source_id])

        #     print('raw_record')
        #     pprint.pp(raw_record)
        #     print('self.to_dict()')
        #     pprint.pp(self.to_dict())
        #     print('--------------------------------')
        #     # exit(1)
    def create_search_index(self):
        paths = [
            'source_id',
            'data.device_type_name',
            'groups.business_unit_name',
            'groups.unit_name',
            'connection.connection_type',
            'connection.gw_connection_type',
            'name',
            'mac',
            'location.formatted_address',
            'location.city',
            'location.country',
            'gw_id',
            'gw_info.gw_type',
           
        ]
        di = self.to_dict()
        for path in paths:
            value = DictUtils.get_path(di, path, None)
            if value:
                self.search_index += f' {value}'
        self.search_index = self.search_index.strip()
        # pprint.pp(self.search_index)

        # if self.location:
    def to_dict(self):
        return {
            '_id': self.source_id,
            'gw_id': self.gw_id,
            'source_id': self.source_id,
            'mac': self.mac,
            'node_type': self.node_type,
            'name': self.name,
            'node_id': self.node_id,
            'last_updated': self.last_updated,
            'location': self.location,
            'atom_ids': self.atom_ids,
            'data': self.data,
            'groups': self.groups,
            'gw_info': self.gw_info,
            'connection': self.connection,
            'timers': self.timers,
            'sensors': self.sensors,
            'location': self.location,
            'search_index': self.search_index,
            'gw_id': self.gw_id
        }

    def to_mini_api_dict(self):
        ret = {}
        DictUtils.set_value(ret, 'source_id', self.source_id)
        DictUtils.set_value(ret, 'name', self.name)
        DictUtils.set_value(ret, 'groups', self.groups)
        DictUtils.set_value(ret, 'search_index', self.search_index)
        return ret
    
class AppNodeBuilder:
    @classmethod
    def get_collection(cls):
        return GrofitCapsuleNode.get_collection()

    
    @classmethod
    def load_nodes(cls, source_ids=None):
        col = cls.get_collection()
        if not source_ids:
            source_ids = ServerConfig.get_self().get_source_ids()
        return list(col.find({'source_id': {'$in': source_ids}}))
    
    def __init__(self):
        # self.server_config = ServerConfig.get_server_config()
        self.allowed_nodes = {}
        self.nodes = {}


    def build_nodes(self, source_ids=None, as_dict=False):
        if not source_ids:
            source_ids = ServerConfig.get_self().get_source_ids()
        tm = time.time()
        source_records = SourceRecordUtils().get_last_source_records(index_name='atom_source_info', source_ids=source_ids)
        web_records = SourceRecordUtils().get_last_source_records(index_name='atom_source_web', source_ids=source_ids)
        raw_records = SourceRecordUtils().get_last_source_records(index_name='atom_source_raw', source_ids=source_ids)
        
        gw_records = SourceRecordUtils().get_last_gw_records()
        web_records = {rec['_id']: rec for rec in web_records}
        raw_records = {rec['_id']: rec for rec in raw_records}
        gw_records = {rec['_id']: rec for rec in gw_records}
        # for rec in tqdm(source_records, desc='Building nodes', total=len(source_records)):
        for rec in source_records:
            node = GrofitCapsuleNode(rec['_id'])
            node.populate_data(source_record=rec, web_record=web_records.get(rec['_id'], {}), raw_record=raw_records.get(rec['_id'], {}), gw_record=gw_records)
            if as_dict:
                self.nodes[node.source_id] = node.to_dict()
            else:
                self.nodes[node.source_id] = node
        tm = time.time() - tm
        # pprint.pp(node.to_dict())   
        print(len(self.nodes))
        pprint.pp(f'Time to build nodes: {tm}')
        return self.nodes

    def save_to_db(self):
        DBUtils.update_bulk_records(self.get_collection(), list(self.nodes.values()))
        print(f'Saved {len(self.nodes)} nodes to db')

    def to_dict(self):
        return {
        }
    
    def to_api_dict(self, flat=True):
        vals = self.nodes.values()
        nds = []
        for val in vals:
            if isinstance(val, GrofitCapsuleNode):
                val = val.to_dict()
            val = DictUtils.to_camel_case(val)
            if flat:
                val = DictUtils.flatten_dict(val)
            
            nds.append(val)
        return nds

    def to_mini_api_dict(self, flat=True):
        vals = self.nodes.values()
        nds = []
        for val in vals:
            if isinstance(val, GrofitCapsuleNode):
                val = val.to_mini_api_dict()
            val = DictUtils.to_camel_case(val)
            if flat:
                val = DictUtils.flatten_dict(val)
            nds.append(val)
        return nds
if __name__ == '__main__':
    builder = AppNodeBuilder()
    builder.build_nodes()
    builder.save_to_db()
    # pprint.pp(builder.nodes['gd_FF_65_8B_F6_B2_E8'].to_dict())
    # pprint.pp(builder.nodes['gd_D3_C0_2A_65_CE_E8'].to_dict())