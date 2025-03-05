import json
import pprint
import time
from datetime import datetime, timedelta
import requests
from tqdm import tqdm


from miniagro.db.data_manager_sdk import DMSDK
from miniagro.utils.grofit_id_utils import IDUtils
from miniagro.utils.param_utils import DictUtils


atom_url = "https://atapi.atomation.net/api/v1/s2s/v1_0/auth/login"
atom_headers = {'app_version': '1.8.5', 'access_type': '5'}
atom_data = {"email": "amizorach@gmail.com", "password": "10265ben"}


class TooManyReadingsException(Exception):
    pass


class AtomApi:
    login_bearer = None
    bearer_expires = None
    refresh_token = None

    def __init__(self):
        self.max_retry = 3
        self.attempt = 0

    def login(self, attempt=0, force=False):
        """
        Logs in to the Atom API and retrieves a bearer token.
        """
        if not AtomApi.login_bearer:
            di = DMSDK().admin_db.get_collection('atom_token').find_one({'_id': 'atom_token'})
            if di:
                AtomApi.login_bearer = di['token']
                AtomApi.bearer_expires = DictUtils.get_datetime(di, 'expires', None)
                AtomApi.refresh_token = di['refresh_token']
        if AtomApi.login_bearer is not None and AtomApi.bearer_expires > datetime.utcnow():
            return "Bearer {}".format(AtomApi.login_bearer)

        response = requests.post(atom_url, headers=atom_headers, data=atom_data)
        if response.status_code == 200:
            login_response = response.json()
            AtomApi.login_bearer = login_response['data']['token']
            AtomApi.bearer_expires = datetime.utcnow() + timedelta(days=1)
            AtomApi.refresh_token = DictUtils.get_value(login_response['data'], 'refresh_token', None)
            DMSDK().admin_db.get_collection('atom_token').update_one(
                {'_id': 'atom_token'},
                {'$set': {'token': AtomApi.login_bearer,
                          'expires': AtomApi.bearer_expires,
                          'refresh_token': AtomApi.refresh_token}},
                upsert=True
            )
            print("login success ", AtomApi.login_bearer)
            return "Bearer {}".format(AtomApi.login_bearer)
        else:
            print("retry {}".format(response.status_code))
            if self.attempt < self.max_retry:
                time.sleep(2)
                return self.login(attempt=attempt + 1)
            return None

    def get_readings_page(self, current_page, macs, start, end=None, use_gw_dt=False, page_size=1000):
        """
        Retrieves a page of sensor readings from the Atom API.
        """
        bearer = self.login()
        if bearer is None:
            return None
        if not end:
            end = datetime.utcnow()
        if isinstance(start, datetime):
            start = start.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(end, datetime):
            end = end.strftime('%Y-%m-%d %H:%M:%S')
        jso = {
            "filters": {
                "start_date": start,
                "end_date": end,
                "mac": macs,
                "createdAt": use_gw_dt
            },
            "limit": {
                "page": current_page,
                "page_size": page_size
            }
        }
      
        response = requests.post("https://atapi.atomation.net/api/v1/s2s/v1_0/sensors_readings",
                                 headers={'Authorization': bearer},
                                 json=jso)
        return response

    def get_atom_info(self, macs):
        """
        Retrieves device configuration information from the Atom API.
        """
        macs = [IDUtils.gd_source_id_to_mac(m) for m in macs]
        bearer = self.login()
        if bearer is None:
            print('no bearer')
            return None

        jso = {
            "filters": {
                "mac": macs
            }
        }
        response = requests.post("https://atapi.atomation.net/api/v1/s2s/v1_0/device_config",
                                 headers={'Authorization': bearer},
                                 json=jso)

        
        if response.status_code == 200:
            rj = response.json()
            if 'data' in rj:
                data = rj['data']
                return data
        return {}
    
    def get_atom_readings(self, macs, start, end, use_gw_dt=False):
        if not macs:
            return []
        macs = [IDUtils.gd_source_id_to_mac(s) for s in macs]

        self.login()
        records = []
        current_page = 1

        while True:
            print(f'Getting page {current_page}')
            response = self.get_readings_page(current_page, macs, start, end, use_gw_dt=use_gw_dt)
            if response.status_code == 200:
                data = response.json()['data']
                # print(data)
                if not data['readings_data']:
                    break
                recs = data['readings_data']
                records.extend(recs)
                total_count = data['totalCount']
                # print(f'Got {len(records)} records {total_count}')

                # processed_records.extend(self.process_records(recs, upload_time))
                current_page += 1
                if len(recs) < 1000:
                    break
            elif response.status_code == 429:
                print('Too many requests')
                time.sleep(60)
            else:
                print(response.status_code)
                break

        # print(f'Got {len(records)} records')
        return records
    
    def get_sensor_events_page(self, macs, start, end=datetime.utcnow(), current_page=1):
        """
        Retrieves a page of sensor events from the Atom API.
        """
        bearer = self.login()
        if bearer is None:
            return None
        macs = [IDUtils.gd_source_id_to_mac(m) for m in macs]
        url = "https://atapi.atomation.net/api/v1/s2s/v1_0/sensors_events"

        payload = json.dumps({
            "filters": {
                "start_date": start.strftime('%Y-%m-%d %H:%M:%S'),
                "end_date": end.strftime('%Y-%m-%d %H:%M:%S'),
                "mac": macs,
                "createdAt": True
            },
            "limit": {
                "page": current_page,
                "page_size": 1000
            }
        })
        headers = {
            'Authorization': bearer,
            'Content-Type': 'application/json',
            'Cookie': 'connect.sid=s%3APuk5-8TPrMMDaUjhvkt7DoDaIpwqj4eR.kCK6ttaDu%2Blu5qfEiLG3YYZhCLRPVt2jhgc8vHdOn%2Fk'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        return response.json()
    
    def download_events(self, macs, start, end):
        records = []
        for i in tqdm(range(0, len(macs), 100), desc='getting atom events', total=len(macs)//100):
            current_page = 1
            while True:
                
                recs = self.get_sensor_events_page(macs=macs[i:i+100], start=start, end=end, current_page=current_page)
                dat = recs.get('data', [])
                page_count = dat.get('pageCount', 0)
                total_count = dat.get('totalCount', 0)
                print(f'page {current_page} {page_count} {total_count}')
                records.extend(DictUtils.get_path(dat, 'events', []))
              
                if page_count <= current_page:
                    break
                current_page += 1  
        return records
    def download_sync_packets(self, base_url, last_record=None, current_page=1, page_size=1000):
        """
        Downloads synchronization packets from the Atom API.
        """
        page = current_page
        bearer = self.login()
        if bearer is None:
            return None
        headers = {
            'Authorization': f'{bearer}'
        }
        packets = []
        if not last_record:
            url = base_url + f'?page={page}&page_size={page_size}'
            response = requests.request("GET", url, headers=headers, data={})
            res_data = response.json()
            if not res_data or 'data' not in res_data:
               
                return packets
            data = res_data['data']

            if 'data' not in data:
              
                return packets
            packets.extend(data['data'])
            return packets
        while True:
            url = base_url + f'?page={page}&page_size={page_size}'
            response = requests.request("GET", url, headers=headers, data={})
            res_data = response.json()
            if not res_data or 'data' not in res_data:
               
                return packets
            data = res_data['data']

            if 'data' not in data:
               
                return packets
            packets.extend(data['data'])
            if len(data['data']) < page_size:
                return packets
            pkt = data['data']
            packets.extend(pkt)
            if last_record and last_record > DictUtils.get_datetime(pkt[-1], 'gw_read_time_utc'):
                return packets
            page += 1
            if page - current_page > 10:
                return packets

    def get_event_sync_packets(self, page=1, page_size=1000, last_record=None):
        """
        Retrieves event synchronization packets.
        """
        url = f"https://atapi.atomation.net/api/v1/report/events-data"
        return self.download_sync_packets(url, current_page=page, page_size=page_size, last_record=last_record)

    def get_readings_sync_packets(self, page=1, page_size=1000, last_record=None):
        """
        Retrieves sensor readings synchronization packets.
        """
        url = f"https://atapi.atomation.net/api/v1/report/sensors-data"
        return self.download_sync_packets(url, current_page=page, page_size=page_size, last_record=last_record)

    def get_source_info_from_sync_packet(self, page=1, page_size=1000):
        """
        Retrieves source information from synchronization packets.
        """
        bearer = self.login()
        if bearer is None:
            return None
        headers = {
            'Authorization': f'{bearer}'
        }
        url = f"https://atapi.atomation.net/api/v1/device/{page}/{page_size}"
        payload = {}
        headers.update(atom_headers)
        response = requests.request("GET", url, headers=headers, data=payload)
        data = response.json()
        if not data or 'data' not in data:
            return {}
        data = data['data']['data']
        return data

    @staticmethod
    def get_sensor_type_and_class(sensor_key):
        """
        Maps sensor keys to their respective types and classes.
        """
        sk = sensor_key.lower().replace(' ', '_')
        if sk == 'temperature':
            return 'temperature', 'temperature'
        if sk == 'humidity':
            return 'humidity', 'humidity'
        if sk == 'radiation':
            return 'radiation', 'radiation'
        if sk == 'tension_t':
            return 'tension_top', 'tension'
        if sk in ['tension_d', 'tension_b']:
            return 'tension_deep', 'tension'
        if sk == 'ec' or sk == 'ec_rk520_02':
            return 'ec', 'ec'
        if sk == 'soil_temperature' or sk == 'soil_temp_rk520_02':
            return 'soil_temperature', 'temperature'
        if sk == 'soil_humidity' or sk == 'soil_humid_rk520_02':
            return 'soil_humidity', 'soil_humidity'
        if sk == 'vibration_raw_data':
            return 'vibration', 'vibration'
        if sk == 'battery_level':
            return 'battery_level', 'battery_level'
        if sk == 'vibration_sd':
            return 'vibration', 'vibration'
        if sk == 'flow':
            return 'flow', 'flow'
        if sk == 'soil_moisture' or sk == 'soil_moisture_rk520_02':
            return 'soil_moisture', 'soil_moisture'
        if sk == 'watermark_top':
            return 'watermark_top', 'watermark'
        if sk == 'watermark_deep':
            return 'watermark_deep', 'watermark'
        if sk == 'gw_read_time_utc':
            return 'upload_time', 'upload_time'
        if sk == 'vpd':
            return 'vpd', 'vpd'
        if sk == 'dew_point':
            return 'dew_point', 'dew_point'
        if sk == 'high_g':
            return 'unknown', 'unknown'
        return None, None

    def get_gw_sync_packet(self, page=1, page_size=1000, last_record=None):
        """
        Retrieves gateway synchronization packets.
        """
        url = f"https://atapi.atomation.net/api/v1/gateway/1/50"
        return self.download_sync_packets(url, current_page=page, page_size=page_size, last_record=last_record)


    def get_gw_data(self):
        """
        Retrieves gateway data for specified MAC addresses.
        """
        bearer = self.login()
        if bearer is None:
            return None
        headers = {
            'Authorization': f'{bearer}'
        }
        # for mac in macs:
        url = f"https://atapi.atomation.net/api/v1/gateway/"
        response = requests.request("GET", url, headers=headers, data={})
        if response.status_code != 200:
            return None
        try:
            res_data = response.json()
        except:
            return None
        if 'data' not in res_data:
            return None
        res_data = res_data['data']

        return res_data
    
    def get_gw_data_full(self, macs):
        """
        Retrieves gateway data for specified MAC addresses.
        """
        bearer = self.login()
        if bearer is None:
            return None
        headers = {
            'Authorization': f'{bearer}'
        }
        if isinstance(macs, str):
            macs = [macs]
        res_data = {}
        for mac in tqdm(macs, desc='getting gw data', total=len(macs)):
            url = f"https://atapi.atomation.net/api/v1/gateway/{mac}"
            response = requests.request("GET", url, headers=headers, data={})
            if response.status_code != 200:
                continue
            try:
                js = response.json()
            except:
                continue

            dat = js.get('data', {})
            if not dat:
                continue
            res_data[mac] = dat[0]
            # pprint.pp(res_data)

        return res_data
    
    def get_source_web_info(self, atom_id):
        """
        Retrieves web information for a specific source.
        """
        bearer = self.login()
        if bearer is None:
            return None
        headers = {
            'Authorization': f'{bearer}',
        }
        headers.update(atom_headers)
        url = f"https://atapi.atomation.net/api/v1/device/web/{atom_id}"
        response = requests.request("GET", url, headers=headers, data={})
        if response.status_code != 200:
            return None
        try:
            ret = response.json()
        except:
            return None
        if 'data' not in ret:
            return None
        res_data = ret['data']
        return res_data

    def update_source_info(self, source_id, source_atom_id, info):
        """
        Updates the source information in the Atom API.
        """
        bearer = self.login()
        if bearer is None:
            return None

        jso = {
            "_id": source_atom_id,
        }
        jso.update(info)
        response = requests.post("https://atapi.atomation.net/api/v1/s2s/v1_0/set_device_config",
                                 headers={'Authorization': bearer},
                                 json=jso)
        
        res_data = response.json()
        return res_data

    def get_source_edit_info(self, source_id):
        """
        Retrieves editable information for a specific source.
        """
        bearer = self.login()
        if bearer is None:
            return None
        macs = [IDUtils.gd_source_id_to_mac(source_id)]

        headers = {
            'Authorization': f'{bearer}'
        }
        jso = {
            "filters": {
                "mac": macs,
            },
        }
        response = requests.post("https://atapi.atomation.net/api/v1/s2s/v1_0/device_config",
                                 headers={'Authorization': bearer},
                                 json=jso)
        res_data = response.json()
        return res_data

    def update_unit_id(self, atom_source_id, atom_unit_id):
        """
        Updates the unit ID for a specific source in the Atom API.
        """
        bearer = self.login()
        if bearer is None:
            return None
        headers = {
            'Authorization': f'{bearer}'
        }
        headers.update(atom_headers)
        jso = {
            "unit": [
                atom_unit_id
            ],
            "devices": [
                {
                    "unit_id": [
                        atom_unit_id
                    ],
                    "_id": atom_source_id,
                    "contacts_to_remove": []
                }
            ]
        }
        response = requests.put("https://atapi.atomation.net/api/v1/device/modify",
                                headers=headers,
                                json=jso)
        res_data = response.json()
        return res_data

    def create_unit(self, unit_info):
        """
        Creates a new unit in the Atom API.
        """
        bearer = self.login()
        if bearer is None:
            return None
        headers = {
            'Authorization': f'{bearer}'
        }
        headers.update(atom_headers)
        response = requests.post("https://atapi.atomation.net/api/v1/unit/save",
                                 headers=headers,
                                 json=unit_info)
        res_data = response.json()
        return res_data

    def get_units(self):
        """
        Retrieves all units from the Atom API.
        """
        bearer = self.login()
        if bearer is None:
            return None
        headers = {
            'Authorization': f'{bearer}'
        }
        url = f"https://atapi.atomation.net/api/v1/unit/"
        response = requests.request("GET", url, headers=headers, data={})
        if response.status_code != 200:
            return None
        try:
            res_data = response.json()
        except:
            return None
        if 'data' not in res_data:
            return None
        res_data = res_data['data']
        return res_data

    def get_business_units(self):
        """
        Retrieves all business units from the Atom API.
        """
        bearer = self.login()
        if bearer is None:
            return None
        # macs = ['C9:EC:AE:40:D9:7E']#, 'E4:AB:3E:74:7F:1E']
        fn = 'business_unit_data.json'
        self.login()
        headers = {
            'Authorization': f'{bearer}'
        }
        url = f"https://atapi.atomation.net/api/v1/business-unit/1/500"    
        response = requests.request("GET", url, headers=headers, data={})
        if response.status_code != 200:
            return None
        try:
            res_data = response.json()
        except:
            return None
        if 'data' not in res_data:
            return None
        res_data = res_data['data']
        if 'data' in res_data:
            res_data = res_data['data']
        return res_data
     
    def get_users(self):
        bearer = self.login()
        if bearer is None:
            return None
        headers = {
            'Authorization': f'{bearer}'
        }
        url = f"https://atapi.atomation.net/api/v1/user/"
        response = requests.request("GET", url, headers=headers, data={})
        if response.status_code != 200:
            return None
        try:
            res_data = response.json()
        except:
            return None
       
        
        res_data = DictUtils.get_path(res_data, 'msg.data', None)
        return res_data
       
    
if __name__ == '__main__':
    a = AtomApi()
    original_info = {'location': {'lng': 34.5448456, 'lat': 31.452753},
                     'name': 'תמוז 11.2', "unit_id": "6772ccef22f0a30022a15d30",}
    testing = '6772ccef22f0a30022a15d30'
    testing2 = '6772ce1622f0a30022a16114'
    create_unit = {
        "unit": {
            "name": "testing 3",
            "type": 8,
            "state": 3,
            "location": {
                "lat": 0,
                "lng": 0
            },
            "gateway_id": None,
            "business_unit_id": "5ecb834d48d1b588feec92a9",
            "contacts": []
        }
    }
    a.get_units()