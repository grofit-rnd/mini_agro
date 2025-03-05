from datetime import datetime, timedelta
import pprint

from pymongo import DESCENDING
from tqdm import tqdm


from miniagro.ext_api.atom_api import AtomApi
from miniagro.config.server_config import ServerConfig
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.downloaders.atom.atom_source_record import AtomSourceRecord
from miniagro.utils.db_utils import DBUtils
from miniagro.utils.grofit_id_utils import IDUtils
from miniagro.utils.param_utils import DictUtils

class AtomRecordDownloader:
    def __init__(self):
        self.sources = {}

        self.atom_api = AtomApi()
        # self.load_from_db()
        # self.sync_utils = AtomSyncUtils()
        self.is_admin = True
        self.source_utils = ServerConfig().load_from_db('self')
        # self.allowed_source_ids = self.source_utils.get_source_ids()
        # self.source_ids = self.allowed_source_ids

    # def load_sources_from_db(self, match=None):
    #     if self.sources:
    #         return self.sources
    #     col = self.get_record_collection()
    #     if not match:
    #         match = {}
    #     match['stream_id'] = self.stream_id
    #     records = list(col.find(match))
    #     rd = {}
    #     for r in records:
    #         pr = AtomSourceRecord().populate_from_dict(r)
    #         rd[pr.source_id] = pr
    #     self.sources = rd
    #     return self.sources

    def get_record_collection(self):
        return DMSDK().info_db.get_collection('provider_source_info')
    
    def get_atom_info(self, source_ids, save=True, atom_ids={}):
        source_ids = self.source_utils.filter_source_ids(source_ids)
        ret = self.atom_api.get_atom_info(source_ids)
        for r in ret:
            source_id = IDUtils.gd_mac_to_source_id(r['mac'])
            atom_id = atom_ids.get(source_id, None)
            rec = AtomRecord(source_id=source_id, 
                                index_name='source_info', 
                                datetime=DictUtils.get_datetime(r, 'last_sample_time_utc', None), 
                                atom_id=atom_id, data=r)
            rec.save_to_db()
        
        return ret
   
    def download_raw_data(self, save=True):
        sync_di_list = []
        page = 1
        while True:
            sl = self.atom_api.get_source_info_from_sync_packet(page=page)
            if not sl:
                break
            sync_di_list.extend(sl)
            page += 1
            if len(sl) < 1000:
                break
        if self.is_admin:
            allowed_source_ids = self.source_utils.get_source_ids()

            n_source_ids = []
            for r in sync_di_list:
                source_id = IDUtils.gd_mac_to_source_id(r['mac'])
                if source_id not in allowed_source_ids:
                    n_source_ids.append(source_id)
            if n_source_ids:
                self.source_utils.add_node(n_source_ids)
        allowed_source_ids = self.source_utils.get_source_ids()
        if save:
            recs = []
            last_recs = []
            for r in tqdm(sync_di_list, desc='saving raw data', total=len(sync_di_list)):
                source_id = IDUtils.gd_mac_to_source_id(r['mac'])
                atom_id = r['_id']
                rec = AtomRecord(source_id=source_id, index_name='source_raw', datetime=DictUtils.get_datetime(r, 'updatedAt', None), atom_id=atom_id, data=r)
                recs.append(rec)
                rec.save_to_db()
            for rec in recs:
                last_recs.append(LastRecord.create_from_config(
                    index_name='source_raw',
                    config=rec.to_dict()))
            print(len(last_recs))
            print(last_recs[10].to_dict())
            LastRecord.save_bulk(index_name='source_raw', records=last_recs)
        return sync_di_list



    def update_atom_data(self):
        macs = [IDUtils.gd_source_id_to_mac(s) for s in self.sources.keys()]
        at_sources = self.atom_api.get_atom_info(macs)
        print('got atom data', len(at_sources))
        new = 0
        updated = 0
        for source in at_sources:
            source_id = IDUtils.gd_mac_to_source_id(source['mac'])
            if source_id in self.sources.keys():
                self.sources[source_id].data['atom'] = source
                updated += 1
            else:
                self.sources[source_id] = AtomSourceRecord(source_id=source_id,
                                                           data={'atom': source})
                new += 1
        return {'count': len(at_sources), 'new': new,  'last_updated': datetime.utcnow()}

    def download_sensor_records(self, source_ids, start, end, use_gw_dt=True, save=True):
        full_recs = {}
        for i in tqdm(range(0, len(source_ids), 100), desc='getting atom info', total=len(source_ids)//100):
            records = self.atom_api.get_atom_readings(source_ids[i:i+100], start, end, use_gw_dt=use_gw_dt)
            if save:
                recs = {}
                allowed_source_ids = self.source_utils.get_source_ids()
                for r in records:
                    source_id = IDUtils.gd_mac_to_source_id(r['mac'])
                    if source_id not in allowed_source_ids:
                        continue
                    upload_time = DictUtils.get_datetime(r, 'gw_read_time_utc', None)
                    sample_time = DictUtils.get_datetime(r, 'sample_time_utc', None)
                    if not sample_time:
                        continue
                    rec = SensorRecord(source_id=source_id, datetime=sample_time, upload_time=upload_time, data=r)
                    if not source_id in recs:
                        recs[source_id] = []
                    recs[source_id].append(rec)
                for source_id, rec_list in recs.items():
                    print(source_id, len(rec_list))
                    print(rec_list[0].to_dict())
                    DBUtils.update_bulk_records(DMSDK().sensor_db.get_source_collection(source_id, 'sensor_raw'), rec_list)
                    LastRecord.create_from_config(index_name='sensor_raw', config=rec_list[-1].to_dict()).save_to_db()
                full_recs.update(recs)
        return full_recs


    def download_recent_sensor_records(self, source_ids, start=None, save=True):
        if start is None:
            start = datetime.utcnow() - timedelta(days=2)
        elif isinstance(start, str):
            start = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        if start < datetime.utcnow() - timedelta(days=2):
            start = datetime.utcnow() - timedelta(days=2)
        end = datetime.utcnow()
        records = self.download_sensor_records(source_ids, start, end, use_gw_dt=True, save=save)
        return records
    
    def get_historical_sensor_records(self, source_ids, start=None, end=None, save=True):
        if start is None:
            start = datetime.utcnow() - timedelta(days=13)
        elif isinstance(start, str):
            start = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        if end is None:
            end = datetime.utcnow()
        if isinstance(end, str):
            end = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
        if end > datetime.utcnow():
            end = datetime.utcnow()
        if start+timedelta(days=14) < end:
            end = start+timedelta(days=14)
        records = self.download_sensor_records(source_ids, start, end, use_gw_dt=False, save=save)
        return records
    # def process_records(self, records, server_time=None):
    #     server_time = server_time or datetime.utcnow()
    #     precords = []
    #     for r in records:
    #         rec = AtomSensRecord.create_from_raw(r, server_time=server_time)
    #         precords.append(rec)
    #     return precords

    def save_last_record(self, records):
        if isinstance(records, dict):
            records = list(records.values())
        if not isinstance(records[0], dict):
            records = [r.to_dict() for r in records]
        for r in records:
            r['_id'] = r['source_id']
        DBUtils.update_bulk_records(
            DMSDK().info_db.get_collection('provider_last_record'),
            records
        )
      
       
    def save_provider_records(self, processed_records):
        if not processed_records:
            return
        if self.supports_sync_bank:
            col = DMSDK().pm_db.get_collection('sync_bank')
            filtered = []
            for r in processed_records:
                if r.time_info.gw_time > datetime.utcnow() - timedelta(days=30):
                    filtered.append(r)

            print(len(filtered))
            DBUtils.update_bulk_records(col, filtered)
        if self.supports_provider_source:
            DMSDK().provider_source_db.save_source_records(processed_records)
        if self.supports_provider_history:
            DMSDK().provider_history_db.save_provider_history_records(processed_records)
        self.save_last_record(processed_records)
      

  
    
    # def download_events(self, source_ids, start, end):
    #     self.atom_api.login()
    #     records = self.atom_api.get_sensor_events_page(macs=source_ids, start=start, end=end)
    #     if not records:
    #         print('no events')
    #         return []
    #     print(records)
    #     return records

    def download_source_web_info(self, atom_id, save=True):
        # if atom_id not in self.source_utils.get_source_ids():
        #     return None
        self.atom_api.login()
        record = self.atom_api.get_source_web_info(atom_id)
        if not record:
            return None
        record= DictUtils.get_value(record, 'data', {})
        
        if save:        
            source_id = IDUtils.gd_mac_to_source_id(record['mac'])
            if source_id not in self.source_utils.get_source_ids():
                return None
            dt = DictUtils.get_datetime(record, 'updatedAt', None)
            rec = AtomRecord(source_id=source_id, index_name='source_web', datetime=dt, atom_id=atom_id, data=record)
            rec.save_to_db()
            LastRecord.create_from_config(index_name='source_web', config=rec.to_dict()).save_to_db()
            # DMSDK().record_db.get_source_collection(source_id, 'source_web').update_one({'_id': rec.get_id()}, {'$set': rec.to_dict()}, upsert=True)
        return record
   
    def download_units(self, save=True):
        units = self.atom_api.get_units()
        if not units:
            return {}
        units = units.get('data', [])
        if self.is_admin:
            n_unit_ids = []
            unit_ids = self.source_utils.get_unit_ids()
            for unit in units:
                unit_id = unit['_id']
                if unit_id not in unit_ids:
                    n_unit_ids.append(unit_id)
            if n_unit_ids:
                self.source_utils.add_unit_ids(n_unit_ids)
        unit_ids = self.source_utils.get_unit_ids()
        if save:
            records = []
            for unit in units:
                unit_id = unit['_id']
                source_id = IDUtils.unit_id_to_source_id(unit_id)
                if unit_id not in unit_ids:
                    continue
                dt = DictUtils.get_datetime(unit, 'updatedAt', None)
                rec = AtomNoTimeRecord(source_id=source_id, index_name='units', atom_id=unit_id, data=unit, datetime=dt)
                records.append(rec)
            DBUtils.update_bulk_records(DMSDK().info_db.get_collection('atom_units'), records)
        return units
    
    def download_business_units(self, save=True):
        business_units = self.atom_api.get_business_units()
        data = list(business_units['data'].values())
        records = []
        if self.is_admin:
            n_bu_ids = []
            bu_ids = self.source_utils.get_bu_ids()
            for bu in data[0]:
                bu_id = bu['_id']
                source_id = IDUtils.bu_id_to_source_id(bu_id)
                if source_id not in bu_ids:
                    n_bu_ids.append(source_id)
            if n_bu_ids:
                self.source_utils.add_bu_ids(n_bu_ids)
        bu_ids = self.source_utils.get_bu_ids()
        for bu in data[0]:
            bu_id = bu['_id']
            if bu_id not in bu_ids:
                continue
            source_id = IDUtils.bu_id_to_source_id(bu_id)
            rec = AtomNoTimeRecord(source_id=source_id, index_name='business_unit', atom_id=bu_id, data=bu, datetime=None)
            records.append(rec)
        DBUtils.update_bulk_records(DMSDK().info_db.get_collection('atom_business_units'), records)
 
    def sync_sources_records(self, sources):
        records = self.download_records(sources, start=datetime.utcnow() - timedelta(days=1), end=datetime.utcnow(), use_gw_dt=True)
        return records

    def download_users(self, save=True):
        users = self.atom_api.get_users()
        pprint.pp(users[0])
        if self.is_admin:
            n_user_ids = []
            user_ids = self.source_utils.get_user_ids()
            for r in users:
                user_id = r['_id']
                source_id = IDUtils.user_id_to_source_id(user_id)
                if source_id not in user_ids:
                    n_user_ids.append(source_id)
            if n_user_ids:
                self.source_utils.add_user_ids(n_user_ids)
        user_ids = self.source_utils.get_user_ids()
        if save:
            records = []
            for user in users:
                user_id = user['_id']
                if user_id not in user_ids:
                    continue
                source_id = IDUtils.user_id_to_source_id(user_id)
                rec = AtomNoTimeRecord(source_id=source_id, index_name='user', atom_id=user_id, data=user, datetime=None)
                records.append(rec)
            DBUtils.update_bulk_records(DMSDK().info_db.get_collection('atom_users'), records)
        return users
    
    def download_gw_data(self, save=True):
        recs = self.atom_api.get_gw_data()
        pprint.pp(recs['data'][0])
        macs = [gw['uniq_id'] for gw in recs.get('data', [])]
        pprint.pp(macs)
        if self.is_admin:
            n_source_ids = []
            gw_ids = self.source_utils.get_gw_ids() 
            for r in macs:
                source_id = IDUtils.gw_uniq_id_to_source_id(r)
                if source_id not in gw_ids:
                    n_source_ids.append(source_id)
            if n_source_ids:
                self.source_utils.add_gw_ids(n_source_ids)
        
        allowed_gw_ids = self.source_utils.get_gw_ids()         # full_data = self.atom_api.get_gw_data_full(macs)
        if save:
            records = []
            for gw in tqdm(recs.get('data', []), desc='saving gw data', total=len(recs.get('data', []))):
                mac = gw['uniq_id']
                source_id = IDUtils.gw_uniq_id_to_source_id(mac)
                if source_id not in allowed_gw_ids:
                    continue
                rec = AtomRecord(source_id=source_id, 
                                 index_name='gw_data', atom_id=gw['_id'], data=gw, 
                                 datetime=DictUtils.get_datetime(gw, 'updatedAt', None))
                full_data = self.atom_api.get_gw_data_full(mac)
                if full_data and full_data.get(mac, None):
                    rec.data.update(full_data[mac])
                else:
                    print('no full data', mac)
                rec.save_to_db()
                
                records.append(rec)
            for rec in records:
                rec._id = rec.source_id 
            DBUtils.update_bulk_records(DMSDK().info_db.get_collection('atom_gw_data'), records)
        return recs
    
    def download_events(self, source_ids=None, start=None, end=None):
        if not source_ids:
            source_ids = self.source_ids
        pprint.pp(source_ids)
        if not start:
            start = datetime.utcnow() - timedelta(days=10)
        if not end:
            end = datetime.utcnow()
        self.atom_api.login()
        records = []
        allowed_source_ids = self.source_utils.get_source_ids()
        for i in tqdm(range(0, len(source_ids), 100), desc='getting atom events', total=len(source_ids)//100):
            recs = self.atom_api.download_events(macs=source_ids[i:i+100], start=start, end=end)
            
            if recs:
                sr = []
                for rec in recs:
                    source_id = IDUtils.gd_mac_to_source_id(rec['mac'])
                    if source_id not in allowed_source_ids:
                        continue
                    dt = DictUtils.get_datetime(rec, 'event_time_utc', None)
                    sr.append(AtomRecord(source_id=IDUtils.gd_mac_to_source_id(rec['mac']), data=rec, index_name='atom_events', datetime=dt))
                DBUtils.update_bulk_records(DMSDK().info_db.get_collection('atom_events'), sr)
            records.extend(sr)
        if not records:
            print('no events')
            return []
        return records
    
class AtomRecord:
    def __init__(self, source_id, index_name, datetime=None, atom_id=None, data=None):
        self._id = None
        self.source_id = source_id
        self.index_name = index_name
        self.datetime = datetime
        self.atom_id = atom_id
        self.data = data
        self.mac = IDUtils.gd_source_id_to_mac(source_id)
    
    def get_id(self):
        if not self.source_id or not self.datetime:
            return None
        return self.source_id + self.datetime.strftime('%Y%m%d%H%M%S')
    
    def to_dict(self):
        if not self._id:
            self._id = self.get_id()

        return {
            '_id': self._id,
            'source_id': self.source_id,
            'index_name': self.index_name,
            'datetime': self.datetime,
            'atom_id': self.atom_id,
            'mac': self.mac,
            'data': self.data,
        }


    def save_to_db(self):
        if not self.datetime or not self.source_id:
            return
        if not self._id:
            self._id = self.get_id()
        DMSDK().record_db.get_source_collection(self.source_id, self.index_name).update_one({'_id': self._id}, {'$set': self.to_dict()}, upsert=True)

class AtomNoTimeRecord(AtomRecord):
    
    def get_id(self):
        return self.source_id
    
    def save_to_db(self):
        if not self.source_id:
            return
        DMSDK().info_db.get_collection(self.index_name).update_one({'_id': self._id}, {'$set': self.to_dict()}, upsert=True)

class SensorRecord:
    def __init__(self, source_id, datetime, upload_time, data):
        self.source_id = source_id
        self.datetime = datetime
        self.upload_time = upload_time
        self.data = data

    def get_id(self):
        return self.source_id +'_'+ self.datetime.strftime('%Y%m%d%H%M%S')
    
    def to_dict(self):
        return {
            '_id': self.get_id(),
            'source_id': self.source_id,
            'datetime': self.datetime,
            'upload_time': self.upload_time,
            'data': self.data
        }
    
    def save_to_db(self):
        if not self.datetime or not self.source_id:
            return
        if not self._id:
            self._id = self.get_id()
        DMSDK().sensor_db.get_source_collection(self.source_id, 'sensor_raw').update_one({'_id': self._id}, {'$set': self.to_dict()}, upsert=True)

class LastRecord:
    @staticmethod
    def create_from_config(index_name, config):
        return LastRecord(source_id=config['source_id'], index_name=index_name, datetime=config['datetime'], atom_id=config['atom_id'], data=config['data'])
    
    @classmethod
    def get_collection(cls, index_name):
        return DMSDK().info_db.get_collection(index_name)
    
    @classmethod
    def save_bulk(cls, index_name, records):
        col = cls.get_collection(index_name)
        old_records = list(col.find({}))
        old_records = {rec['source_id']: rec for rec in old_records}
        new_records = []
        for rec in records:
            if rec['source_id'] in old_records:
                if old_records[rec['source_id']]['datetime'] > rec['datetime']:
                    continue
            else:
                new_records.append(rec) 
        DBUtils.update_bulk_records(col, new_records)

    def __init__(self, source_id, index_name, datetime, atom_id, data):
        self.source_id = source_id
        self.index_name = index_name
        self.datetime = datetime
        self.atom_id = atom_id
        self.data = data

    def to_dict(self):
        return {
            '_id': self.source_id,
            'source_id': self.source_id,
            'datetime': self.datetime,
            'atom_id': self.atom_id,
            'data': self.data
        }
    
    def save_to_db(self, force=False):
        if not self.datetime or not self.source_id:
            return
        col = self.get_collection(self.index_name)
        if force:
            col.update_one({'_id': self.source_id}, {'$set': self.to_dict()}, upsert=True)
        else:
            record = col.find_one({'_id': self.source_id})
            if not record or record['datetime'] < self.datetime :  
                col.update_one({'_id': self.source_id}, {'$set': self.to_dict()}, upsert=True)

class CapsuleDataRecord:
    def __init__(self, source_id=None, name=None, mac=None, atom_id=None, source_type=None, location=None):
        self.source_id = source_id
        self.name = name
        self.mac = mac
        self.atom_id = atom_id
        self.source_type = source_type if source_type else 'capsule'
        self.location = location
        self.datetime = None
        self.times = {
            'source_info': None,
            'sensor_time': None,
            'gw_time': None,
            'raw_time': None,
            'web_time': None,
            'info_time': None,
            'event_time': None,
            'upload_time': None,
            'keep_alive_time': None
        }


    def get_id(self):
        return self.source_id
 
    def to_dict(self):
        return {
            'source_id': self.source_id,
            'name': self.name,
            'mac': self.mac,
            'atom_id': self.atom_id,
            'source_type': self.source_type,
            'location': self.location,
            'last_updated_time': self.last_updated_time,
            'last_sensor_time': self.last_sensor_time,
            'last_gw_time': self.last_gw_time,
            'last_raw_time': self.last_raw_time,
            'last_web_time': self.last_web_time,
            'last_info_time': self.last_info_time,
            'last_event_time': self.last_event_time,
            'last_upload_time': self.last_upload_time
        }

    def save_to_db(self):
        DMSDK().info_db.get_collection('capsule_data').update_one({'_id': self.source_id}, {'$set': self.to_dict()}, upsert=True)

def create_new_server():
    aid = AtomRecordDownloader()
    aid.download_raw_data()


if __name__ == '__main__':
    aid = AtomRecordDownloader()
    aid.download_users()
    exit(1)
    source_id = 'gd_F0_98_83_35_8F_EC'
    aid = AtomRecordDownloader()
    # aid.download_raw_data()
    aid.download_recent_sensor_records(source_ids=['gd_F0_98_83_35_8F_EC'])
    exit(1)
    aid.download_gw_data()
    exit(1)
    users = aid.download_users()
    pprint.pp(users)
    exit(1)
    # units = aid.download_units()
    # business_units = aid.download_business_units()
    # pprint.pp(business_units)
    # exit(1)
    gws = aid.get_gw_sync_packet()
    pprint.pp(gws[0])
    # records = aid.sync_sources_records(gws)
    # pprint.pp(records[0])
    exit(1)
    raw_data = aid.download_raw_data()
    raw_sources = {IDUtils.gd_mac_to_source_id(r['mac']): r for r in raw_data}
    full_info = []
    for i in tqdm(range(0, len(raw_sources), 100), desc='getting atom info', total=len(raw_sources)//100):
        info = aid.get_atom_info(list(raw_sources.keys())[i:i+100])
        full_info.extend(info)
        # break
    full_info = {IDUtils.gd_mac_to_source_id(i['mac']): i for i in full_info}
    full_web = {}
    for i, src_id in tqdm(enumerate(raw_sources.keys()), desc='getting web info', total=len(raw_sources)):
        atom_id = raw_sources[src_id]['_id']
        web = aid.download_source_web_info(atom_id)
        if not web:
            continue
        full_web[src_id] = web
        # if i > 5:
        #     break
    for src_id in tqdm(full_web.keys(), desc='saving records', total=len(full_web)):
        record = AtomSourceRecord(source_id=src_id)
        record.add_web(full_web[src_id])
        record.add_info(full_info[src_id])
        record.add_raw(raw_sources[src_id])
        pprint.pp(record.to_dict())

        record.save_to_db()
    exit(1)
    atom_id = sources[source_id]['_id']
    web = aid.download_source_web_info(atom_id)
    pprint.pp(web)
    record = AtomSourceRecord(source_id=source_id)
    record.add_web(web)
    record.add_info(full_info[source_id])
    record.add_raw(sources[source_id])
    record.save_to_db()
    exit(1)
    raw_data = aid.download_raw_data()
    pprint.pp(len(raw_data))

    exit(1)
    aid.get_atom_info(['gd_F0_98_83_35_8F_EC'])

    aid.download_records(source_ids=['gd_F0_98_83_35_8F_EC'], start=datetime.utcnow() - timedelta(days=1), end=datetime.utcnow(), use_gw_dt=True)
    aid.download_events(source_ids=['gd_F0_98_83_35_8F_EC'], start=datetime.utcnow() - timedelta(days=100), end=datetime.utcnow())
    # DMSDK().info_db.get_collection('provider_last_record').drop()
    # exit(1)
    # db = DMSDK().provider_source_db.db
    # cols = db.list_collection_names()
    # sources = DMSDK().info_db.get_collection('provider_source_info').find({})
    # sources = {s['source_id']: s for s in sources}
    # print(cols)
    # new_recs = []
    # for col in cols:
    #     last_rec = db[col].find_one(sort=[('datetime', DESCENDING)])
    #     if not last_rec:
    #         print('no last record', col)
    #         continue
    #     last_rec_dt = last_rec.get('datetime', None)
    #     source_id = last_rec.get('source_id', None)
    #     if not last_rec_dt or not source_id:
    #         print('no datetime or source_id', col)
    #         continue
    #     print(col, last_rec_dt)
    #     source = sources.get(source_id, None)
    #     if not source:
    #         print('source not found', source_id)
    #         continue
    #     new_rec = {}
    #     new_rec['_id'] = source_id
    #     new_rec['source_id'] = source_id
    #     new_rec['provider_id'] = 'atomation'
    #     new_rec['updated'] = datetime.utcnow()
    #     new_rec['source'] = DictUtils.get_path(source, 'data', {})
    #     new_rec['data'] = last_rec.get('data', {})
    #     new_rec['datetime'] = last_rec_dt
    #     new_rec['atom_id'] = DictUtils.get_path(new_rec['source'], 'raw._id', None)
    #     new_recs.append(new_rec)

    #     #  pprint.pp(new_rec)
    # DMSDK().info_db.get_collection('provider_last_record').insert_many(new_recs)
    #  DBUtils.update_bulk_records(DMSDK().info_db.get_collection('provider_last_record'), new_recs)
         
#     aid.download(full_sync=False)
#     aid.download(full_sync=False)