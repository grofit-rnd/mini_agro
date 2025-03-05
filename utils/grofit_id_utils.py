import hashlib
import random
import time
from datetime import datetime


class IDUtils:
    id_sep = '-'

    @classmethod
    def basic_id_builder(cls, prefix):
        timestamp = str(time.time())
        random_number = str(random.randint(0, 1000000))
        _id = cls.short_hash(f'{timestamp}{random_number}')
        return f'{prefix}{cls.id_sep}{_id}'


    @classmethod
    def short_hash(cls, input_str, length=10):
        hash_object = hashlib.sha256(input_str.encode())
        short_hash = hash_object.hexdigest()[:length]
        return short_hash

    @classmethod

    def obj_to_string(cls, obj):
        if isinstance(obj, list):
            return ','.join(cls.obj_to_string(o) for o in obj)
        elif isinstance(obj, dict):
            return ','.join(f'{k}:{cls.obj_to_string(v)}' for k, v in obj.items())
        else:
            return str(obj)

    @classmethod
    def get_id(cls, prefix, obj):
        hs = cls.short_hash(cls.obj_to_string(obj))
        if prefix:
            return f'{prefix}-{hs}'
        return hs

    @classmethod
    def unit_id_to_source_id(cls, unit_id):
        if unit_id.startswith('un_'):
            return unit_id
        return f'un_{unit_id}'
    
    @classmethod
    def unit_source_id_to_id(cls, source_id):
      
        return source_id.replace(f'un_', '')

    @classmethod
    def bu_id_to_source_id(cls, bu_id):
        if bu_id.startswith('bu_'):
            return bu_id
        return f'bu_{bu_id}'
    
    @classmethod
    def bu_source_id_to_id(cls, source_id):
        return source_id.replace(f'bu_', '')

    @classmethod 
    def event_id_to_source_id(cls, event_id):
        return f'ev_{event_id}'
    
    @classmethod
    def event_source_id_to_id(cls, source_id):
        return source_id.replace(f'ev_', '')
    
    @classmethod
    def user_id_to_source_id(cls, user_id):
        return f'au_{user_id}'
    
    @classmethod
    def user_source_id_to_id(cls, source_id):
        return source_id.replace(f'au_', '')

    @classmethod
    def gd_source_id_to_mac(cls, source_id):
        return source_id.replace(f'gd_', '').replace('_', ':')

    @classmethod
    def gd_mac_to_source_id(cls, mac):
        if mac.startswith('gd_'):
            return mac
        return f'gd_{mac}'.replace(':', '_')
  
    @classmethod
    def gw_uniq_id_to_source_id(cls, uniq_id):
        if uniq_id.startswith('gw_'):
            return uniq_id
        return f'gw_{uniq_id}'.replace(':', '_')
    @classmethod
    def gw_source_id_to_uniq_id(cls, source_id):
        return source_id.replace(f'gw_', '').replace('_', ':')

    @classmethod
    def ims_station_id_to_source_id(cls, station_id, name):
        return f"ims_{station_id}_{name.replace(' ', '_').lower()}"

    @classmethod
    def ims_source_id_to_station_id(cls, source_id):
        return source_id.split('_')[1]

    @classmethod
    def owm_source_id_to_lat_lon(cls, source_id):
        spl = source_id.split('_')
        lat = f'{spl[1]}.{spl[2]}'
        lon = f'{spl[3]}.{spl[4]}'
        # lon = source_id.split('_')[2].replace('_', '.')
        return lat, lon

    @classmethod
    def owm_lat_lon_to_source_id(cls, lat, lon):

        lat_str = f"{lat:.2f}".replace('.', '_')
        lon_str = f"{lon:.2f}".replace('.', '_')

        return f'owm_{lat_str}_{lon_str}'

    @classmethod
    def wai_lat_lon_to_source_id(cls, lat, lon):

        lat_str = f"{lat:.2f}".replace('.', '_')
        lon_str = f"{lon:.2f}".replace('.', '_')

        return f'wai_{lat_str}_{lon_str}'

    @classmethod
    def wai_source_id_to_lat_lon(cls, source_id):
        spl = source_id.split('_')
        lat = f'{spl[1]}.{spl[2]}'
        lon = f'{spl[3]}.{spl[4]}'
        # lon = source_id.split('_')[2].replace('_', '.')
        return lat, lon
    @classmethod
    def _id_builder(cls, vis_parts=None, scramble=None):
        _id = None
        if vis_parts:
            if isinstance(vis_parts, str):
                _id = vis_parts
            if isinstance(vis_parts, list):
                _id = cls.id_sep.join(vis_parts)
        if scramble:
            if isinstance(scramble, list):
                scramble = ''.join(scramble)
            elif not isinstance(scramble, str):
                scramble = str(scramble)
            scramble = IDUtils.short_hash(scramble)
            if _id:
                _id = f'{_id}{cls.id_sep}{scramble}'
            else:
                _id = scramble
        return _id

    @classmethod

    def node_id_builder(cls, prefix):
        _id = IDUtils.short_hash(f'{str(time.time())}')
        return f'{prefix}{cls.id_sep}{_id}'

    @classmethod
    def stream_id_builder(cls, sensor_type, vis_parts=None, scramble=None):
        vis_parts = vis_parts or []
        vis_parts = [sensor_type] + vis_parts
        # print(f'vis_parts: {vis_parts}')
        return cls._id_builder(vis_parts=vis_parts, scramble=scramble)

    @classmethod
    def md_stream_id_builder(cls, md_type, sensor_type, vis_parts=None, scramble=None):
        vis_parts = vis_parts or []
        vis_parts = [md_type, sensor_type] + vis_parts
        # print(f'vis_parts: {vis_parts}')
        return cls._id_builder(vis_parts=vis_parts, scramble=scramble)

    @classmethod
    def server_source_stream_info_id_builder(cls, source_id, vis_parts=None, scramble=None):
        vis_parts = vis_parts or []
        vis_parts = [source_id] + vis_parts
        return cls._id_builder(vis_parts=vis_parts, scramble=scramble)

    @classmethod
    def client_source_stream_info_id_builder(cls, client_id, source_id, vis_parts=None, scramble=None):
        vis_parts = vis_parts or []
        vis_parts = [client_id, source_id] + vis_parts
        return cls._id_builder(vis_parts=vis_parts, scramble=scramble)

    @classmethod
    def get_event_id(cls, stream_id, start):

        return cls._id_builder(vis_parts=[stream_id, str(int(datetime.utcnow().timestamp()))])

    @classmethod
    def source_id_to_provider_stream_id(cls, source_id):
        return cls._id_builder(vis_parts=[source_id, 'pr_records'])
        pass
