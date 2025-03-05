
import pprint
from geopy.geocoders import OpenCage

from miniagro.db.data_manager_sdk import DMSDK
from miniagro.utils.param_utils import DictUtils


class NodeLocation:
    @classmethod 
    def load_or_get_location(cls, lat, lng):
        loc = cls.get_location_by_lat_lng(lat, lng)
        if not loc:
           loc = cls(lat, lng)
           loc.get_location_info()
           loc.save_to_db()
        if isinstance(loc, dict):
            loc = cls(lat, lng, loc)
        return loc
    
    @classmethod
    def get_col(cls):
        return DMSDK().info_db.get_collection('locations')

    @classmethod
    def get_location_by_id(cls, _id):
        return cls.get_col().find_one({'_id': _id})
    
    @classmethod
    def get_location_by_lat_lng(cls, lat, lng):
        _id = f'{lat}_{lng}'
        return cls.get_location_by_id(_id)
    
    def __init__(self, lat=None, lng=None, location=None):
        self.location = {
            'lat': lat,
            'lng': lng,
            'formatted_address': None,
            'city': None,
            'state': None,
            'country': None,
            'city_district': None,
            'county': None,
            'state_district': None,
            'suburb': None,
            'district': None,
            'region': None,
        }
        if location:
            self.location.update(location)

    def save_to_db(self):
        di = self.to_dict()
        di['_id'] = f'{di["lat"]}_{di["lng"]}'
        self.get_col().update_one({'_id': di['_id']}, {'$set': di}, upsert=True)

    def to_dict(self):
        ret = self.location.copy()
        ret['_id'] = f'{self.location["lat"]},{self.location["lng"]}'
        return ret
    
    def to_api_dict(self):
        ret = {}
        DictUtils.set_value(ret, 'lat', self.location['lat'])
        DictUtils.set_value(ret, 'lng', self.location['lng'])
        DictUtils.set_value(ret, 'formatted_address', self.location['formatted_address'])
        DictUtils.set_value(ret, 'city', self.location['city'])
        DictUtils.set_value(ret, 'country', self.location['country'])
        DictUtils.set_value(ret, 'country_code', self.location['country_code'])
        DictUtils.set_value(ret, 'flag', self.location['flag'])
        return ret
    

    def update_from_data_source(self, data_source):
        if data_source:
            if not isinstance(data_source, dict):
                data_source = data_source.to_dict()
            if not 'location' in data_source or not data_source['location']:
                return self
            self.location['lat'] = DictUtils.get_value(data_source, 'location.lat', self.location.get('lat', None))
            self.location['lon'] = DictUtils.get_value(data_source, 'location.lon', self.location.get('lon', None))
            self.location['formatted_address'] = DictUtils.get_value(data_source, 'location.formatted_address', self.location.get('formatted_address', None))
            self.location['city'] = DictUtils.get_value(data_source, 'location.city', self.location.get('city', None))
            self.location['state'] = DictUtils.get_value(data_source, 'location.state', self.location.get('state', None))
            self.location['country'] = DictUtils.get_value(data_source, 'location.country', self.location.get('country', None))
            self.location['city_district'] = DictUtils.get_value(data_source, 'location.city_district', self.location.get('city_district', None))
            self.location['county'] = DictUtils.get_value(data_source, 'location.county', self.location.get('county', None))
            self.location['state_district'] = DictUtils.get_value(data_source, 'location.state_district', self.location.get('state_district', None))
            self.location['suburb'] = DictUtils.get_value(data_source, 'location.suburb', self.location.get('suburb', None))
            self.location['district'] = DictUtils.get_value(data_source, 'location.district', self.location.get('district', None))
            self.location['region'] = DictUtils.get_value(data_source, 'location.region', self.location.get('region', None))
            self.location['polygon'] = DictUtils.get_value(data_source, 'location.polygon', self.location.get('polygon', None))
            self.location['multi_polygon'] = DictUtils.get_value(data_source, 'location.multi_polygon', self.location.get('multi_polygon', None))
            if self.location['country'] == 'Palestinian Territory':
                self.location['formatted_address'] = self.location['formatted_address'].replace('Palestinian Territory', 'Israel')

                self.location['country'] = 'Israel'
                self.location['country_code'] = 'IL'
                self.location['flag'] = '🇮🇱'
        return self

    def get_location_info(self, force=False):
        if not self.location or 'lat' not in self.location or 'lng' not in self.location:
            return False
        if not force and 'formatted_address' in self.location and self.location['formatted_address']:
            return self.location
        # return self.location
        # time.sleep(1)

        lat = self.location['lat']
        lng = self.location['lng']
        if (not lat or not lng) or (lat == 0 and lng == 0) or (lat == 0.0 and lng == 0.0):
            return self.location
        print(f'Getting location info for {lat}, {lng}')
        geolocator = OpenCage('cf91eb9f91334353b52e79927a40afaf')
        location = geolocator.reverse((lat, lng), language='en')
        pprint.pp(location.raw)
        if location:
            address = location.raw['components']
            annotations = location.raw['annotations']
            city = address.get('city', '')
            state = address.get('state', '')
            country = address.get('country', '')

            self.location['formatted_address'] = location.address
            self.location['city'] = city
            self.location['state'] = state
            self.location['country'] = country
            self.location['country_code'] = address.get('country_code', '')
            self.location['city_district'] = address.get('city_district', '')
            self.location['county'] = address.get('county', '')
            self.location['state_district'] = address.get('state_district', '')

            self.location['continent'] = address.get('continent', '')
            self.location['suburb'] = address.get('suburb', '')
            self.location['timezone'] = annotations.get('timezone', '')
            self.location['flag'] = annotations.get('flag', '')

            if self.location['country'] == 'Palestinian Territory':
                self.location['formatted_address'] = self.location['formatted_address'].replace('Palestinian Territory', 'Israel')
                self.location['country'] = 'Israel'
                self.location['country_code'] = 'IL'
                self.location['flag'] = '🇮🇱'

            # self.sun = annotations.get('sun', {})
            # for key in ['rise', 'set']:
            #     for k in ['apparent', 'astronomical', 'civil', 'nautical']:
            #         self.sun[key][k] = datetime.fromtimestamp(self.sun[key][k])
            return self
        else:
            return None
    def update_from_ds_info(self, source):
        if not isinstance(source, dict):
            source = source.to_dict()
        location = DictUtils.get_path(source, 'location', None)
        
        if (location and 'lat' in location and 'lon' in location and location['lat'] != 0 and location['lon'] != 0 and
                location['lat'] != 0.0 and location['lon'] != 0.0):
            if not self.location:
                self.location = location
            elif 'lat' in location and 'lon' in location:
                if 'lat' not in self.location or 'lon' not in self.location or location['lat'] != self.location['lat'] or location['lon'] != self.location['lon']:
                    self.location['lat'] = location['lat']
                    self.location['lon'] = location['lon']
                    # print(f'Location mismatch:  {self.location} {location}')
                # self.location['lat'] = location['lat']
                # self.location['lon'] = location
            self.get_location_info(force=False)

        return self

    def populate_from_dict(self, di):

        self.location = DictUtils.get_value(di, 'location', self.location)
        return self


if __name__ == '__main__':
    location = {'lng': 34.9999542,
                'lat': 32.3802566}
    loc = NodeLocation(location['lat'], location['lng'])
    dloc = NodeLocation.get_col().find_one({'_id': f'{location["lat"]},{location["lng"]}'})
    pprint.pp(dloc)
    if not dloc:
        loc.get_location_info()
        pprint.pp(loc.location)
        loc.save_to_db()
    else:
       pprint.pp(dloc)
