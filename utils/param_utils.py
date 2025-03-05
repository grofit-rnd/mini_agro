import math
import pprint
import re
import sys
from datetime import datetime, time, date

import numpy as np
from pytz import UTC
import xml.etree.ElementTree as ET


def set_if_not_null(di, name, value, replace=True):
    """
    Sets the value of a key in a dictionary to a given value if the value is not None.
    """
    if value is None or name is None:
        return
    if name in di and not replace:
        return
    di[name] = value


def set_if_value_exists(di, name, value):
    """
    Sets the value of a key in a dictionary to a given value if the value is not None or an empty string.
    """
    if not value or name is None:
        return
    di[name] = value


def set_if_date_exists(di, name, dt):
    """
    Sets the value of a key in a dictionary to a given datetime object if the object is not None and is after January 2, 2000.
    """
    if dt is None or name is None or datetime.timestamp(dt) < datetime.timestamp(datetime(2000, 1, 2)):
        return
    di[name] = dt.replace(microsecond=0)


def set_dict_as_list(di, name, value):
    """
    Sets the value of a key in a dictionary to a given value if the value is not None or an empty string.
    """
    if not value or name is None:
        return
    di[name] = list(value.values())


def get_or_default_dict(di, name, default):
    """
    Gets the value of a key in a dictionary, or returns a default value if the key does not exist or the dictionary is None.
    """
    if di is None or name is None:
        return default

    if name in di:
        if di[name] == 'None' or di[name] == 'nan':
            return default
        if isinstance(di[name], float) and math.isnan(di[name]):
            return default
        return di[name]
    return default


def get_or_default_map_from_list(di, name, key_field, default):
    """
    Gets the value of a key in a dictionary, or returns a default value if the key does not exist or the dictionary is None.
    """
    if di is None or name is None:
        return default

    if name in di:
        return {item[key_field]: item for item in di[name] if item[key_field] is not None}

    return default


def get_or_default_df(df, col, l, default):
    """
    Gets the value of a cell in a Pandas DataFrame at a given row and column, or returns a default value if the cell does not exist.
    """
    try:
        return df[col][l]
    except:
        return default


def get_location_or_default(di, name='location', lat='lat', lng='lng', default=None):
    """
    Gets the latitude and longitude from a dictionary as a list, or returns a default value if the dictionary is None or does not contain the relevant keys.
    """
    _loc = get_or_default_dict(di, name, None)
    if _loc is None:
        return default
    _lat = get_or_default_dict(_loc, lat, None)
    _lng = get_or_default_dict(_loc, lng, None)
    if _lat is not None and _lng is not None:
        return [_lat, _lng]
    return default


option_dt_parse_strings = [
"%Y-%m-%d",
"%Y-%m-%dT%H:%M",
"%Y-%m-%d %H:%M:%S",
"%Y-%m-%dT%H:%M:%S%z",
"%Y-%m-%d %H:%M:%S.%f",
"%Y-%m-%dT%H:%M:%S.%fZ",
'%a, %d %b %Y %H:%M:%S %Z']

def get_or_default_datetime(di, name, default=None, parse_string="%Y-%m-%dT%H:%M:%S.%fZ"):
    ret = _get_or_default_datetime(di, name, default=default, parse_string=parse_string)
    if ret is None:
        return default
    ret = ret.replace(tzinfo=None)
    return ret

def _get_or_default_datetime(di, name, default=None, parse_string="%Y-%m-%dT%H:%M:%S.%fZ"):
    """
    Gets the value of a key in a dictionary as a datetime object, or returns a default value if the key does not exist or the value cannot be parsed as a datetime object.
    """
    val = get_or_default_dict(di, name, None)
    return parse_datetime(val, default=default, parse_string=parse_string)


def parse_datetime(val, default=None, parse_string="%Y-%m-%dT%H:%M:%S.%fZ"):

    if val is None or val == 'None':
        return default
    if isinstance(val, datetime):
        val.replace(tzinfo=UTC)
        return val
    if isinstance(val, int):
        return datetime.utcfromtimestamp(val)
    try:
        dt = datetime.strptime(val, parse_string)
        dt.replace(tzinfo=UTC)

        return dt
    except:
        pass

    try:
        if val.endswith('Z'):
            dt = datetime.fromisoformat(val[:-1])
            dt.replace(tzinfo=UTC)

            return dt
    except:
        pass
    try:
        dt = datetime.strptime(val, "%Y-%m-%d")

        dt.replace(tzinfo=UTC)
        return dt
    except:
        pass

    for parse_string in option_dt_parse_strings:
        try:
            dt = datetime.strptime(val, parse_string)
            dt.replace(tzinfo=UTC)
            return dt
        except:
            pass
    try:    
        dt = datetime.strptime(val, "%Y-%m-%dT%H:%M")
        dt.replace(tzinfo=UTC)
        return dt
    except:
        pass
    try:
            dt = datetime.strptime(val, "%Y-%m-%dT%H:%M")
            dt.replace(tzinfo=UTC)
            return dt
    except:
            try:
                dt = datetime.strptime(val, '%Y-%m-%d %H:%M:%S%z')
                dt.replace(tzinfo=UTC)
                return dt
            except:
                try:
                    dt = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
                    dt.replace(tzinfo=UTC)
                    return dt
                except:
                    try:
                        dt = datetime.strptime(val, "%Y-%m-%dT%H:%M:%S%z")
                        dt.replace(tzinfo=UTC)

                        return dt
                    except:
                        try:
                            dt = datetime.strptime(
                                val, "%Y-%m-%d %H:%M:%S.%f")
                            dt.replace(tzinfo=UTC)
                            return dt
                        except:
                            try:
                                dt = datetime.strptime(
                                    val, '%Y-%m-%dT%H:%M:%S')
                                dt.replace(tzinfo=UTC)
                                return dt
                            except:
                                print('did not catch', val)
                                try:
                                    dt = datetime.strptime(
                                        val, '%a, %d %b %Y %H:%M:%S %Z')
                                    dt.replace(tzinfo=UTC)
                                    return dt
                                except:
                                    print('cant parse datetime', val)
                                    return default


def set_datetime_to_dict(di, name, dt, parse_string="%Y-%m-%dT%H:%M:%S.%fZ"):
    """
    Sets the value of a key in a dictionary to a string representation of a datetime object.
    """
    if dt is None:
        return
    di[name] = dt.strftime(parse_string)


def correct_encoding(di):
    """
    Recursively iterates through the keys and values of a dictionary and attempts to decode any bytes objects as UTF-8.
    """
    if isinstance(di, dict):
        return {correct_encoding(k): correct_encoding(v) for k, v in di.items()}
    elif isinstance(di, bytes):
        return di.decode('utf-8')
    return di


def convert_to_numpy_if_possible(lst):
    """
    Converts a list to a NumPy array if all of the elements in the list are of the same type.
    """
    if lst is None or len(lst) == 0:
        return lst
    if all(isinstance(i, type(lst[0])) for i in lst):
        return np.array(lst)
    return lst


# this function does not work on inner lists
def get_path_value(di, sp, sep='-', _print=False):
    if _print:
        print(sp, di)

    if isinstance(sp, str):
        return get_path_value(di, sp.split(sep), sep=sep)
    if not sp:
        return None
    elif sp[0] not in di:
        return None

    if len(sp) == 1:
        return get_or_default_dict(di, sp[0], None)
    return get_path_value(di[sp[0]], sp[1:], sep=sep)

# def get_value_from_path(self, f):
#     sp = f.split('-')
#         print(sp)
#         di = None
#         if sp[0] == 'meta':
#             di = self.meta
#         elif sp[0] == 'profile':
#             di = self.profile
#         elif sp[0] == 'fields':
#             di = self.fields
#         elif sp[0] == 'info':
#             di = self.info
#         elif sp[0] == 'location':
#             return self.location
#         if not di:
#             return None
#         return self._get_inner_field(di, sp[1:])


def flatten_dict(d, parent_key='', sep='-', allow_list=True):
    """
           Flatten a nested dictionary into a single level dictionary, using the specified separator to join the keys.
           """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        # print(new_key)

        if isinstance(v, dict):
            if not v:
                items.append((new_key, v))
            else:
                items.extend(flatten_dict(v, new_key, sep=sep,
                         allow_list=allow_list).items())
        elif isinstance(v, list):
            if not allow_list:
                items.append([])
            li = []
            for list_item in v:
                if isinstance(list_item, dict):
                    li.append(flatten_dict(list_item, '',
                              sep=sep, allow_list=allow_list))
                else:
                    li.append(list_item)
            items.append((new_key, li))
        else:
            items.append((new_key, v))

    return dict(items)


def get_or_default_time(di, name, default, parse_string):
    value = di.get(name, default)

    # If value is None or it's already a time object, return it directly
    if value is None or isinstance(value, time):
        return value

    # If value is an int, interpret it as minutes since midnight
    if isinstance(value, int):
        hours = value // 60
        minutes = value % 60
        return time(hours, minutes)

    # If value is a datetime, extract the time
    if isinstance(value, datetime):
        return value.time()

    # If value is a string, parse it
    if isinstance(value, str):
        if parse_string is None:
           parse_string = '%H:%M'
        return datetime.strptime(value, parse_string).time()

    # If none of the above, return the default value
    return default


class DictUtils:
    @classmethod
    def get_value(cls, di, name, default=None):
        if not di or not name:
            return default
        if '.' in name:
            p = cls.get_path(di, name, default=default)
        else:
            p = di.get(name, default)
        if p == 'None' or p == 'nan' or p == 'NaN':
            return default
        if isinstance(p, float) and math.isnan(p):
            return default
        return p

    @classmethod
    def get_datetime(cls, di, name, default=None, parse_string="%Y-%m-%dT%H:%M:%S.%fZ"):
        if not di or not name:
            return default
        if '.' in name:
            p = cls.get_path(di, name, default=default)
            if not p:
                return default
            return parse_datetime(p, default=default, parse_string=parse_string)

        # print(di[name])
        return get_or_default_datetime(di, name=name, default=default, parse_string=parse_string)

    @classmethod
    def get_time(cls, di, name, default=None, parse_string="%H:%M"):
        if not di or not name:
            return default
        # print(di[name])

        return get_or_default_time(di, name=name, default=default, parse_string=parse_string)

    @classmethod
    def set_value(cls, di, name, value, replace=True, allow_none=False, allow_empty=False):
        if  value == 'NaN' or value == 'nan' or (isinstance(value, float) and math.isnan(value)):
            return
        if di is None or not name:
            return
        if value is None and not allow_none:
            return
        if not value and not allow_empty and \
                (isinstance(value, str) or isinstance(value, list) or isinstance(value, dict)):
            return
        # if isinstance(value, str) and (value == 'None' or value =='nan'):
        #     return
        di[name] = value

    @classmethod
    def set_time(cls, di, name, value, replace=True, allow_none=False):
        if di is None or not name:
            return
        if value is None and not allow_none:
            return
        if isinstance(value, time):
            tm = value.strftime("%H:%M")
        else:
            tm = value
        di[name] = tm


    @classmethod
    def set_datetime(cls, di, name, dt, parse_string="%Y-%m-%dT%H:%M:%S.%fZ", replace=True, allow_none=False, as_string=False):

        if di is None or not name:
            return
        if not replace and name in di:
            return
        if dt is None:
            if allow_none:
                di[name] = None
            return
        if not isinstance(dt, datetime) and isinstance(dt, date):
            dt = datetime.combine(dt, time())
        if as_string:
            di[name] = dt.strftime(parse_string)
        else:
            di[name] = dt

    @classmethod
    def get_path(cls, di, path, default=None):
        if not path or not di:
            return default
        sl = path.split('.')
        for s in sl:
            if not di or s not in di:
                return default
            di = di[s]
        return di

    @classmethod
    def set_path(cls, di, path, value):
        if not path or di is None:
            return
        path_list = path.split('.')
        result = di
        current = result
        for key in path_list[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        current[path_list[-1]] = value
        return result

    @classmethod
    def replace_with_override(cls, di, rep, override, path=None):

        out = {}
        for p in override:
            v = DictUtils.get_path(di, p)
            if v:
                DictUtils.set_path(out, p, v)
        for key, value in rep.items():
            _path = f'{path}.{key}' if path else key
            if _path in override:
                continue

            if isinstance(value, dict):
                out[key] = cls.replace_with_override(di.get(key, {}), value, override, path=_path)
            else:
                out[key] = value
        return out

    @classmethod
    def convert_keys_to_snake_case(cls, input_dict):
        def to_snake_case(name):
            s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
            return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

        output_dict = {}
        for key, value in input_dict.items():
            key = to_snake_case(key)
            if type(value) is dict:
                output_dict[key] = DictUtils.convert_keys_to_snake_case(value)
            if isinstance(value, list):
                output_dict[key] = []
                for v in value:
                    if isinstance(v, list):
                        # we don't allow double list [[],[]]
                        v = {'list': v}

                    if isinstance(v, dict):
                        output_dict[key].append(DictUtils.convert_keys_to_snake_case(v))
                    else:
                        output_dict[key].append(v)


            else:
                output_dict[key] = value
        return output_dict

    @classmethod
    def to_camel_case(cls, snake_obj):
        if isinstance(snake_obj, list):
            return [cls.to_camel_case(x) if isinstance(x, dict) else x for x in snake_obj]
        if not isinstance(snake_obj, dict):
            return snake_obj

        def camel_case_key(snake_str):
            if snake_str == '_id':
                return 'id'
            parts = snake_str.split('_')
            return parts[0] + ''.join(word.capitalize() for word in parts[1:])

        return {camel_case_key(k): cls.to_camel_case(v) for k, v in snake_obj.items()}

    @classmethod
    def xml_to_json(cls, xml_path):
        def infer_data_type(value):
            try:
                return int(value)
            except ValueError:
                pass
            try:
                return float(value)
            except ValueError:
                pass
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
            return value

        def parse_element(element):
            parsed = {}
            for child in element:
                child_data = parse_element(child)
                if child.tag not in parsed:
                    parsed[child.tag] = child_data
                else:
                    if not isinstance(parsed[child.tag], list):
                        parsed[child.tag] = [parsed[child.tag]]
                    parsed[child.tag].append(child_data)
            parsed.update(element.attrib)
            if element.text and element.text.strip():
                return infer_data_type(element.text.strip())
            return parsed

        try:
            if isinstance(xml_path, str):
                tree = ET.parse(xml_path)
                root = tree.getroot()
            else:
                root = xml_path
            json_data = parse_element(root)
            return json_data
        except Exception as e:
            print(f"Error: {e}")
            return None

    @classmethod
    def flatten_dict(cls, record, sep='.', allow_list=True):
        if isinstance(record, dict):
            return flatten_dict(record, sep=sep, allow_list=allow_list)
        elif isinstance(record, list):
            return [cls.flatten_dict(item) for item in record]
        return record

    @classmethod
    def extract_keys(cls, d):
        keys = []
        for key, value in d.items():
            keys.append(key)
            if isinstance(value, dict):
                keys.extend(cls.extract_keys(value))
        return keys

    @classmethod
    def check_for_numpy_int64(cls, obj):
        if isinstance(obj, list):
            for item in obj:
                cls.check_for_numpy_int64(item)
        elif isinstance(obj, dict):
            for key, value in obj.items():
                # print(key)
                cls.check_for_numpy_int64(value)
        elif isinstance(obj, np.int64):
            print(f'Found numpy.int64: {obj}')
            exit(1)

    @classmethod
    def fix_nan_to_none(cls, di):
        if isinstance(di, dict):
            return {k: cls.fix_nan_to_none(v) for k, v in di.items()}
        elif isinstance(di, list):
            return [cls.fix_nan_to_none(v) for v in di]
        elif isinstance(di, float) and math.isnan(di):
            return None
        return di

    @classmethod
    def deep_getsizeof(cls, di,  ids):
        """ Recursively finds size of objects in bytes """
        d = cls.deep_getsizeof
        if id(di) in ids:
            return 0

        r = sys.getsizeof(di)
        ids.add(id(di))

        if isinstance(di, str) or isinstance(di, bytes):
            return r

        if isinstance(di, dict):
            return r + sum(d(k, ids) + d(v, ids) for k, v in di.items())

        if isinstance(di, (list, tuple, set, frozenset)):
            return r + sum(d(x, ids) for x in di)

        return r

    @classmethod
    def remove_empty_values(cls, di):
        if isinstance(di, dict):
            return {k: cls.remove_empty_values(v) for k, v in di.items() if v}
        elif isinstance(di, list):
            return [cls.remove_empty_values(v) for v in di if v]
        return di
