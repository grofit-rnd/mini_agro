from datetime import datetime, timedelta
from flask import json, jsonify
from flask_cors import cross_origin
from flask_jwt_extended import get_jwt_identity, jwt_required
from flask_restx import Namespace, Resource, reqparse

from miniagro.config.user_config import UserConfig
from miniagro.data_utils.sensor_utils import SensorUtils
from miniagro.data_utils.source_record_utils import SourceRecordUtils
from miniagro.utils.api_utils import APIUtils
from miniagro.utils.param_utils import DictUtils


ns_sensor = Namespace('sensor', description='Sensor related operations', decorators=[cross_origin()])


ns_sensor_parser = reqparse.RequestParser()
ns_sensor_parser.add_argument(
    'source_id', help='source id', type=str, required=True)

# index_name, start_date, end_date, limit=10000, full=False, last=False

ns_sensor_parser.add_argument(
    'start_date', help='start date', type=str, required=False, default=None)
ns_sensor_parser.add_argument(
    'end_date', help='end date', type=str, required=False, default=None)
ns_sensor_parser.add_argument(
    'limit', help='limit', type=int, required=False, default=10000)
ns_sensor_parser.add_argument(
    'skip', help='skip', type=int, required=False, default=0)
ns_sensor_parser.add_argument(
    'agg', help='agg', type=str, required=False, default='1h')
ns_sensor_parser.add_argument(
    'agg_fn', help='agg_fn', type=str, required=False, default='mean')
ns_sensor_parser.add_argument(
    'diff', help='diff', type=bool, required=False, default=False)
ns_sensor_parser.add_argument(
    'sec_diff', help='sec_diff', type=bool, required=False, default=False)

multi_source_parser = reqparse.RequestParser()
multi_source_parser.add_argument(
    'source_ids', help='source ids', type=str, action='split', required=False, default=None)

def parse_data_access_args(args):
    print(f'get_jwt_identity() = {get_jwt_identity()}')
    args['source_id'] = args.get('source_id', None)
    if not APIUtils.is_id_allowed(get_jwt_identity(), args['source_id']):
        return None
    args['start _date'] = DictUtils.get_datetime(args, 'start_date', datetime.utcnow()-timedelta(days=10))
    args['end_date'] = DictUtils.get_value(args, 'end_date', datetime.utcnow())
    args['limit'] = DictUtils.get_value(args, 'limit', 10000)
    args['skip'] = DictUtils.get_value(args, 'skip', 0)
    args['full'] = DictUtils.get_value(args, 'full', False)
    args['options'] = { 
        'agg': DictUtils.get_value(args, 'agg', '1h'),
        'agg_fn': DictUtils.get_value(args, 'agg_fn', 'mean'),
        'diff': DictUtils.get_value(args, 'diff', False),
        'sec_diff': DictUtils.get_value(args, 'sec_diff', False)
    }
    return args

# def parse_multi_source_args(args):
#     args['source_ids'] = APIUtils.filter_user_source_ids(get_jwt_identity(), args.get('source_ids', None))
#     if args['source_ids'] is None:
#         return None
#     return args

def get_sensor_records(data_parser, app=False):
    args = parse_data_access_args(data_parser.parse_args())
    if args is None:
        return jsonify({'error': 'No client found'}), 404
    su = SensorUtils(args['source_id'], None)
    options = args['options']
    # recs = su.get_advanced_sensor_records(start=args['start_date'], end=args['end_date'], limit=args['limit'], options=options, as_df=False)
    if app:
        recs = su.get_app_records(start=args['start_date'], end=args['end_date'], limit=args['limit'], options=options)
    else:
        recs = su.get_advanced_sensor_records(start=args['start_date'], end=args['end_date'], limit=args['limit'], options=options, as_df=False)
    print(recs)
    js = json.dumps(recs, default=str)
    return jsonify(json.loads(js)), 200

def get_raw_data(data_parser):
    args = parse_data_access_args(data_parser.parse_args())
    su = SensorUtils(args['source_id'], None)
    recs = su.get_raw_data(args['start_date'], args['end_date'], limit=args['limit'], as_df=False)
    js = json.dumps(recs, default=str)
    return jsonify(json.loads(js)), 200

def get_track_records(data_parser, app=False):
    args = parse_data_access_args(data_parser.parse_args())
    su = SensorUtils(args['source_id'], None)
    if app:
        recs = su.get_app_track_records(args['start_date'], args['end_date'], limit=args['limit'])
    else:
        recs = su.get_track_records(args['start_date'], args['end_date'], limit=args['limit'])
    js = json.dumps(recs, default=str)
    return jsonify(json.loads(js)), 200

def get_multi_source_records(index_name, source_ids=None):
    # args = parse_multi_source_args(data_parser.parse_args())
    # if args is None:
    #     return jsonify({'error': 'No client found'}), 404
   
    source_record_utils = SourceRecordUtils()
    ret = source_record_utils.get_last_source_records(source_ids=source_ids, index_name=index_name)
    js = json.dumps(ret, default=str)
    return jsonify(json.loads(js)), 200

@ns_sensor.route('/raw_data')
@ns_sensor.expect(ns_sensor_parser)
class SourceInfo(Resource):
    @ns_sensor.doc('raw_data')
    @cross_origin()
    @jwt_required()
    def get(self):
        return get_raw_data(ns_sensor_parser)

@ns_sensor.route('/sensor_data')
@ns_sensor.expect(ns_sensor_parser)
class SourceInfo(Resource):
    @ns_sensor.doc('sensor_data')
    @cross_origin()
    @jwt_required()
    def get(self):
        return get_sensor_records(ns_sensor_parser)
    

@ns_sensor.route('/sensor_app_data')
@ns_sensor.expect(ns_sensor_parser)
class SourceAppData(Resource):
    @ns_sensor.doc('sensor_app_data')
    @cross_origin()
    @jwt_required()
    def get(self):
        return get_sensor_records(ns_sensor_parser, app=True)
    
@ns_sensor.route('/sensor_app_track')
@ns_sensor.expect(ns_sensor_parser)
class SourceAppTrack(Resource):
    @ns_sensor.doc('sensor_app_track')
    @cross_origin()
    @jwt_required()
    def get(self):
        return get_track_records(ns_sensor_parser, app=True)