from datetime import datetime, timedelta
from flask import json, jsonify
from flask_cors import cross_origin
from flask_jwt_extended import get_jwt_identity, jwt_required
from flask_restx import Namespace, Resource, reqparse

from miniagro.config.user_config import UserConfig
from miniagro.data_utils.source_record_utils import SourceRecordUtils
from miniagro.utils.api_utils import APIUtils
from miniagro.utils.param_utils import DictUtils


ns_data_access = Namespace('data_access', description='Data access related operations', decorators=[cross_origin()])


data_access_parser = reqparse.RequestParser()
# data_access_parser.add_argument('source_id', required=True)
data_access_parser.add_argument(
    'source_id', help='source id', type=str, required=True)

# index_name, start_date, end_date, limit=10000, full=False, last=False

data_access_parser.add_argument(
    'start_date', help='start date', type=str, required=False, default=None)
data_access_parser.add_argument(
    'end_date', help='end date', type=str, required=False, default=None)
data_access_parser.add_argument(
    'limit', help='limit', type=int, required=False, default=10000)
data_access_parser.add_argument(
    'skip', help='skip', type=int, required=False, default=0)
data_access_parser.add_argument(
    'full', help='full', type=bool, required=False, default=False)


multi_source_parser = reqparse.RequestParser()
multi_source_parser.add_argument(
    'source_ids', help='source ids', type=str, action='split', required=False, default=None)

def parse_data_access_args(args):
    print(f'get_jwt_identity() = {get_jwt_identity()}')
    args['source_id'] = args.get('source_id', None)
    if not APIUtils.is_id_allowed(get_jwt_identity(), args['source_id']):
        return None
    args['start_date'] = DictUtils.get_datetime(args, 'start_date', datetime.utcnow()-timedelta(days=10))
    args['end_date'] = DictUtils.get_value(args, 'end_date', datetime.utcnow())
    args['limit'] = DictUtils.get_value(args, 'limit', 10000)
    args['skip'] = DictUtils.get_value(args, 'skip', 0)
    args['full'] = DictUtils.get_value(args, 'full', False)
    return args

# def parse_multi_source_args(args):
#     args['source_ids'] = APIUtils.filter_user_source_ids(get_jwt_identity(), args.get('source_ids', None))
#     if args['source_ids'] is None:
#         return None
#     return args

def get_source_records(data_parser, index_name):
    args = parse_data_access_args(data_parser.parse_args())
    if args is None:
        return jsonify({'error': 'No client found'}), 404
    source_record_utils = SourceRecordUtils()
    ret = source_record_utils.get_source_records(
                                                    source_id=args['source_id'],
                                                index_name=index_name,
                                                  start_date=args['start_date'],
                                                  end_date=args['end_date'],
                                                  limit=args['limit'],
                                                  skip=args['skip'],
                                                  full=args['full'])
    js = json.dumps(ret, default=str)
    return jsonify(json.loads(js)), 200

def get_multi_source_records(index_name, source_ids=None):
    # args = parse_multi_source_args(data_parser.parse_args())
    # if args is None:
    #     return jsonify({'error': 'No client found'}), 404
   
    source_record_utils = SourceRecordUtils()
    ret = source_record_utils.get_last_source_records(source_ids=source_ids, index_name=index_name)
    js = json.dumps(ret, default=str)
    return jsonify(json.loads(js)), 200

@ns_data_access.route('/source_info')
@ns_data_access.expect(data_access_parser)
class SourceInfo(Resource):
    @ns_data_access.doc('source_info')
    @cross_origin()
    @jwt_required()
    def get(self):
        return get_source_records(data_access_parser, 'atom_source_info')

@ns_data_access.route('/source_web')
@ns_data_access.expect(data_access_parser)
class SourceWeb(Resource):
    @ns_data_access.doc('source_web')
    @cross_origin()
    @jwt_required()
    def get(self):
        return get_source_records(data_access_parser, 'atom_source_web')

@ns_data_access.route('/source_raw')
@ns_data_access.expect(data_access_parser)
class SourceRaw(Resource):
    @ns_data_access.doc('source_raw')
    @cross_origin()
    @jwt_required()
    def get(self):
        return get_source_records(data_access_parser, 'atom_source_raw')

@ns_data_access.route('/source_info_current')
class SourceInfoCurrent(Resource):
    @ns_data_access.doc('source_info_current')
    @cross_origin()
    @jwt_required()
    def get(self):
        source_ids = APIUtils.filter_user_source_ids(get_jwt_identity(), None)
        if source_ids is None:
            return jsonify({'error': 'No client found'}), 404
        return get_multi_source_records('atom_source_info', source_ids)

@ns_data_access.route('/source_web_current')
class SourceWebCurrent(Resource):
    @ns_data_access.doc('source_web_current')
    @cross_origin()
    @jwt_required()
    def get(self):
        source_ids = APIUtils.filter_user_source_ids(get_jwt_identity(), None)
        if source_ids is None:
            return jsonify({'error': 'No client found'}), 404
        return get_multi_source_records('atom_source_web', source_ids)
    
@ns_data_access.route('/source_raw_current')
class SourceRawCurrent(Resource):
    @ns_data_access.doc('source_raw_current')
    @cross_origin()
    @jwt_required()
    def get(self):
        source_ids = APIUtils.filter_user_source_ids(get_jwt_identity(), None)
        if source_ids is None:
            return jsonify({'error': 'No client found'}), 404
        return get_multi_source_records('atom_source_raw', source_ids)


@ns_data_access.route('/business_unit_info')
class BusinessUnitInfo(Resource):
    @ns_data_access.doc('business_unit_info')
    @cross_origin()
    @jwt_required()
    def get(self):
        user = UserConfig.get_user_or_none(get_jwt_identity())
        if user is None:
            return jsonify({'error': 'No client found'}), 404
        source_ids = user.get_bu_ids()
        if source_ids is None:
            return jsonify({'error': 'No client found'}), 404
        return get_multi_source_records('atom_business_units', source_ids)

@ns_data_access.route('/atom_units')
class AtomUnits(Resource):
    @ns_data_access.doc('atom_units')
    @cross_origin()
    @jwt_required()
    def get(self):
        user = UserConfig.get_user_or_none(get_jwt_identity())
        if user is None:
            return jsonify({'error': 'No client found'}), 404
        source_ids = user.get_unit_ids()
        if source_ids is None:
            return jsonify({'error': 'No client found'}), 404
        return get_multi_source_records('atom_units', source_ids)
    
@ns_data_access.route('/atom_users')
class AtomUsers(Resource):
    @ns_data_access.doc('atom_users')
    @cross_origin()
    @jwt_required()
    def get(self):
        user = UserConfig.get_user_or_none(get_jwt_identity())
        if user is None:
            return jsonify({'error': 'No client found'}), 404
        user.update_config_from_parent()
        user.save_to_db()
        user.dump_to_config_file()
        source_ids = user.get_atom_user_ids()
        if source_ids is None:
            return jsonify({'error': 'No client found'}), 404
        return get_multi_source_records('atom_users', source_ids)