from datetime import datetime, timedelta
import pprint
from flask import json, jsonify
from flask_cors import cross_origin
from flask_jwt_extended import get_jwt_identity, jwt_required
from flask_restx import Namespace, Resource, reqparse

from miniagro.config.server_config import ServerConfig
from miniagro.config.user_config import UserConfig
from miniagro.data_utils.sensor_utils import SensorUtils
from miniagro.data_utils.source_record_utils import SourceRecordUtils
from miniagro.utils.api_utils import APIUtils
from miniagro.utils.grofit_id_utils import IDUtils
from miniagro.utils.param_utils import DictUtils


ns_admin = Namespace('admin', description='Admin related operations', decorators=[cross_origin()])




# @ns_admin.route('/get_server_config')
# class GetServerConfig(Resource):
#     @ns_admin.doc('get_server_config')
#     @cross_origin()
#     @jwt_required()
#     def get(self):
#         user_id = get_jwt_identity()
#         ret = ServerConfig.get_self().to_dict()
#         print(ret.keys())
#         ret_di = {
#             'grofit_business_unit': ret['grofit_business_unit'],
#             'grofit_unit': ret['grofit_unit'],
#             'grofit_capsule': ret['grofit_capsule'],
#             'grofit_gateway': ret['grofit_gateway'],
#             'ims_source': ret['ims_source'],
#             'atom_users': ret['atom_users'],
         
#         }
#         return jsonify(ret_di)

@ns_admin.route('/get_mini_server_configs')
class getMiniServerConfigs(Resource):
    @ns_admin.doc('get_mini_server_configs')
    @cross_origin()
    @jwt_required()
    def get(self):
        ret = ServerConfig.get_all_servers()
        ret = DictUtils.to_camel_case(ret)
        return jsonify(ret)

@ns_admin.route('/get_user_config')
class GetUserConfig(Resource):
    @ns_admin.doc('get_user_config')
    @cross_origin()
    @jwt_required()
    def get(self):
        ret = UserConfig.get_all_users()
        ur_list = []

        for user in ret:
            pprint.pprint(user.keys())
            ur = {} 
            ur['_id'] = str(user['_id'])
            ur['name'] = user['name']
            ur['email'] = user.get('email', None)
            ur['server_id'] = user['server_id']
            ur['is_admin'] = user['is_admin']
            ur['full_data_access'] = user['full_data_access']
            if not ur['full_data_access']:
                ur['grofit_business_unit'] = user['grofit_business_unit']
                ur['grofit_unit'] = user['grofit_unit']
                ur['ims_source'] = user['ims_source']
                ur['grofit_capsule'] = user['grofit_capsule']
                ur['grofit_gateway'] = user['grofit_gateway']
            ur_list.append(ur)
        # pprint.pprint(ret)
        return jsonify(ur_list)



add_source_parser = reqparse.RequestParser()
add_source_parser.add_argument('server_id', type=str, required=True, help='The id of the server to add the source to')
add_source_parser.add_argument('source_id', type=str, required=True, help='The id of the source to add')
add_source_parser.add_argument('source_type', type=str, required=True, help='The type of the source to add', choices=['capsule', 'gw', 'bu', 'unit'], 
                               default='capsule')

@ns_admin.route('/add_source_to_server')

class AddSourceToServer(Resource):
    @ns_admin.doc('add_source_to_server')
    @cross_origin()
    @ns_admin.expect(add_source_parser)
    @jwt_required()
    def get(self):
        server_config = ServerConfig.get_self()
        args = add_source_parser.parse_args()
        source_id = args['source_id']
        source_type = args['source_type']
        server_id = args['server_id']
        if server_id == 'self' or server_id == server_config.server_id:
            return jsonify({'error': 'Invalid server id'}), 400
        index_id = None
        if source_type == 'capsule':
            src_id = IDUtils.gd_mac_to_source_id(source_id)
            index_id = 'atom_source_info'
        elif source_type == 'gw':
            src_id = IDUtils.gw_uniq_id_to_source_id(source_id)
            index_id = 'atom_gw_data'
        elif source_type == 'bu':
            src_id = IDUtils.bu_id_to_source_id(source_id)
            index_id = 'atom_business_units'
        elif source_type == 'unit':
            src_id = IDUtils.unit_id_to_source_id(source_id)
            index_id = 'atom_units'
        else:
            return jsonify({'error': 'Invalid source type'}), 400
        
        src = SourceRecordUtils().get_last_source_records(index_id, [src_id])
        pprint.pprint(src)
        if not src:
            return jsonify({'error': 'Source not found'}), 404
        srv = ServerConfig(server_id=args['server_id']).load_from_db()
        pprint.pprint(srv.to_dict())
        if not srv.filter_ids([src_id]):
            srv.add_ids([src_id])
            srv.save_to_db()
            return jsonify({'success': 'Source added'}), 200
        else:
            return jsonify({'no_change': 'Source already exists'}), 200



@ns_admin.route('/remove_source_from_server')
class RemoveSourceFromServer(Resource):
    @ns_admin.doc('remove_source_from_server')
    @cross_origin()
    @ns_admin.expect(add_source_parser)
    @jwt_required()
    def get(self):
        args = add_source_parser.parse_args()
        source_id = args['source_id']
        source_type = args['source_type']
        server_id = args['server_id']
        if server_id == 'self' or server_id == 'meta_server':
            return jsonify({'error': 'Invalid server id'}), 400
        srv = ServerConfig().load_from_db(server_id)
        if not srv:
            return jsonify({'error': 'Invalid server id'}), 400
        if srv.remove_ids([source_id]):
            srv.save_to_db()
            return jsonify({'success': 'Source removed'}), 200
        else:
            return jsonify({'no_change': 'Source not found'}), 200


add_user_parser = add_source_parser.copy()
add_user_parser.add_argument('user_id', type=str, required=True, help='The id of the user to add the source to')


@ns_admin.route('/add_source_to_user')
class AddSourceToUser(Resource):
    @ns_admin.doc('add_source_to_user')
    @cross_origin()
    @ns_admin.expect(add_user_parser)
    @jwt_required()
    def get(self):
        args = add_user_parser.parse_args()
        source_id = args['source_id']
        source_type = args['source_type']
        user_id = args['user_id']
        server_id = args['server_id']
        user = UserConfig.get_user_or_none(user_id, server_id)

        if user is None or server_id is None or server_id != user.server_id:
            return jsonify({'error': 'Invalid user id'}), 400
        if user.filter_ids([source_id]):
            return jsonify({'no_change': 'Source already exists'}), 200
        user.add_ids([source_id])
        user.save_to_db()
        return jsonify({'success': 'Source added'}), 200


@ns_admin.route('/remove_source_from_user')
class RemoveSourceFromUser(Resource):
    @ns_admin.doc('remove_source_from_user')
    @cross_origin()
    @ns_admin.expect(add_user_parser)
    @jwt_required()
    def get(self):
        args = add_user_parser.parse_args()
        source_id = args['source_id']
        source_type = args['source_type']
        user_id = args['user_id']
        server_id = args['server_id']
        user = UserConfig.get_user_or_none(user_id, server_id)
        if user is None or server_id is None or server_id != user.server_id:
            return jsonify({'error': 'Invalid user id'}), 400
        if not user.filter_ids([source_id]):
            return jsonify({'no_change': 'Source not found'}), 200
        user.remove_ids([source_id])
        user.save_to_db()
        return jsonify({'success': 'Source removed'}), 200


add_user_parser = reqparse.RequestParser()
add_user_parser.add_argument('username', type=str, required=True, help='The username of the user to add')
add_user_parser.add_argument('password', type=str, required=True, help='The password of the user to add')
add_user_parser.add_argument('name', type=str, required=True, help='The name of the user to add')
add_user_parser.add_argument('server_id', type=str, required=True, help='The server id of the user to add')
add_user_parser.add_argument('is_admin', type=bool, required=True, help='Whether the user is an admin')
add_user_parser.add_argument('full_data_access', type=bool, required=True, help='Whether the user has full data access')


@ns_admin.route('/add_user')
class AddUser(Resource):
    @ns_admin.doc('add_user')
    @cross_origin()
    @ns_admin.expect(add_user_parser)
    @jwt_required()
    def get(self):
        args = add_user_parser.parse_args()
        srv = ServerConfig(server_id=args['server_id']).load_from_db()
        if srv is None:
            return jsonify({'error': 'Invalid server id'}), 400
        user = UserConfig(server_id=args['server_id'], username=args['username'], password=args['password'], name=args['name'], is_admin=args['is_admin'], full_data_access=args['full_data_access'])
        user.save_to_db()
        return jsonify({'success': 'User added'}), 200


remove_user_parser = reqparse.RequestParser()
remove_user_parser.add_argument('username', type=str, required=True, help='The username of the user to remove')
remove_user_parser.add_argument('server_id', type=str, required=True, help='The server id of the user to remove')

@ns_admin.route('/remove_user')
class RemoveUser(Resource):
    @ns_admin.doc('remove_user')
    @cross_origin()
    @jwt_required()
    def get(self):
        args = add_user_parser.parse_args()
        user = UserConfig.get_user_or_none(args['username'], args['server_id'])
        if user is None:
            return jsonify({'error': 'Invalid user id'}), 400
        UserConfig.get_collection().delete_one({'_id': user.get_id()})
        return jsonify({'success': 'User removed'}), 200
    
set_user_password_parser = reqparse.RequestParser()
set_user_password_parser.add_argument('username', type=str, required=True, help='The username of the user to set the password for')
set_user_password_parser.add_argument('server_id', type=str, required=True, help='The server id of the user to set the password for')
set_user_password_parser.add_argument('password', type=str, required=True, help='The password to set for the user')

@ns_admin.route('/set_user_password')
class SetUserPassword(Resource):
    @ns_admin.doc('set_user_password')
    @cross_origin()
    @ns_admin.expect(set_user_password_parser)
    @jwt_required()
    def get(self):
        args = set_user_password_parser.parse_args()
        user = UserConfig.get_user_or_none(args['username'], args['server_id'])
        if user is None:
            return jsonify({'error': 'Invalid user id'}), 400
        pwd = args['password']
    
        if not user.set_password(pwd):
            return jsonify({'error': 'Invalid password'}), 400
        user.save_to_db()
        return jsonify({'success': 'User password set'}), 200

