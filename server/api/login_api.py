from datetime import timedelta, datetime
from flask import request, jsonify
from flask_jwt_extended import create_access_token, get_jwt_identity, create_refresh_token, jwt_required, \
    decode_token
from flask_restx import Namespace, Resource, reqparse
from flask_cors import cross_origin

from miniagro.db.data_manager_sdk import DMSDK
from miniagro.config.user_config import UserConfig
is_live = True

ns_login = Namespace('login', description='Login related operations', decorators=[cross_origin()])

login_parser = reqparse.RequestParser()
login_parser.add_argument('username', required=True)
login_parser.add_argument('password', required=True)

# Use the AgroSDK collection for storing revoked tokens
token_ref_collection = DMSDK().admin_db.get_collection('user_token_ref')
# login_user_name = 'atomation_push'
# login_password = 'atom_password'
login_user_name = 'atomation_api'
login_password = 'TBlv9pNzdj'

login_test = 'test'
login_test_password = 'test'

@ns_login.route('/login/')
@ns_login.expect(login_parser)
class Login(Resource):
    @ns_login.doc('login', security=[])
    @ns_login.expect(login_parser)
    @cross_origin()
    def post(self):
        args = login_parser.parse_args()
        user_name = args['username']
        password = args['password']
        print(f'login user_name = {user_name}, password = {password}')

        # dm_user = GRUser.load_from_db(user_name)
        dm_user = UserConfig.validate_user(server_id='meta_server', username=user_name, password=password)

        # dm_user = AgroAppUser.load_from_db(user_name)
        print(f'dm_user = {dm_user}')
        if dm_user is None:
            return jsonify({'message': 'Wrong username or password'}), 401
     
        access_token = create_access_token(identity=dm_user.get_id(), expires_delta=timedelta(days=30))
        refresh_token = create_refresh_token(identity=dm_user.get_id())
        expires_in = datetime.utcnow() + timedelta(days=29)
        expires_in = expires_in.isoformat()

        # dat = decode_token(access_token)
        # drt = decode_token(refresh_token)
        print(f'access_token = {access_token}')
        return jsonify({'access_token': access_token, 'refresh_token': refresh_token, 'expires_in': expires_in})



@ns_login.route("/logout")
class Logout(Resource):
    @jwt_required()
    @cross_origin()
    def post(self):
        identity = get_jwt_identity()

        # Add the access token to the blacklist in the user_token_ref collection
        access_token = request.headers.get('Authorization').split()[1]

        # Store the revoked token in the user_token_ref collection
        token_ref_collection.update_one({'_id': identity}, {"$push": {"access_token":  decode_token(access_token)}}, upsert=True)

        return jsonify({'message': 'Logout successful'}), 200


@ns_login.route("/ping")
class Ping(Resource):
    @cross_origin()
    @jwt_required()
    def get(self):
        return jsonify({'message': 'Ping successful'}), 200


@ns_login.route("/refresh")
class Refresh(Resource):
    @cross_origin()
    @jwt_required(refresh=True)
    def post(self):
        identity = get_jwt_identity()
        access_token = create_access_token(identity=identity)
        return jsonify({'access_token': access_token})


