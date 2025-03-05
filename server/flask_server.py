from flask import Flask, jsonify
from flask_cors import CORS
from flask_jwt_extended.exceptions import NoAuthorizationError
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_jwt_extended import JWTManager

from miniagro.server.api import api


app = Flask(__name__, template_folder='templates')

app.config["JWT_SECRET_KEY"] = "mHuPWHJhYJokI3AXoyc5vz752HwDVqNZuml3JYHbAlv8ZdbpI0ISLUcMlsodukq"
app.config["JWT_BLACKLIST_ENABLED"] = True
app.config["PROPAGATE_EXCEPTIONS"] = True
apis_jwt = JWTManager(app)


@apis_jwt.unauthorized_loader
def unauthorized_response(callback):
    return jsonify({'message': 'Unauthorized access. Please provide a valid JWT token.'}), 401


# app.config["JWT_SECRET_KEY"] = "super-secret"  # Change this!
app.wsgi_app = ProxyFix(app.wsgi_app)
CORS(app)

api.init_app(app)


def handle_auth_error(e):
    return jsonify({'message': 'Missing Authorization Header'}), 401


app.register_error_handler(NoAuthorizationError, handle_auth_error)


# app = build_app(reaper_on=False)


# jwt_gr.set_app(app)
#
# apis_jwt = JWTManager(app)
#
# def get_api_client(user_name):
#     return APIClient.get_from_db(user_name)

@apis_jwt.token_in_blocklist_loader
def check_if_token_in_blacklist(jwt_header, jwt_payload: dict) -> bool:

    jti = jwt_payload["jti"]
    _id = jwt_payload["sub"]
    query = {
        '_id': _id,
        'access_token': {
            '$elemMatch': {
                'jti': jti
            }
        }
    }
    # Perform the query and check the result
    # token = DMSDK().get_collection('user_token_ref').find_one(query)
    # if token is None:
    #     return False
    return False


if __name__ == '__main__':
    print('oko')
    #  app.run(debug=True, ssl_context=('cert.pem', 'key.pem'))
    app.run(debug=False, host='0.0.0.0')
