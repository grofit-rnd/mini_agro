from flask_restx import Api

from .login_api import ns_login
from .data_access_api import ns_data_access
from .sensor_api import ns_sensor
from .mini_agro_api import ns_mini_agro
from .admin_api import ns_admin

jwt = None
# server_config = AgroAppServerConfig()
authorizations = {
    'Bearer Auth': {
        'type': 'apiKey',
        'in': 'header',
        'name': 'Authorization'
    },
}

api = Api(
    security='Bearer Auth',
    authorizations=authorizations,
    version='1.0',
    title='MiniAgro server',
    description='MiniAgro server')

api.add_namespace(ns_login)
api.add_namespace(ns_data_access)
api.add_namespace(ns_sensor)
api.add_namespace(ns_mini_agro)
api.add_namespace(ns_admin)

