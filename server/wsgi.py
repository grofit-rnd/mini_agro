from miniagro.server.flask_server import app
from miniagro.db.data_manager_sdk import DMSDK

if __name__ == "__main__":
    DMSDK()

    flask_server=app
    flask_server.run(host='0.0.0.0', port=5033)
