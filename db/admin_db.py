from pymongo import ASCENDING

from miniagro.db.gdb_plugin import GDBPlugin

class AdminPlugin(GDBPlugin):

    def __init__(self, host=None, port=None, db_name=None, config=None):
        if not db_name:
            db_name = 'grofit_admin_db'

        super().__init__(host, port, db_name, config)
        self.col_names = ['server_config', 'atom_token', 'user_token_ref', 'user_config']

        self.prepare_collections()


    def get_indexes(self, key):
        if key == 'server_config':
            return [dict(name='server_id_idx', fields=[('server_id', ASCENDING)], options={}),
                    ]
        if key == 'atom_token':
            return [dict(name='token_idx', fields=[('token', ASCENDING)], options={}),
                    ]
        if key == 'user_token_ref':
            return [dict(name='user_id_idx', fields=[('user_id', ASCENDING)], options={}),
                    ]
        if key == 'user_config':
            return [dict(name='user_id_idx', fields=[('user_id', ASCENDING)], options={}),
                    dict(name='server_id_idx', fields=[('server_id', ASCENDING)], options={}),
                    dict(name='username_idx', fields=[('username', ASCENDING)], options={}),
                    ]
        return []


