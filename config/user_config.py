import json
import pprint
import re

import requests

from miniagro.config.base_config import BaseConfig
from miniagro.db.data_manager_sdk import DMSDK
from miniagro.utils.param_utils import DictUtils
   

class UserConfig(BaseConfig):
    @classmethod
    def get_user(cls, user_id):
        return cls().load_from_db(user_id)
    
    @classmethod
    def get_user_or_none(cls, username, server_id=None):
        user = UserConfig(server_id=server_id or 'meta_server',
                          username=username)
        user.load_from_db()
        if user.username != username:
            return None
        return user
    @classmethod
    def get_all_users(cls):
        return list(cls().get_collection().find())
    @classmethod
    def get_user_from_id_or_none(cls, user_id):
        user = UserConfig()
        user.populate_from_dict(user.get_collection().find_one({'_id': user_id}))
        if not user.password:
            return None
        return user
    @classmethod
    def validate_user(cls, server_id, username, password):
        user = UserConfig(server_id=server_id, username=username)
        user.load_from_db()
        if user.password == password:    
            return user
        return None
    
    def __init__(self, server_id=None, 
                 name=None, username=None, password=None, is_admin=False, 
                 full_data_access=True):
        self.username = username
        self.password = password
        self.full_data_access = full_data_access
        super().__init__(server_id=server_id, name=name, is_admin=is_admin)

    def set_password(self, password):
        if not password:
            return False
        if len(password) < 6:
            return False
        if len(password) > 24:
            return False
        if not re.match(r'^[a-zA-Z0-9]+$', password):
            return False
        self.password = password
        self.save_to_db()
        self.dump_to_config_file()
        return True

    def get_collection(self):
        return DMSDK().admin_db.get_collection('user_config')

    def get_id(self):

        return self.username+'_'+self.server_id
   
    def update_config_from_parent(self):
        if self.full_data_access:
            from miniagro.config.server_config import ServerConfig

            server_config = ServerConfig().load_from_db(self.server_id)
            self.grofit_capsule = server_config.grofit_capsule
            self.grofit_gateway = server_config.grofit_gateway
            self.grofit_business_unit = server_config.grofit_business_unit
            self.grofit_unit = server_config.grofit_unit
            self.ims_source = server_config.ims_source
            self.atom_users = server_config.atom_users

    def to_dict(self):
        ret = super().to_dict()
        ret['username'] = self.username
        ret['password'] = self.password
        ret['full_data_access'] = self.full_data_access
        return ret
        
  
    def populate_from_dict(self, di):
        super().populate_from_dict(di)
        self.username = DictUtils.get_value(di, 'username', self.username)
        self.password = DictUtils.get_value(di, 'password', self.password)
        self.full_data_access = DictUtils.get_value(di, 'full_data_access', self.full_data_access)
        return self
    

def create_new_user(username, password, name, server_id, is_admin=False, full_data_access=True):
    uc = UserConfig(username=username, 
                    password=password, 
                    name=name, 
                    server_id=server_id, 
                    is_admin=is_admin, 
                    full_data_access=full_data_access)
    uc.update_config_from_parent()
    uc.save_to_db()
    uc.dump_to_config_file()
    return uc

if __name__ == '__main__':
    # uc = create_new_user(username='amiz', password='Yarden10', name='amiz', server_id='meta_server', is_admin=True, full_data_access=True)
    # print(uc)

    uc = create_new_user(username='syn_amiz', password='Yarden10', name='amiz', server_id='syngenta_spain', is_admin=False, full_data_access=False)
