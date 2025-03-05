import json
import os
import pprint

import requests

from miniagro.db.data_manager_sdk import DMSDK
from miniagro.config.base_config import BaseConfig
from miniagro.utils.dir_utils import DirUtils
from miniagro.utils.param_utils import DictUtils

class ServerConfig(BaseConfig):

    @classmethod
    def get_self(cls):
        return cls().load_from_db('self')
    
    def __init__(self, _id=None, server_id=None,
                 name=None, is_admin=False, 
                 cloud_url=None, cloud_username=None, cloud_password=None, 
                 base_dir=None):
        super().__init__(server_id=server_id, name=name, is_admin=is_admin)
        self._id = _id or server_id
        self.cloud_url = cloud_url
        self.cloud_username = cloud_username
        self.cloud_password = cloud_password
        self.base_dir = base_dir or "/home/rnd/dev/grofit_rnd/miniagro/"
        self.data_dir = self.base_dir + '/data'
        self.config_dir = self.base_dir + '/config'
    
    @classmethod
    def get_all_servers(cls):
        ret =list(cls.get_collection().find())
        ret = [r for r in ret if r['_id'] != 'self' and not r['is_admin']]
        return ret
    
    @classmethod
    def get_collection(cls):
        return DMSDK().admin_db.get_collection('server_config')

    def get_id(self):
        return self._id
    
    def to_dict(self):
        di = super().to_dict()
        di['cloud_url'] = self.cloud_url
        di['cloud_username'] = self.cloud_username
        di['cloud_password'] = self.cloud_password
        di['base_dir'] = self.base_dir
        di['data_dir'] = self.data_dir
        di['config_dir'] = self.config_dir
        return di

    def populate_from_dict(self, di):
        super().populate_from_dict(di)
        self.cloud_url = DictUtils.get_value(di, 'cloud_url', self.cloud_url)
        self.cloud_username = DictUtils.get_value(di, 'cloud_username', self.cloud_username)
        self.cloud_password = DictUtils.get_value(di, 'cloud_password', self.cloud_password)
        self.base_dir = DictUtils.get_value(di, 'base_dir', self.base_dir)
        self.data_dir = DictUtils.get_value(di, 'data_dir', self.data_dir)
        self.config_dir = DictUtils.get_value(di, 'config_dir', self.config_dir)
        return self
    
    def dump_to_config_file(self):
        p = DirUtils.get_path(self.config_dir, create=True)
        file_path = os.path.join(p, self.get_id()+'.json')
        print(f'dumping to {file_path}')
        with open(file_path, 'w') as f:
            json.dump(self.to_dict(), f)
        if self._id != self.server_id   :
            file_path = os.path.join(p, f'{self.server_id}.json')
            with open(file_path, 'w') as f:
                json.dump(self.to_dict(), f)

    def save_to_db(self):
        self.update_config_from_parent()
        col = self.get_collection()
        col.update_one({'_id': self._id}, {'$set': self.to_dict()}, upsert=True)
        if self._id != self.server_id:
            di = self.to_dict()
            di['_id'] = self.server_id
            col.update_one({'_id': self.server_id}, {'$set': di}, upsert=True)

def create_new_server(_id, server_id, name='meta_server', is_admin=False, cloud_url=None, cloud_username=None, cloud_password=None, base_dir=None,):
    sc = ServerConfig(_id=_id,
                      is_admin=is_admin,
                      name=name, 
                      server_id=server_id, 
                      cloud_url=cloud_url, 
                      cloud_username=cloud_username, 
                      cloud_password=cloud_password, 
                      base_dir=base_dir or '/home/rnd/dev/grofit_rnd/miniagro/data/')
    DirUtils.get_path(sc.base_dir, create=True)
    DirUtils.get_path(sc.config_dir, create=True)
    DirUtils.get_path(sc.data_dir, create=True)
    sc.dump_to_config_file()
    sc.save_to_db()
    return sc

if __name__ == '__main__':
    sc = create_new_server(_id='syngenta_spain',
                            server_id='syngenta_spain',
                            name='syngenta_spain', 
                            is_admin=False,
                              cloud_url='https://grofit.com', 
                              cloud_username='admin', 
                              cloud_password='Yarden10', 
                              base_dir='/home/rnd/dev/grofit_rnd/miniagro/data/')
    pprint.pp(sc.to_dict())
