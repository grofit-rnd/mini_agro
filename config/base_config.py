import json
import os
import pprint

import requests

from miniagro.utils.dir_utils import DirUtils
from miniagro.utils.param_utils import DictUtils
   

class BaseConfig:
    
    def __init__(self, server_id=None, 
                 name=None, is_admin=False):
        self.server_id = server_id
        self.name = name
        self.is_admin = is_admin   
        self.grofit_capsule = []
        self.grofit_gateway = []
        self.grofit_business_unit = []
        self.grofit_unit = []
        self.ims_source = []
        self.atom_users = []
    def get_collection(self):
        raise NotImplementedError("Subclass must implement get_collection method")

    def get_config(self, config_id):
        col = self.get_collection()
        di = col.find_one({'_id': config_id})
        if di:
            return self.populate_from_dict(di)
        return None
    
    def get_id(self):
        raise NotImplementedError("Subclass must implement get_id method")
   
    def to_dict(self):
        return {
            '_id': self.get_id(),
            'server_id': self.server_id,
            'name': self.name,
            'is_admin': self.is_admin,
            'grofit_capsule': self.grofit_capsule,
            'grofit_gateway': self.grofit_gateway,
            'grofit_business_unit': self.grofit_business_unit,
            'grofit_unit': self.grofit_unit,
            'ims_source': self.ims_source,
            'atom_users': self.atom_users,
        }
    
    def populate_from_dict(self, di):
        self._id = DictUtils.get_value(di, '_id', None)
        self.server_id = DictUtils.get_value(di, 'server_id', self.server_id)
        self.name = DictUtils.get_value(di, 'name', self.name)
        self.is_admin = DictUtils.get_value(di, 'is_admin', self.is_admin)
       
        self.grofit_capsule = DictUtils.get_value(di, 'grofit_capsule', self.grofit_capsule)
        self.grofit_gateway = DictUtils.get_value(di, 'grofit_gateway', self.grofit_gateway)
        self.grofit_business_unit = DictUtils.get_value(di, 'grofit_business_unit', self.grofit_business_unit)
        self.grofit_unit = DictUtils.get_value(di, 'grofit_unit', self.grofit_unit)
        self.ims_source = DictUtils.get_value(di, 'ims_source', self.ims_source)
        self.atom_users = DictUtils.get_value(di, 'atom_users', self.atom_users)
        return self
    
    def update_config_from_parent(self):
        pass
    def load_from_config_file(self, file_path):
        with open(file_path, 'r') as f:
            di = json.load(f)
        self.populate_from_dict(di)
        return self
    
    def save_to_db(self):
        col = self.get_collection()
        self.update_config_from_parent()
        col.update_one({'_id': self.get_id()}, {'$set': self.to_dict()}, upsert=True)
    
    def load_from_db(self, _id=None):
        col = self.get_collection()
        if not _id:
            _id = self.get_id()
        if not _id:
            return None
        di = col.find_one({'_id': _id})
        if di:
            self.populate_from_dict(di)
        return self
    
    def dump_to_config_file(self):
        file_path = os.path.join(DirUtils.get_config_dir(), self.get_id()+'.json')
        with open(file_path, 'w') as f:
            json.dump(self.to_dict(), f)

    def load_from_config_file(self, file_path):
        file_path = DirUtils.get_config_file(self.get_id()+'.json')
        try:
            with open(file_path, 'r') as f:
                di = json.load(f)
        except Exception as e:
            print(f'error loading config file {file_path}: {e}')
            return None
        self.populate_from_dict(di)
        return self
    
    def get_node_list(self, node_type):
        if node_type == 'grofit_capsule':
            return self.grofit_capsule
        elif node_type == 'grofit_gateway':
            return self.grofit_gateway
        elif node_type == 'grofit_business_unit':
            return self.grofit_business_unit
        elif node_type == 'grofit_unit':
            return self.grofit_unit
        elif node_type == 'ims_source':
            return self.ims_source
        elif node_type == 'atom_users':
            return self.atom_users
        else:
            raise Exception(f'invalid node type: {node_type}')
        
    def get_node_type(self, node_id):
        if node_id.startswith('gd_'):
            return 'grofit_capsule'
        elif node_id.startswith('gw_'):
            return 'grofit_gateway'
        elif node_id.startswith('bu_'):
            return 'grofit_business_unit'
        elif node_id.startswith('un_'):
            return 'grofit_unit'
        elif node_id.startswith('ims_'):
            return 'ims_source'
        elif node_id.startswith('au_'):
            return 'atom_users'
        else:
            return None
    
    def add_ids(self, ids):
        full_ids = self.get_all_ids()
        new_ids = [id for id in ids if id not in full_ids]
        if not new_ids:
            return
        nt = {}
        for id in new_ids:
            node_type = self.get_node_type(id)
            if node_type not in nt:
                nt[node_type] = []
            nt[node_type].append(id)
        for node_type, ids in nt.items():
            nodes = self.get_node_list(node_type)
            nodes.extend(ids)
            nodes = list(set(nodes))
            nodes.sort()
        self.save_to_db()
        self.dump_to_config_file()

    # def _add_node(self, node_ids):
    #     if not isinstance(node_ids, list):
    #         node_ids = [node_ids]
    #     nodes = None
    #     for node_id in node_ids:
    #         if (node_id.startswith('gd_')):
    #             nodes = self.grofit_capsule
    #         elif (node_id.startswith('gw_')):
    #             nodes = self.grofit_gateway
    #         elif (node_id.startswith('bu_')):
    #             nodes = self.grofit_business_unit
    #         elif (node_id.startswith('un_')):
    #             nodes = self.grofit_unit
    #         elif (node_id.startswith('ims_')):
    #             nodes = self.ims_source
    #         elif (node_id.startswith('au_')):
    #             nodes = self.atom_users
    #         else:
    #             raise Exception(f'invalid node id: {node_id}')
    #         nodes.append(node_id)
    #         nodes = list(set(nodes))
    #         nodes.sort()
    #     self.save_to_db()
    #     self.dump_to_config_file()

    def _remove_node(self, node_id):
        nodes = self.get_node_list()
        if not isinstance(node_id, list):
            node_id = [node_id]
        for node in node_id:
            if node in nodes:
                nodes.remove(node)
        self.save_to_db()
        self.dump_to_config_file()
    
    def get_source_ids(self):
        return self.grofit_capsule
        
    def get_gw_ids(self):
        return self.grofit_gateway
    
    def get_bu_ids(self)    :
        return self.grofit_business_unit
    
    def get_unit_ids(self):
        return self.grofit_unit
    
    def get_ims_ids(self):
        return self.ims_source
    
    def get_all_ids(self):
        return self.grofit_capsule + self.grofit_gateway + self.grofit_business_unit + self.grofit_unit + self.ims_source
    
    def filter_ids(self, ids):
        return [id for id in ids if id in self.get_all_ids()]
    
    def filter_source_ids(self, source_ids):
        return [source_id for source_id in source_ids if source_id in self.get_source_ids()]

    
    def get_atom_user_ids(self):
        return self.atom_users
    
    def get_ids(self, node_type):
        if not node_type:
            return self.get_all_ids()
        if node_type == 'gd':
            return self.get_source_ids()
        if node_type == 'gw':
            return self.get_gw_ids()
        if node_type == 'bu':
            return self.get_bu_ids()
        elif node_type == 'au':
            return self.get_unit_ids()
        elif node_type == 'au':
            return self.get_atom_user_ids()
        else:
            raise Exception(f'invalid node type: {node_type}')

    def remove_ids(self, ids):
        nt = {}
        for id in ids:
            node_type = self.get_node_type(id)
            if node_type is None:
                return False
            if node_type not in nt:
                nt[node_type] = []
            nt[node_type].append(id)
        for node_type, ids in nt.items():
            nodes = self.get_node_list(node_type)
            for id in ids:
                if id in nodes:
                    nodes.remove(id)
                else:
                    return False
        self.save_to_db()
        self.dump_to_config_file()
        return True
