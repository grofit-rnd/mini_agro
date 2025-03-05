import os
import pathlib





class DirUtils:

    @classmethod
    def get_server_config(cls):
        from miniagro.config.server_config import ServerConfig
        return ServerConfig().load_from_db('self')

    @classmethod
    def create_data_dir(cls, path):
        return cls.get_path(path, create=True)

    @classmethod
    def get_data_dir(cls):
        server_config = cls.get_server_config()
        if not server_config:
            return None
        
        return server_config.data_dir


    @classmethod
    def get_path(cls, p, create=True):
        p = pathlib.Path(p)
        if os.path.exists(p):
            return p
        if not create:
            return None
        p.mkdir(parents=True, exist_ok=True)
        return p

    @classmethod
    def get_data_path(cls, p, create=True):
        server_config = cls.get_server_config()
        if not p:
            return server_config.data_dir

        if isinstance(p, list):
            p = '/'.join(p)
        ret = f'{server_config.data_dir}/{p}'
        return cls.get_path(ret, create=create)

    @classmethod
    def get_config_file(cls, p):
        server_config = cls.get_server_config()
        return f'{server_config.config_dir}/{p}'
    
    @classmethod
    def get_config_dir(cls):
        server_config = cls.get_server_config()
        return server_config.config_dir

