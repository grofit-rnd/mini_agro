from miniagro.config.server_config import ServerConfig
from miniagro.config.user_config import UserConfig


class APIUtils:

    @classmethod
    def validate_user_id(cls, username):
        user = UserConfig.get_user_or_none(username)
        if user is None:
            return False
        return True
    
    @classmethod
    def check_user_role(cls, user_id, role):
        user = UserConfig.get_user_from_id_or_none(user_id)
        if user is None:
            return False
        return user.role == role

    @classmethod
    def filter_source_ids(cls, source_ids):
        allowed_source_ids = ServerConfig.get_self().get_source_ids()
        return [source_id for source_id in source_ids if source_id in allowed_source_ids]

    @classmethod
    def filter_ids(cls, ids):   
        allowed_ids = ServerConfig.get_self().get_all_ids()   
        return [i for i in ids if i in allowed_ids]

    @classmethod
    def filter_username_source_ids(cls, username, source_ids):
        user = UserConfig.get_user_or_none(username)
        if user is None:
            return None
        return user.filter_source_ids(source_ids)
    
    @classmethod
    def filter_user_source_ids(cls, user_id, source_ids=None):
        user = UserConfig.get_user_from_id_or_none(user_id)
        if user is None:
            return None
        if source_ids is None:
            return user.get_source_ids()
        return user.filter_source_ids(source_ids)

    @classmethod
    def is_id_allowed(cls, user_id, id):
        user = UserConfig.get_user_from_id_or_none(user_id)
        if user is None:
            return False
        return id in user.get_all_ids()
    
    @classmethod
    def filter_user_ids(cls, username, ids):
        user = UserConfig.get_user_or_none(username)
        if user is None:
            return None
        return user.filter_ids(ids)