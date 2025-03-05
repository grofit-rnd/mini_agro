

from miniagro.config.server_config import ServerConfig, create_meta_server_config


class BringUpAdmin:
    def __init__(self):
        self.server_config = None

    def create_admin_server_config(self):
        server_config = create_meta_server_config()
   
        return server_config


if __name__ == "__main__":
    bring_up_admin = BringUpAdmin()
    bring_up_admin.create_admin_server_config()
