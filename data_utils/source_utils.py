from miniagro.config.server_config import ServerConfig


class SourceUtils:
    def __init__(self, server_id='self'):
        self.server_config = ServerConfig.get_server_config(server_id)
    
    def get_source_ids(self):
        return self.server_config.grofit_capsule
    
    def get_gw_ids(self):
        return self.server_config.grofit_gateway
    
    def get_bu_ids(self):
        return self.server_config.grofit_business_unit
    
    def get_unit_ids(self):
        return self.server_config.grofit_unit
    
    def get_ims_ids(self):
        return self.server_config.ims_source
    
    def get_all_ids(self):
        return self.server_config.grofit_capsules + self.server_config.grofit_gateways + self.server_config.grofit_business_units + self.server_config.grofit_units + self.server_config.ims_sources

    def get_all_ids_dict(self):
        return {
            'grofit_capsules': self.server_config.grofit_capsules,
            'grofit_gateways': self.server_config.grofit_gateways,
            'grofit_business_units': self.server_config.grofit_business_units,
            'grofit_units': self.server_config.grofit_units,
            'ims_sources': self.server_config.ims_sources
        }


    def add_source_id(self, source_id):
        if source_id not in self.server_config.grofit_capsule:
            self.server_config.grofit_capsule.append(source_id)
        self.server_config.save_to_db()

    def add_source_ids(self, source_ids):
        self.server_config.grofit_capsule.extend(source_ids)
        self.server_config.save_to_db()

    def remove_source_id(self, source_id):
        if source_id in self.server_config.grofit_capsule:
            self.server_config.grofit_capsule.remove(source_id)
        self.server_config.save_to_db()

    def remove_source_ids(self, source_ids):
        for source_id in source_ids:
            self.remove_source_id(source_id)

    def add_gw_ids(self, gw_ids):
        self.server_config.grofit_gateway.extend(gw_ids)
        self.server_config.save_to_db()

    def remove_gw_ids(self, gw_ids):
        for gw_id in gw_ids:
            self.remove_gw_id(gw_id)

    def remove_gw_id(self, gw_id):
        if gw_id in self.server_config.grofit_gateway:
            self.server_config.grofit_gateway.remove(gw_id)
        self.server_config.save_to_db() 

    def add_bu_ids(self, bu_ids):
        self.server_config.grofit_business_unit.extend(bu_ids)
        self.server_config.save_to_db()

    def remove_bu_ids(self, bu_ids):
        for bu_id in bu_ids:
            self.remove_bu_id(bu_id)

    def remove_bu_id(self, bu_id):
        if bu_id in self.server_config.grofit_business_unit:
            self.server_config.grofit_business_unit.remove(bu_id)
        self.server_config.save_to_db()

    def add_unit_ids(self, unit_ids):
        self.server_config.grofit_unit.extend(unit_ids)
        self.server_config.save_to_db()

    def remove_unit_ids(self, unit_ids):
        for unit_id in unit_ids:
            self.remove_unit_id(unit_id)

    def remove_unit_id(self, unit_id):
        if unit_id in self.server_config.grofit_unit:
            self.server_config.grofit_unit.remove(unit_id)
        self.server_config.save_to_db()


    def add_ims_ids(self, ims_ids):
        self.server_config.ims_source.extend(ims_ids)
        self.server_config.save_to_db()

    def remove_ims_ids(self, ims_ids):
        for ims_id in ims_ids:
            self.remove_ims_id(ims_id)

    def remove_ims_id(self, ims_id):
        if ims_id in self.server_config.ims_source:
            self.server_config.ims_source.remove(ims_id)
        self.server_config.save_to_db()
    
    def add_user_ids(self, user_ids):
        self.server_config.grofit_user.extend(user_ids)
        self.server_config.save_to_db()

    def remove_user_ids(self, user_ids):
        for user_id in user_ids:
            self.remove_user_id(user_id)
