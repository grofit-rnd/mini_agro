from miniagro.db.data_manager_sdk import DMSDK


class ReportView:
    @classmethod
    def get_col(cls):
        return DMSDK().info_db.get_collection('report_views')
    
    @classmethod
    def get_report_views(cls, user_id):
        views = list(cls.get_col().find({'user_id': user_id}, projection={'_id': 0, 'user_id': 0}))
        return views

    @classmethod
    def delete_report_view(cls, user_id, report_id):
        cls.get_col().delete_one({'_id': f'{user_id}_{report_id}'})

    def __init__(self, report=None):
      self.report = report or {}

    def to_dict(self):
        return self.report
    
    def save_to_db(self, user_id):
        if not user_id or not self.report:
            return None
        report_id = self.report.get('reportId')
        if not report_id:
            return None
        self.report['_id'] = f'{user_id}_{report_id}'
        self.report['user_id'] = user_id
        self.get_col().update_one({'_id': self.report['_id']}, {'$set': self.report}, upsert=True)
        return self.report
   