from pymongo import UpdateOne, InsertOne
from pymongo.errors import BulkWriteError

from miniagro.utils.param_utils import DictUtils



class DBUtils:

    @classmethod
    def update_bulk_records(cls, col, records, allow_update=True, allow_insert=True, silent=False):
        if not allow_update and not allow_insert:
            raise Exception(
                'update_bulk_records: allow_update and allow_insert are both False')
        if col is None:
            print('update_bulk_records: col is None')
            return False
        if not records:
            return False
        if not isinstance(records, list):
            records = [records]
        if not isinstance(records[0], dict):
            records = [item.to_dict() for item in records]

        for i in range(0, len(records), 10000):
            update_operations = []
            insert_operations = []
            lst = min(i+10000, len(records))
            for r in records[i:lst]:
                _id = DictUtils.get_value(r, '_id', None)
                if (allow_insert and not _id) or (not allow_update):
                    insert_operations.append(InsertOne(r))
                else:
                    uo = UpdateOne(
                        {'_id': _id},
                        {"$set": r}, upsert=allow_insert)
                    update_operations.append(uo)
            # print(insert_operations)
            # print(update_operations)
            cls.execute_bulk_write(col, update_operations, insert_operations, silent=silent)


    @classmethod
    def execute_bulk_write(cls, col, update_operations, insert_operations, silent=False):
        if col is None:
            print('execute_bulk_write: col is None')
            return False
        success_update = len(update_operations)

        if update_operations:
            try:
                col.bulk_write(update_operations, ordered=False)
            except BulkWriteError as e:
                success_update = e.details['nModified']
            except Exception as e:
                print(e)
                # print(e.details)
            # col.bulk_write(update_operations)
            # print(
            #     f"updated {len(update_operations)}/{len(update_operations)} records to {col.name}")
        success_insert = len(insert_operations)
        if insert_operations:
            try:
                col.bulk_write(insert_operations, ordered=False)

            except BulkWriteError as e:
                ins = e.details['nInserted']
                success_insert = ins
                # print(e.details)
        if not silent:
            print(
                f"updated {success_update}/{len(update_operations)}:{success_insert}/{len(insert_operations)} records to {col.name}")
        return True

    @classmethod
    def load_records(cls, col, match=None, sort=None, limit=None, projection=None):
        if col is None:
            print('load_records: col is None')
            return False
        if match is None:
            match = {}
        if sort is None:
            sort = {}
        if projection is None:
            projection = {}
        pipeline = []
        if match:
            pipeline.append({'$match': match})
        if sort:
            pipeline.append({'$sort': sort})
        if limit:
            pipeline.append({'$limit': limit})
        if projection:
            pipeline.append({'$project': projection})
        print(pipeline)
        return list(col.aggregate(pipeline))