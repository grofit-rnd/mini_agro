

class MongoIndexUtils:
    @classmethod
    def create_index(cls, collection, index_name, index_fields, index_options):
        # Check if the index exists
        if collection is None  or  index_fields is None or index_options is None or not index_name:
            return False
        print(f"Creating index '{index_name}' on collection '{collection.name}' with fields '{index_fields}' and options '{index_options}'.")
        collection.create_index(index_fields, name=index_name, **index_options)
        print(f"Index '{index_name}' created.")
        return True

    @classmethod
    def has_index(cls, db, collection, index_name):
        index_names = [index["name"] for index in collection.list_indexes()]
        return index_name in index_names

    @classmethod
    def create_indexes_if_needed(cls, db, col_name, indexes):
        if not indexes:
            return db[col_name]
        index_names = [index["name"] for index in db[col_name].list_indexes()]
        for index in indexes:
            if 'name' not in index or 'fields' not in index or 'options' not in index:
                print(f"Index name: {index['name']}, fields: {index['fields']}, options: {index['options']}")
                raise Exception("Index name, fields and options must be provided.")
            name = index.get("name", None)
            fields = index.get("fields", None)
            options = index.get("options", None)

            if not name or  fields is None or  options is None:
                print(f"Index name: {name}, fields: {fields}, options: {options}")
                raise Exception("Index name, fields and options must be provided.")
            if name in index_names:
                # print(f"Index '{name}' already exists.")
                continue
            cls.create_index(db[col_name], index_name=name, index_fields=fields, index_options=options)
        return db[col_name]
