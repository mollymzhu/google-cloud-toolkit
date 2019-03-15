from google.cloud import datastore


class DataStore():

    builtin_list = list

    def __init__(self, kind, project="gro-analytics"):
        """kind: the kind of document in datastore"""
        self.kind = kind
        self._project = project
        self._client = None

    def get_client(self):
        if self._client is None:
            self._client = datastore.Client(self._project)
        return self._client

    def update(self, data, id=None):
        """Update Datastore entries using:
        1. insert (id is not given), update with a new entry
        2. upsert (override record, id must be given)
        This returns:
            [Entity{key: (kind, id), prop: val, ...}]
        """
        ds = self.get_client()
        if id:
            key = ds.key(self.kind, id)
        else:
            key = ds.key(self.kind)

        entity = datastore.Entity(
            key=key,
            exclude_from_indexes=['description'])

        entity.update(data)
        ds.put(entity)
        return self.from_datastore(entity)

    def from_datastore(self, entity):
        """Translates Datastore results into the format expected by the
        application.
        Datastore typically returns:
            [Entity{key: (kind, id), prop: val, ...}]
        This returns:
            {id: id, prop: val, ...}
        """
        if not entity:
            return None
        if isinstance(entity, self.builtin_list):
            entity = entity.pop()

        entity['id'] = entity.key.id
        return entity

    def list_entities(self, limit=10, cursor=None):
        ds = self.get_client()

        query = ds.query(kind=self.kind)
        query_iterator = query.fetch(limit=limit, start_cursor=cursor)
        page = next(query_iterator.pages)

        entities = self.builtin_list(map(self.from_datastore, page))
        next_cursor = (
            query_iterator.next_page_token.decode('utf-8')
            if query_iterator.next_page_token else None)

        return entities, next_cursor

    def read(self, id):
        ds = self.get_client()
        key = ds.key(self.kind, id)
        results = ds.get(key)
        return self.from_datastore(results)
