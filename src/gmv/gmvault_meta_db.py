import types
import collections
import json
import sqlite3
from gmvault import GmailStorerFS

# TODO:
#   - quarantine
#   - caching?

class GmailStorerDB(GmailStorerFS):
    META_DB_FNAME = 'metadata.sqlite3'
    
    CATEGORY_K = 'category'
    DIRECTORY_K = 'directory'
    
    def __init__(self, a_storage_dir, encrypt_data = False):
        super(GmailStorerDB, self).__init__(a_storage_dir, encrypt_data)
        self._meta_db = '%s/%s' % (self._db_dir, self.META_DB_FNAME)
        self._conn = sqlite3.connect(self._meta_db)
        self._conn.row_factory = sqlite3.Row
        self._create_tables()
        self._init_index(self.INT_DATE_K, self.CATEGORY_K)
    
    def _create_tables(self):
        self._conn.executescript('''
            CREATE TABLE IF NOT EXISTS messages (
                gm_id INTEGER PRIMARY KEY,
				data TEXT
            );
            CREATE TABLE IF NOT EXISTS fields (
                field TEXT PRIMARY KEY,
                force INTEGER
            );
            CREATE TABLE IF NOT EXISTS indexed (
                gm_id INTEGER,
                field TEXT,
                value TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_message ON indexed (gm_id, field);
            CREATE INDEX IF NOT EXISTS idx_value ON indexed (field, value);
        ''')
    
    def _init_index(self, *indices):
        self._conn.executemany('INSERT OR IGNORE INTO fields VALUES (?, 1)',
            [(i,) for i in indices])
        
        self._indexes = []
        force = []
        cur = self._conn.cursor()
        for row in cur.execute('SELECT * FROM fields'):
            field = row['field']
            self._indexes.append(field)
            if row['force'] > 0:
                force.append(field)
        
        if force:
            for row in cur.execute('SELECT data FROM messages'):
                self._index_metadata_obj(json.loads(row['data']), force)
        
        self._conn.execute('UPDATE fields SET force = 0')
        self._conn.commit()
    
    def _indexed_values(self, v):
        '''For a list, index each item. For other types, index the value'''
        if hasattr(v, '__iter__') and not isinstance(v, types.StringTypes):
            return v
        return (v,)
    
    def _index_metadata_obj(self, obj, fields):
        gm_id = obj[self.ID_K]
        rows = []
        for f in fields:
            for v in self._indexed_values(obj[f]):
                rows.append((gm_id, f, v))
        self._conn.executemany('INSERT INTO indexed VALUES (?, ?, ?)', rows)
    
    def _delete_metadata(self, gm_id, the_dir):
        self._conn.execute('DELETE FROM messages WHERE gm_id = ?', (gm_id,))
        self._conn.execute('DELETE FROM indexed WHERE gm_id = ?', (gm_id,))
        self._conn.commit()
    
    def _bury_metadata_obj(self, local_dir, obj):
        if local_dir and local_dir.startswith(self.CHATS_AREA):
            obj[self.CATEGORY_K] = self.CATEGORY_CHAT
        else:
            obj[self.CATEGORY_K] = self.CATEGORY_EMAIL
        obj[self.DIRECTORY_K] = local_dir
        
        gm_id = obj[self.ID_K]
        self._conn.execute('REPLACE INTO messages VALUES (?,?)', (
            obj[self.ID_K], json.dumps(obj, local_dir, ensure_ascii = False)
        ))
        self._conn.execute('DELETE FROM indexed WHERE gm_id = ?', (gm_id,))
        self._index_metadata_obj(obj, self._indexes)
        self._conn.commit()
    
    def _unbury_metadata_obj(self, a_id, a_id_dir):
        cur = self._conn.cursor()
        cur.execute('SELECT data FROM messages WHERE gm_id = ?', (a_id,))
        row = cur.fetchone()
        return json.loads(row['data']) if row else None
    
    def get_directory_from_id(self, a_id, a_local_dir = None):
        obj = self._unbury_metadata_obj(a_id, a_local_dir)
        if obj:
            return '%s/%s' % (self._db_dir, obj[self.DIRECTORY_K])
        return None
    
    def _get_ids(self, category, start_time = None):
        stmt = 'SELECT DISTINCT gm_id, data FROM messages %(join)s WHERE %(where)s'
        join = ['JOIN indexed AS cat USING (gm_id)']
        where = ['cat.field = ?', 'cat.value = ?']
        params = [self.CATEGORY_K, category]
        if start_time:
            join.append('JOIN indexed AS date USING (gm_id)')
            where.extend(['date.field = ?', 'date.value >= ?'])
            begin = gmvault_utils.datetime2e(start_time)
            params.extend([self.INT_DATE_K, begin])
        stmt = stmt % { 'join': ' '.join(join), 'where': ' AND '.join(where) }
        
        for row in self._conn.execute(stmt, params):
            obj = json.loads(row['data'])
            yield (obj[self.ID_K], obj[self.DIRECTORY_K])

if __name__ == '__main__':
    import sys
    db = GmailStorerDB(sys.argv[1])

    db._bury_metadata_obj(None, {
        "msg_id": "CAJnB4vBb5eS43vQPSphQiyo8oGQGtmDsMgFKZq9Pk1L1W6uZDA@mail.gmail.com",
        "gm_id": 1412679471642059988,
        "labels": ["\\Inbox", "\\Important", "Perso/Foo"],
        "thread_ids": 1412630003922319997,
        "flags": ["\\Seen", "\\Flagged"],
        "internal_date": 1347243329,
        "subject": "Re: some subject\r"
    })
    db._bury_metadata_obj(None, {
        "msg_id": "CAJnB4vBb5eS43vQPSphQizo8oGQGtmDsMgFKZq9Pk1L1W6uZDA@mail.gmail.com",
        "gm_id": 1412679471642012345,
        "labels": ["\\Inbox", "Test"],
        "thread_ids": 1412630003922318997,
        "flags": [],
        "internal_date": 1347243328,
        "subject": "Re: another subject\r"
    })
    db._bury_metadata_obj(None, {
        "msg_id": "CAJnB4vBb5eS43vQPSphQiyo8oGQGtmDsMgFKZq9Pk1L1W6uZDA@mail.gmail.com",
        "gm_id": 1412679471642059988,
        "labels": ["\\Inbox", "\\Important", "Perso/Foo", "Added"],
        "thread_ids": 1412630003922319997,
        "flags": ["\\Seen", "\\Flagged"],
        "internal_date": 1347243329,
        "subject": "Re: some subject\r"
    })

    print db._unbury_metadata_obj(1412679471642059988, None)
