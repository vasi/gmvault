import json
import sqlite3
from gmvault import GmailStorerFS

# TODO:
#   - quarantine
#   - caching?

class GmailStorerDB(GmailStorerFS):
    META_DB_FNAME = 'metadata.sqlite3'
    
    def __init__(self, a_storage_dir, encrypt_data = False):
        super(GmailStorerDB, self).__init__(a_storage_dir, encrypt_data)
        self._meta_db = '%s/%s' % (self._db_dir, self.META_DB_FNAME)
        self._conn = sqlite3.connect(self._meta_db)
        self._conn.row_factory = sqlite3.Row
        self._create_tables()
    
    def _create_tables(self):
        self._conn.executescript('''
            CREATE TABLE IF NOT EXISTS messages (
                gm_id INTEGER PRIMARY KEY,
				data TEXT
            );
        ''')
    
    def _delete_metadata(self, gm_id, the_dir):
        self._conn.execute('DELETE FROM messages WHERE gm_id = ?', (gm_id,))
        self._conn.commit()
    
    def _bury_metadata_obj(self, local_dir, obj):
        gm_id = obj[self.ID_K]
        self._conn.execute('REPLACE INTO messages VALUES (?,?)', (
            obj[self.ID_K], json.dumps(obj, ensure_ascii = False)
        ))
        self._conn.commit()
    
    def _unbury_metadata_obj(self, a_id, a_id_dir):
        cur = self._conn.cursor()
        cur.execute('SELECT data FROM messages WHERE gm_id = ?', (a_id,))
        return json.loads(cur.fetchone()['data'])

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
