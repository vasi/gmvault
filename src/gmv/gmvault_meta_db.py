import string
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
        # NOTE: Flags are denormalized, space separated
        self._conn.executescript('''
            CREATE TABLE IF NOT EXISTS messages (
                gm_id INTEGER PRIMARY KEY,
                flags TEXT,
                thread_ids INTEGER,
                internal_date INTEGER,
                subject TEXT,
                msg_id TEXT
            );
            CREATE TABLE IF NOT EXISTS labels (
                label_id INTEGER PRIMARY KEY,
                name TEXT UNIQUE
            );
            CREATE TABLE IF NOT EXISTS message_labels (
                message INTEGER,
                label INTEGER
            );
            CREATE INDEX IF NOT EXISTS message_labels_message
                ON message_labels (message); 
        ''')
    
    def _delete_metadata(self, gm_id, the_dir):
        self._conn.execute('DELETE FROM messages WHERE gm_id = ?', (gm_id,))
        self._conn.execute('DELETE FROM message_labels WHERE message = ?',
            (gm_id,))
        self._conn.commit()
    
    def _bury_metadata_obj(self, local_dir, obj):
        cur = self._conn.cursor()
        gm_id = obj[self.ID_K]
        
        cur.execute('REPLACE INTO messages VALUES (?,?,?,?,?,?)', (
            gm_id,
            string.join(obj[self.FLAGS_K], ' '),
            obj[self.THREAD_IDS_K],
            obj[self.INT_DATE_K],
            obj[self.SUBJECT_K],
            obj[self.MSGID_K]
        ))
        
        cur.execute('DELETE FROM message_labels WHERE message = ?', (gm_id,))
        labels = []
        for label in obj[self.LABELS_K]:
            cur.execute('SELECT label_id FROM labels WHERE name = ?', (label,))
            row = cur.fetchone()
            if row:
                labels.append(row[0])
            else:
                cur.execute('INSERT INTO labels VALUES (NULL, ?)', (label,))
                labels.append(cur.lastrowid)
        cur.executemany('INSERT INTO message_labels VALUES (?,?)',
            [(gm_id, l) for l in labels])
        
        self._conn.commit()
    
    def _unbury_metadata_obj(self, a_id, a_id_dir):
        cur = self._conn.cursor()
        
        cur.execute('''SELECT name FROM message_labels JOIN labels WHERE
            label = label_id AND message = ?''', (a_id,))
        labels = [r[0] for r in cur.fetchall()]
        cur.execute('SELECT * FROM messages WHERE gm_id = ?', (a_id,))
        obj = dict(cur.fetchone())
        
        obj[self.FLAGS_K] = string.split(obj[self.FLAGS_K], ' ')
        obj[self.LABELS_K] = labels
        return obj

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
