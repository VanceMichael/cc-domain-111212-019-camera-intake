import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from threading import Lock

def _utcnow():
    return datetime.now(timezone.utc).isoformat()

class Archive:
    def __init__(self, path):
        self.connection = sqlite3.connect(path, timeout=5)
        self.connection.execute('PRAGMA foreign_keys=ON')
        self.connection.executescript(
            'CREATE TABLE IF NOT EXISTS captures('
            'id INTEGER PRIMARY KEY, source TEXT NOT NULL, digest TEXT NOT NULL,'
            'raw BLOB NOT NULL, rows_json TEXT NOT NULL, UNIQUE(source,digest));')

    def save(self, source, raw, rows):
        if not source.strip():
            raise ValueError('来源标识不能为空')
        digest = hashlib.sha256(raw).hexdigest()
        payload = json.dumps(rows, ensure_ascii=False, sort_keys=True)
        with self.connection:
            self.connection.execute(
                'INSERT OR IGNORE INTO captures(source,digest,raw,rows_json) VALUES(?,?,?,?)',
                (source, digest, raw, payload))
            record = self.connection.execute(
                'SELECT id FROM captures WHERE source=? AND digest=?', (source, digest)).fetchone()
        return record[0]

    def list(self):
        return [dict(id=row[0], source=row[1], digest=row[2], rows=json.loads(row[3]))
                for row in self.connection.execute(
                    'SELECT id,source,digest,rows_json FROM captures ORDER BY id')]

    def raw(self, capture_id):
        row = self.connection.execute('SELECT raw FROM captures WHERE id=?', (capture_id,)).fetchone()
        if row is None:
            raise KeyError(capture_id)
        return bytes(row[0])

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

class FrameStore:
    """采样帧与缺口说明的本地留存。

    只追加不按时间覆盖：来源时间回拨或同时间多帧各自成行，
    行 id 即证据顺序；digest 用于完整性核对，不去重。
    写操作由接收会话的写线程执行，读操作可在关闭后进行。
    """

    def __init__(self, path):
        self.lock = Lock()
        self.connection = sqlite3.connect(path, timeout=5, check_same_thread=False)
        self.connection.executescript(
            'CREATE TABLE IF NOT EXISTS frames('
            'id INTEGER PRIMARY KEY, source TEXT NOT NULL, connection INTEGER NOT NULL,'
            'sequence INTEGER, source_time TEXT, received_at TEXT NOT NULL,'
            'received_mono REAL NOT NULL, reason TEXT NOT NULL,'
            'digest TEXT NOT NULL, bytes INTEGER NOT NULL, raw BLOB NOT NULL);'
            'CREATE TABLE IF NOT EXISTS gaps('
            'id INTEGER PRIMARY KEY, source TEXT NOT NULL, connection INTEGER,'
            'at TEXT NOT NULL, kind TEXT NOT NULL, detail_json TEXT NOT NULL);')

    def save_frame(self, source, connection, sequence, source_time, received_at,
                   received_mono, reason, raw):
        digest = hashlib.sha256(raw).hexdigest()
        with self.lock, self.connection:
            cursor = self.connection.execute(
                'INSERT INTO frames(source,connection,sequence,source_time,received_at,'
                'received_mono,reason,digest,bytes,raw) VALUES(?,?,?,?,?,?,?,?,?,?)',
                (source, connection, sequence, source_time, received_at,
                 received_mono, reason, digest, len(raw), raw))
            return cursor.lastrowid

    def save_gap(self, source, connection, kind, detail, at=None):
        payload = json.dumps(detail, ensure_ascii=False, sort_keys=True)
        with self.lock, self.connection:
            cursor = self.connection.execute(
                'INSERT INTO gaps(source,connection,at,kind,detail_json) VALUES(?,?,?,?,?)',
                (source, connection, at or _utcnow(), kind, payload))
            return cursor.lastrowid

    def list_frames(self, source=None):
        sql = 'SELECT id,source,connection,sequence,source_time,received_at,reason,' \
              'digest,bytes FROM frames'
        args = ()
        if source is not None:
            sql += ' WHERE source=?'
            args = (source,)
        return [dict(id=row[0], source=row[1], connection=row[2], sequence=row[3],
                     source_time=row[4], received_at=row[5], reason=row[6],
                     digest=row[7], bytes=row[8])
                for row in self.connection.execute(sql + ' ORDER BY id', args)]

    def list_gaps(self, source=None):
        sql = 'SELECT id,source,connection,at,kind,detail_json FROM gaps'
        args = ()
        if source is not None:
            sql += ' WHERE source=?'
            args = (source,)
        return [dict(id=row[0], source=row[1], connection=row[2], at=row[3],
                     kind=row[4], detail=json.loads(row[5]))
                for row in self.connection.execute(sql + ' ORDER BY id', args)]

    def frame_raw(self, frame_id):
        row = self.connection.execute('SELECT raw FROM frames WHERE id=?',
                                      (frame_id,)).fetchone()
        if row is None:
            raise KeyError(frame_id)
        return bytes(row[0])

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
