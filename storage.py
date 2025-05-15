import os
import sqlite3

class ChunkStorage:
    def __init__(self, chunk_dir):
        self.chunk_dir = chunk_dir
        os.makedirs(chunk_dir, exist_ok=True)
        self.db_path = os.path.join(chunk_dir, "chunk_index.db")
        self._init_db()
        self.container_handles = {}

    def _init_db(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS chunk_index (
                chunk_hash TEXT PRIMARY KEY,
                container_file TEXT,
                offset INTEGER,
                size INTEGER
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS file_map (
                file_path TEXT,
                chunk_index INTEGER,
                chunk_hash TEXT,
                chunk_size INTEGER,
                PRIMARY KEY (file_path, chunk_index)
            )
        ''')
        self.conn.commit()

    def write_chunk(self, path, chunk_hash, chunk):
        container_name = path.strip("/").replace("/", "_") + ".container"
        container_path = os.path.join(self.chunk_dir, container_name)

        if path not in self.container_handles:
            os.makedirs(os.path.dirname(container_path), exist_ok=True)
            f = open(container_path, 'ab+')
            f.seek(0, os.SEEK_END)
            offset = f.tell()
            self.container_handles[path] = (f, offset)
        else:
            f, offset = self.container_handles[path]
            f.seek(0, os.SEEK_END)
            offset = f.tell()

        f.write(chunk)
        f.flush()

        self.cursor.execute(
            "INSERT INTO chunk_index (chunk_hash, container_file, offset, size) VALUES (?, ?, ?, ?)",
            (chunk_hash, container_name, offset, len(chunk))
        )
        self.conn.commit()

    def store_file_map(self, path, chunk_list):
        self.cursor.execute("DELETE FROM file_map WHERE file_path = ?", (path,))
        for idx, (chunk_hash, chunk_size) in enumerate(chunk_list):
            self.cursor.execute(
                "INSERT INTO file_map (file_path, chunk_index, chunk_hash, chunk_size) VALUES (?, ?, ?, ?)",
                (path, idx, chunk_hash, chunk_size)
            )
        self.conn.commit()

    def get_chunk_metadata(self, chunk_hash):
        self.cursor.execute(
            "SELECT container_file, offset, size FROM chunk_index WHERE chunk_hash = ?",
            (chunk_hash,)
        )
        return self.cursor.fetchone()

    def load_file_chunks(self):
        self.cursor.execute('SELECT file_path, chunk_index, chunk_hash, chunk_size FROM file_map ORDER BY file_path, chunk_index')
        rows = self.cursor.fetchall()
        file_chunks = {}
        for file_path, _, chunk_hash, chunk_size in rows:
            file_chunks.setdefault(file_path, []).append((chunk_hash, chunk_size))
        return file_chunks

    def chunk_exists(self, chunk_hash):
        self.cursor.execute("SELECT 1 FROM chunk_index WHERE chunk_hash = ?", (chunk_hash,))
        return self.cursor.fetchone() is not None

    def flush(self):
        self.conn.commit()

    def close(self):
        self.conn.close()
        for f, _ in self.container_handles.values():
            f.close()
        self.container_handles.clear()
