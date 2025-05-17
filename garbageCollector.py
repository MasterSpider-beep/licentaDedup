import os
import sqlite3
from typing import Set

class GarbageCollector:
    def __init__(self, db_path: str, chunk_dir: str):
        self.db_path = db_path
        self.chunk_dir = chunk_dir
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()

    def mark(self) -> Set[str]:
        """Collect all chunk hashes that are still referenced by files."""
        self.cursor.execute("SELECT DISTINCT chunk_hash FROM file_map")
        marked = {row[0] for row in self.cursor.fetchall()}
        return marked

    def sweep(self, marked_chunks: Set[str]):
        """Remove all unreferenced chunks from chunk_index and optionally container files."""
        self.cursor.execute("SELECT chunk_hash, container_file, offset, size FROM chunk_index")
        all_chunks = self.cursor.fetchall()

        for chunk_hash, container_file, offset, size in all_chunks:
            if chunk_hash not in marked_chunks:
                # Remove from DB
                self.cursor.execute("DELETE FROM chunk_index WHERE chunk_hash = ?", (chunk_hash,))
                
                # Optional: log or remove from container (not implemented here)
                # print(f"Marked unused: {chunk_hash} in {container_file}")

        self.conn.commit()

    def run(self):
        marked = self.mark()
        self.sweep(marked)
        print(f"Garbage collection complete. {len(marked)} chunks retained.")

    def close(self):
        self.conn.close()
