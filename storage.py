import os
import json
import threading
import queue
import shutil

class ChunkStorage:
    def __init__(self, chunk_dir):
        self.chunk_dir = chunk_dir
        os.makedirs(self.chunk_dir, exist_ok=True)

        self.file_chunks_path = os.path.join(self.chunk_dir, "file_chunks.json")
        self.chunk_metadata_path = os.path.join(self.chunk_dir, "chunk_metadata.json")

        # Metadata caches
        self.file_chunks = {}
        self.chunk_metadata = {}

        # Locks
        self.chunk_metadata_lock = threading.RLock()
        self.file_chunks_lock = threading.RLock()
        self.container_locks = {}

        # Background I/O queue
        self.io_queue = queue.Queue()
        self.io_thread = threading.Thread(target=self._background_writer, daemon=True)
        self.io_thread.start()

        # Load existing state
        self._load_file_chunks()
        self._load_chunk_metadata()

    def store_file_chunks(self, path, chunks):
        with self.file_chunks_lock:
            self.file_chunks[path] = chunks
            self._schedule_metadata_dump('file_chunks')

    def write_chunk_metadata(self, new_metadata: dict):
        transformed_metadata = {}
        for chunk_hash, (path, offset, size) in new_metadata.items():
            container_name = path.strip("/").replace("/", "_") + ".container"
            transformed_metadata[chunk_hash] = (container_name, offset, size)

        with self.chunk_metadata_lock:
            self.chunk_metadata.update(transformed_metadata)
            self._schedule_metadata_dump('chunk_metadata')

    def write_container(self, path, data, offset):
        container_name = path.strip("/").replace("/", "_") + ".container"
        container_path = os.path.join(self.chunk_dir, container_name)

        lock = self._get_container_lock(container_name)
        with lock:
            mode = 'r+b' if os.path.exists(container_path) else 'wb'
            with open(container_path, mode) as f:
                f.seek(offset)
                f.write(data)

    def get_chunk_metadata(self, chunk_hash):
        with self.chunk_metadata_lock:
            return self.chunk_metadata.get(chunk_hash)

    def chunk_exists(self, chunk_hash):
        with self.chunk_metadata_lock:
            return chunk_hash in self.chunk_metadata

    def get_container_size(self, container_path):
        full_path = os.path.join(self.chunk_dir, container_path)
        return os.path.getsize(full_path) if os.path.exists(full_path) else 0

    def get_all_file_chunks(self):
        with self.file_chunks_lock:
            return dict(self.file_chunks)

    #Internal Operations

    def _load_file_chunks(self):
        if os.path.exists(self.file_chunks_path):
            try:
                with open(self.file_chunks_path, 'r') as f:
                    self.file_chunks = json.load(f)
            except (json.JSONDecodeError, IOError):
                self.file_chunks = {}

    def _load_chunk_metadata(self):
        if os.path.exists(self.chunk_metadata_path):
            try:
                with open(self.chunk_metadata_path, 'r') as f:
                    self.chunk_metadata = json.load(f)
            except (json.JSONDecodeError, IOError):
                self.chunk_metadata = {}

    def _schedule_metadata_dump(self, target):
        self.io_queue.put(target)

    def _atomic_write(self, path, data):
        tmp_path = path + ".tmp"
        with open(tmp_path, 'w') as f:
            json.dump(data, f)
        os.replace(tmp_path, path)

    def _background_writer(self):
        while True:
            try:
                target = self.io_queue.get()
                if target == 'file_chunks':
                    with self.file_chunks_lock:
                        self._atomic_write(self.file_chunks_path, self.file_chunks)
                elif target == 'chunk_metadata':
                    with self.chunk_metadata_lock:
                        self._atomic_write(self.chunk_metadata_path, self.chunk_metadata)
            except Exception as e:
                #Posibble log
                print(f"[ChunkStorage] Error in background writer: {e}")

    def _get_container_lock(self, name):
        if name not in self.container_locks:
            self.container_locks[name] = threading.Lock()
        return self.container_locks[name]
