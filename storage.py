import os
import json
from threading import Lock

class ChunkStorage:
    def __init__(self, chunk_dir):
        self.chunk_dir = chunk_dir
        os.makedirs(self.chunk_dir, exist_ok=True)

        self.file_chunks_path = os.path.join(self.chunk_dir, "file_chunks.json")
        self.chunk_metadata_path = os.path.join(self.chunk_dir, "chunk_metadata.json")

        self.chunk_metadata = {}
        self.lock = Lock()

        # Load on init
        self.load_chunk_metadata()

    def store_file_chunks(self, path, chunks):
        with self.lock:
            file_map = self.load_file_chunks()
            file_map[path] = chunks
            with open(self.file_chunks_path, 'w') as f:
                json.dump(file_map, f)

    def write_chunk_metadata(self, new_metadata: dict):
        with self.lock:
            transformed_metadata = {}

            for chunk_hash, (path, offset, size) in new_metadata.items():
                container_name = path.strip("/").replace("/", "_") + ".container"
                transformed_metadata[chunk_hash] = (container_name, offset, size)

            self.chunk_metadata.update(transformed_metadata)

            with open(self.chunk_metadata_path, 'w') as f:
                json.dump(self.chunk_metadata, f)

        self.load_chunk_metadata()

    def write_container(self, path, data, offset):
        path = path.strip("/").replace("/", "_") + ".container"
        container_path = os.path.join(self.chunk_dir, path)
        with self.lock:
            mode = 'r+b' if os.path.exists(container_path) else 'wb'
            with open(container_path, mode) as f:
                f.seek(offset)
                f.write(data)

    def get_chunk_metadata(self, chunk_hash):
        print(self.chunk_metadata.get(chunk_hash))
        return self.chunk_metadata.get(chunk_hash)

    def load_file_chunks(self):
        if os.path.exists(self.file_chunks_path):
            with open(self.file_chunks_path, 'r') as f:
                return json.load(f)
        return {}

    def load_chunk_metadata(self):
        if os.path.exists(self.chunk_metadata_path):
            with open(self.chunk_metadata_path, 'r') as f:
                self.chunk_metadata = json.load(f)
        else:
            self.chunk_metadata = {}

    def chunk_exists(self, chunk_hash):
        return chunk_hash in self.chunk_metadata

    def get_container_size(self, container_path):
        if os.path.exists(container_path):
            return os.path.getsize(container_path)
        return 0
