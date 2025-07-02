import threading
import os
import io
import shutil
from storage import ChunkStorage
import json

class GarbageCollector(threading.Thread):
    def __init__(self, storage: ChunkStorage, chunk_dir: str, interval: int = 0):
        super().__init__(daemon=True)
        self.storage = storage
        self.chunk_dir = chunk_dir
        self.interval = interval
        self.lock = threading.Lock()
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def run(self):
        while not self._stop_event.is_set():
            if self.interval > 0:
                self._stop_event.wait(self.interval)
                self.collect_garbage()
            else:
                self._stop_event.wait()

    def trigger(self):
        self.collect_garbage()
        self._stop_event.clear()  # Wake thread if waiting manually

    def collect_garbage(self):
        with self.lock:
            #print("[GC] Starting garbage collection")

            # MARK: collect used chunks
            file_chunks = self.storage.get_all_file_chunks()
            used_chunks = set()
            for chunks in file_chunks.values():
                for chunk_hash, _ in chunks:
                    used_chunks.add(chunk_hash)

            # Map container files to chunks that need to be preserved
            container_map = {}  # container_file: list of (chunk_hash, offset, size)
            for chunk_hash in used_chunks:
                meta = self.storage.get_chunk_metadata(chunk_hash)
                if not meta:
                    continue  # skip broken entries
                container, offset, size = meta
                container_map.setdefault(container, []).append((chunk_hash, offset, size))

            # SWEEP: rewrite containers with only live chunks
            temp_metadata_file = os.path.join(self.chunk_dir, "temp_metadata.json")
            with open(temp_metadata_file, 'w') as temp_out:
                pass
            for container_file, chunks in container_map.items():
                container_path = os.path.join(self.chunk_dir, container_file)

                if not os.path.exists(container_path):
                    continue

                # Sort chunks by original offset
                chunks.sort(key=lambda x: x[1])

                with open(container_path, 'rb') as f:
                    new_container = io.BytesIO()
                    new_metadata = {}

                    for chunk_hash, offset, size in chunks:
                        f.seek(offset)
                        chunk_data = f.read(size)

                        new_offset = new_container.tell()
                        new_container.write(chunk_data)
                        new_metadata[chunk_hash] = (container_file, new_offset, size)

                # Write new container file (atomic replace)
                temp_path = container_path + ".tmp"
                with open(temp_path, 'wb') as out:
                    out.write(new_container.getvalue())
                shutil.move(temp_path, container_path)

                # Build updated metadata table
                with open(temp_metadata_file, 'w') as out:
                    out.write(json.dumps(new_metadata))
                
            # Replace old metadata with new metadata
            shutil.move(temp_metadata_file, os.path.join(self.chunk_dir, "chunk_metadata.json"))

            # Now find all containers, and remove any not used
            all_containers = set(os.listdir(self.chunk_dir))
            live_containers = set(container_map.keys())
            for filename in all_containers:
                if filename not in live_containers and filename.endswith(".container"):
                    path = os.path.join(self.chunk_dir, filename)
                    #print(f"[GC] Removing unused container: {filename}")
                    os.remove(path)

            #print("[GC] Completed garbage collection")
