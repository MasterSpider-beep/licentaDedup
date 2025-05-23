import os
import hashlib
from fusepy import FUSE, FuseOSError, Operations, fuse_get_context, errno
from collections import defaultdict
from chunker import Chunker
from storage import ChunkStorage
import io
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from garbageCollector import GarbageCollector

class FilesystemDedup(Operations):
    def __init__(self, root):
        self.root = root
        self.chunk_dir = os.path.join(root, ".dedup_store")
        self.chunker = Chunker()
        self.storage = ChunkStorage(self.chunk_dir)
        self.file_chunks = defaultdict(list, self.storage.get_all_file_chunks())
        self.executor = ThreadPoolExecutor(max_workers=8)
        self.garbage_collector = GarbageCollector(self.storage, self.chunk_dir, interval=120)
        self.garbage_collector.start()

        # Locks
        self.file_locks = defaultdict(threading.RLock)

    def _full_path(self, partial_path):
        return os.path.join(self.root, partial_path.lstrip('/'))

    def _hash_chunk(self, chunk: bytes) -> str:
        return hashlib.sha256(chunk).hexdigest()

    def getattr(self, path, fh=None):
        full_path = self._full_path(path)

        if path in self.file_chunks:
            now = int(time.time())
            return {
                'st_mode': 0o100644,
                'st_nlink': 1,
                'st_size': sum(size for _, size in self.file_chunks[path]),
                'st_ctime': now,
                'st_mtime': now,
                'st_atime': now,
                'st_uid': fuse_get_context()[0],
                'st_gid': fuse_get_context()[1],
            }

        try:
            st = os.lstat(full_path)
            return {key: getattr(st, key) for key in (
                'st_atime', 'st_ctime', 'st_gid', 'st_mode',
                'st_mtime', 'st_nlink', 'st_uid', 'st_size'
            )}
        except FileNotFoundError:
            raise FuseOSError(errno.ENOENT)

    def readdir(self, path, fh):
        full_path = self._full_path(path)
        entries = ['.', '..']
        if os.path.isdir(full_path):
            entries.extend(os.listdir(full_path))
        return entries

    def mkdir(self, path, mode):
        os.mkdir(self._full_path(path), mode)

    def rmdir(self, path):
        os.rmdir(self._full_path(path))

    def statfs(self, path):
        stv = os.statvfs(self._full_path(path))
        return {key: getattr(stv, key) for key in (
            'f_bavail', 'f_bfree', 'f_blocks', 'f_bsize',
            'f_favail', 'f_ffree', 'f_files', 'f_flag', 'f_frsize', 'f_namemax'
        )}

    def unlink(self, path):
        with self.file_locks[path]:
            if path in self.file_chunks:
                del self.file_chunks[path]
                self.storage.store_file_chunks(path, [])
            full_path = self._full_path(path)
            if os.path.exists(full_path):
                os.unlink(full_path)
        return 0

    def flush(self, path, fh):
        return 0

    def fsync(self, path, datasync, fh):
        return 0

    def create(self, path, mode):
        with self.file_locks[path]:
            full_path = self._full_path(path)
            fd = os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)
            self.file_chunks[path] = []
            self.storage.store_file_chunks(path, [])
            return fd

    def release(self, path, fh):
        os.close(fh)
        return 0

    def open(self, path, flags):
        full_path = self._full_path(path)
        if path not in self.file_chunks:
            if not os.path.exists(full_path):
                raise FuseOSError(errno.ENOENT)
        return os.open(full_path, flags)

    def write(self, path, data, offset, fh):
        with self.file_locks[path]:
            existing_chunks = self.file_chunks[path]
            new_chunks = []

            # Determine where to insert
            chunk_start = 0
            chunk_index = 0
            while chunk_index < len(existing_chunks) and chunk_start + existing_chunks[chunk_index][1] <= offset:
                chunk_start += existing_chunks[chunk_index][1]
                chunk_index += 1

            # Chunk the data and compute hashes in parallel
            chunk_metadata = {}
            container_buffer = io.BytesIO()
            futures = []

            start = 0
            while start < len(data):
                chunk_size = self.chunker.determine_chunk_size(data, start)
                chunk = data[start:start + chunk_size]
                futures.append((chunk, self.executor.submit(self._hash_chunk, chunk)))
                start += chunk_size

            current_offset = self.storage.get_container_size(
                path.strip("/").replace("/", "_") + ".container"
            )
            container_offset = current_offset

            for chunk, future in futures:
                chunk_hash = future.result()
                if not self.storage.chunk_exists(chunk_hash) and chunk_hash not in chunk_metadata:
                    container_buffer.write(chunk)
                    chunk_metadata[chunk_hash] = (path, current_offset, len(chunk))
                    current_offset += len(chunk)
                new_chunks.append((chunk_hash, len(chunk)))

            # Write chunk data and metadata
            if chunk_metadata:
                container_buffer.seek(0)
                self.storage.write_container(path, container_buffer.read(), container_offset)
                self.storage.write_chunk_metadata(chunk_metadata)

            # Update chunk list
            self.file_chunks[path] = (
                existing_chunks[:chunk_index] + new_chunks + existing_chunks[chunk_index + 1:]
            )
            self.storage.store_file_chunks(path, self.file_chunks[path])
            return len(data)

    def read(self, path, size, offset, fh):
        with self.file_locks[path]:
            if path not in self.file_chunks:
                raise FuseOSError(errno.ENOENT)

            chunks = self.file_chunks[path]
            data = bytearray()
            current_offset = 0
            remaining_size = size
            i = 0

            while i < len(chunks) and len(data) < size:
                chunk_hash, chunk_size = chunks[i]

                if current_offset + chunk_size <= offset:
                    current_offset += chunk_size
                    i += 1
                    continue

                # Start grouping chunks with the same container
                current_chunk_meta = self.storage.get_chunk_metadata(chunk_hash)
                if not current_chunk_meta:
                    raise FuseOSError(errno.EIO)

                container_file, base_offset, _ = current_chunk_meta
                # Group by container
                group_container = container_file
                first_offset = base_offset
                last_offset = 0
                while i < len(chunks):
                    chunk_hash, chunk_size = chunks[i]
                    meta = self.storage.get_chunk_metadata(chunk_hash)
                    if not meta:
                        raise FuseOSError(errno.EIO)
                    container, chunk_offset, chunk_len = meta

                    if container != group_container:
                        last_offset = chunk_offset + chunk_len
                        break
                    i += 1
                    if i >= len(chunks):
                        last_offset = chunk_offset + chunk_len

                # Read all grouped chunks from the container in a single read
                container_path = os.path.join(self.chunk_dir, group_container)
                with open(container_path, 'rb') as f:
                    f.seek(first_offset)
                    bulk_data = f.read(last_offset - first_offset)

                data.extend(bulk_data[0:min(len(bulk_data), remaining_size)])
                remaining_size -= len(bulk_data)

            return bytes(data)
    
    def __del__(self):
        self.executor.shutdown(wait=True)
        self.garbage_collector.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('mountpoint', type=str)
    parser.add_argument('rootpoint', type=str)
    args = parser.parse_args()

    if not os.path.exists(args.mountpoint):
        os.makedirs(args.mountpoint)

    FUSE(FilesystemDedup(args.rootpoint), args.mountpoint, foreground=True, allow_other=True)
