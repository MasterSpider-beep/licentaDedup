import os
import hashlib
from fusepy import FUSE, FuseOSError, Operations, errno
from collections import defaultdict
from chunker import Chunker
from storage import ChunkStorage
import io
import time
from concurrent.futures import ThreadPoolExecutor

class FilesystemDedup(Operations):
    def __init__(self, root):
        self.root = root
        self.chunk_dir = os.path.join(root, ".dedup_store")
        self.chunker = Chunker()
        self.storage = ChunkStorage(self.chunk_dir)
        self.file_chunks = defaultdict(list)
        self.executor = ThreadPoolExecutor(max_workers=8)

        # Load metadata from DB
        self.file_chunks.update(self.storage.load_file_chunks())

    def write_container(self, name, data: bytes):
        path = os.path.join(self.chunk_dir, name)
        with open(path, 'wb') as f:
            f.write(data)

    def _full_path(self, partial_path):
        return os.path.join(self.root, partial_path.lstrip('/'))

    def _hash_chunk(self, chunk: bytes) -> str:
        return hashlib.sha256(chunk).hexdigest()

    def getattr(self, path, fh=None):
        full_path = self._full_path(path)

        # If virtual file
        if path in self.file_chunks:
            now = int(time.time())
            return {
                'st_mode': (0o100644),
                'st_nlink': 1,
                'st_size': sum(size for _, size in self.file_chunks[path]),
                'st_ctime': now,
                'st_mtime': now,
                'st_atime': now,
                'st_uid': os.getuid(),
                'st_gid': os.getgid(),
            }

        # Fall back to actual filesystem
        try:
            st = os.lstat(full_path)
            attr = dict((key, getattr(st, key)) for key in (
                'st_atime', 'st_ctime', 'st_gid', 'st_mode',
                'st_mtime', 'st_nlink', 'st_uid', 'st_size'
            ))
            return attr
        except FileNotFoundError:
            raise FuseOSError(errno.ENOENT)

    def readdir(self, path, fh):
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

    def mkdir(self, path, mode):
        os.mkdir(self._full_path(path), mode)

    def rmdir(self, path):
        os.rmdir(self._full_path(path))

    def statfs(self, path):
        stv = os.statvfs(self._full_path(path))
        return dict((key, getattr(stv, key)) for key in (
            'f_bavail', 'f_bfree', 'f_blocks', 'f_bsize', 'f_favail',
            'f_ffree', 'f_files', 'f_flag', 'f_frsize', 'f_namemax'
        ))

    def unlink(self, path):
        if path in self.file_chunks:
            del self.file_chunks[path]
        full_path = self._full_path(path)
        if os.path.exists(full_path):
            os.unlink(full_path)
        return 0

    def flush(self, path, fh):
        return 0

    def fsync(self, path, datasync, fh):
        self.flush()
        return 0

    def create(self, path, mode):
        full_path = self._full_path(path)
        fd = os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)
        self.file_chunks[path] = []
        return fd

    def release(self, path, fh):
        os.close(fh)
        return 0

    def open(self, path, flags):
        if path in self.file_chunks:
            return 0
        full_path = self._full_path(path)
        if not os.path.exists(full_path):
            raise FuseOSError(errno.ENOENT)
        return os.open(full_path, flags)

    def write(self, path, data, offset, fh):
        file_chunks = self.file_chunks[path]
        new_chunks = []

        # Locate the affected chunk index
        chunk_start = 0
        chunk_index = 0
        while chunk_index < len(file_chunks) and chunk_start + file_chunks[chunk_index][1] <= offset:
            chunk_start += file_chunks[chunk_index][1]
            chunk_index += 1

        container_buffer = io.BytesIO()
        current_offset = 0
        disk_container_offset = 0
        chunk_metadata = {}

        # Break data into chunks and process in parallel
        start = 0
        futures = []
        while start < len(data):
            chunk_size = self.chunker.determine_chunk_size(data, start)
            chunk = data[start: start + chunk_size]
            futures.append(self.executor.submit(self._hash_chunk, chunk))
            start += chunk_size

        # Process results
        for future in futures:
            chunk_hash = future.result()
            chunk = data[start - chunk_size: start]
            if not self.storage.chunk_exists(chunk_hash) and chunk_hash not in chunk_metadata:
                container_buffer.write(chunk)
                chunk_metadata[chunk_hash] = (path, current_offset, len(chunk))
                current_offset += len(chunk)

            new_chunks.append((chunk_hash, len(chunk)))

        if chunk_metadata:
            container_buffer.seek(0)
            self.storage.write_container(path, container_buffer.read(), disk_container_offset)

            # Save chunk metadata on disk
            self.storage.write_chunk_metadata(chunk_metadata)

        self.file_chunks[path] = file_chunks[:chunk_index] + new_chunks + file_chunks[chunk_index + 1:]
        self.storage.store_file_chunks(path, self.file_chunks[path])
        return len(data)

    def read(self, path, size, offset, fh):
        if path not in self.file_chunks:
            raise FuseOSError(errno.ENOENT)

        chunks = self.file_chunks[path]
        data = bytearray()
        current_offset = 0
        i = 0

        while i < len(chunks) and len(data) < size:
            chunk_hash, chunk_size = chunks[i]

            if current_offset + chunk_size <= offset:
                # Skip this chunk, it's before the requested offset
                current_offset += chunk_size
                i += 1
                continue

            meta = self.storage.get_chunk_metadata(chunk_hash)
            if not meta:
                raise FuseOSError(errno.EIO)

            container_file, chunk_offset, chunk_len = meta
            container_path = os.path.join(self.chunk_dir, container_file)
            with open(container_path, 'rb') as f:
                f.seek(chunk_offset)
                chunk_data = f.read(chunk_len)

            # Calculate how much to read from this chunk
            chunk_read_start = max(0, offset - current_offset)
            chunk_read_end = min(chunk_size, chunk_read_start + size - len(data))
            data.extend(chunk_data[chunk_read_start:chunk_read_end])

            current_offset += chunk_size
            i += 1
        return bytes(data)

# Run the FUSE filesystem
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('mountpoint', type=str)
    parser.add_argument('rootpoint', type=str)
    args = parser.parse_args()

    if not os.path.exists(args.mountpoint):
        os.makedirs(args.mountpoint)

    FUSE(FilesystemDedup(args.rootpoint), args.mountpoint, foreground=True, allow_other=True)
