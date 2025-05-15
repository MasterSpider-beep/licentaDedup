import os
import hashlib
from fusepy import FUSE, FuseOSError, Operations, errno
from collections import defaultdict
from chunker import Chunker
from storage import ChunkStorage

class FilesystemDedup(Operations):
    def __init__(self, root):
        self.root = root
        self.chunk_dir = os.path.join(root, ".dedup_store")
        self.chunker = Chunker()
        self.storage = ChunkStorage(self.chunk_dir)
        self.file_chunks = defaultdict(list)

        # Load metadata from DB
        self.file_chunks.update(self.storage.load_file_chunks())

    def _full_path(self, partial_path):
        return os.path.join(self.root, partial_path.lstrip('/'))

    def _hash_chunk(self, chunk: bytes) -> str:
        return hashlib.sha256(chunk).hexdigest()

    def getattr(self, path, fh=None):
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        attr = dict((key, getattr(st, key)) for key in (
            'st_atime', 'st_ctime', 'st_gid', 'st_mode',
            'st_mtime', 'st_nlink', 'st_uid'
        ))
        attr['st_size'] = sum(size for _, size in self.file_chunks.get(path, []))
        return attr

    def readdir(self, path, fh):
        full_path = self._full_path(path)
        yield '.'
        yield '..'
        if os.path.isdir(full_path):
            for item in os.listdir(full_path):
                yield item

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
        self.storage.flush()
        return 0

    def fsync(self, path, datasync, fh):
        self.storage.flush()
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

        # Break data into chunks
        start = 0
        while start < len(data):
            chunk_size = self.chunker.determine_chunk_size(data, start)
            chunk = data[start: start + chunk_size]
            chunk_hash = self._hash_chunk(chunk)

            if not self.storage.chunk_exists(chunk_hash):
                self.storage.write_chunk(path, chunk_hash, chunk)

            new_chunks.append((chunk_hash, chunk_size))
            start += chunk_size

        self.file_chunks[path] = file_chunks[:chunk_index] + new_chunks + file_chunks[chunk_index + 1:]
        self.storage.store_file_map(path, self.file_chunks[path])
        return len(data)

    def read(self, path, size, offset, fh):
        if path not in self.file_chunks:
            raise FuseOSError(errno.ENOENT)

        chunks = self.file_chunks[path]
        data = b''
        read_offset = 0
        buffer = b''

        i = 0
        while i < len(chunks):
            chunk_hash, chunk_size = chunks[i]

            if read_offset + chunk_size <= offset:
                read_offset += chunk_size
                i += 1
                continue

            # Group consecutive chunks from the same container
            group = []
            container_file = None
            group_start_offset = None
            group_total_size = 0

            # Collect chunks in the same container file
            j = i
            while j < len(chunks):
                chunk_hash_j, chunk_size_j = chunks[j]
                meta = self.storage.get_chunk_metadata(chunk_hash_j)
                if not meta:
                    raise FuseOSError(errno.EIO)

                file_j, offset_j, size_j = meta
                if container_file is None:
                    container_file = file_j
                    group_start_offset = offset_j
                elif file_j != container_file or offset_j != group_start_offset + group_total_size:
                    break  # Non-contiguous or different container

                group.append((chunk_hash_j, chunk_size_j, offset_j, size_j))
                group_total_size += size_j
                j += 1

            # Read the whole group in one go
            container_path = os.path.join(self.chunk_dir, container_file)
            with open(container_path, 'rb') as f:
                f.seek(group_start_offset)
                buffer = f.read(group_total_size)

            buf_offset = 0
            for k in range(len(group)):
                hash_k, logical_size, physical_offset, physical_size = group[k]
                chunk_data = buffer[buf_offset: buf_offset + physical_size]

                start = max(0, offset - read_offset)
                end = min(logical_size, start + size - len(data))

                data += chunk_data[start:end]

                if len(data) >= size:
                    return data

                read_offset += logical_size
                buf_offset += physical_size
                i += 1

        return data


# Run the FUSE filesystem
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('mountpoint', type=str)
    parser.add_argument('rootpoint', type=str)
    args = parser.parse_args()

    if not os.path.exists(args.mountpoint):
        os.makedirs(args.mountpoint)

    FUSE(FilesystemDedup(args.rootpoint), args.mountpoint, nothreads=True, foreground=True, allow_other=True)
