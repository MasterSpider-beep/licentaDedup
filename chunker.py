import threading

WINDOW_SIZE = 48
#POLYNOMIAL = 0xEDB88320
POLYNOMIAL = 0x3DA3358B4DC173
TARGET_MASK = 0xFFF #average 4 KB
MAX_CHUNK_SIZE = 1024 * 16 #16 KB
MIN_CHUNK_SIZE = 20 #1 KB

class Chunker:
    def __init__(self):
        self.local_storage = threading.local()
        self.table = self._precompute_table()

    def _precompute_table(self):
        table = [0] * 256
        for i in range(256):
            hash_val = i
            for _ in range(8):
                if hash_val & 1:
                    hash_val = (hash_val >> 1) ^ POLYNOMIAL
                else:
                    hash_val >>= 1
            table[i] = hash_val
        return table

    def _init_thread_local(self):
        if not hasattr(self.local_storage, "window"):
            self.local_storage.window = [0] * WINDOW_SIZE
            self.local_storage.hash = 0
            self.local_storage.window_pos = 0

    def _update_hash(self, byte):
        self._init_thread_local()
        dropped_byte = self.local_storage.window[self.local_storage.window_pos]
        self.local_storage.window[self.local_storage.window_pos] = byte
        self.local_storage.window_pos = (self.local_storage.window_pos + 1) % WINDOW_SIZE

        self.local_storage.hash = (
            (self.local_storage.hash << 8) & 0xFFFFFFFFFFFFFFFF
        ) ^ self.table[byte] ^ self.table[dropped_byte]

    def determine_chunk_size(self, data: bytes, start: int) -> int:
        self._init_thread_local()
        end = min(start + MAX_CHUNK_SIZE, len(data))
        window_fill = min(WINDOW_SIZE, len(data) - start)

        for i in range(window_fill):
            self._update_hash(data[start + i])

        i = start + max(window_fill, MIN_CHUNK_SIZE)
        while i < end:
            self._update_hash(data[i])
            if (self.local_storage.hash & TARGET_MASK) == 0:
                return i - start
            i += 1

        return end - start