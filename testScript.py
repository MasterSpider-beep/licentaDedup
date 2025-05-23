import os
import random
import time
import threading
import time


MOUNT_PATH = './mountPoint'
NUM_FILES = 10
NUM_THREADS = 1
MIN_SIZE = 1024 * 100      # 100 KB
MAX_SIZE = 1024 * 1024 * 4 # 4 MB
BLOCK_SIZE = 4096          # 4 KB
FRAGMENT_CHANCE = 0.05     # 5% chance to insert noise
FRAGMENT_MIN = 100
FRAGMENT_MAX = 500

# Shared counter with lock
total_bytes_written = 0
counter_lock = threading.Lock()

def random_block(size):
    return os.urandom(size)

def write_files(thread_id, start_index, num_files):
    global total_bytes_written

    for i in range(start_index, start_index + num_files):
        size = random.randint(MIN_SIZE, MAX_SIZE)
        filename = f"random_file_{i}.bin"
        filepath = os.path.join(MOUNT_PATH, filename)

        print(f"[Thread {thread_id}] Writing {filename} of size {size} bytes...")
        with open(filepath, 'wb') as f:
            written = 0
            while written < size:
                to_write = min(BLOCK_SIZE, size - written)
                f.write(random_block(to_write))
                written += to_write

                with counter_lock:
                    total_bytes_written += to_write

                if random.random() < FRAGMENT_CHANCE:
                    frag_size = random.randint(FRAGMENT_MIN, FRAGMENT_MAX)
                    f.write(random_block(frag_size))
                    written += frag_size
                    with counter_lock:
                        total_bytes_written += frag_size

        time.sleep(0.5)

def main():
    threads = []
    files_per_thread = NUM_FILES // NUM_THREADS
    remainder = NUM_FILES % NUM_THREADS
    start_index = 0

    start_time = time.time()
    for i in range(NUM_THREADS):
        count = files_per_thread + (1 if i < remainder else 0)
        t = threading.Thread(target=write_files, args=(i, start_index, count))
        threads.append(t)
        t.start()
        start_index += count

    for t in threads:
        t.join()

    end_time = time.time()
    duration = end_time - start_time
    
    total_mb = total_bytes_written / (1024 * 1024)
    print(f"\nFinished writing {NUM_FILES} files with {NUM_THREADS} threads.")
    print(f"Total data written (including noise): {total_mb:.2f} MB")
    print(f"Total elapsed time: {duration:.2f} seconds")
     
if __name__ == '__main__':
    main()
