import os
import random
import time

# Configuration
MOUNT_PATH = './mountPoint'  # ← Set this to your mount point
NUM_FILES = 120
MIN_SIZE = 1024 * 100      # 100 KB
MAX_SIZE = 1024 * 1024 * 4 # 4 MB
BLOCK_SIZE = 4096          # 4 KB
FRAGMENT_CHANCE = 0.05     # 5% chance to insert noise
FRAGMENT_MIN = 100
FRAGMENT_MAX = 500

def random_block(size):
    return os.urandom(size)

def main():
    total_bytes_written = 0

    for i in range(NUM_FILES):
        size = random.randint(MIN_SIZE, MAX_SIZE)
        filename = f"random_file_{i}.bin"
        filepath = os.path.join(MOUNT_PATH, filename)

        print(f"Writing {filename} of size {size} bytes...")
        with open(filepath, 'wb') as f:
            written = 0
            while written < size:
                to_write = min(BLOCK_SIZE, size - written)
                f.write(random_block(to_write))
                written += to_write
                total_bytes_written += to_write

                # With 5% chance, write an extra fragment
                if random.random() < FRAGMENT_CHANCE:
                    frag_size = random.randint(FRAGMENT_MIN, FRAGMENT_MAX)
                    f.write(random_block(frag_size))
                    written += frag_size
                    total_bytes_written += frag_size

        time.sleep(0.5)

    total_mb = total_bytes_written / (1024 * 1024)
    print(f"\nFinished writing {NUM_FILES} files.")
    print(f"Total data written (including noise): {total_mb:.2f} MB")

if __name__ == '__main__':
    main()
