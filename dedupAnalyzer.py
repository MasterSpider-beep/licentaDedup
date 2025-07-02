import os
import hashlib
import statistics
from collections import defaultdict
from chunker import Chunker

class DedupAnalyzer:
    def __init__(self, directory):
        self.directory = directory
        self.chunker = Chunker()
        self.chunk_hashes = defaultdict(int)
        self.chunk_sizes = []
        self.total_chunks = 0

    def _hash_chunk(self, chunk: bytes) -> str:
        return hashlib.sha256(chunk).hexdigest()

    def analyze_file(self, file_path):
        with open(file_path, 'rb') as f:
            data = f.read()
        start = 0
        while start < len(data):
            chunk_size = self.chunker.determine_chunk_size(data, start)
            chunk = data[start:start + chunk_size]
            chunk_hash = self._hash_chunk(chunk)

            self.chunk_hashes[chunk_hash] += 1
            self.chunk_sizes.append(len(chunk))
            self.total_chunks += 1

            start += chunk_size

    def analyze_directory(self):
        for root, _, files in os.walk(self.directory):
            for filename in files:
                filepath = os.path.join(root, filename)
                try:
                    self.analyze_file(filepath)
                except Exception as e:
                    print(f"Skipping {filepath}: {e}")

    def report(self):
        total_unique = len(self.chunk_hashes)
        redundant_chunks = self.total_chunks - total_unique
        redundancy_ratio = redundant_chunks / self.total_chunks if self.total_chunks > 0 else 0

        return {
            "Total Chunks": self.total_chunks,
            "Unique Chunks": total_unique,
            "Redundant Chunks": redundant_chunks,
            "Redundancy Ratio": round(redundancy_ratio * 100, 2),
            "Average Chunk Size": int(statistics.mean(self.chunk_sizes)) if self.chunk_sizes else 0,
            "Min Chunk Size": min(self.chunk_sizes) if self.chunk_sizes else 0,
            "Max Chunk Size": max(self.chunk_sizes) if self.chunk_sizes else 0,
            "Median Chunk Size": int(statistics.median(self.chunk_sizes)) if self.chunk_sizes else 0,
        }

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python dedup_analyzer.py <directory>")
        sys.exit(1)

    folder = sys.argv[1]
    analyzer = DedupAnalyzer(folder)
    analyzer.analyze_directory()
    stats = analyzer.report()

    print("\nDeduplication Suitability Report:")
    for k, v in stats.items():
        print(f"{k}: {v}")
