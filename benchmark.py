import os
import random
import string
import tempfile
import time

from pydb import Database
from pydb.db import DB_FILE_NAME, IDX_FILE_NAME, WAL_FILE_NAME

NUM_INSERTS = 10000
NUM_READS = 1000


def random_string(length):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def size_mb(directory, name):
    return os.path.getsize(os.path.join(directory, name)) / (1024 * 1024)


def run_benchmark():
    directory = tempfile.mkdtemp(prefix="pydb-bench-")
    db = Database(directory)

    # Pre-generate data so string generation is not part of the measurement
    records = []
    for i in range(NUM_INSERTS):
        user = random_string(10)
        records.append((i, user, f"{user}@benchmark.com"))

    print("--- PHASE 1: WRITE THROUGHPUT ---")
    print(f"Inserting {NUM_INSERTS} rows, one fsync'd transaction each...")
    start = time.time()
    for row_id, username, email in records:
        db.insert(row_id, username, email)
    write_duration = time.time() - start
    print(f"Write Time:  {write_duration:.2f} seconds")
    print(f"Throughput:  {NUM_INSERTS / write_duration:.2f} operations / second")

    print("\n--- PHASE 2: READ LATENCY (SECONDARY INDEX) ---")
    targets = random.sample(records, NUM_READS)
    start = time.time()
    found = sum(db.find_by_email(email) is not None for _, _, email in targets)
    read_duration = time.time() - start
    print(f"Found:       {found}/{NUM_READS} records")
    print(f"Avg Latency: {read_duration / NUM_READS * 1000:.4f} ms per query")

    print("\n--- PHASE 3: CHECKPOINT AND DISK FOOTPRINT ---")
    wal_mb = size_mb(directory, WAL_FILE_NAME)
    start = time.time()
    db.close()
    print(f"Checkpoint:  {(time.time() - start) * 1000:.0f} ms")
    db_mb, idx_mb = size_mb(directory, DB_FILE_NAME), size_mb(directory, IDX_FILE_NAME)
    print(f"Primary DB:  {db_mb:.2f} MB")
    print(f"Index DB:    {idx_mb:.2f} MB")
    print(f"WAL (before checkpoint): {wal_mb:.2f} MB, truncated to 0 after")

    print("\n--- PHASE 4: RECOVERY ---")
    crashed = Database(tempfile.mkdtemp(prefix="pydb-bench-"))
    for row_id, username, email in records:
        crashed.insert(row_id, username, email)
    crashed_dir = os.path.dirname(crashed.wal.filename)
    start = time.time()
    recovered = Database(crashed_dir)  # reopened without a checkpoint: redo the whole WAL
    print(f"Replayed {recovered.recovered} transactions in {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    run_benchmark()
