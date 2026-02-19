import os
import time
import random
import string
import struct
from main import BTree, WAL, serialize_row, hash_email, DB_FILE_NAME, IDX_FILE_NAME, WAL_FILE_NAME, ROW_SIZE

# --- Configuration ---
NUM_INSERTS = 10000
NUM_READS = 1000

def generate_random_string(length):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def clean_database():
    """Destroys old database files to ensure a clean benchmark run."""
    for file in [DB_FILE_NAME, IDX_FILE_NAME, WAL_FILE_NAME]:
        if os.path.exists(file):
            os.remove(file)
    print("Cleaned old database files.")

def run_benchmark():
    clean_database()
    
    print(f"\n--- INITIALIZING ENGINE ---")
    db = BTree(DB_FILE_NAME, val_size=ROW_SIZE)
    idx = BTree(IDX_FILE_NAME, val_size=4)
    wal = WAL(WAL_FILE_NAME)
    
    # Pre-generate data to avoid measuring Python string generation time
    print(f"Pre-generating {NUM_INSERTS} synthetic records...")
    records = []
    for i in range(NUM_INSERTS):
        user = generate_random_string(10)
        email = f"{user}@benchmark.com"
        records.append((i, user, email))
        
    print("\n--- PHASE 1: WRITE THROUGHPUT ---")
    print(f"Inserting {NUM_INSERTS} rows (ACID Transactions enabled)...")
    
    start_time = time.time()
    
    for row_id, username, email in records:
        # 1. WAL Start
        txn_id = wal.log_start(row_id, username, email)
        
        # 2. Engine Operations
        row_bytes = serialize_row(row_id, username, email)
        email_hash = hash_email(email)
        db.insert(row_id, row_bytes)
        idx.insert(email_hash, struct.pack('I', row_id))
        
        # 3. WAL Commit
        wal.log_commit(txn_id)
        
    end_time = time.time()
    write_duration = end_time - start_time
    write_ops = NUM_INSERTS / write_duration
    
    print(f"Write Time:  {write_duration:.2f} seconds")
    print(f"Throughput:  {write_ops:.2f} Operations / Second")
    
    print("\n--- PHASE 2: READ LATENCY (SECONDARY INDEX) ---")
    # Pick random emails to search for
    search_targets = random.sample(records, NUM_READS)
    print(f"Querying {NUM_READS} random emails...")
    
    read_start_time = time.time()
    
    found_count = 0
    for _, _, target_email in search_targets:
        target_hash = hash_email(target_email)
        
        # O(log N) lookup in index
        id_bytes = idx.search(target_hash)
        if id_bytes:
            row_id = struct.unpack('I', id_bytes)[0]
            # O(log N) lookup in primary
            row_bytes = db.search(row_id)
            if row_bytes:
                found_count += 1

    read_end_time = time.time()
    read_duration = read_end_time - read_start_time
    avg_read_latency_ms = (read_duration / NUM_READS) * 1000
    
    print(f"Found:       {found_count}/{NUM_READS} records")
    print(f"Read Time:   {read_duration:.4f} seconds")
    print(f"Avg Latency: {avg_read_latency_ms:.4f} ms per query")

    print("\n--- PHASE 3: DISK FOOTPRINT ---")
    db.close()
    idx.close()
    
    db_size_mb = os.path.getsize(DB_FILE_NAME) / (1024 * 1024)
    idx_size_mb = os.path.getsize(IDX_FILE_NAME) / (1024 * 1024)
    wal_size_mb = os.path.getsize(WAL_FILE_NAME) / (1024 * 1024)
    
    print(f"Primary DB:  {db_size_mb:.2f} MB")
    print(f"Index DB:    {idx_size_mb:.2f} MB")
    print(f"WAL Log:     {wal_size_mb:.2f} MB")
    print(f"Total Size:  {(db_size_mb + idx_size_mb + wal_size_mb):.2f} MB")
    print("--------------------------------------------------\n")

if __name__ == "__main__":
    run_benchmark()