import sys
import struct
import os
import zlib
import json

# --- Configuration & Constants ---
DB_FILE_NAME = "mydb.db"
IDX_FILE_NAME = "email.idx"
WAL_FILE_NAME = "wal.log"

# INCREASED PAGE SIZE: 8KB allows internal nodes to hold ~1022 pointers.
PAGE_SIZE = 8192 
TABLE_MAX_PAGES = 5000

# Row Format: ID (4) + Username (32) + Email (255) = 291 bytes
ID_SIZE = 4
USERNAME_SIZE = 32
EMAIL_SIZE = 255
ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
ROW_FMT = f'I{USERNAME_SIZE}s{EMAIL_SIZE}s'

NODE_INTERNAL = 0
NODE_LEAF = 1
MAX_INT_KEY = 4294967295

# --- Global Helper Functions ---
def serialize_row(row_id, username, email):
    return struct.pack(ROW_FMT, row_id, username.encode('utf-8'), email.encode('utf-8'))

def deserialize_row(data):
    row_id, user_b, email_b = struct.unpack(ROW_FMT, data)
    return row_id, user_b.decode('utf-8').rstrip('\x00'), email_b.decode('utf-8').rstrip('\x00')

def hash_email(email):
    return zlib.crc32(email.encode('utf-8')) & 0xffffffff

# --- Write-Ahead Log ---
class WAL:
    def __init__(self, filename=WAL_FILE_NAME):
        self.filename = filename
        self.file = open(filename, "a+")
        self.txn_counter = 0

    def log_start(self, row_id, username, email):
        self.txn_counter += 1
        entry = {"txn_id": self.txn_counter, "status": "START", "action": "INSERT", "data": {"id": row_id, "user": username, "email": email}}
        self.file.write(json.dumps(entry) + "\n")
        self.file.flush()
        os.fsync(self.file.fileno())
        return self.txn_counter

    def log_commit(self, txn_id):
        entry = {"txn_id": txn_id, "status": "COMMIT"}
        self.file.write(json.dumps(entry) + "\n")
        self.file.flush()
        os.fsync(self.file.fileno())

    def recover(self, db, idx):
        self.file.seek(0)
        active_txns = {}
        for line in self.file:
            if not line.strip(): continue
            entry = json.loads(line)
            if entry["status"] == "START": active_txns[entry["txn_id"]] = entry["data"]
            elif entry["status"] == "COMMIT" and entry["txn_id"] in active_txns: del active_txns[entry["txn_id"]]
        
        if active_txns:
            print(f"CRASH DETECTED. Recovering {len(active_txns)} txns...")
            for txn_id, data in active_txns.items():
                db.insert(data['id'], serialize_row(data['id'], data['user'], data['email']))
                idx.insert(hash_email(data['email']), struct.pack('I', data['id']))
                self.log_commit(txn_id)

# --- Disk Pager ---
class Pager:
    def __init__(self, filename):
        self.filename = filename
        if not os.path.exists(filename): open(filename, "wb").close()
        self.file = open(filename, "r+b")
        self.file.seek(0, 2)
        self.file_length = self.file.tell()
        self.num_pages = self.file_length // PAGE_SIZE
        if self.file_length % PAGE_SIZE: self.num_pages += 1
        self.pages = [None] * TABLE_MAX_PAGES

    def get_page(self, page_num):
        if self.pages[page_num] is None:
            if page_num < self.num_pages:
                self.file.seek(page_num * PAGE_SIZE)
                data = self.file.read(PAGE_SIZE)
                self.pages[page_num] = bytearray(data.ljust(PAGE_SIZE, b'\x00'))
            else:
                self.pages[page_num] = bytearray(PAGE_SIZE)
                self.num_pages = max(self.num_pages, page_num + 1)
        return self.pages[page_num]

    def flush(self, page_num):
        if self.pages[page_num] is None: return
        self.file.seek(page_num * PAGE_SIZE)
        self.file.write(self.pages[page_num])

    def close(self):
        for i, page in enumerate(self.pages):
            if page: self.flush(i)
        self.file.close()

# --- The Advanced B-Tree Engine ---
class BTree:
    def __init__(self, filename, val_size):
        self.val_size = val_size
        self.key_size = 4
        self.leaf_cell_size = self.key_size + self.val_size
        self.internal_cell_size = 8
        
        self.OFF_TYPE, self.OFF_ROOT, self.OFF_PARENT, self.OFF_CELLS, self.OFF_NEXT = 0, 1, 2, 6, 10
        self.header_size = 14
        self.max_leaf_cells = (PAGE_SIZE - self.header_size) // self.leaf_cell_size
        self.max_internal_cells = (PAGE_SIZE - self.header_size) // self.internal_cell_size
        
        self.pager = Pager(filename)
        if self.pager.file_length == 0:
            self._init_leaf(self.pager.get_page(0), is_root=True)

    def _init_leaf(self, node, is_root=False):
        struct.pack_into('BBII', node, 0, NODE_LEAF, 1 if is_root else 0, 0, 0)
        struct.pack_into('I', node, self.OFF_NEXT, 0)

    def _init_internal(self, node, is_root=False):
        struct.pack_into('BBII', node, 0, NODE_INTERNAL, 1 if is_root else 0, 0, 0)

    # --- List-Based Memory Parsers ---
    def _read_leaf(self, node):
        cells = []
        for i in range(struct.unpack_from('I', node, self.OFF_CELLS)[0]):
            off = self.header_size + i * self.leaf_cell_size
            cells.append((struct.unpack_from('I', node, off)[0], node[off+4:off+self.leaf_cell_size]))
        return cells

    def _write_leaf(self, node, cells):
        struct.pack_into('I', node, self.OFF_CELLS, len(cells))
        for i, (k, v) in enumerate(cells):
            off = self.header_size + i * self.leaf_cell_size
            struct.pack_into('I', node, off, k)
            node[off+4:off+self.leaf_cell_size] = v

    def _read_internal(self, node):
        cells = []
        for i in range(struct.unpack_from('I', node, self.OFF_CELLS)[0]):
            off = self.header_size + i * self.internal_cell_size
            cells.append({'key': struct.unpack_from('I', node, off)[0], 'ptr': struct.unpack_from('I', node, off+4)[0]})
        return cells

    def _write_internal(self, node, cells):
        struct.pack_into('I', node, self.OFF_CELLS, len(cells))
        for i, c in enumerate(cells):
            off = self.header_size + i * self.internal_cell_size
            struct.pack_into('I', node, off, c['key'])
            struct.pack_into('I', node, off+4, c['ptr'])

    # --- Core Logic ---
    def find_leaf_page(self, key):
        page_num = 0
        node = self.pager.get_page(page_num)
        while struct.unpack_from('B', node, self.OFF_TYPE)[0] == NODE_INTERNAL:
            cells = self._read_internal(node)
            page_num = next((c['ptr'] for c in cells if key <= c['key']), cells[-1]['ptr'])
            node = self.pager.get_page(page_num)
        return page_num

    def search(self, key):
        node = self.pager.get_page(self.find_leaf_page(key))
        return next((v for k, v in self._read_leaf(node) if k == key), None)

    def insert(self, key, val_bytes):
        page_num = self.find_leaf_page(key)
        node = self.pager.get_page(page_num)
        cells = self._read_leaf(node)
        
        cells.append((key, val_bytes))
        cells.sort(key=lambda x: x[0])
        
        if len(cells) <= self.max_leaf_cells:
            self._write_leaf(node, cells)
            return

        # SPLIT LEAF LOGIC
        split_idx = len(cells) // 2
        self._write_leaf(node, cells[:split_idx])
        
        new_page_num = self.pager.num_pages
        new_node = self.pager.get_page(new_page_num)
        self._init_leaf(new_node)
        self._write_leaf(new_node, cells[split_idx:])
        
        # Pointers
        struct.pack_into('I', new_node, self.OFF_NEXT, struct.unpack_from('I', node, self.OFF_NEXT)[0])
        struct.pack_into('I', node, self.OFF_NEXT, new_page_num)
        left_max_key = cells[:split_idx][-1][0]
        
        # PARENT ROUTING
        if struct.unpack_from('B', node, self.OFF_ROOT)[0]:
            left_page_num = self.pager.num_pages
            left_node = self.pager.get_page(left_page_num)
            left_node[:] = node[:]
            struct.pack_into('B', left_node, self.OFF_ROOT, 0)
            
            self._init_internal(node, is_root=True)
            self._write_internal(node, [{'key': left_max_key, 'ptr': left_page_num}, {'key': MAX_INT_KEY, 'ptr': new_page_num}])
            struct.pack_into('I', left_node, self.OFF_PARENT, 0)
            struct.pack_into('I', new_node, self.OFF_PARENT, 0)
        else:
            parent_page = struct.unpack_from('I', node, self.OFF_PARENT)[0]
            struct.pack_into('I', new_node, self.OFF_PARENT, parent_page)
            self._insert_internal(parent_page, left_max_key, page_num, new_page_num)

    def _insert_internal(self, page_num, left_max_key, left_child, right_child):
        node = self.pager.get_page(page_num)
        cells = self._read_internal(node)
        
        for i, c in enumerate(cells):
            if c['ptr'] == left_child:
                old_key = c['key']
                cells[i]['key'] = left_max_key
                cells.insert(i + 1, {'key': old_key, 'ptr': right_child})
                break
                
        if len(cells) <= self.max_internal_cells:
            self._write_internal(node, cells)
        else:
            print("FATAL: Internal Node limit reached. Increase PAGE_SIZE further.")
            sys.exit(1)

    def close(self):
        self.pager.close()

# --- Database Interface ---
def execute_insert(command, db, idx, wal):
    parts = command.strip().split()
    if len(parts) != 4: return print("Error: Syntax 'insert <id> <user> <email>'")
    try: row_id = int(parts[1])
    except: return print("Error: ID must be int")
    
    txn_id = wal.log_start(row_id, parts[2], parts[3])
    db.insert(row_id, serialize_row(row_id, parts[2], parts[3]))
    idx.insert(hash_email(parts[3]), struct.pack('I', row_id))
    wal.log_commit(txn_id)
    print("Executed.")

def execute_where(command, db, idx):
    parts = command.strip().split('=')
    if len(parts) != 2: return print("Error: Syntax 'where email=<email>'")
    id_bytes = idx.search(hash_email(parts[1].strip()))
    if not id_bytes: return print("Not found.")
    row_bytes = db.search(struct.unpack('I', id_bytes)[0])
    print(f"Result: {deserialize_row(row_bytes)}" if row_bytes else "Corruption detected.")

def main():
    db, idx, wal = BTree(DB_FILE_NAME, ROW_SIZE), BTree(IDX_FILE_NAME, 4), WAL()
    wal.recover(db, idx)
    try:
        while True:
            print("db > ", end="", flush=True)
            try: cmd = input().strip()
            except EOFError: break
            if not cmd: continue
            if cmd == ".exit": break
            if cmd.startswith("insert"): execute_insert(cmd, db, idx, wal)
            elif cmd.startswith("where"): execute_where(cmd, db, idx)
    finally:
        db.close(); idx.close()

if __name__ == "__main__":
    main()