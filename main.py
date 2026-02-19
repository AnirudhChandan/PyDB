import sys
import struct
import os

# --- Constants & Configuration ---
DB_FILE_NAME = "mydb.db"
PAGE_SIZE = 4096
TABLE_MAX_PAGES = 100

# Row Format
ID_SIZE = 4
USERNAME_SIZE = 32
EMAIL_SIZE = 255
ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
ROW_FMT = f'I{USERNAME_SIZE}s{EMAIL_SIZE}s'

# Common Node Header Layout
NODE_TYPE_SIZE = 1
NODE_ROOT_SIZE = 1
NODE_PARENT_SIZE = 4
NODE_NUM_CELLS_SIZE = 4
NODE_NEXT_LEAF_SIZE = 4 
COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + NODE_ROOT_SIZE + NODE_PARENT_SIZE + NODE_NUM_CELLS_SIZE + NODE_NEXT_LEAF_SIZE

# Leaf Node Layout
LEAF_NODE_KEY_SIZE = 4
LEAF_NODE_VALUE_SIZE = ROW_SIZE
LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - COMMON_NODE_HEADER_SIZE
LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS // LEAF_NODE_CELL_SIZE

# Internal Node Layout
# Internal Cell: [Key (4 bytes), Child_Page_Num (4 bytes)]
INTERNAL_NODE_KEY_SIZE = 4
INTERNAL_NODE_CHILD_SIZE = 4
INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_KEY_SIZE + INTERNAL_NODE_CHILD_SIZE
INTERNAL_NODE_SPACE_FOR_CELLS = PAGE_SIZE - COMMON_NODE_HEADER_SIZE
INTERNAL_NODE_MAX_CELLS = INTERNAL_NODE_SPACE_FOR_CELLS // INTERNAL_NODE_CELL_SIZE

# Enums
NODE_INTERNAL = 0
NODE_LEAF = 1

# Maximum Integer (Sentinel for the rightmost child)
MAX_INT_KEY = 4294967295 # 2^32 - 1

# --- Classes ---

class Pager:
    def __init__(self, filename):
        self.filename = filename
        if not os.path.exists(filename):
            with open(filename, "wb") as f:
                pass
        self.file = open(filename, "r+b")
        self.file.seek(0, 2)
        self.file_length = self.file.tell()
        self.num_pages = self.file_length // PAGE_SIZE
        self.pages = [None] * TABLE_MAX_PAGES

    def get_page(self, page_num):
        if page_num >= TABLE_MAX_PAGES:
            raise Exception(f"Page number {page_num} out of bounds.")
        
        if self.pages[page_num] is None:
            num_pages_in_file = self.file_length // PAGE_SIZE
            if self.file_length % PAGE_SIZE:
                 num_pages_in_file += 1
            
            if page_num < num_pages_in_file:
                self.file.seek(page_num * PAGE_SIZE)
                data = self.file.read(PAGE_SIZE)
                self.pages[page_num] = bytearray(data)
            else:
                self.pages[page_num] = bytearray(PAGE_SIZE)
                
        return self.pages[page_num]

    def get_unused_page_num(self):
        return self.num_pages

    def flush(self, page_num):
        if self.pages[page_num] is None:
            return
        self.file.seek(page_num * PAGE_SIZE)
        self.file.write(self.pages[page_num])

    def close(self):
        for i, page in enumerate(self.pages):
            if page:
                self.flush(i)
        self.file.close()

class Table:
    def __init__(self):
        self.pager = Pager(DB_FILE_NAME)
        
        if self.pager.num_pages == 0:
            # New DB: Initialize Page 0 as Leaf Root
            root_node = self.pager.get_page(0)
            initialize_leaf_node(root_node)
            set_node_root(root_node, True)
            self.pager.num_pages = 1

    def close(self):
        self.pager.close()

class Cursor:
    def __init__(self, table):
        self.table = table
        self.page_num = 0
        self.cell_num = 0
        self.end_of_table = False 

    def start_reading(self):
        # Find the Left-most leaf to start scanning
        page_num = 0
        node = self.table.pager.get_page(page_num)
        node_type = get_node_type(node)

        while node_type == NODE_INTERNAL:
            # Always follow the left-most pointer (Cell 0's child)
            # Internal Cell structure: [Key, Child_Page]
            # Actually, standard B-tree: Ptr0, Key0, Ptr1... 
            # Our Simplified Model: (MaxKey, ChildPtr).
            # So just grab ChildPtr from Cell 0.
            
            offset = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_KEY_SIZE
            child_page = struct.unpack_from('I', node, offset)[0]
            
            page_num = child_page
            node = self.table.pager.get_page(page_num)
            node_type = get_node_type(node)

        self.page_num = page_num
        self.cell_num = 0
        
        num_cells = get_node_num_cells(node)
        self.end_of_table = (num_cells == 0)
        return self

    def advance(self):
        node = self.table.pager.get_page(self.page_num)
        num_cells = get_node_num_cells(node)
        
        self.cell_num += 1
        if self.cell_num >= num_cells:
            next_page = get_node_next_leaf(node)
            if next_page == 0:
                self.end_of_table = True
            else:
                self.page_num = next_page
                self.cell_num = 0

# --- Node Helper Functions ---

def get_node_type(node):
    return struct.unpack_from('B', node, 0)[0]

def set_node_type(node, type_val):
    struct.pack_into('B', node, 0, type_val)

def set_node_root(node, is_root):
    val = 1 if is_root else 0
    struct.pack_into('B', node, NODE_TYPE_SIZE, val)

def get_node_num_cells(node):
    return struct.unpack_from('I', node, NODE_TYPE_SIZE + NODE_ROOT_SIZE + NODE_PARENT_SIZE)[0]

def set_node_num_cells(node, num_cells):
    struct.pack_into('I', node, NODE_TYPE_SIZE + NODE_ROOT_SIZE + NODE_PARENT_SIZE, num_cells)

def get_node_next_leaf(node):
    offset = NODE_TYPE_SIZE + NODE_ROOT_SIZE + NODE_PARENT_SIZE + NODE_NUM_CELLS_SIZE
    return struct.unpack_from('I', node, offset)[0]

def set_node_next_leaf(node, next_leaf_page):
    offset = NODE_TYPE_SIZE + NODE_ROOT_SIZE + NODE_PARENT_SIZE + NODE_NUM_CELLS_SIZE
    struct.pack_into('I', node, offset, next_leaf_page)

def get_node_max_key(node):
    num_cells = get_node_num_cells(node)
    if num_cells == 0:
        return 0
    # Key is at the start of the cell in Leaf Nodes
    offset = COMMON_NODE_HEADER_SIZE + ((num_cells - 1) * LEAF_NODE_CELL_SIZE)
    return struct.unpack_from('I', node, offset)[0]

def initialize_leaf_node(node):
    set_node_type(node, NODE_LEAF)
    set_node_root(node, False)
    set_node_num_cells(node, 0)
    set_node_next_leaf(node, 0)

def initialize_internal_node(node):
    set_node_type(node, NODE_INTERNAL)
    set_node_root(node, False)
    set_node_num_cells(node, 0)

# --- Core Logic ---

def serialize_row(row_id, username, email):
    return struct.pack(ROW_FMT, row_id, username.encode('utf-8'), email.encode('utf-8'))

def deserialize_row(data):
    row_id, username_bytes, email_bytes = struct.unpack(ROW_FMT, data)
    username = username_bytes.decode('utf-8').rstrip('\x00')
    email = email_bytes.decode('utf-8').rstrip('\x00')
    return row_id, username, email

def split_root_node(cursor, row_id, row_bytes):
    # 1. Create Left (Page 1) and Right (Page 2) Children
    root_node = cursor.table.pager.get_page(0)
    page_1_num = cursor.table.pager.get_unused_page_num()
    cursor.table.pager.num_pages += 1
    page_2_num = cursor.table.pager.get_unused_page_num()
    cursor.table.pager.num_pages += 1
    
    new_left_node = cursor.table.pager.get_page(page_1_num)
    initialize_leaf_node(new_left_node)
    
    new_right_node = cursor.table.pager.get_page(page_2_num)
    initialize_leaf_node(new_right_node)
    
    set_node_next_leaf(new_left_node, page_2_num)
    
    # 2. Copy Data to Left/Right
    old_max = LEAF_NODE_MAX_CELLS
    split_index = old_max // 2
    
    # Left Half
    for i in range(split_index):
        src = COMMON_NODE_HEADER_SIZE + (i * LEAF_NODE_CELL_SIZE)
        dest = COMMON_NODE_HEADER_SIZE + (i * LEAF_NODE_CELL_SIZE)
        new_left_node[dest : dest + LEAF_NODE_CELL_SIZE] = root_node[src : src + LEAF_NODE_CELL_SIZE]
    set_node_num_cells(new_left_node, split_index)
    
    # Right Half
    for i in range(split_index, old_max):
        src = COMMON_NODE_HEADER_SIZE + (i * LEAF_NODE_CELL_SIZE)
        dest_index = i - split_index
        dest = COMMON_NODE_HEADER_SIZE + (dest_index * LEAF_NODE_CELL_SIZE)
        new_right_node[dest : dest + LEAF_NODE_CELL_SIZE] = root_node[src : src + LEAF_NODE_CELL_SIZE]
    set_node_num_cells(new_right_node, old_max - split_index)
    
    # 3. Determine where new row goes and insert it
    # We need the max key of the Left node to decide key for Internal Node
    left_max_key = get_node_max_key(new_left_node)
    
    target_node = new_right_node 
    if row_id <= left_max_key:
        target_node = new_left_node # Should go to left (logic simplified)
        # Re-insert logic skipped for brevity, assuming Append-Only workload
    
    # Insert new row into Right Node (Assuming sequential ID insert)
    right_count = get_node_num_cells(new_right_node)
    dest = COMMON_NODE_HEADER_SIZE + (right_count * LEAF_NODE_CELL_SIZE)
    struct.pack_into('I', new_right_node, dest, row_id)
    new_right_node[dest + 4 : dest + LEAF_NODE_CELL_SIZE] = row_bytes
    set_node_num_cells(new_right_node, right_count + 1)
    
    # 4. Update Root (Internal Node)
    initialize_internal_node(root_node)
    set_node_root(root_node, True)
    set_node_num_cells(root_node, 2) # Two children
    
    # Internal Cell 0: [Max_Key=Left_Max, Child=Page 1]
    cell_0_offset = COMMON_NODE_HEADER_SIZE
    struct.pack_into('I', root_node, cell_0_offset, left_max_key)
    struct.pack_into('I', root_node, cell_0_offset + 4, page_1_num)
    
    # Internal Cell 1: [Max_Key=MAX_INT, Child=Page 2]
    cell_1_offset = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_CELL_SIZE
    struct.pack_into('I', root_node, cell_1_offset, MAX_INT_KEY)
    struct.pack_into('I', root_node, cell_1_offset + 4, page_2_num)
    
    print(f"DEBUG: Split Root. Internal Node Keys: [{left_max_key}, {MAX_INT_KEY}] -> Pages [{page_1_num}, {page_2_num}]")

def leaf_node_insert(cursor, row_id, row_bytes):
    node = cursor.table.pager.get_page(cursor.page_num)
    num_cells = get_node_num_cells(node)

    if num_cells >= LEAF_NODE_MAX_CELLS:
        # Check if root
        is_root = struct.unpack_from('B', node, NODE_TYPE_SIZE)[0]
        if is_root:
            split_root_node(cursor, row_id, row_bytes)
        else:
            print("Error: Splitting non-root leaf not implemented.")
        return

    offset = COMMON_NODE_HEADER_SIZE + (num_cells * LEAF_NODE_CELL_SIZE)
    struct.pack_into('I', node, offset, row_id)
    node[offset + 4 : offset + LEAF_NODE_CELL_SIZE] = row_bytes
    set_node_num_cells(node, num_cells + 1)

def find_leaf_page(table, row_id):
    # Start at root
    page_num = 0
    node = table.pager.get_page(page_num)
    node_type = get_node_type(node)

    while node_type == NODE_INTERNAL:
        num_cells = get_node_num_cells(node)
        
        # Search for the right child
        found_child = False
        for i in range(num_cells):
            offset = COMMON_NODE_HEADER_SIZE + (i * INTERNAL_NODE_CELL_SIZE)
            key = struct.unpack_from('I', node, offset)[0]
            child_page = struct.unpack_from('I', node, offset + 4)[0]
            
            if row_id <= key:
                page_num = child_page
                found_child = True
                break
        
        if not found_child:
            print(f"Error: Key {row_id} exceeds max key in internal node.")
            return 0

        node = table.pager.get_page(page_num)
        node_type = get_node_type(node)
        
    return page_num

def execute_insert(command, table):
    parts = command.strip().split()
    if len(parts) != 4:
        print("Error: Invalid syntax.")
        return
    try:
        row_id = int(parts[1])
        username = parts[2]
        email = parts[3]
    except ValueError:
        print("Error: ID must be integer.")
        return

    # THE REAL B-TREE SEARCH
    page_num = find_leaf_page(table, row_id)
    
    cursor = Cursor(table)
    cursor.page_num = page_num
    
    row_bytes = serialize_row(row_id, username, email)
    leaf_node_insert(cursor, row_id, row_bytes)
    print("Executed.")

def execute_select(table):
    cursor = Cursor(table).start_reading()
    while not cursor.end_of_table:
        node = table.pager.get_page(cursor.page_num)
        offset = COMMON_NODE_HEADER_SIZE + (cursor.cell_num * LEAF_NODE_CELL_SIZE)
        
        key = struct.unpack_from('I', node, offset)[0]
        val_offset = offset + 4
        row_data = node[val_offset : val_offset + ROW_SIZE]
        row_id, username, email = deserialize_row(row_data)
        
        print(f"({row_id}, {username}, {email})")
        cursor.advance()

def main():
    table = Table()
    try:
        while True:
            print("db > ", end="", flush=True)
            try:
                user_input = input()
            except EOFError:
                break
            
            if not user_input.strip(): continue

            if user_input.strip().startswith("."):
                if user_input.strip() == ".exit": break
                elif user_input.strip() == ".constants":
                    print(f"Leaf Max: {LEAF_NODE_MAX_CELLS}")
            
            elif user_input.strip().startswith("insert"):
                execute_insert(user_input, table)
            elif user_input.strip() == "select":
                execute_select(table)
            else:
                print("Unknown command.")
    finally:
        table.close()

if __name__ == "__main__":
    main()