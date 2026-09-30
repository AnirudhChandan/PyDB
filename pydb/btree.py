"""B-Tree over fixed-size pages: 4-byte integer keys, fixed-size values."""
import struct

from .pager import PAGE_SIZE, Pager

NODE_INTERNAL = 0
NODE_LEAF = 1
MAX_INT_KEY = 4294967295


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
        struct.pack_into("BBII", node, 0, NODE_LEAF, 1 if is_root else 0, 0, 0)
        struct.pack_into("I", node, self.OFF_NEXT, 0)

    def _init_internal(self, node, is_root=False):
        struct.pack_into("BBII", node, 0, NODE_INTERNAL, 1 if is_root else 0, 0, 0)

    # --- page <-> list-of-cells ---
    def _read_leaf(self, node):
        cells = []
        for i in range(struct.unpack_from("I", node, self.OFF_CELLS)[0]):
            off = self.header_size + i * self.leaf_cell_size
            cells.append((struct.unpack_from("I", node, off)[0], node[off + 4:off + self.leaf_cell_size]))
        return cells

    def _write_leaf(self, node, cells):
        struct.pack_into("I", node, self.OFF_CELLS, len(cells))
        for i, (k, v) in enumerate(cells):
            off = self.header_size + i * self.leaf_cell_size
            struct.pack_into("I", node, off, k)
            node[off + 4:off + self.leaf_cell_size] = v

    def _read_internal(self, node):
        cells = []
        for i in range(struct.unpack_from("I", node, self.OFF_CELLS)[0]):
            off = self.header_size + i * self.internal_cell_size
            cells.append({"key": struct.unpack_from("I", node, off)[0], "ptr": struct.unpack_from("I", node, off + 4)[0]})
        return cells

    def _write_internal(self, node, cells):
        struct.pack_into("I", node, self.OFF_CELLS, len(cells))
        for i, c in enumerate(cells):
            off = self.header_size + i * self.internal_cell_size
            struct.pack_into("I", node, off, c["key"])
            struct.pack_into("I", node, off + 4, c["ptr"])

    # --- core ---
    def find_leaf_page(self, key):
        page_num = 0
        node = self.pager.get_page(page_num)
        while struct.unpack_from("B", node, self.OFF_TYPE)[0] == NODE_INTERNAL:
            cells = self._read_internal(node)
            page_num = next((c["ptr"] for c in cells if key <= c["key"]), cells[-1]["ptr"])
            node = self.pager.get_page(page_num)
        return page_num

    def search(self, key):
        node = self.pager.get_page(self.find_leaf_page(key))
        return next((v for k, v in self._read_leaf(node) if k == key), None)

    def insert(self, key, val_bytes):
        """Insert, or replace the value if the key exists.

        Replace-on-duplicate is what makes WAL replay idempotent: redoing an
        insert that already reached the data file changes nothing.
        """
        page_num = self.find_leaf_page(key)
        node = self.pager.get_page(page_num)
        cells = self._read_leaf(node)

        for i, (k, _) in enumerate(cells):
            if k == key:
                cells[i] = (key, val_bytes)
                self._write_leaf(node, cells)
                return

        cells.append((key, val_bytes))
        cells.sort(key=lambda x: x[0])

        if len(cells) <= self.max_leaf_cells:
            self._write_leaf(node, cells)
            return

        # split the leaf
        split_idx = len(cells) // 2
        self._write_leaf(node, cells[:split_idx])

        new_page_num = self.pager.num_pages
        new_node = self.pager.get_page(new_page_num)
        self._init_leaf(new_node)
        self._write_leaf(new_node, cells[split_idx:])

        struct.pack_into("I", new_node, self.OFF_NEXT, struct.unpack_from("I", node, self.OFF_NEXT)[0])
        struct.pack_into("I", node, self.OFF_NEXT, new_page_num)
        left_max_key = cells[:split_idx][-1][0]

        if struct.unpack_from("B", node, self.OFF_ROOT)[0]:
            # root stays on page 0: move the left half out and turn page 0 into an internal node
            left_page_num = self.pager.num_pages
            left_node = self.pager.get_page(left_page_num)
            left_node[:] = node[:]
            struct.pack_into("B", left_node, self.OFF_ROOT, 0)

            self._init_internal(node, is_root=True)
            self._write_internal(node, [{"key": left_max_key, "ptr": left_page_num}, {"key": MAX_INT_KEY, "ptr": new_page_num}])
            struct.pack_into("I", left_node, self.OFF_PARENT, 0)
            struct.pack_into("I", new_node, self.OFF_PARENT, 0)
        else:
            parent_page = struct.unpack_from("I", node, self.OFF_PARENT)[0]
            struct.pack_into("I", new_node, self.OFF_PARENT, parent_page)
            self._insert_internal(parent_page, left_max_key, page_num, new_page_num)

    def _insert_internal(self, page_num, left_max_key, left_child, right_child):
        node = self.pager.get_page(page_num)
        cells = self._read_internal(node)

        for i, c in enumerate(cells):
            if c["ptr"] == left_child:
                old_key = c["key"]
                cells[i]["key"] = left_max_key
                cells.insert(i + 1, {"key": old_key, "ptr": right_child})
                break

        if len(cells) > self.max_internal_cells:
            raise RuntimeError("internal node is full: splitting internal nodes is not implemented")
        self._write_internal(node, cells)

    def close(self):
        self.pager.close()
