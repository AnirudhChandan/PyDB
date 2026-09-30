import random
import struct

from pydb.btree import BTree


def val(n):
    return struct.pack("I", n)


def test_insert_and_search_across_leaf_splits(tmp_path):
    # 291-byte values: 27 cells per leaf, so 600 keys force many splits
    tree = BTree(str(tmp_path / "t.db"), val_size=291)
    keys = random.Random(7).sample(range(1, 100_000), 600)
    for k in keys:
        tree.insert(k, val(k).ljust(291, b"\x00"))

    assert tree.pager.num_pages > 2
    for k in keys:
        assert bytes(tree.search(k)[:4]) == val(k)
    assert tree.search(100_001) is None


def test_insert_replaces_existing_key(tmp_path):
    tree = BTree(str(tmp_path / "t.db"), val_size=4)
    tree.insert(5, val(1))
    tree.insert(5, val(2))

    assert bytes(tree.search(5)) == val(2)
    assert len(tree._read_leaf(tree.pager.get_page(0))) == 1


def test_tree_survives_checkpoint_and_reopen(tmp_path):
    path = str(tmp_path / "t.db")
    tree = BTree(path, val_size=4)
    for k in range(3000):  # enough to split the root leaf
        tree.insert(k, val(k * 2))
    tree.pager.checkpoint()
    tree.close()

    reopened = BTree(path, val_size=4)
    assert all(bytes(reopened.search(k)) == val(k * 2) for k in range(3000))


def test_nothing_reaches_the_data_file_before_a_checkpoint(tmp_path):
    path = tmp_path / "t.db"
    tree = BTree(str(path), val_size=4)
    tree.insert(1, val(1))
    assert path.stat().st_size == 0
