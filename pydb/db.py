"""The engine: a users table (id, username, email) with a secondary index on email."""
import os
import struct
import zlib

from .btree import BTree, MAX_INT_KEY
from .wal import WAL

DB_FILE_NAME = "mydb.db"
IDX_FILE_NAME = "email.idx"
WAL_FILE_NAME = "wal.log"

# Row format: id (4) + username (32) + email (255) = 291 bytes
USERNAME_SIZE = 32
EMAIL_SIZE = 255
ROW_FMT = f"I{USERNAME_SIZE}s{EMAIL_SIZE}s"
ROW_SIZE = struct.calcsize(ROW_FMT)


def serialize_row(row_id, username, email):
    return struct.pack(ROW_FMT, row_id, username.encode("utf-8"), email.encode("utf-8"))


def deserialize_row(data):
    row_id, user_b, email_b = struct.unpack(ROW_FMT, data)
    return row_id, user_b.decode("utf-8").rstrip("\x00"), email_b.decode("utf-8").rstrip("\x00")


def hash_email(email):
    return zlib.crc32(email.encode("utf-8")) & 0xFFFFFFFF


def _failpoint(name):
    # Test hook: lets the crash-recovery tests kill the process at an exact step.
    if os.environ.get("PYDB_FAILPOINT") == name:
        os._exit(1)


class Database:
    def __init__(self, directory="."):
        self.rows = BTree(os.path.join(directory, DB_FILE_NAME), ROW_SIZE)
        self.index = BTree(os.path.join(directory, IDX_FILE_NAME), 4)
        self.wal = WAL(os.path.join(directory, WAL_FILE_NAME))
        self.recovered = self._recover()

    def _apply(self, row_id, username, email):
        self.rows.insert(row_id, serialize_row(row_id, username, email))
        self.index.insert(hash_email(email), struct.pack("I", row_id))

    def _recover(self):
        """Redo every committed insert since the last checkpoint; drop uncommitted ones.

        Data files only change at a checkpoint, so after a crash they hold the last
        checkpointed state and the WAL holds everything committed since.
        """
        pending = self.wal.committed()
        for data in pending:
            self._apply(data["id"], data["user"], data["email"])
        if os.path.getsize(self.wal.filename):
            self.checkpoint()
        return len(pending)

    def insert(self, row_id, username, email):
        if not 0 <= row_id <= MAX_INT_KEY:
            raise ValueError("id must fit in an unsigned 32-bit integer")
        if len(username.encode("utf-8")) > USERNAME_SIZE or len(email.encode("utf-8")) > EMAIL_SIZE:
            raise ValueError(f"username is limited to {USERNAME_SIZE} bytes and email to {EMAIL_SIZE}")
        if self.rows.search(row_id) is not None:
            raise KeyError(f"duplicate id {row_id}")

        txn_id = self.wal.log_start(row_id, username, email)  # 1. log first
        self._apply(row_id, username, email)                  # 2. change pages in memory
        self.wal.log_commit(txn_id)                           # 3. durable from here

    def get(self, row_id):
        row = self.rows.search(row_id)
        return deserialize_row(row) if row is not None else None

    def find_by_email(self, email):
        id_bytes = self.index.search(hash_email(email))
        if id_bytes is None:
            return None
        row = self.get(struct.unpack("I", id_bytes)[0])
        # The index stores a hash, so confirm the row really has this email.
        return row if row and row[2] == email else None

    def checkpoint(self):
        """Write both trees to disk, then drop the WAL.

        A crash anywhere in here is safe: the WAL is still intact and replay is
        idempotent, so recovery just redoes it on top of whichever files made it.
        """
        self.rows.pager.checkpoint()
        _failpoint("between_checkpoints")
        self.index.pager.checkpoint()
        _failpoint("before_wal_truncate")
        self.wal.truncate()

    def close(self):
        self.checkpoint()
        self.rows.close()
        self.index.close()
        self.wal.close()
