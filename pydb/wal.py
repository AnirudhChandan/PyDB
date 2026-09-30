"""Write-ahead log: append-only, one JSON record per line, fsync'd per record."""
import json
import os


class WAL:
    def __init__(self, filename):
        self.filename = filename
        self.file = open(filename, "a+")
        self.txn_counter = max((e["txn_id"] for e in self._entries()), default=0)

    def _entries(self):
        """Yield well-formed records, stopping at a torn tail left by a crash mid-append."""
        self.file.seek(0)
        for line in self.file:
            try:
                yield json.loads(line)
            except ValueError:
                return

    def _append(self, entry):
        self.file.write(json.dumps(entry) + "\n")
        self.file.flush()
        os.fsync(self.file.fileno())

    def log_start(self, row_id, username, email):
        self.txn_counter += 1
        self._append({"txn_id": self.txn_counter, "status": "START", "action": "INSERT",
                      "data": {"id": row_id, "user": username, "email": email}})
        return self.txn_counter

    def log_commit(self, txn_id):
        self._append({"txn_id": txn_id, "status": "COMMIT"})

    def committed(self):
        """Inserts whose COMMIT record reached disk, in commit order."""
        started, done = {}, []
        for entry in self._entries():
            if entry["status"] == "START":
                started[entry["txn_id"]] = entry["data"]
            elif entry["txn_id"] in started:
                done.append(started.pop(entry["txn_id"]))
        return done

    def truncate(self):
        """Drop the log. Only safe right after every data file has been checkpointed."""
        self.file.truncate(0)
        self.file.flush()
        os.fsync(self.file.fileno())
        self.txn_counter = 0

    def close(self):
        self.file.close()
