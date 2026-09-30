import json

from pydb import Database


def crash(db):
    """Drop the process state without a checkpoint, as a kill -9 would."""
    db.rows.close()
    db.index.close()
    db.wal.close()


def test_committed_inserts_are_redone_after_a_crash(tmp_path):
    db = Database(str(tmp_path))
    for i in range(1, 6):
        db.insert(i, f"user{i}", f"user{i}@example.com")
    crash(db)
    assert (tmp_path / "mydb.db").stat().st_size == 0  # nothing was checkpointed

    recovered = Database(str(tmp_path))
    assert recovered.recovered == 5
    assert [recovered.get(i)[1] for i in range(1, 6)] == [f"user{i}" for i in range(1, 6)]
    assert recovered.find_by_email("user3@example.com")[0] == 3


def test_a_start_without_commit_is_discarded(tmp_path):
    db = Database(str(tmp_path))
    db.insert(1, "kept", "kept@example.com")
    crash(db)
    with open(tmp_path / "wal.log", "a") as wal:
        wal.write(json.dumps({"txn_id": 2, "status": "START", "action": "INSERT",
                              "data": {"id": 2, "user": "lost", "email": "lost@example.com"}}) + "\n")

    recovered = Database(str(tmp_path))
    assert recovered.recovered == 1
    assert recovered.get(1) is not None
    assert recovered.get(2) is None
    assert recovered.find_by_email("lost@example.com") is None


def test_a_torn_final_record_is_ignored(tmp_path):
    db = Database(str(tmp_path))
    for i in range(1, 4):
        db.insert(i, f"user{i}", f"user{i}@example.com")
    crash(db)
    with open(tmp_path / "wal.log", "a") as wal:
        wal.write('{"txn_id": 4, "status": "STA')  # the process died mid-write

    recovered = Database(str(tmp_path))
    assert recovered.recovered == 3
    assert recovered.get(3) is not None and recovered.get(4) is None


def test_recovery_checkpoints_so_a_second_restart_has_nothing_to_redo(tmp_path):
    db = Database(str(tmp_path))
    db.insert(1, "a", "a@example.com")
    crash(db)

    crash(Database(str(tmp_path)))  # recovers, then dies again
    third = Database(str(tmp_path))
    assert third.recovered == 0
    assert third.get(1) == (1, "a", "a@example.com")
