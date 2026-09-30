"""Kill a real process mid-workload and check what the next process finds."""
import os
import subprocess
import sys

import pytest

from pydb import Database

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="needs SIGKILL")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHILD = os.path.join(ROOT, "tests", "crash_child.py")


def spawn(directory, count, *extra, failpoint=None):
    env = dict(os.environ, PYTHONPATH=ROOT)
    if failpoint:
        env["PYDB_FAILPOINT"] = failpoint
    return subprocess.Popen([sys.executable, CHILD, str(directory), str(count), *extra],
                            stdout=subprocess.PIPE, text=True, env=env)


def expected(i):
    return (i, f"user{i}", f"user{i}@example.com")


@pytest.mark.parametrize("kill_after", [1, 40, 250])
def test_kill_9_mid_workload_loses_no_acknowledged_insert(tmp_path, kill_after):
    child = spawn(tmp_path, 100_000)
    for _ in range(kill_after):
        child.stdout.readline()
    child.kill()  # SIGKILL: no cleanup, no checkpoint
    acked = kill_after + len(child.stdout.read().split())
    child.wait()

    db = Database(str(tmp_path))

    # durability: every insert the client saw acknowledged is there, through both trees
    for i in range(1, acked + 1):
        assert db.get(i) == expected(i)
        assert db.find_by_email(f"user{i}@example.com") == expected(i)
    # atomicity: at most the one in-flight insert beyond that, and it is whole or absent
    in_flight = db.get(acked + 1)
    assert in_flight in (None, expected(acked + 1))
    if in_flight:
        assert db.find_by_email(f"user{acked + 1}@example.com") == in_flight
    assert db.get(acked + 2) is None


@pytest.mark.parametrize("failpoint", ["between_checkpoints", "before_wal_truncate"])
def test_crash_in_the_middle_of_a_checkpoint_is_recoverable(tmp_path, failpoint):
    child = spawn(tmp_path, 300, "close", failpoint=failpoint)
    assert child.wait() == 1  # died inside close()
    child.stdout.close()

    db = Database(str(tmp_path))
    for i in range(1, 301):
        assert db.get(i) == expected(i)
        assert db.find_by_email(f"user{i}@example.com") == expected(i)
