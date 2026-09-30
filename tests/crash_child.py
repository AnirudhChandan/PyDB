"""Insert rows and acknowledge each one on stdout; the test kills this process."""
import sys
import time

from pydb import Database

directory, count = sys.argv[1], int(sys.argv[2])
db = Database(directory)
for i in range(1, count + 1):
    db.insert(i, f"user{i}", f"user{i}@example.com")
    print(i, flush=True)  # printed only after the COMMIT record is fsync'd
if "close" in sys.argv:
    db.close()
time.sleep(60)
