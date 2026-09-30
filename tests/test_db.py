import pytest

from pydb import Database


def test_insert_get_and_find_by_email(tmp_path):
    db = Database(str(tmp_path))
    db.insert(1, "anirudh", "anirudh@example.com")

    assert db.get(1) == (1, "anirudh", "anirudh@example.com")
    assert db.find_by_email("anirudh@example.com") == (1, "anirudh", "anirudh@example.com")
    assert db.get(2) is None
    assert db.find_by_email("nobody@example.com") is None


def test_duplicate_id_is_rejected_and_not_logged(tmp_path):
    db = Database(str(tmp_path))
    db.insert(1, "a", "a@example.com")
    wal_size = (tmp_path / "wal.log").stat().st_size

    with pytest.raises(KeyError):
        db.insert(1, "b", "b@example.com")
    assert db.get(1) == (1, "a", "a@example.com")
    assert (tmp_path / "wal.log").stat().st_size == wal_size


@pytest.mark.parametrize("row_id,user,email", [
    (-1, "a", "a@example.com"),
    (2**32, "a", "a@example.com"),
    (1, "u" * 33, "a@example.com"),
    (1, "a", "e" * 256),
])
def test_values_that_do_not_fit_the_row_format_are_rejected(tmp_path, row_id, user, email):
    db = Database(str(tmp_path))
    with pytest.raises(ValueError):
        db.insert(row_id, user, email)
    assert (tmp_path / "wal.log").stat().st_size == 0


def test_clean_close_checkpoints_and_empties_the_wal(tmp_path):
    db = Database(str(tmp_path))
    for i in range(1, 101):
        db.insert(i, f"user{i}", f"user{i}@example.com")
    db.close()

    assert (tmp_path / "wal.log").stat().st_size == 0
    reopened = Database(str(tmp_path))
    assert reopened.recovered == 0
    assert reopened.get(100) == (100, "user100", "user100@example.com")
