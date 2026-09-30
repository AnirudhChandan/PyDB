"""REPL: insert <id> <user> <email> | where email=<email> | .exit"""
from .db import Database


def execute_insert(command, db):
    parts = command.split()
    if len(parts) != 4:
        return print("Error: Syntax 'insert <id> <user> <email>'")
    try:
        row_id = int(parts[1])
    except ValueError:
        return print("Error: ID must be int")
    try:
        db.insert(row_id, parts[2], parts[3])
    except (ValueError, KeyError) as e:
        return print(f"Error: {e}")
    print("Executed.")


def execute_where(command, db):
    parts = command.split("=")
    if len(parts) != 2:
        return print("Error: Syntax 'where email=<email>'")
    row = db.find_by_email(parts[1].strip())
    print(f"Result: {row}" if row else "Not found.")


def main():
    db = Database()
    if db.recovered:
        print(f"CRASH DETECTED. Recovered {db.recovered} committed txns from the WAL.")
    try:
        while True:
            print("db > ", end="", flush=True)
            try:
                cmd = input().strip()
            except EOFError:
                break
            if not cmd:
                continue
            if cmd == ".exit":
                break
            if cmd.startswith("insert"):
                execute_insert(cmd, db)
            elif cmd.startswith("where"):
                execute_where(cmd, db)
    finally:
        db.close()
