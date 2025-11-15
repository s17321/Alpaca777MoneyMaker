import sqlite3
from pathlib import Path
from typing import Iterable


_DB = Path("data/runtime.sqlite")


DDL = """
CREATE TABLE IF NOT EXISTS trades (
id INTEGER PRIMARY KEY AUTOINCREMENT,
ts INTEGER,
bot_id TEXT,
symbol TEXT,
side TEXT,
qty REAL,
price REAL,
client_id TEXT
);
CREATE TABLE IF NOT EXISTS equity (
id INTEGER PRIMARY KEY AUTOINCREMENT,
ts INTEGER,
bot_id TEXT,
equity REAL
);
"""


def get_conn() -> sqlite3.Connection:
    _DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(_DB)
    conn.executescript(DDL)
    return conn


def insert_trades(conn: sqlite3.Connection, rows: Iterable[tuple]):
    conn.executemany(
        "INSERT INTO trades(ts, bot_id, symbol, side, qty, price, client_id) VALUES (?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()


def insert_equity(conn: sqlite3.Connection, ts: int, bot_id: str, equity: float):
    conn.execute(
        "INSERT INTO equity(ts, bot_id, equity) VALUES (?,?,?)",
        (ts, bot_id, equity),
    )
    conn.commit()