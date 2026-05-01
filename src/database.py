import os
import sqlite3
from pathlib import Path
from typing import Any

_data_dir = Path(os.getenv("DATA_DIR", str(Path(__file__).parent.parent / "data")))
DB_PATH = _data_dir / "filings.db"


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS investors (
            cik        TEXT PRIMARY KEY,
            name       TEXT NOT NULL,
            company    TEXT,
            strategy   TEXT,
            active     INTEGER DEFAULT 1,
            added_at   TEXT DEFAULT (date('now'))
        );

        CREATE TABLE IF NOT EXISTS filings (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            cik              TEXT NOT NULL REFERENCES investors(cik),
            filing_date      TEXT NOT NULL,
            period           TEXT NOT NULL,
            accession_number TEXT NOT NULL UNIQUE,
            raw_url          TEXT,
            UNIQUE(cik, period)
        );

        CREATE TABLE IF NOT EXISTS holdings (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id    INTEGER NOT NULL REFERENCES filings(id),
            cusip        TEXT,
            ticker       TEXT,
            company_name TEXT,
            shares       INTEGER,
            value_usd    INTEGER,
            put_call     TEXT,
            percent_of_portfolio REAL
        );

        CREATE TABLE IF NOT EXISTS changes (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            cik          TEXT NOT NULL REFERENCES investors(cik),
            period_from  TEXT NOT NULL,
            period_to    TEXT NOT NULL,
            ticker       TEXT,
            cusip        TEXT,
            company_name TEXT,
            change_type  TEXT NOT NULL,
            shares_delta INTEGER,
            value_delta  INTEGER,
            ai_analysis  TEXT,
            created_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS letters (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            cik        TEXT NOT NULL REFERENCES investors(cik),
            period     TEXT NOT NULL,
            source_url TEXT,
            raw_text   TEXT,
            summary    TEXT,
            key_theses TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_letters_cik_period ON letters(cik, period)"
    )
    conn.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_changes_unique
           ON changes(cik, period_from, period_to, cusip, change_type)"""
    )
    conn.commit()


def upsert_investor(conn: sqlite3.Connection, cik: str, name: str, company: str, strategy: str) -> None:
    conn.execute(
        """INSERT INTO investors (cik, name, company, strategy)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(cik) DO UPDATE SET name=excluded.name,
               company=excluded.company, strategy=excluded.strategy""",
        (cik, name, company, strategy),
    )
    conn.commit()


def upsert_filing(conn: sqlite3.Connection, cik: str, filing_date: str, period: str,
                  accession: str, raw_url: str = "") -> int:
    conn.execute(
        """INSERT INTO filings (cik, filing_date, period, accession_number, raw_url)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(cik, period) DO UPDATE SET
               filing_date=excluded.filing_date,
               accession_number=excluded.accession_number,
               raw_url=excluded.raw_url""",
        (cik, filing_date, period, accession, raw_url),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id FROM filings WHERE cik=? AND period=?", (cik, period)
    ).fetchone()
    return row["id"]


def insert_holdings(conn: sqlite3.Connection, filing_id: int, holdings: list[dict]) -> None:
    conn.execute("DELETE FROM holdings WHERE filing_id=?", (filing_id,))
    total_value = sum(h["value_usd"] for h in holdings) or 1
    rows = [
        (
            filing_id,
            h["cusip"],
            h.get("ticker", ""),
            h["name_of_issuer"],
            h["shares"],
            h["value_usd"],
            h.get("put_call", ""),
            round(h["value_usd"] / total_value * 100, 4),
        )
        for h in holdings
    ]
    conn.executemany(
        """INSERT INTO holdings
           (filing_id, cusip, ticker, company_name, shares, value_usd, put_call, percent_of_portfolio)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    conn.commit()


def get_holdings_for_filing(conn: sqlite3.Connection, filing_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM holdings WHERE filing_id=? ORDER BY value_usd DESC", (filing_id,)
    ).fetchall()


def get_two_latest_filings(conn: sqlite3.Connection, cik: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM filings WHERE cik=? ORDER BY period DESC LIMIT 2", (cik,)
    ).fetchall()


def upsert_letter(
    conn: sqlite3.Connection,
    cik: str,
    period: str,
    source_url: str,
    raw_text: str,
    key_theses: str = "",
) -> None:
    existing = conn.execute(
        "SELECT id FROM letters WHERE cik=? AND period=?", (cik, period)
    ).fetchone()
    if existing:
        conn.execute(
            """UPDATE letters SET source_url=?, raw_text=?, key_theses=?
               WHERE cik=? AND period=?""",
            (source_url, raw_text, key_theses, cik, period),
        )
    else:
        conn.execute(
            """INSERT INTO letters (cik, period, source_url, raw_text, key_theses)
               VALUES (?, ?, ?, ?, ?)""",
            (cik, period, source_url, raw_text, key_theses),
        )
    conn.commit()


def init_fool_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fool_portfolios (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            username    TEXT NOT NULL,
            period      TEXT NOT NULL,
            thread_url  TEXT,
            raw_text    TEXT,
            ytd_return  REAL,
            narrative   TEXT,
            created_at  TEXT DEFAULT (datetime('now')),
            UNIQUE(username, period)
        );

        CREATE TABLE IF NOT EXISTS fool_holdings (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL REFERENCES fool_portfolios(id),
            ticker       TEXT NOT NULL,
            pct          REAL NOT NULL
        );
    """)
    conn.commit()


def upsert_fool_portfolio(
    conn: sqlite3.Connection, username: str, period: str, thread_url: str,
    raw_text: str, ytd_return: float | None, narrative: str | None,
) -> int:
    conn.execute(
        """INSERT INTO fool_portfolios (username, period, thread_url, raw_text, ytd_return, narrative)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(username, period) DO UPDATE SET
               thread_url=excluded.thread_url, raw_text=excluded.raw_text,
               ytd_return=excluded.ytd_return, narrative=excluded.narrative""",
        (username, period, thread_url, raw_text, ytd_return, narrative),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id FROM fool_portfolios WHERE username=? AND period=?", (username, period)
    ).fetchone()
    return row["id"]


def insert_fool_holdings(conn: sqlite3.Connection, portfolio_id: int, holdings: list[dict]) -> None:
    conn.execute("DELETE FROM fool_holdings WHERE portfolio_id=?", (portfolio_id,))
    rows = [
        (portfolio_id, h["ticker"], h.get("name") or "", h["pct"])
        for h in holdings
        if h.get("ticker") and h.get("pct") is not None
    ]
    conn.executemany(
        "INSERT INTO fool_holdings (portfolio_id, ticker, name, pct) VALUES (?, ?, ?, ?)", rows,
    )
    conn.commit()


def get_fool_holdings(conn: sqlite3.Connection, portfolio_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT ticker, name, pct FROM fool_holdings WHERE portfolio_id=? ORDER BY pct DESC",
        (portfolio_id,)
    ).fetchall()


def get_fool_two_latest(conn: sqlite3.Connection, username: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM fool_portfolios WHERE username=? ORDER BY period DESC LIMIT 2",
        (username,)
    ).fetchall()


def insert_changes(conn: sqlite3.Connection, changes: list[dict]) -> None:
    if not changes:
        return
    conn.executemany(
        """INSERT OR IGNORE INTO changes
           (cik, period_from, period_to, ticker, cusip, company_name,
            change_type, shares_delta, value_delta)
           VALUES (:cik, :period_from, :period_to, :ticker, :cusip,
                   :company_name, :change_type, :shares_delta, :value_delta)""",
        changes,
    )
    conn.commit()
