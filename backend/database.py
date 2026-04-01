"""SQLite database setup and query helpers."""
import sqlite3
import os
from backend.config import DB_PATH

def get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create tables if they don't exist."""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS agents (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL UNIQUE,
            contact     TEXT DEFAULT '',
            tier        INTEGER DEFAULT 2,
            herp_source TEXT DEFAULT '',
            created_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS daily_funnel (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id        INTEGER NOT NULL REFERENCES agents(id),
            date            TEXT NOT NULL,
            requisition_id  TEXT NOT NULL DEFAULT '',
            rec             INTEGER DEFAULT 0,
            i1              INTEGER DEFAULT 0,
            i2              INTEGER DEFAULT 0,
            i3              INTEGER DEFAULT 0,
            offer           INTEGER DEFAULT 0,
            accept          INTEGER DEFAULT 0,
            synced_at       TEXT DEFAULT (datetime('now')),
            UNIQUE(agent_id, date, requisition_id)
        );

        CREATE TABLE IF NOT EXISTS targets (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            year_month  TEXT NOT NULL UNIQUE,
            rec         INTEGER DEFAULT 0,
            i1          INTEGER DEFAULT 0,
            i2          INTEGER DEFAULT 0,
            i3          INTEGER DEFAULT 0,
            offer       INTEGER DEFAULT 0,
            accept      INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS requisitions (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL DEFAULT '',
            is_archived INTEGER DEFAULT 0,
            enabled     INTEGER DEFAULT 0,
            updated_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_funnel_date ON daily_funnel(date);
        CREATE INDEX IF NOT EXISTS idx_funnel_agent ON daily_funnel(agent_id, date);
        CREATE INDEX IF NOT EXISTS idx_funnel_req ON daily_funnel(requisition_id);
    """)
    conn.commit()
    conn.close()


# ===== Query Helpers =====

def get_all_agents():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM agents ORDER BY tier, id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_agent(name: str, contact: str = "", tier: int = 2, herp_source: str = ""):
    conn = get_connection()
    conn.execute("""
        INSERT INTO agents (name, contact, tier, herp_source)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            contact = excluded.contact,
            herp_source = excluded.herp_source
    """, (name, contact, tier, herp_source))
    conn.commit()
    agent = conn.execute("SELECT * FROM agents WHERE name = ?", (name,)).fetchone()
    conn.close()
    return dict(agent)


def upsert_daily_funnel(agent_id: int, date: str, rec=0, i1=0, i2=0, i3=0, offer=0, accept=0, requisition_id: str = ""):
    conn = get_connection()
    conn.execute("""
        INSERT INTO daily_funnel (agent_id, date, requisition_id, rec, i1, i2, i3, offer, accept)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(agent_id, date, requisition_id) DO UPDATE SET
            rec    = excluded.rec,
            i1     = excluded.i1,
            i2     = excluded.i2,
            i3     = excluded.i3,
            offer  = excluded.offer,
            accept = excluded.accept,
            synced_at = datetime('now')
    """, (agent_id, date, requisition_id, rec, i1, i2, i3, offer, accept))
    conn.commit()
    conn.close()


def query_funnel_range(date_from: str, date_to: str, req_ids: set = None):
    """Aggregate funnel data by agent for a date range, optionally filtered by requisition IDs."""
    conn = get_connection()
    if req_ids:
        placeholders = ",".join("?" for _ in req_ids)
        rows = conn.execute(f"""
            SELECT
                agent_id,
                SUM(rec)    AS rec,
                SUM(i1)     AS i1,
                SUM(i2)     AS i2,
                SUM(i3)     AS i3,
                SUM(offer)  AS offer,
                SUM(accept) AS accept,
                COUNT(DISTINCT date) AS day_count
            FROM daily_funnel
            WHERE date BETWEEN ? AND ?
              AND requisition_id IN ({placeholders})
            GROUP BY agent_id
        """, (date_from, date_to, *req_ids)).fetchall()
    else:
        rows = conn.execute("""
            SELECT
                agent_id,
                SUM(rec)    AS rec,
                SUM(i1)     AS i1,
                SUM(i2)     AS i2,
                SUM(i3)     AS i3,
                SUM(offer)  AS offer,
                SUM(accept) AS accept,
                COUNT(DISTINCT date) AS day_count
            FROM daily_funnel
            WHERE date BETWEEN ? AND ?
            GROUP BY agent_id
        """, (date_from, date_to)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_day_count(date_from: str, date_to: str, req_ids: set = None) -> int:
    conn = get_connection()
    if req_ids:
        placeholders = ",".join("?" for _ in req_ids)
        row = conn.execute(
            f"SELECT COUNT(DISTINCT date) AS cnt FROM daily_funnel WHERE date BETWEEN ? AND ? AND requisition_id IN ({placeholders})",
            (date_from, date_to, *req_ids)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT COUNT(DISTINCT date) AS cnt FROM daily_funnel WHERE date BETWEEN ? AND ?",
            (date_from, date_to)
        ).fetchone()
    conn.close()
    return row["cnt"] if row else 0


def get_target(year_month: str):
    conn = get_connection()
    row = conn.execute("SELECT * FROM targets WHERE year_month = ?", (year_month,)).fetchone()
    conn.close()
    return dict(row) if row else None


def upsert_target(year_month: str, rec=0, i1=0, i2=0, i3=0, offer=0, accept=0):
    conn = get_connection()
    conn.execute("""
        INSERT INTO targets (year_month, rec, i1, i2, i3, offer, accept)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(year_month) DO UPDATE SET
            rec    = excluded.rec,
            i1     = excluded.i1,
            i2     = excluded.i2,
            i3     = excluded.i3,
            offer  = excluded.offer,
            accept = excluded.accept
    """, (year_month, rec, i1, i2, i3, offer, accept))
    conn.commit()
    conn.close()


def get_all_requisitions():
    conn = get_connection()
    rows = conn.execute("SELECT * FROM requisitions ORDER BY enabled DESC, name").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_requisition(req_id: str, name: str, is_archived: int = 0, enabled: int = 0):
    conn = get_connection()
    conn.execute("""
        INSERT INTO requisitions (id, name, is_archived, enabled)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name = excluded.name,
            is_archived = excluded.is_archived,
            updated_at = datetime('now')
    """, (req_id, name, is_archived, enabled))
    conn.commit()
    conn.close()


def set_requisition_enabled(req_id: str, enabled: int):
    conn = get_connection()
    conn.execute("UPDATE requisitions SET enabled = ?, updated_at = datetime('now') WHERE id = ?", (enabled, req_id))
    conn.commit()
    conn.close()


def get_enabled_requisition_ids():
    conn = get_connection()
    rows = conn.execute("SELECT id FROM requisitions WHERE enabled = 1").fetchall()
    conn.close()
    return {r["id"] for r in rows}


def get_daily_raw(date_from: str, date_to: str):
    """Return raw daily rows for export/debug."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT df.*, a.name AS agent_name
        FROM daily_funnel df
        JOIN agents a ON a.id = df.agent_id
        WHERE df.date BETWEEN ? AND ?
        ORDER BY df.date, a.tier, a.id
    """, (date_from, date_to)).fetchall()
    conn.close()
    return [dict(r) for r in rows]
