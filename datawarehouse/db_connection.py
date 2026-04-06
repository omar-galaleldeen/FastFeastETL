"""
db_connection.py
================
Single source of truth for all Postgres connections.

Responsibilities:
  - Create the database `fastfeastapp` if it does not exist
    (connects to the default `postgres` db first, then switches)
  - Provide a thread-safe connection pool via psycopg2's ThreadedConnectionPool
  - Expose get_conn() / put_conn() for all other DWH modules to use

All other datawarehouse modules import from here — never build their own engines.
"""
from __future__ import annotations

import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from config.config_loader import get_config
from utils.logger import get_logger

logger = get_logger(__name__)

_cfg = get_config()
_db  = _cfg.get("database", {})

# ── Connection parameters read from config ────────────────────────────────────
HOST   : str = _db.get("host",     "localhost")
PORT   : int = int(_db.get("port", 5432))
USER   : str = _db.get("user",     "postgres")
PASSWORD: str = _db.get("password","")
DBNAME : str = _db.get("dbname",   "fastfeastapp")

# Thread pool — min 2, max 10 connections
_pool: pg_pool.ThreadedConnectionPool | None = None


# ── Database bootstrap ────────────────────────────────────────────────────────

def _create_database_if_not_exists() -> None:
    """
    Connects to the default 'postgres' database and creates DBNAME if missing.
    Must run before the pool is created — called once at startup.
    """
    try:
        conn = psycopg2.connect(
            host     = HOST,
            port     = PORT,
            user     = USER,
            password = PASSWORD,
            dbname   = "postgres",      # always exists
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DBNAME,))
        exists = cur.fetchone()

        if not exists:
            cur.execute(f'CREATE DATABASE "{DBNAME}"')
            logger.info(f"[db] Database '{DBNAME}' created.")
        else:
            logger.info(f"[db] Database '{DBNAME}' already exists.")

        cur.close()
        conn.close()

    except Exception as exc:
        logger.error(f"[db] Failed to create database '{DBNAME}': {exc}")
        raise


# ── Pool management ───────────────────────────────────────────────────────────

def init_pool() -> None:
    """
    Called once from ingestion_runner.start().
    Creates the database if needed, then opens the connection pool.
    """
    global _pool

    _create_database_if_not_exists()

    _pool = pg_pool.ThreadedConnectionPool(
        minconn  = 2,
        maxconn  = 10,
        host     = HOST,
        port     = PORT,
        user     = USER,
        password = PASSWORD,
        dbname   = DBNAME,
    )
    logger.info(f"[db] Connection pool initialised → {USER}@{HOST}:{PORT}/{DBNAME}")


def get_conn() -> psycopg2.extensions.connection:
    """
    Borrow a connection from the pool.
    Call put_conn(conn) when done — always use try/finally.
    """
    if _pool is None:
        raise RuntimeError("[db] Pool not initialised — call init_pool() first.")
    return _pool.getconn()


def put_conn(conn: psycopg2.extensions.connection) -> None:
    """Return a connection to the pool."""
    if _pool and conn:
        _pool.putconn(conn)


def close_pool() -> None:
    """Called from ingestion_runner.stop() at shutdown."""
    if _pool:
        _pool.closeall()
        logger.info("[db] Connection pool closed.")
