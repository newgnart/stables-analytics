"""Database operations for Kafka consumer."""

import os
from typing import Optional

import psycopg
from psycopg import sql

from scripts.utils.logger import logger
from scripts.kafka.models import TransferData


def get_postgres_dsn() -> str:
    """Build PostgreSQL DSN from environment variables."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "capstone")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def create_transfers_table(dsn: str) -> bool:
    """Create transfers table if it doesn't exist.

    Args:
        dsn: PostgreSQL connection string

    Returns:
        True if successful, False otherwise
    """
    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS raw.kafka_sink_raw_transfer (
                        id TEXT PRIMARY KEY,
                        block_number BIGINT NOT NULL,
                        timestamp TEXT NOT NULL,
                        contract_address TEXT NOT NULL,
                        from_address TEXT NOT NULL,
                        to_address TEXT NOT NULL,
                        value TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                )
                conn.commit()
                logger.info("✅ Transfers table created/verified")
                return True
    except Exception as e:
        logger.error(f"Failed to create transfers table: {e}", exc_info=True)
        return False


def insert_transfer(dsn: str, transfer: TransferData) -> bool:
    """Insert transfer event into database.

    Args:
        dsn: PostgreSQL connection string
        transfer: Transfer data to insert

    Returns:
        True if successful, False otherwise
    """
    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw.kafka_sink_raw_transfer (
                        id, block_number, timestamp, contract_address,
                        from_address, to_address, value
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        transfer.id,
                        transfer.block_number,
                        transfer.timestamp,
                        transfer.contract_address,
                        transfer.from_address,
                        transfer.to_address,
                        transfer.value,
                    ),
                )
                conn.commit()
                return True
    except Exception as e:
        logger.error(f"Failed to insert transfer {transfer.id}: {e}", exc_info=True)
        return False


def batch_insert_transfers(dsn: str, transfers: list[TransferData]) -> int:
    """Insert multiple transfer events in a single transaction.

    Args:
        dsn: PostgreSQL connection string
        transfers: List of transfer data to insert

    Returns:
        Number of records successfully inserted
    """
    if not transfers:
        return 0

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                # Prepare batch data
                batch_data = [
                    (
                        t.id,
                        t.block_number,
                        t.timestamp,
                        t.contract_address,
                        t.from_address,
                        t.to_address,
                        t.value,
                    )
                    for t in transfers
                ]

                # Use executemany for efficient batch insert
                cur.executemany(
                    """
                    INSERT INTO raw.kafka_sink_raw_transfer (
                        id, block_number, timestamp, contract_address,
                        from_address, to_address, value
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    batch_data,
                )
                conn.commit()
                inserted_count = len(transfers)
                logger.debug(f"Batch inserted {inserted_count} transfers")
                return inserted_count
    except Exception as e:
        logger.error(
            f"Failed to batch insert {len(transfers)} transfers: {e}", exc_info=True
        )
        return 0
