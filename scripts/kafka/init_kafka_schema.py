#!/usr/bin/env python3
"""Initialize database schema for Kafka streaming pipeline."""

from dotenv import load_dotenv

load_dotenv()
from scripts.utils.database_client import PostgresClient


def init_schema():
    """Create raw schema and tables for Kafka pipeline."""

    client = PostgresClient.from_env()

    print(f"Connecting to PostgreSQL at {client.host}:{client.port}/{client.database}")

    with client.get_connection() as conn:
        with conn.cursor() as cur:
            # Create raw schema
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw")

            # Create kafka_sink_raw_transfer table
            print("Creating 'raw.kafka_sink_raw_transfer' table...")
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

            # Create indexes for common queries
            print("Creating indexes...")
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_kafka_sink_raw_transfer_block_number
                ON raw.kafka_sink_raw_transfer(block_number DESC)
            """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_kafka_sink_raw_transfer_contract_address
                ON raw.kafka_sink_raw_transfer(contract_address)
            """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_kafka_sink_raw_transfer_timestamp
                ON raw.kafka_sink_raw_transfer(timestamp DESC)
            """
            )

            conn.commit()
            print("✅ Schema initialization complete!")

            # Show table info
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'raw' AND table_name = 'kafka_sink_raw_transfer'
                ORDER BY ordinal_position
            """
            )

            print("\nKafka Sink Table schema:")
            print("-" * 60)
            for row in cur.fetchall():
                print(
                    f"  {row[0]:<20} {row[1]:<15} {'NULL' if row[2] == 'YES' else 'NOT NULL'}"
                )
            print("-" * 60)


if __name__ == "__main__":
    init_schema()
