#!/usr/bin/env python3
"""Kafka consumer for stablecoin transfer data - loads into PostgreSQL."""

import json

from confluent_kafka import Consumer
from dotenv import load_dotenv

from scripts.kafka.database import (
    batch_insert_transfers,
    create_transfers_table,
    get_postgres_dsn,
)
from scripts.kafka.models import TransferData, TransferEvent
from scripts.utils.config import get_kafka_config
from scripts.utils.logger import logger


def main() -> None:
    """Main consumer loop - consumes from Kafka and loads to PostgreSQL."""
    load_dotenv()

    # Initialize PostgreSQL connection
    dsn = get_postgres_dsn()
    if not create_transfers_table(dsn):
        logger.error("Failed to create transfers table. Exiting.")
        return

    # Configure Kafka consumer with production-ready settings
    kafka_config = get_kafka_config()
    consumer_config = {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "group.id": "stables-transfer-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    logger.info(f"Connecting to Kafka at: {kafka_config.bootstrap_servers}")
    logger.info(
        f"Consumer config: group_id={consumer_config['group.id']}, "
        f"auto_commit={consumer_config['enable.auto.commit']}, "
        f"batch_size={kafka_config.batch_size}"
    )
    if kafka_config.max_records:
        logger.info(f"Max records limit: {kafka_config.max_records}")

    consumer = Consumer(consumer_config)
    consumer.subscribe([kafka_config.topic])
    logger.info(f"Subscribed to topic: {kafka_config.topic}")

    rows_written = 0
    messages_processed = 0
    batch: list[TransferData] = []

    def flush_batch() -> int:
        """Flush current batch to database and return number of rows written."""
        nonlocal batch
        if not batch:
            return 0

        inserted = batch_insert_transfers(dsn, batch)
        batch.clear()
        return inserted

    try:
        while True:
            # Check max records limit
            if kafka_config.max_records and messages_processed >= kafka_config.max_records:
                logger.info(
                    f"Reached max records limit ({kafka_config.max_records}). "
                    "Flushing final batch and exiting."
                )
                rows_written += flush_batch()
                break

            msg = consumer.poll(1.0)
            if msg is None:
                # No message available - flush batch if it exists
                if batch:
                    rows_written += flush_batch()
                    logger.debug("Flushed batch during idle period")
                continue

            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                event_dict = json.loads(msg.value().decode("utf-8"))
                messages_processed += 1

                # Parse event with data model
                event = TransferEvent(**event_dict)

                # Add to batch
                transfer_data = TransferData(
                    id=event.id,
                    block_number=event.block_number,
                    timestamp=event.timestamp,
                    contract_address=event.contract_address,
                    from_address=event.from_address,
                    to_address=event.to_address,
                    value=event.value,
                )
                batch.append(transfer_data)

                # Flush batch when it reaches the configured size
                if len(batch) >= kafka_config.batch_size:
                    rows_written += flush_batch()
                    logger.info(
                        f"Progress: {messages_processed} messages processed, "
                        f"{rows_written} rows written"
                    )

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

    except KeyboardInterrupt:
        logger.info("Shutdown requested. Flushing final batch...")
        rows_written += flush_batch()
        logger.info(
            f"Final stats: {messages_processed} messages, {rows_written} rows written"
        )
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
