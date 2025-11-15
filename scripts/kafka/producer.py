#!/usr/bin/env python3
"""Kafka producer for streaming stablecoin transfer data from GraphQL indexer."""

import json
import os
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

from scripts.kafka.data_generator import GraphQLDataGenerator
from scripts.utils.config import get_kafka_config
from scripts.utils.logger import logger


def delivery_callback(err, msg):
    """Callback for Kafka message delivery confirmation."""
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(
            f"Message delivered to {msg.topic()}[{msg.partition()}] "
            f"@ offset {msg.offset()}"
        )


def create_kafka_topic(bootstrap_servers: str, topic_name: str) -> bool:
    """Create Kafka topic if it doesn't exist.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        topic_name: Name of topic to create

    Returns:
        True if successful, False otherwise
    """
    try:
        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

        # Check if topic already exists
        metadata = admin_client.list_topics(timeout=10)
        if topic_name in metadata.topics:
            logger.info(f"Topic '{topic_name}' already exists")
            return True

        # Create new topic
        new_topic = NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)

        fs = admin_client.create_topics([new_topic])

        # Wait for topic creation to complete
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logger.info(f"✅ Topic '{topic}' created successfully")
                return True
            except Exception as e:
                logger.error(f"❌ Failed to create topic '{topic}': {e}")
                return False

    except Exception as e:
        logger.error(f"❌ Error creating topic: {e}")
        return False


def main() -> None:
    """Main producer loop - fetches from GraphQL and streams to Kafka."""
    load_dotenv()

    # GraphQL configuration
    graphql_endpoint = os.getenv("GRAPHQL_ENDPOINT", "http://localhost:8080/v1/graphql")
    poll_interval = int(os.getenv("POLL_INTERVAL", "10"))  # seconds

    # Kafka configuration
    kafka_config = get_kafka_config()
    bootstrap = kafka_config.bootstrap_servers
    topic = kafka_config.topic

    logger.info(f"GraphQL Endpoint: {graphql_endpoint}")
    logger.info(f"Kafka Bootstrap: {bootstrap}")
    logger.info(f"Poll Interval: {poll_interval}s")

    # Create topic if it doesn't exist
    logger.info("Creating Kafka topic...")
    if not create_kafka_topic(bootstrap, topic):
        logger.error("Failed to create topic. Exiting.")
        return

    # Configure producer
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "stables-transfer-producer",
            "acks": "all",
        }
    )

    # Initialize GraphQL data generator
    query = """
    query stablesTransfers {
      stablesTransfers(order_by: { blockNumber: desc }) {
        id
        blockNumber
        timestamp
        contractAddress
        from
        to
        value
      }
    }
    """

    generator = GraphQLDataGenerator(endpoint=graphql_endpoint, query=query)

    try:
        message_count = 0
        logger.info("Starting to produce messages from GraphQL indexer...")

        while True:
            # Fetch new transfers since last block
            transfers = generator.fetch_latest_transfers(
                limit=100000, from_block=generator.last_block
            )

            if not transfers:
                logger.debug(
                    f"No new transfers found (last block: {generator.last_block})"
                )
            else:
                logger.info(f"Fetched {len(transfers)} new transfer(s)")

            # Produce each transfer to Kafka
            for transfer in transfers:
                producer.produce(
                    topic,
                    key=transfer.id,
                    value=json.dumps(transfer.to_dict()),
                    callback=delivery_callback,
                )
                producer.poll(0)  # Trigger delivery reports

                message_count += 1
                # logger.info(
                #     f"📤 Produced message #{message_count}: id={transfer.id} | "
                #     f"block={transfer.block_number} | "
                #     f"contract={transfer.contract_address[:10]}... | "
                #     f"value={transfer.value}"
                # )

            if message_count > 0 and message_count % 10 == 0:
                logger.info(f"Total messages produced: {message_count}")

            # Wait before next poll
            time.sleep(poll_interval)

    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        producer.flush(10.0)
        logger.info(f"Producer shutdown complete. Total messages: {message_count}")


if __name__ == "__main__":
    main()
