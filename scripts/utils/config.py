"""Configuration utilities for Kafka integration."""

import os
from dataclasses import dataclass


@dataclass
class KafkaConfig:
    """Kafka configuration settings."""

    bootstrap_servers: str
    topic: str = "stables-transfers"
    batch_size: int = 100
    max_records: int | None = None

    @classmethod
    def from_env(cls) -> "KafkaConfig":
        """Create configuration from environment variables."""
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
        topic = os.getenv("KAFKA_TOPIC", "stables-transfers")
        batch_size = int(os.getenv("KAFKA_BATCH_SIZE", "100"))
        max_records_str = os.getenv("KAFKA_MAX_RECORDS")
        max_records = int(max_records_str) if max_records_str else None
        return cls(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            batch_size=batch_size,
            max_records=max_records,
        )


def get_kafka_config() -> KafkaConfig:
    """Get Kafka configuration from environment."""
    return KafkaConfig.from_env()
