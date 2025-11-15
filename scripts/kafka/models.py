"""Data models for Kafka messages and database operations."""

from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class TransferEvent:
    """Model for stablecoin transfer event from GraphQL indexer."""

    id: str
    block_number: int
    timestamp: str
    contract_address: str
    from_address: str
    to_address: str
    value: str

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_graphql(cls, data: dict) -> "TransferEvent":
        """Create from GraphQL response data.

        Maps GraphQL field names to internal field names.
        """
        return cls(
            id=data["id"],
            block_number=data["blockNumber"],
            timestamp=data["timestamp"],
            contract_address=data["contractAddress"],
            from_address=data["from"],
            to_address=data["to"],
            value=data["value"],
        )


@dataclass
class TransferData:
    """Transfer data model for database insertion."""

    id: str
    block_number: int
    timestamp: str
    contract_address: str
    from_address: str
    to_address: str
    value: str
