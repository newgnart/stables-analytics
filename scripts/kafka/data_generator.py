"""Data generation service for GraphQL-based Kafka producer."""

from datetime import UTC, datetime
from typing import Dict, Any

import requests
from dotenv import load_dotenv

from scripts.utils.logger import logger
from scripts.kafka.models import TransferEvent


class GraphQLDataGenerator:
    """Fetches real-time transfer data from GraphQL indexer."""

    def __init__(self, endpoint: str, query: str):
        """Initialize GraphQL data generator.

        Args:
            endpoint: GraphQL endpoint URL
            query: GraphQL query string
        """
        self.endpoint = endpoint
        self.query = query
        self.session = requests.Session()
        self.last_block = 0

    def fetch_latest_transfers(
        self, limit: int = 10000, from_block: int = None
    ) -> list[TransferEvent]:
        """Fetch latest transfer events from GraphQL endpoint.

        Args:
            limit: Maximum number of transfers to fetch
            from_block: Starting block number (fetches newer blocks)

        Returns:
            List of TransferEvent objects
        """
        # Modify query to add limit and block filter
        query = self._build_query(limit, from_block)

        payload = {"query": query}

        try:
            response = self.session.post(
                self.endpoint,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()

            data = response.json()
            if "errors" in data:
                logger.error(f"GraphQL errors: {data['errors']}")
                return []

            transfers_data = data.get("data", {}).get("stablesTransfers", [])

            # Convert to TransferEvent objects
            transfers = [
                TransferEvent.from_graphql(transfer) for transfer in transfers_data
            ]

            # Update last seen block
            if transfers:
                self.last_block = max(t.block_number for t in transfers)

            return transfers

        except Exception as e:
            logger.error(f"Failed to fetch transfers: {e}", exc_info=True)
            return []

    def _build_query(self, limit: int, from_block: int = None) -> str:
        """Build GraphQL query with filters.

        Args:
            limit: Maximum number of results
            from_block: Minimum block number filter

        Returns:
            GraphQL query string
        """
        where_clause = ""
        if from_block is not None:
            where_clause = f"where: {{ blockNumber: {{ _gt: {from_block} }} }}, "

        return f"""
        query stablesTransfers {{
          stablesTransfers({where_clause}order_by: {{ blockNumber: asc }}, limit: {limit}) {{
            id
            blockNumber
            timestamp
            contractAddress
            from
            to
            value
          }}
        }}
        """


def generate_transfer_from_indexer(
    endpoint: str, query_file: str = None
) -> TransferEvent:
    """Generate a single transfer event from the indexer.

    This is a simplified interface for compatibility with the producer pattern.

    Args:
        endpoint: GraphQL endpoint URL
        query_file: Optional path to query file

    Returns:
        TransferEvent object
    """
    # Default query
    query = """
    query stablesTransfers {
      stablesTransfers(order_by: { blockNumber: desc }, limit: 1) {
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

    generator = GraphQLDataGenerator(endpoint, query)
    transfers = generator.fetch_latest_transfers(limit=1)

    if transfers:
        return transfers[0]

    # Return a placeholder if no data available
    return TransferEvent(
        id="pending",
        block_number=0,
        timestamp=datetime.now(UTC).isoformat(),
        contract_address="0x0000000000000000000000000000000000000000",
        from_address="0x0000000000000000000000000000000000000000",
        to_address="0x0000000000000000000000000000000000000000",
        value="0",
    )
