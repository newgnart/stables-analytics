## Event data
Raw Events data is indexed with [HyperIndex](https://docs.envio.dev/docs/HyperIndex/overview), a blockchain indexing framework that transforms on-chain events into structured, queryable databases with GraphQL APIs.

More details: [Envio](https://docs.envio.dev/docs/HyperIndex/overview)

**To run the indexer:**
```bash
git clone https://github.com/newgnart/envio-stablecoins.git
pnpm dev
```

When the indexer is running, you have few options:

- **Extract/save to parquet files**, this will save the data to `.data/raw/transfer_{start_block}_{end_block}.parquet`

```bash
uv run scripts/el/extract_graphql.py \
--query-file scripts/el/stables_transfers.graphql \
-f transfer \
--from_block 23650000
--to_block 23660000
-v
```

- **Stream/load directly from the indexer to the postgres**
```bash
uv run scripts/el/stream_graphql.py \
-e http://localhost:8080/v1/graphql \
--fields id,blockNumber,timestamp,contractAddress,from,to,value \
--graphql-table stablesTransfers \
-c postgres \
-s raw \
-t raw_transfer
```

- **Real-time streaming via Kafka** (recommended for production)

  Kafka enables decoupled, scalable real-time data streaming with features like replay, backpressure handling, and multi-consumer support.

  ```bash
  # Terminal 1: Start Kafka producer (polls GraphQL every 5 seconds)
  uv run python scripts/kafka/produce_from_graphql.py \
    --endpoint http://localhost:8080/v1/graphql \
    --kafka-topic stablecoin-transfers \
    --poll-interval 5 \
    -v

  # Terminal 2: Start PostgreSQL consumer (batch writes)
  uv run python scripts/kafka/consume_to_postgres.py \
    --kafka-topic stablecoin-transfers \
    --schema raw \
    --table transfers_kafka \
    --batch-size 100 \
    -v

  # Terminal 3: Start alert monitor (optional - detects large transfers)
  uv run python scripts/kafka/monitor_alerts.py \
    --large-transfer 1000000 \
    --critical-transfer 10000000 \
    -v
  ```

  Benefits:
  - **Decoupling**: Separate data ingestion from processing
  - **Scalability**: Multiple consumers can process the same stream
  - **Replay**: Reprocess historical events from Kafka logs (7-day retention)
  - **Real-time alerting**: Detect large transfers within seconds

  See [scripts/kafka/README.md](../../scripts/kafka/README.md) for detailed configuration and monitoring.

- **Move data from postgres to snowflake**
```bash
uv run python scripts/el/pg2sf_raw_transfer.py \
--from_block 23650000 \
--to_block 23660000 \
-v
```

