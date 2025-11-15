"""
Stables Analytics - Kafka Consumer DAG

Scheduled consumption of transfer events from Kafka to PostgreSQL.
This DAG runs on a schedule to consume batches of messages from Kafka
and load them into the PostgreSQL raw layer.

DAG ID: stables_kafka_consumer_dag
Schedule: Every 10 minutes (configurable)
"""

import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum


@dag(
    dag_id="stables_kafka_consumer_dag",
    schedule="*/10 * * * *",  # Every 10 minutes
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["stables", "kafka", "consumer", "etl"],
    max_active_runs=1,  # Prevent overlapping runs
    dagrun_timeout=timedelta(minutes=15),
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
    },
    description="Consume transfer events from Kafka and load into PostgreSQL",
    doc_md="""
    ### Stables Analytics - Kafka Consumer DAG

    This DAG consumes transfer events from Kafka and loads them into PostgreSQL.

    **Schedule**: Every 10 minutes (configurable)

    **Tasks**:
    1. **check_kafka_connection**: Verify Kafka broker is reachable
    2. **check_postgres_connection**: Verify PostgreSQL is reachable
    3. **consume_kafka_batch**: Consume messages from Kafka and load to database
    4. **log_consumption_stats**: Log statistics and update metrics

    **Configuration**:
    - `MAX_MESSAGES_PER_RUN`: Maximum messages to consume per run (default: None = unlimited)
    - `MAX_DURATION_SECONDS`: Maximum time per run in seconds (default: 480 = 8 minutes)
    - `BATCH_SIZE`: Number of messages per database batch (default: 100)

    **Environment Variables** (configured in Airflow):
    - `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address (e.g., kafka:9092)
    - `KAFKA_TOPIC`: Topic to consume from (default: stables-transfers)
    - `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`

    **Monitoring**:
    - Check task logs for consumption statistics
    - Monitor XCom for detailed metrics
    - Check `raw.kafka_sink_raw_transfer` table for loaded data
    """,
)
def stables_kafka_consumer_dag():
    """
    ### Kafka Consumer DAG

    Scheduled consumption of transfer events from Kafka to PostgreSQL.
    """

    # Configuration (can be overridden via Airflow Variables)
    MAX_MESSAGES_PER_RUN = None  # None = unlimited (time-bound only)
    MAX_DURATION_SECONDS = 480  # 8 minutes (leave 2 min buffer for 10 min schedule)
    BATCH_SIZE = 100  # Messages per database batch

    @task()
    def check_kafka_connection():
        """
        #### Check Kafka Connection

        Verify that Kafka broker is reachable and topic exists.
        """
        logger = logging.getLogger(__name__)
        logger.info("🔍 Checking Kafka connection...")

        try:
            from confluent_kafka.admin import AdminClient
            import os

            bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
            topic_name = os.getenv("KAFKA_TOPIC", "stables-transfers")

            admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
            metadata = admin_client.list_topics(timeout=10)

            if topic_name not in metadata.topics:
                raise Exception(f"Topic '{topic_name}' not found in Kafka")

            topic_metadata = metadata.topics[topic_name]
            partition_count = len(topic_metadata.partitions)

            logger.info(f"✅ Kafka connected: {bootstrap_servers}")
            logger.info(f"✅ Topic '{topic_name}' exists with {partition_count} partition(s)")

            return {
                "status": "success",
                "bootstrap_servers": bootstrap_servers,
                "topic": topic_name,
                "partition_count": partition_count,
            }

        except Exception as e:
            logger.error(f"❌ Kafka connection check failed: {e}")
            raise

    @task()
    def check_postgres_connection():
        """
        #### Check PostgreSQL Connection

        Verify that PostgreSQL is reachable and schema exists.
        """
        logger = logging.getLogger(__name__)
        logger.info("🔍 Checking PostgreSQL connection...")

        try:
            pg_hook = PostgresHook(postgres_conn_id="postgres_kafka_default")
            conn = pg_hook.get_conn()
            cursor = conn.cursor()

            # Test connection
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]

            # Check schema
            cursor.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = 'raw'
                """
            )
            raw_schema = cursor.fetchone()

            if not raw_schema:
                logger.warning("⚠️ Schema 'raw' does not exist - will be created")

            # Get current row count
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'raw' AND table_name = 'kafka_sink_raw_transfer'
                """
            )
            table_exists = cursor.fetchone()[0] > 0

            row_count = 0
            if table_exists:
                cursor.execute("SELECT COUNT(*) FROM raw.kafka_sink_raw_transfer")
                row_count = cursor.fetchone()[0]

            cursor.close()
            conn.close()

            logger.info(f"✅ PostgreSQL connected")
            logger.info(f"✅ Current rows in raw.kafka_sink_raw_transfer: {row_count:,}")

            return {
                "status": "success",
                "row_count_before": row_count,
                "table_exists": table_exists,
            }

        except Exception as e:
            logger.error(f"❌ PostgreSQL connection check failed: {e}")
            raise

    @task()
    def consume_kafka_batch(kafka_check, postgres_check):
        """
        #### Consume Kafka Batch

        Consume messages from Kafka and load into PostgreSQL.
        """
        logger = logging.getLogger(__name__)
        logger.info("🚀 Starting Kafka consumption...")

        try:
            # Import here to avoid module-level imports
            from scripts.kafka.consume_batch import consume_kafka_batch

            # Run consumption
            result = consume_kafka_batch(
                max_messages=MAX_MESSAGES_PER_RUN,
                max_duration_seconds=MAX_DURATION_SECONDS,
                batch_size=BATCH_SIZE,
            )

            # Log results
            logger.info("=" * 60)
            logger.info("CONSUMPTION RESULTS")
            logger.info("=" * 60)
            logger.info(f"Status: {result['status']}")
            logger.info(f"Messages consumed: {result['messages_consumed']:,}")
            logger.info(f"Rows inserted: {result['rows_inserted']:,}")
            logger.info(f"Batches processed: {result['batches_processed']}")
            logger.info(f"Duration: {result['duration_seconds']:.2f}s")

            if "errors" in result:
                logger.warning(f"⚠️ Errors encountered: {result['total_errors']}")
                for error in result.get("errors", [])[:5]:
                    logger.warning(f"  - {error}")

            logger.info("=" * 60)

            # Raise error if consumption completely failed
            if result["status"] == "error":
                raise Exception("Kafka consumption failed - check logs for details")

            return result

        except Exception as e:
            logger.error(f"❌ Kafka consumption failed: {e}", exc_info=True)
            raise

    @task()
    def log_consumption_stats(consumption_result, postgres_check_before):
        """
        #### Log Consumption Statistics

        Log final statistics and verify data was loaded correctly.
        """
        logger = logging.getLogger(__name__)

        try:
            # Get final row count
            pg_hook = PostgresHook(postgres_conn_id="postgres_kafka_default")
            conn = pg_hook.get_conn()
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM raw.kafka_sink_raw_transfer")
            final_row_count = cursor.fetchone()[0]

            cursor.close()
            conn.close()

            # Calculate statistics
            initial_count = postgres_check_before["row_count_before"]
            rows_added = final_row_count - initial_count
            messages_consumed = consumption_result["messages_consumed"]
            rows_inserted = consumption_result["rows_inserted"]

            logger.info("=" * 60)
            logger.info("FINAL STATISTICS")
            logger.info("=" * 60)
            logger.info(f"Initial row count: {initial_count:,}")
            logger.info(f"Final row count: {final_row_count:,}")
            logger.info(f"Rows added: {rows_added:,}")
            logger.info(f"Messages consumed: {messages_consumed:,}")
            logger.info(f"Rows inserted (deduplicated): {rows_inserted:,}")
            logger.info(f"Duration: {consumption_result['duration_seconds']:.2f}s")

            if messages_consumed > 0:
                throughput = messages_consumed / consumption_result["duration_seconds"]
                logger.info(f"Throughput: {throughput:.1f} messages/second")

            logger.info("=" * 60)

            # Store in XCom for downstream tasks
            return {
                "initial_count": initial_count,
                "final_count": final_row_count,
                "rows_added": rows_added,
                "messages_consumed": messages_consumed,
                "rows_inserted": rows_inserted,
                "duration_seconds": consumption_result["duration_seconds"],
                "status": consumption_result["status"],
            }

        except Exception as e:
            logger.error(f"❌ Failed to log statistics: {e}", exc_info=True)
            raise

    # Define task dependencies
    kafka_check = check_kafka_connection()
    postgres_check = check_postgres_connection()

    # Both checks must pass before consuming
    consumption = consume_kafka_batch(kafka_check, postgres_check)

    # Log stats after consumption
    stats = log_consumption_stats(consumption, postgres_check)


# Instantiate the DAG
stables_kafka_consumer_dag()
