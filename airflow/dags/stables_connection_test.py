"""
Stables Analytics - Connection Test DAG

Tests connections to PostgreSQL and Kafka infrastructure.
Run this DAG first after setting up Airflow to verify all connections work.

DAG ID: stables_connection_test
Schedule: Manual trigger only
"""

import logging

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum


@dag(
    dag_id="stables_connection_test",
    schedule=None,  # Manual trigger only
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["connection-test"],
    max_active_runs=1,
    description="Test connections to PostgreSQL",
)
def stables_connection_test():
    """
    ### Stables Analytics Connection Test DAG

    Tests connections to:
    - PostgreSQL (main data warehouse)
    - Kafka (message broker)

    Run this DAG after initial setup to verify infrastructure is configured correctly.
    """

    @task()
    def test_postgres_connection():
        """
        #### PostgreSQL Connection Test
        Tests connection to PostgreSQL and validates basic functionality.
        """
        logger = logging.getLogger(__name__)
        logger.info("🔍 Testing PostgreSQL connection...")

        try:
            pg_hook = PostgresHook(postgres_conn_id="postgres_kafka_default")
            conn = pg_hook.get_conn()
            cursor = conn.cursor()

            # Test basic connection
            cursor.execute("SELECT 1")
            test_result = cursor.fetchone()

            if test_result and test_result[0] == 1:
                logger.info("✅ PostgreSQL connection successful")
                cursor.close()
                conn.close()
                return "PostgreSQL connection verified"
            else:
                raise Exception("PostgreSQL connection test failed")
        except Exception as e:
            error_msg = f"PostgreSQL connection failed: {e}"
            logger.error(f"❌ {error_msg}")
            raise Exception(error_msg) from e

    # Execute connection tests in parallel (no dependencies)
    postgres_test = test_postgres_connection()
    # kafka_test = test_kafka_connection()
    # docker_test = test_docker_socket()

    # Generate summary after all tests complete
    # summary = generate_summary_report(postgres_test, kafka_test, docker_test)


# Instantiate the DAG
stables_connection_test()
