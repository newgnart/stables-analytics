# Stables Analytics - Airflow DAGs

This directory contains Airflow DAGs for orchestrating the Stables Analytics pipeline.

## DAG Overview

### Core DAGs

1. **`stables_connection_test.py`** (Manual trigger)
   - Tests connections to PostgreSQL and Kafka
   - Run this first to verify infrastructure setup

2. **`stables_kafka_monitoring_dag.py`** (Every 10 minutes)
   - Monitors Kafka cluster health
   - Measures consumer lag
   - Triggers consumer if lag exceeds threshold

3. **`stables_consumer_control_dag.py`** (Triggered by monitoring or manual)
   - Controls Kafka consumer lifecycle
   - Processes messages from Kafka to PostgreSQL
   - Validates data loading

4. **`stables_transform_dag.py`** (Hourly or triggered)
   - Runs dbt transformations
   - Executes snapshots (SCD Type 2)
   - Runs data quality tests

5. **`stables_master_pipeline_dag.py`** (Daily at 2 AM or manual)
   - Orchestrates complete end-to-end pipeline
   - Health checks → Consumer → Transform → Report

## DAG Development Guidelines

### File Naming Convention
- Use snake_case for DAG files
- Prefix with `stables_` for project identification
- Suffix with `_dag.py` for clarity

### DAG ID Convention
- Use same name as file (without `.py`)
- Example: `stables_connection_test.py` → dag_id: `stables_connection_test`

### Tags
Always include relevant tags:
- `stables` - Project identifier
- `monitoring` / `etl` / `transform` - Pipeline stage
- `kafka` / `postgres` / `dbt` - Technology used

### Best Practices
1. Use `@dag` decorator pattern for cleaner code
2. Include comprehensive docstrings
3. Set `catchup=False` to avoid backfilling
4. Set `max_active_runs=1` for sequential execution
5. Use `pendulum` for timezone-aware dates
6. Add task-level retries with exponential backoff

## Testing DAGs

### Local Testing (before deploying)
```bash
# Parse DAG (check for syntax errors)
python airflow/dags/your_dag.py

# Test DAG structure
docker exec stables-airflow-scheduler airflow dags list

# Test specific task
docker exec stables-airflow-scheduler \
    airflow tasks test stables_connection_test check_postgres_ready 2024-01-15
```

### Manual Trigger
```bash
docker exec stables-airflow-scheduler \
    airflow dags trigger stables_connection_test
```

## Environment Variables

DAGs can access environment variables passed through docker-compose:
- `POSTGRES_*` - PostgreSQL credentials
- `KAFKA_*` - Kafka configuration
- `GRAPHQL_ENDPOINT` - GraphQL API endpoint

Use Airflow Variables for runtime configuration:
```python
from airflow.models import Variable

max_records = Variable.get("kafka_max_records_per_batch", default_var=10000)
```

## Airflow Connections

Configure these connections via `scripts/setup_airflow_connections.sh`:
- `postgres_kafka_default` - Main PostgreSQL database
- `snowflake_default` - Snowflake warehouse (optional)

## Directory Structure

```
airflow/
├── dags/                          # DAG definitions (Python files)
│   ├── README.md                 # This file
│   ├── stables_connection_test.py
│   ├── stables_kafka_monitoring_dag.py
│   ├── stables_consumer_control_dag.py
│   ├── stables_transform_dag.py
│   └── stables_master_pipeline_dag.py
├── plugins/                       # Custom operators/hooks
│   └── kafka_operators.py        # (Future) Custom Kafka operators
├── config/                        # Airflow configuration files
└── logs/                          # Execution logs (auto-generated)
```

## Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
