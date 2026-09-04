"""
Cloud Composer (Apache Airflow) DAG to Automate Hourly Dataform Bronze to Silver 1 Pipeline.

Architecture Pattern:
1. Oracle GoldenGate continuously streams CDC changes in real time to BigQuery Bronze (oracle_ebs_bronze).
   Each row includes BQ_CREATED_TIMESTAMP (ingestion arrival time in BigQuery) and LAST_UPDATE_DATE (EBS business update time).
2. Every one hour, this Airflow DAG triggers on schedule ('0 * * * *') to invoke Dataform Core v3.
3. Dataform executes incremental deduplication and atomic MERGE into Silver 1 (oracle_ebs_silver1):
   - Ingestion Watermark: BQ_CREATED_TIMESTAMP >= MAX(bq_created_timestamp) - INTERVAL 5 MINUTE
   - Business Deduplication: ROW_NUMBER() OVER (PARTITION BY pk ORDER BY LAST_UPDATE_DATE DESC)
4. Dataform automatically validates 7 Data Quality & Referential Integrity Assertions.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

# -----------------------------------------------------------------------------
# Configuration Constants
# -----------------------------------------------------------------------------
PROJECT_ID = "YOUR_GCP_PROJECT_ID"
REGION = "us-central1"
REPOSITORY_ID = "dataform_bronze_to_silver1"
GIT_COMMITISH = "main"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["data-alerts@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -----------------------------------------------------------------------------
# DAG Definition: Hourly Dataform Bronze to Silver 1 Incremental Pipeline
# -----------------------------------------------------------------------------
with DAG(
    dag_id="ebs_bronze_to_silver1_pipeline",
    default_args=DEFAULT_ARGS,
    description="Hourly automated Dataform Bronze to Silver 1 incremental pipeline for Oracle EBS",
    schedule_interval="0 * * * *",  # Runs hourly at minute 0
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,  # Concurrency guard: prevents overlapping hourly runs
    tags=["dataform", "oracle_ebs", "golden_gate", "bronze_to_silver1", "hourly"],
) as dag:

    # Task 1: Compile Dataform repository from latest 'main' branch
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": GIT_COMMITISH,
        },
    )

    # Task 2: Execute Dataform workflow to run incremental transformations & assertions
    execute_dataform_workflow = DataformCreateWorkflowInvocationOperator(
        task_id="execute_dataform_workflow",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": (
                "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
            ),
        },
    )

    # Orchestration Flow: Compile Repository -> Execute Workflow Invocation
    create_compilation_result >> execute_dataform_workflow
