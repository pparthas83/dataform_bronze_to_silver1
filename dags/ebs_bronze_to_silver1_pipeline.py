"""
Cloud Composer (Apache Airflow) DAG to Automate Dataform Bronze to Silver 1 Pipeline.

Orchestrates:
1. Dataform repository compilation via Dataform API.
2. Incremental MERGE execution of Oracle EBS Silver 1 tables in BigQuery.
3. Execution of 7 automated Data Quality & Referential Integrity Assertions.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from google.cloud.airflow.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

# -----------------------------------------------------------------------------
# Configuration Variables (Configured via Airflow Variables or Environment)
# -----------------------------------------------------------------------------
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID", default_var="YOUR_GCP_PROJECT_ID")
GCP_REGION = Variable.get("GCP_REGION", default_var="us-central1")
DATAFORM_REPOSITORY_ID = Variable.get("DATAFORM_REPOSITORY_ID", default_var="dataform_bronze_to_silver1")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# -----------------------------------------------------------------------------
# Airflow DAG Definition
# -----------------------------------------------------------------------------
with DAG(
    dag_id="ebs_bronze_to_silver1_pipeline",
    default_args=DEFAULT_ARGS,
    description="Automated Dataform Bronze to Silver 1 transformation for Oracle EBS OM data",
    schedule_interval="0 * * * *",  # Runs hourly at minute 0
    catchup=False,
    tags=["dataform", "oracle_ebs", "bronze_to_silver1", "order_management"],
) as dag:

    # Task 1: Compile Dataform repository from latest 'main' branch
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main",
        },
    )

    # Task 2: Trigger Dataform workflow execution (Silver 1 tables + Assertions)
    execute_dataform_workflow = DataformCreateWorkflowInvocationOperator(
        task_id="execute_dataform_workflow",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        repository_id=DATAFORM_REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
        },
    )

    # Task Dependencies
    create_compilation_result >> execute_dataform_workflow
