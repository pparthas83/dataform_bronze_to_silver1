#!/usr/bin/env python3
"""
Architecture Diagram Generator: Oracle GoldenGate -> BQ Bronze -> Airflow -> Dataform -> BQ Silver 1

Prerequisites:
    pip install diagrams
    # System package: graphviz (dot)

Execution:
    python3 scripts/generate_architecture_diagram.py
    # or with uv:
    uv run --with diagrams python3 scripts/generate_architecture_diagram.py
"""

from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.database import Oracle
from diagrams.gcp.analytics import BigQuery, Composer
from diagrams.gcp.devtools import Code
from diagrams.gcp.operations import Monitoring

graph_attr = {
    "fontsize": "16",
    "fontname": "Helvetica",
    "bgcolor": "#FAFAFA",
    "pad": "0.6",
    "rankdir": "LR",
    "splines": "ortho",
}

node_attr = {
    "fontname": "Helvetica",
    "fontsize": "11",
}

edge_attr = {
    "fontname": "Helvetica",
    "fontsize": "10",
}

with Diagram(
    "Oracle GoldenGate to BigQuery Silver 1 Architecture (Dual-Timestamp Pattern)",
    filename="pipeline_architecture",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
):
    # 1. External Tier: Oracle GoldenGate Continuous CDC
    with Cluster("External Tier (Oracle EBS / GoldenGate)"):
        oracle_ogg = Oracle("Oracle GoldenGate\n(Real-Time CDC Stream)\n[LAST_UPDATE_DATE]")

    # 2. Google Cloud Platform Environment
    with Cluster("Google Cloud Platform"):

        # Storage Tier: BigQuery Medallion Lakehouse
        with Cluster("BigQuery Storage Tier"):
            bronze_tbl = BigQuery("Bronze Raw CDC\n(oracle_ebs_bronze)\n[BQ_CREATED_TIMESTAMP]")
            silver_tbl = BigQuery("Silver 1 Cleansed\n(oracle_ebs_silver1)\n[stg_ebs_oe_order_headers]")

        # Orchestration Tier: Cloud Composer (Hourly Airflow DAG)
        with Cluster("Cloud Composer (Airflow Orchestration Scope)"):
            with Cluster("Hourly DAG: ebs_bronze_to_silver1_pipeline\n(Schedule: '0 * * * *' | max_active_runs=1)"):
                compile_task = Composer("1. DataformCreateCompilation\nResultOperator")
                invoke_task = Composer("2. DataformCreateWorkflow\nInvocationOperator")
                compile_task >> Edge(label="Compiles 'main'", color="#1976D2", style="bold") >> invoke_task

        # Transformation Engine: Dataform Core v3
        with Cluster("Dataform Core v3 Execution Engine"):
            dataform = Code("Incremental MERGE\n(BQ_CREATED_TIMESTAMP Watermark\n& LAST_UPDATE_DATE Deduplication)")
            assertions = Monitoring("7 Automated Data\nQuality Assertions")

    # Pipeline End-to-End Execution Flow
    # 1. GoldenGate streams real-time CDC appends into Bronze with BQ_CREATED_TIMESTAMP
    oracle_ogg >> Edge(label="Real-time CDC appends\n(Independent stream)", color="#E65100", style="bold") >> bronze_tbl

    # 2. Airflow invoke task triggers Dataform workflow
    invoke_task >> Edge(label="Invokes hourly\nworkflow execution", color="#1976D2", style="bold") >> dataform

    # 3. Dataform reads Bronze incremental delta with BQ_CREATED_TIMESTAMP watermark
    bronze_tbl >> Edge(label="Scans arrival delta\n(BQ_CREATED_TIMESTAMP)", color="#388E3C", style="dashed") >> dataform

    # 4. Dataform executes atomic MERGE into Silver 1
    dataform >> Edge(label="Atomic MERGE\n(ROW_NUMBER deduplication)", color="#388E3C", style="bold") >> silver_tbl

    # 5. Assertions validate data integrity in Silver 1
    silver_tbl >> Edge(label="Validates integrity\n(Uniqueness & FK)", color="#00796B", style="dashed") >> assertions

if __name__ == "__main__":
    print("Architecture diagram successfully generated at pipeline_architecture.png")
