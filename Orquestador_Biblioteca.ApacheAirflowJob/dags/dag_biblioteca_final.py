from airflow import DAG
from datetime import datetime
from airflow.providers.microsoft.fabric.operators.run_item import MSFabricRunItemOperator

with DAG(
    dag_id="biblioteca_final_v100",
    start_date=datetime(2026, 4, 10),
    schedule_interval=None,
    catchup=False
) as dag:

    ingestion = MSFabricRunItemOperator(
        task_id="ejecutar_ingestion",
        workspace_id="89c188da-fb80-4076-9a89-31304909b91d",
        item_id="64217098-fc01-4f29-bf19-13b4efd19a64",
        job_type="RunNotebook",
        fabric_conn_id="fabric_default",
        deferrable=False,
        wait_for_termination=True
    )

    transformation = MSFabricRunItemOperator(
        task_id="ejecutar_transformacion",
        workspace_id="89c188da-fb80-4076-9a89-31304909b91d",
        item_id="f12bed54-b968-4e4c-9bfc-7fbbc3d8383d",
        job_type="RunNotebook",
        fabric_conn_id="fabric_default",
        deferrable=False,
        wait_for_termination=True
    )

    ingestion >> transformation

    # Versi�n final recuperada