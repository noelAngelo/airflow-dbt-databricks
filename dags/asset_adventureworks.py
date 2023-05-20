import json
import pendulum
from datetime import timedelta
from jinja2.exceptions import UndefinedError
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.datasets import Dataset
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.models.param import Param


# Define connections
DATABRICKS_CONN_ID = 'databricks_default'
AWS_CONN_ID = 'aws_conn_default'
GITHUB_CONN_ID = 'github_actions_default'

# Define GitHub configurations
GITHUB_OWNER = 'Deloitte'
GITHUB_REPOSITORY = 'mdp-dbt-databricks'
GITHUB_BRANCH = 'deployment/test'
GITHUB_WORKFLOW_ID = 'run-elementary.yml'

# Define S3 configurations
ELEMENTARY_S3_BUCKET = 'ddloa-asset'
ELEMENTARY_REPORT_FILENAME = 'elementary_report.html'

# Define datasets
elementary_report = Dataset(f"s3://{ELEMENTARY_S3_BUCKET}/{ELEMENTARY_REPORT_FILENAME}")

# Define defaults
default_args = {
    'catchup': False,
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': pendulum.datetime(2021, 1, 1, tz="UTC")
}
default_config = {
    'databricks': {
        'notebook_params': {
            'destination_catalog': 'edp',
            'destination_schema': 'lakehouse_bronze_aw',
            'source_directory': 's3://ddloa-artifacts/source_data/adventureworks',
            'target_directory': 's3://ddloa-artifacts/adventureworks/lakehouse_bronze_aw',
            'clean_run': 'False',
            'action': 'load'
        },
        'bronze_workflow_job_id': '10627615967856',
        'silver_workflow_id': '899035929538342',
        'gold_workflow_id': '236761744252830'
    },
    'elementary': {
        's3_bucket': ELEMENTARY_S3_BUCKET,
        'report_filename': ELEMENTARY_REPORT_FILENAME
    }
}

# Define parameters
dag_params = {
    "DATABRICKS__NOTEBOOK_PARAMS": Param(
        default=default_config['databricks']['notebook_params'],
        type="object"
    ),
    "DATABRICKS__BRONZE_WORKFLOW_JOB_ID": Param(
        default=default_config['databricks']['bronze_workflow_job_id'],
        type="string",
        description="The AdventureWorks Bronze Workflow in Databricks"
    ),
    "DATABRICKS__SILVER_WORKFLOW_JOB_ID": Param(
        default=default_config['databricks']['silver_workflow_id'],
        type="string",
        description="The AdventureWorks SILVER Workflow in Databricks"
    ),
    "DATABRICKS__GOLD_WORKFLOW_JOB_ID": Param(
        default=default_config['databricks']['gold_workflow_id'],
        type="string",
        description="The AdventureWorks GOLD Workflow in Databricks"
    ),

}


# Helpers
def get_config(config_key: str, default) -> str:
    try:
        config_value = "{{ dag_run.conf['" + config_key + "'] }}"
        return config_value
    except UndefinedError:
        return default


with DAG(
        dag_id='asset_adventureworks',
        default_args=default_args,
        tags=['asset', 'adventureworks'],
        max_active_runs=1,
        max_active_tasks=1,
        params=dag_params,
        schedule=None,
        schedule_interval=None
) as dag:

    bronze_workflow = DatabricksRunNowOperator(
        task_id='bronze_workflow_task',
        job_id="{{ params.DATABRICKS__BRONZE_WORKFLOW_JOB_ID }}",
        notebook_params={
            'destination_catalog': '{{ params.DATABRICKS__NOTEBOOK_PARAMS.destination_catalog }}',
            'destination_schema': '{{ params.DATABRICKS__NOTEBOOK_PARAMS.destination_schema }}',
            'source_directory': '{{ params.DATABRICKS__NOTEBOOK_PARAMS.source_directory }}',
            'target_directory': '{{ params.DATABRICKS__NOTEBOOK_PARAMS.target_directory }}',
            'clean_run': '{{ params.DATABRICKS__NOTEBOOK_PARAMS.clean_run }}',
            'action': '{{ params.DATABRICKS__NOTEBOOK_PARAMS.action }}',
        },
        databricks_conn_id=DATABRICKS_CONN_ID
    )

    silver_workflow = DatabricksRunNowOperator(
        task_id='silver_workflow_task',
        job_id="{{ params.DATABRICKS__SILVER_WORKFLOW_JOB_ID }}",
        databricks_conn_id=DATABRICKS_CONN_ID
    )

    gold_workflow = DatabricksRunNowOperator(
        task_id='gold_workflow_task',
        job_id="{{ params.DATABRICKS__GOLD_WORKFLOW_JOB_ID }}",
        databricks_conn_id=DATABRICKS_CONN_ID
    )

    conn = BaseHook.get_connection(GITHUB_CONN_ID)
    gh_job = SimpleHttpOperator(
        task_id='elementary_report_task',
        http_conn_id=GITHUB_CONN_ID,
        method='POST',
        endpoint=f'/repos/{GITHUB_OWNER}/{GITHUB_REPOSITORY}/actions/workflows/{GITHUB_WORKFLOW_ID}/dispatches',
        headers={
            'Accept': 'application/vnd.github+json',
            'Authorization': f'Bearer {conn.password}',
            'X-GitHub-Api-Version': '2022-11-28'
        },
        trigger_rule='all_done',
        data=json.dumps({'ref': GITHUB_BRANCH}),
        outlets=[Dataset(f"s3://{ELEMENTARY_S3_BUCKET}/{ELEMENTARY_REPORT_FILENAME}")],
        dag=dag
    )

    # Dummy operators
    start_op = EmptyOperator(task_id='start_op')
    end_op = EmptyOperator(task_id='end_op', outlets=[elementary_report])

    # Define orchestration workflow
    start_op >> bronze_workflow >> silver_workflow >> gold_workflow >> gh_job >> end_op
