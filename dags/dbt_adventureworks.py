import json
import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.models.param import Param

# Define connections
DATABRICKS_CONN_ID = 'databricks_default'
DBT_CLOUD_CONN_ID = 'dbt_cloud_default'
GITHUB_CONN_ID = 'github_actions_default'

# Define Databricks configurations
DATABRICKS_CATALOG = Variable.get('databricks_catalog', default_var='edp_test')
DATABRICKS_SCHEMA = Variable.get('databricks_schema', default_var='lakehouse_bronze_aw')
DATABRICKS_HTTP_PATH = Variable.get('databricks_http_path', default_var='/sql/1.0/warehouses/e8a4932bf9b4298e')
DATABRICKS_SOURCE_DIR = Variable.get('databricks_source_dir', default_var='s3://ddloa-artifacts/source_data/adventureworks')
DATABRICKS_TARGET_DIR = Variable.get('databricks_target_dir', default_var='s3://ddloa-artifacts/adventureworks/lakehouse_bronze_aw')
DATABRICKS_BRONZE_WORKFLOW_ID = Variable.get('databricks_bronze_workflow_id', default_var='59223933359434')

# Define dbt Cloud configurations
DBT_ELEMENTARY_JOB_ID = Variable.get('dbt_elementary_job_id', default_var=306479)
DBT_SILVER_JOB_ID = Variable.get('dbt_silver_job_id', default_var=306421)
DBT_GOLD_JOB_ID = Variable.get('dbt_gold_job_id', default_var=306434)

# Define GitHub configurations
GITHUB_OWNER = Variable.get('github_owner', default_var='Deloitte')
GITHUB_REPOSITORY = Variable.get('github_repository', default_var='mdp-dbt-databricks')
GITHUB_BRANCH = Variable.get('github_branch', default_var='deployment/test')
GITHUB_WORKFLOW_ID = Variable.get('github_workflow_id', default_var='run-elementary.yml')

# Define S3 configurations
ELEMENTARY_S3_BUCKET = Variable.get('elementary_s3_bucket', default_var='ddloa-asset')
ELEMENTARY_REPORT_FILENAME = Variable.get('elementary_report_filename', default_var='elementary_report.html')

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

# Define parameters
dag_params = {


    "DATABRICKS__CATALOG": Param(
        default=DATABRICKS_CATALOG,
        type="string",
        description='The AdventureWorks catalog'
    ),

    "DATABRICKS__SCHEMA": Param(
        default=DATABRICKS_SCHEMA,
        type="string",
        description='The AdventureWorks schema'
    ),

    "DATABRICKS__SOURCE_DIR": Param(
        default=DATABRICKS_SOURCE_DIR,
        type="string",
        description='The AdventureWorks source directory from S3'
    ),

    "DATABRICKS__TARGET_DIR": Param(
        default=DATABRICKS_TARGET_DIR,
        type="string",
        description='The AdventureWorks target directory from S3'
    ),

    "DATABRICKS__REBUILD_TABLES": Param(
        default="False",
        type="string",
        description='Check if we want to rebuild the tables'
    ),

    "DATABRICKS__BRONZE_WORKFLOW_JOB_ID": Param(
        default=DATABRICKS_BRONZE_WORKFLOW_ID,
        type="string",
        description="The AdventureWorks Bronze Workflow in Databricks"
    ),
    "ELEMENTARY_WORKFLOW_JOB_ID": Param(
        default=DBT_ELEMENTARY_JOB_ID,
        type="string",
        description="The AdventureWorks Silver Workflow in dbt"
    ),
    "DBT__SILVER_WORKFLOW_JOB_ID": Param(
        default=DBT_SILVER_JOB_ID,
        type="string",
        description="The AdventureWorks Silver Workflow in dbt"
    ),
    "DBT__GOLD_WORKFLOW_JOB_ID": Param(
        default=DBT_GOLD_JOB_ID,
        type="string",
        description="The AdventureWorks Gold Job in dbt"
    )
}


# Define Task Groups [Databricks]
def build_aw() -> TaskGroup:
    """
    Groups the task necessary to build the catalog for hosing AdventureWorks data
    :return: TaskGroup
    """
    with TaskGroup('build_phase', tooltip='Build the AdventureWorks Catalog') as group:
        build_catalog = DatabricksSqlOperator(
            task_id='build_catalog_task',
            sql="CREATE CATALOG IF NOT EXISTS " + "{{ params.DATABRICKS__CATALOG }}",
            http_path=DATABRICKS_HTTP_PATH,
            databricks_conn_id=DATABRICKS_CONN_ID
        )

        build_schema = DatabricksSqlOperator(
            task_id='build_schema_task',
            sql="CREATE SCHEMA IF NOT EXISTS " + "{{ params.DATABRICKS__CATALOG}}.{{ params.DATABRICKS__SCHEMA }}",
            http_path=DATABRICKS_HTTP_PATH,
            databricks_conn_id=DATABRICKS_CONN_ID
        )

        build_catalog >> build_schema

        return group


# Define Task Groups [dbt]
def transform_aw() -> TaskGroup:
    """
    Groups the task for transforming AdventureWorks data using dbt
    :return:
    """
    with TaskGroup('transform_phase', tooltip='Transform the AdventureWorks data') as group:
        elementary_workflow = DbtCloudRunJobOperator(
            task_id='elementary_workflow_task',
            dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
            job_id="{{ params.ELEMENTARY_WORKFLOW_JOB_ID }}"
        )

        silver_workflow = DbtCloudRunJobOperator(
            task_id='silver_workflow_task',
            dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
            job_id="{{ params.DBT__SILVER_WORKFLOW_JOB_ID }}"
        )

        gold_workflow = DbtCloudRunJobOperator(
            task_id='gold_workflow_task',
            dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
            job_id="{{ params.DBT__GOLD_WORKFLOW_JOB_ID }}"
        )

        elementary_workflow >> silver_workflow >> gold_workflow

        return group


with DAG(
        dag_id='dbt_adventureworks',
        default_args=default_args,
        tags=['dbt', 'adventureworks', 'test'],
        max_active_runs=1,
        max_active_tasks=1,
        params=dag_params,
        schedule=None,
        schedule_interval=None
) as dag:
    # Import task groups
    build_db_workflow = build_aw()
    transform_dbt_workflow = transform_aw()

    # Define tasks
    bronze_workflow = DatabricksRunNowOperator(
        task_id='bronze_workflow_task',
        job_id="{{ params.DATABRICKS__BRONZE_WORKFLOW_JOB_ID }}",
        notebook_params={
            'destination_catalog': '{{ params.DATABRICKS__CATALOG }}',
            'destination_schema': '{{ params.DATABRICKS__SCHEMA }}',
            'source_directory': '{{ params.DATABRICKS__SOURCE_DIR }}',
            'target_directory': '{{ params.DATABRICKS__TARGET_DIR }}',
            'clean_run': '{{ params.DATABRICKS__REBUILD_TABLES }}',
        },
        databricks_conn_id=DATABRICKS_CONN_ID
    )

    gh_job = SimpleHttpOperator(
        task_id='elementary_report_task',
        http_conn_id=GITHUB_CONN_ID,
        method='POST',
        endpoint=f'/repos/{GITHUB_OWNER}/{GITHUB_REPOSITORY}/actions/workflows/{GITHUB_WORKFLOW_ID}/dispatches',
        headers={
            'Accept': 'application/vnd.github+json',
            'Authorization': f'Bearer {BaseHook.get_connection(GITHUB_CONN_ID).password}',
            'X-GitHub-Api-Version': '2022-11-28'
        },
        trigger_rule='all_done',
        data=json.dumps({'ref': GITHUB_BRANCH}),
        outlets=[Dataset(f"s3://{ELEMENTARY_S3_BUCKET}/{ELEMENTARY_REPORT_FILENAME}")],
        dag=dag
    )

    # Define orchestration workflow
    build_db_workflow >> bronze_workflow >> transform_dbt_workflow >> gh_job
