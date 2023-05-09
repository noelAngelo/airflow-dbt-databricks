import json
import pendulum
from datetime import timedelta
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models.dagrun import DagRun
from airflow.models.param import Param

# Define connections
DATABRICKS_CONN_ID = 'databricks_default'
DBT_CLOUD_CONN_ID = 'dbt_cloud_default'
AWS_CONN_ID = 'aws_conn_default'
GITHUB_CONN_ID = 'github_actions_default'

# Define GitHub configurations
GITHUB_OWNER = 'Deloitte'
GITHUB_REPO = 'mdp-dbt-databricks'
GITHUB_BRANCH = 'deployment/test'
GITHUB_WORKFLOW_ID = 'run-elementary.yml'

# Define S3 configurations
ELEMENTARY_S3_BUCKET = 'ddloa-asset'
ELEMENTARY_REPORT_FILENAME = 'elementary_report.html'

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
        'autoloader_job_id': '457857757061064',
        'notebook_params': {
            'S3BucketName': 'ddloa-artifacts',
            'SourceDir': '/source_data/adventureworks',
            'SourceFile': 'Person.CountryRegion',
            'DestCatalog': 'lakehouse_dev',
            'DestSchema': 'nborneo_dev_raw'
        },
    },
    'dbt': {
        'job_id': 290669,
    },
    'github': {
        'owner': GITHUB_OWNER,
        'repository': GITHUB_REPO,
        'branch': GITHUB_BRANCH,
        'workflow_id': GITHUB_WORKFLOW_ID
    },
    'elementary': {
        's3_bucket': ELEMENTARY_S3_BUCKET,
        'report_filename': ELEMENTARY_REPORT_FILENAME
    }
}
dag_params = {

    "DATABRICKS__AUTOLOADER_JOB_ID": Param(
        default=default_config['databricks']['autoloader_job_id'],
        type="string",
        description="The Job ID of the Autoloader in Databricks"
    ),

    "DATABRICKS__NOTEBOOK_PARAMS": Param(
        default=default_config['databricks']['notebook_params'],
        type="object"
    ),

    "DBT__JOB_ID": Param(
        default=default_config['dbt']['job_id'],
        type="integer",
        description="The Job ID in elementary"
    ),

    "GITHUB__OWNER": Param(
        default=default_config['github']['owner'],
        type="string",
        description="The account owner of the repository. The name is not case sensitive."
    ),

    "GITHUB__REPOSITORY": Param(
        default=default_config['github']['repository'],
        type="string",
        description="The name of the repository. The name is not case sensitive."
    ),

    "GITHUB__BRANCH": Param(
        default=default_config['github']['branch'],
        type="string",
        description="The git reference for the workflow. The reference in this case is a branch"
    ),

    "GITHUB__WORKFLOW_ID": Param(
        default=default_config['github']['workflow_id'],
        type="string",
        description="The ID of the workflow.In this case, pass the workflow file name as a string"
    )
}


# Task definitions
def run_autoloader(**kwargs):
    ti: TaskInstance = kwargs["ti"]
    dag_run: DagRun = ti.dag_run
    dag_conf = dag_run.conf

    # Databricks operators
    db_job = DatabricksRunNowOperator(
        task_id='run_autoloader_task',
        job_id=dag_conf.get("DATABRICKS__AUTOLOADER_JOB_ID", default_config['databricks']['autoloader_job_id']),
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_params=dag_conf.get('DATABRICKS__NOTEBOOK_PARAMS',
                                     default_config['databricks']['notebook_params']),
        dag=dag
    )
    db_job.execute(kwargs)


def run_dbt(**kwargs):
    ti: TaskInstance = kwargs["ti"]
    dag_run: DagRun = ti.dag_run
    dag_conf = dag_run.conf

    # dbt cloud operator
    dbt_job = DbtCloudRunJobOperator(
        task_id='run_dbt_task',
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=dag_conf.get('DBT__JOB_ID', default_config['dbt']['job_id']),
        dag=dag
    )
    dbt_job.execute(kwargs)


def run_github_action(**kwargs):
    ti: TaskInstance = kwargs["ti"]
    dag_run: DagRun = ti.dag_run
    dag_conf = dag_run.conf

    # Http Connection
    conn = BaseHook.get_connection(GITHUB_CONN_ID)

    # GitHub Action Setup
    github_owner = dag_conf.get('GITHUB__OWNER', default_config['github']['owner'])
    github_repo = dag_conf.get('GITHUB__REPOSITORY', default_config['github']['repository'])
    github_branch = dag_conf.get('GITHUB__BRANCH', default_config['github']['branch'])
    workflow_id = dag_conf.get('GITHUB__WORKFLOW_ID', default_config['github']['workflow_id'])

    # Trigger GitHub Action
    gh_job = SimpleHttpOperator(
        task_id='run_elementary',
        http_conn_id=GITHUB_CONN_ID,
        method='POST',
        endpoint=f'/repos/{github_owner}/{github_repo}/actions/workflows/{workflow_id}/dispatches',
        headers={
            'Accept': 'application/vnd.github+json',
            'Authorization': f'Bearer {conn.password}',
            'X-GitHub-Api-Version': '2022-11-28'
        },
        trigger_rule='all_done',
        data=json.dumps({'ref': github_branch}),
        outlets=[Dataset(f"s3://{ELEMENTARY_S3_BUCKET}/{ELEMENTARY_REPORT_FILENAME}")],
        dag=dag
    )
    gh_job.execute(kwargs)


# DAG definition
with DAG(
        dag_id='managed_adventureworks',
        default_args=default_args,
        tags=["managed", "adventureworks"],
        max_active_runs=1,
        max_active_tasks=1,
        params=dag_params,
        schedule=None,
        schedule_interval=None) as dag:
    # Dummy operators
    start_op = EmptyOperator(task_id='start_op')
    end_op = EmptyOperator(task_id='end_op')

    autoloader_task = PythonOperator(
        task_id='autoloader_task',
        python_callable=run_autoloader
    )

    dbt_task = PythonOperator(
        task_id='dbt_task',
        python_callable=run_dbt
    )

    github_task = PythonOperator(
        task_id='github_task',
        python_callable=run_github_action,
        trigger_rule='all_done'
    )

    start_op >> autoloader_task >> dbt_task >> github_task >> end_op
