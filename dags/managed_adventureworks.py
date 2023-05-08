import pendulum
from datetime import timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.taskinstance import TaskInstance
from airflow.models.dagrun import DagRun
from airflow.models.param import Param

# Define connections
DATABRICKS_CONN_ID = 'databricks_default'
DBT_CLOUD_CONN_ID = 'dbt_cloud_default'
AWS_CONN_ID = 'aws_conn_default'

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
    'autoloader_job_id': '457857757061064',
    'dbt_job_id': 290669,
    'notebook_params': {}
}
dag_params = {

    "autoloader_job_id": Param(
        default=default_config['autoloader_job_id'],
        type="string",
        description="The Job ID of the Autoloader in Databricks"
    ),

    "dbt_job_id": Param(
        default=default_config['dbt_job_id'],
        type="integer",
        description="The Job ID in elementary"
    ),

    "notebook_params": Param(
        default={
            'S3BucketName': 'ddloa-artifacts',
            'SourceDir': '/source_data/adventureworks',
            'SourceFile': 'Person.CountryRegion',
            'DestCatalog': 'lakehouse_dev',
            'DestSchema': 'nborneo_dev_raw'
        },
        type="object"
    )
}
default_elementary = {
    'file_path': '/opt/airflow/reports/reports.html',
    'commands': {
        'monitor': 'edr report'
    }
}


# DAG definition
@dag(
    default_args=default_args,
    tags=["managed", "adventureworks"],
    max_active_runs=1,
    max_active_tasks=1,
    params=dag_params,
    schedule=None,
    schedule_interval=None,
)
def managed_adventureworks():
    """
    ### Adventureworks Pipeline Documentation
    Pipeline to run the jobs in elementary and Databricks for AdventureWorks
    """

    # Dummy operators
    start_op = EmptyOperator(task_id='start_op')
    end_op = EmptyOperator(task_id='end_op')

    # dbt cloud operator
    run_dbt = DbtCloudRunJobOperator(
        task_id='run_dbt_task',
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=default_config['dbt_job_id']
    )

    # Task definitions
    @task
    def run_autoloader(**kwargs):
        ti: TaskInstance = kwargs["ti"]
        dag_run: DagRun = ti.dag_run
        dag_conf = dag_run.conf

        # Databricks operators
        db_job = DatabricksRunNowOperator(
            task_id='run_autoloader_task',
            job_id=dag_conf.get("autoloader_job_id", default_config['autoloader_job_id']),
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_params=dag_conf.get('notebook_params', default_config['notebook_params']))
        db_job.execute(kwargs)

    # Describe workflows
    start_op >> run_autoloader() >> run_dbt >> end_op


# Run workflow
managed_adventureworks()
