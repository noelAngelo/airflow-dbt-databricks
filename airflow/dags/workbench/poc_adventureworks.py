import pendulum
from datetime import timedelta
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.models.taskinstance import TaskInstance
from airflow.models.dagrun import DagRun
from airflow.models.param import Param

# Define connections
DATABRICKS_CONN_ID = 'databricks_default'
DBT_CLOUD_CONN_ID = 'dbt_cloud_default'

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
    'bronze_silver_job_id': '012345678',
    'silver_to_gold_id': '012345678',
    'notebook_params': {}
}
dag_params = {

    "autoloader_job_id": Param(
        default=default_config['autoloader_job_id'],
        type="string",
        description="The Job ID of the Autoloader in Databricks"
    ),

    "bronze_silver_job_id": Param(
        default=default_config['bronze_silver_job_id'],
        type="string",
        description="The Job ID of Bronze to Silver in dbt"
    ),

    "silver_to_gold_id": Param(
        default=default_config['silver_to_gold_id'],
        type="string",
        description="The Job ID of Silver to Gold in dbt"
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


# Task definitions
@task(task_id='run_autoloader')
def run_autoloader(**kwargs):
    ti: TaskInstance = kwargs["ti"]
    dag_run: DagRun = ti.dag_run

    # Databricks operators
    db_job = DatabricksRunNowOperator(
        task_id='run_autoloader_task',
        job_id=dag_run.conf["autoloader_job_id"],
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_params=dag_run.conf['notebook_params'])
    db_job.execute(kwargs)


# # dbt operators
# run_bronze_to_silver_dbt = DbtCloudRunJobOperator(
#     task_id='run_bronze_to_silver',
#     job_id=bronze_silver_job_id,
#     dbt_cloud_conn_id=DBT_CLOUD_CONN_ID
# )
# run_silver_to_gold_dbt = DbtCloudRunJobOperator(
#     task_id='run_silver_to_gold',
#     job_id=silver_to_gold_id,
#     dbt_cloud_conn_id=DBT_CLOUD_CONN_ID
# )


# DAG definition
@dag(
    default_args=default_args,
    tags=["poc", "adventureworks"],
    max_active_runs=1,
    max_active_tasks=1,
    params=dag_params,
    schedule=None,
    schedule_interval=None,
)
def poc_adventureworks():
    """
    ### Adventureworks Pipeline Documentation
    Pipeline to run the jobs in dbt and Databricks for AdventureWorks
    """

    # Dummy operators
    start_op = EmptyOperator(task_id='start_op')
    end_op = EmptyOperator(task_id='end_op')

    # Tasks
    run_autoloader_task = run_autoloader()

    # Describe workflows
    start_op >> run_autoloader_task >> end_op


# Run workflow
poc_adventureworks()
