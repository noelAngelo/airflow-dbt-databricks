import pendulum
from datetime import timedelta
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.models import Variable
from airflow.models.param import Param


# Define connections
DATABRICKS_CONN_ID = 'databricks_default'
DBT_CONN_ID = 'dbt_default'

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
default_job_ids = {
    'autoloader_job_id': '457857757061064',
    'bronze_silver_job_id': '012345678',
    'silver_to_gold_id': '012345678'
}

@dag(
    default_args=default_args,
    tags=["poc", "adventureworks"],
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "autoloader_job_id": Param(
            default=default_job_ids['autoloader_job_id'],
            type="string",
            description="The Job ID of the Autoloader in Databricks"
        ),
        "bronze_silver_job_id": Param(
            default=default_job_ids['bronze_silver_job_id'],
            type="string",
            description="The Job ID of Bronze to Silver in dbt"
        ),
        "silver_to_gold_id": Param(
            default=default_job_ids['silver_to_gold_id'],
            type="string",
            description="The Job ID of Silver to Gold in dbt"
        )
    },
    schedule=None,
    schedule_interval=None,
)
def adventure_works_pipeline():
    """
    ### Adventureworks Pipeline Documentation
    Pipeline to run the jobs in dbt and Databricks for AdventureWorks
    """

    dag_conf = Variable.get('dag_run.conf', default_var=default_job_ids, deserialize_json=True)
    autoloader_job_id = dag_conf['autoloader_job_id']
    bronze_silver_job_id = dag_conf['bronze_silver_job_id']
    silver_to_gold_id = dag_conf['silver_to_gold_id']

    # Databricks operators
    run_autoloader_databricks = DatabricksRunNowOperator(
        task_id='run_autoloader',
        job_id=autoloader_job_id,
        databricks_conn_id=DATABRICKS_CONN_ID
    )

    # # dbt operators
    # run_bronze_to_silver_dbt = DbtCloudRunJobOperator(
    #     task_id='run_bronze_to_silver',
    #     job_id=bronze_silver_job_id,
    #     dbt_cloud_conn_id=DBT_CONN_ID
    # )
    # run_silver_to_gold_dbt = DbtCloudRunJobOperator(
    #     task_id='run_silver_to_gold',
    #     job_id=silver_to_gold_id,
    #     dbt_cloud_conn_id=DBT_CONN_ID
    # )

    # Dummy operators
    start_op = EmptyOperator(task_id='start_op')
    end_op = EmptyOperator(task_id='end_op')

    # Describe workflows
    start_op >> run_autoloader_databricks >> end_op


adventure_works_pipeline()
