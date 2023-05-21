from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3CopyObjectOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.param import Param
from datetime import timedelta
from airflow.operators.empty import EmptyOperator
from datahub_provider.entities import Dataset, Urn
import pendulum

# Define connections
AWS_CONN_ID = 'aws_conn_default'

# Define AWS S3 configurations
S3_SOURCE_BUCKET = Variable.get('s3_source_bucket', default_var='ddloa-artifacts')
S3_SOURCE_KEY = Variable.get('s3_source_key', default_var='source_data/adventureworks/')

S3_DESTINATION_BUCKET = Variable.get('s3_destination_bucket', default_var='ddloa-artifacts')
S3_DESTINATION_KEY = Variable.get('s3_source_key', default_var='source_data/adventureworks_test/')

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
    "S3__SOURCE_BUCKET": Param(
        default=S3_SOURCE_BUCKET,
        type='string'
    ),
    "S3__SOURCE_KEY": Param(
        default=S3_SOURCE_KEY,
        type='string'
    ),
    "S3__DESTINATION_BUCKET": Param(
        default=S3_DESTINATION_BUCKET,
        type='string'
    ),
    "S3__DESTINATION_KEY": Param(
        default=S3_DESTINATION_KEY,
        type='string'
    ),
    "S3__SOURCE_NAME": Param(
        default='ALL',
        type='string'
    ),
    "S3__SOURCE_YEAR": Param(
        default='2011',
        type='string'
    ),
}


def list_sources(bucket_name: str, prefix: str):
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    object_keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix)

    source_files = {}

    for object_key in object_keys:
        parts = object_key.split('/')
        if len(parts) < 5:  # directory
            continue
        source_file = parts[2]
        source_year = parts[3]
        source_file_name = parts[4]

        if source_file in source_files:
            if source_year in source_files[source_file]:
                source_files[source_file][source_year].append((object_key, source_file_name))
            else:
                source_files[source_file][source_year] = [(object_key, source_file_name)]
        else:
            source_files[source_file] = {source_year: [(object_key, source_file_name)]}

    return source_files


def copy_source(bucket_name: str, prefix: str, destination_key: str, source_name: str, source_year: str):
    sources = list_sources(bucket_name, prefix)

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    for source_file, val in sources.items():
        for year, files in val.items():
            for _ in files:
                if (source_file == source_name and year == source_year) or (source_name == 'ALL' and year == source_year):
                    hook.copy_object(source_bucket_name=bucket_name,
                                     source_bucket_key=_[0],
                                     dest_bucket_name=bucket_name,
                                     dest_bucket_key=f'{destination_key}{source_file}/{year}/{_[1]}')


with DAG(dag_id='data_delivery_workflow',
         default_args=default_args,
         tags=['s3', 'adventureworks', 'test'],
         max_active_runs=1,
         max_active_tasks=6,
         params=dag_params,
         schedule=None,
         schedule_interval=None) as dag:
    # Define tasks
    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_landing_bucket",
        aws_conn_id=AWS_CONN_ID,
        bucket_name="{{ params.S3__DESTINATION_BUCKET }}",
        region_name='ap-southeast-2'
    )

    create_s3_destination_folder = S3CopyObjectOperator(
        task_id="create_landing_folder",
        aws_conn_id=AWS_CONN_ID,
        source_bucket_name="{{ params.S3__SOURCE_BUCKET }}",
        dest_bucket_name="{{ params.S3__DESTINATION_BUCKET }}",
        source_bucket_key="{{ params.S3__SOURCE_KEY }}",
        dest_bucket_key="{{ params.S3__DESTINATION_KEY }}"
    )

    copy_sources_task = PythonOperator(
        task_id='copy_source',
        python_callable=copy_source,
        op_kwargs={
            'bucket_name': "{{ params.S3__SOURCE_BUCKET }}",
            'prefix': '{{ params.S3__SOURCE_KEY }}',
            'destination_key': '{{ params.S3__DESTINATION_KEY }}',
            'source_name': '{{ params.S3__SOURCE_NAME }}',
            'source_year': '{{ params.S3__SOURCE_YEAR }}'
        }
    )

    # Design orchestration workflow
    create_s3_bucket >> create_s3_destination_folder >> copy_sources_task
