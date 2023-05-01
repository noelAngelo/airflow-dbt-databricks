import airflow
from airflow.utils.email import send_email
from airflow.utils.db import provide_session
from airflow.models import Variable

environment = Variable.get("environment")
email_group = Variable.get("email_group")

def custom_failure_function(context):
    if environment == 'prod':
        dag_run = context['dag'].dag_id
        task_id = context['task'].task_id
        msg = f"The DAG <b>{dag_run}</b> failed at the task <b>{task_id}</b>"
        subject = f"Airflow DAG Failed - {dag_run}"
        send_email(to=email_group, subject=subject, html_content=msg)
    else:
        return