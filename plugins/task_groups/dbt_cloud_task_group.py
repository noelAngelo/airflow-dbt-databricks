from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import logging
import os
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

def getDbtHeader(authorizationToken):
  dbt_header = {
  'Content-Type': 'application/json',
  'Authorization': 'Token {}'.format(authorizationToken)}
  return dbt_header

def listDbtJobsInEnvironment(task_id, environmentId, accountId, authorizationToken, environmentLimit, dag):
  return SimpleHttpOperator(
    task_id=task_id,
    method='GET',
    http_conn_id='dbt_api',
    endpoint='accounts/{0}/jobs/?environment_id={1}&limit={2}'.format(accountId,environmentId,environmentLimit),
    headers=getDbtHeader(authorizationToken),
    dag=dag,
    retries=1,
    log_response=True,
    do_xcom_push=True
  )

def getDbtJobIdFromName(**kwargs):
  ti = kwargs['ti']
  raw_json = ti.xcom_pull(task_ids=kwargs['task_id'])
  job_id = next(filter(lambda response: response.get('name') == kwargs['job_name'], json.loads(raw_json)['data']), None)['id']

  return job_id

def getDbtJob(task_id, jobId, accountId, authorizationToken, dag):
  return SimpleHttpOperator(
    task_id=task_id,
    method='GET',
    http_conn_id='dbt_api',
    endpoint='accounts/{0}/jobs/{1}/'.format(accountId, jobId),
    headers=getDbtHeader(authorizationToken),
    dag=dag,
    retries=1,
    log_response=True,
    do_xcom_push=True
  )

def updateDbtJobWithVars(**kwargs):
  ti = kwargs['ti']

  raw_json = ti.xcom_pull(task_ids=kwargs['task_id'])
  
  execute_step_as_list = json.loads(raw_json)['data']['execute_steps']
  new_execute_step_list = []
  dag_run_id = (os.environ["AIRFLOW_CTX_DAG_RUN_ID"])
  logging.info("The dag run id: {}".format(dag_run_id))
  logging.info("Execute step as list returns: {}".format(execute_step_as_list))
  #execute_step_as_string = json.loads(raw_json)['data']['execute_steps'][0]
  for execute_step_as_string in execute_step_as_list:
    logging.info("Execute step as string returns: {}".format(execute_step_as_string))

    if kwargs['job_params'] == "no_additional_params":
      logging.info("There are no additional job parameters for this job")
      execute_step_as_string_with_vars = execute_step_as_string + " --vars '{{sessionid: {0}}}'".format(dag_run_id)

    else:
      logging.info("There are additional job parameters for this job")
      if '}}' in kwargs['job_params']:
        job_params = kwargs['job_params'].replace("}}", "}, sessionid: ") + dag_run_id + "}"
      else:
        job_params = kwargs['job_params'].replace("}", ", sessionid: ") + dag_run_id + "}"
      execute_step_as_string_with_vars = execute_step_as_string + " --vars '{0}'".format(job_params)

    logging.info(execute_step_as_string_with_vars)
    new_execute_step_list.append(execute_step_as_string_with_vars)
    logging.info("Execute step has been updated to: {}".format(new_execute_step_list))

  return {'cause':'Triggered by Airflow', 'steps_override': new_execute_step_list}

def triggerDbtJobToRun(taskId, jobId, accountId, authorizationToken, dag, dataValue):
  return SimpleHttpOperator(
    task_id=taskId,
    method='POST',
    data=dataValue,
    http_conn_id='dbt_api',
    endpoint='accounts/{0}/jobs/{1}/run/'.format(accountId, jobId),
    headers=getDbtHeader(authorizationToken),
    dag=dag,
    retries=1,
    log_response=True,
    response_filter = lambda response: response.json()['data']['id']
  )

def getDbtJobRun(taskId, jobRunId, accountId, authorizationToken, dag):
  return HttpSensor(
    task_id=taskId,
    http_conn_id='dbt_api',
    endpoint='accounts/{0}/runs/{1}/'.format(accountId, jobRunId),
    headers=getDbtHeader(authorizationToken),
    poke_interval=15,
    response_check=lambda response: response.json()['data']['is_complete'] == True,
    do_xcom_push=True,
    retries=1,
    dag=dag
  )

def returnDbtJobRun(taskId, jobRunId, accountId, authorizationToken, dag):
  return SimpleHttpOperator(
    task_id=taskId,
    method='GET',
    http_conn_id='dbt_api',
    endpoint='accounts/{0}/runs/{1}/'.format(accountId, jobRunId),
    headers=getDbtHeader(authorizationToken),
    do_xcom_push=True,
    dag=dag,
    retries=1,
    log_response=True
  )

def task_to_fail(**kwargs):
  ti = kwargs['ti']
  raw_json = ti.xcom_pull(task_ids=kwargs['task_id'])
  job_status = json.loads(raw_json)['data']['status']

  logging.info("The job status: {}".format(job_status))

  if (job_status == 10):
    return "DBT job was successful!"
  elif (job_status == 20):
    raise AirflowFailException('Error on DBT job run!')
  elif (job_status == 30):
    raise AirflowFailException('DBT job run cancelled!')
    # 1-Queued / 3-Running / 10-Success / 20-Error / 30-Cancelled

def build_taskgroup(dag: DAG, jobName, taskGroupId, environmentId, jobParams="no_additional_params", environmentLimit=999) -> TaskGroup:

  dbt_account_id = Variable.get("dbt_account_id")
  dbt_authorisation_token = Variable.get("dbt_authorisation_token")
    
  with TaskGroup(group_id=taskGroupId) as taskgroup:
      list_dbt_jobs_in_environment_task = listDbtJobsInEnvironment(
        task_id='list_dbt_jobs_in_environment_task',
        environmentId=environmentId,
        accountId=dbt_account_id,
        authorizationToken=dbt_authorisation_token,
        environmentLimit=environmentLimit,
        dag=dag
        )

      get_dbt_job_id_from_name_task = PythonOperator(
        task_id='get_dbt_job_id_from_name_task',
        python_callable=getDbtJobIdFromName,
        provide_context=True,
        do_xcom_push=True,
        op_kwargs={'task_id': list_dbt_jobs_in_environment_task.task_id, 'job_name': jobName},
        dag=dag
        )

      get_dbt_job_details_task = getDbtJob(
        task_id='get_dbt_job_details_task',
        jobId="{{{{ ti.xcom_pull(task_ids='{0}.get_dbt_job_id_from_name_task') |tojson }}}}".format(taskGroupId), 
        accountId=dbt_account_id,
        authorizationToken=dbt_authorisation_token,
        dag=dag
        )

      update_dbt_job_with_vars_task = PythonOperator(
        task_id='update_dbt_job_with_vars_task',
        python_callable=updateDbtJobWithVars,
        provide_context=True,
        do_xcom_push=True,
        op_kwargs={'task_id': get_dbt_job_details_task.task_id, 'job_params': jobParams },
        dag=dag
        )

      run_dbt_job_task = triggerDbtJobToRun(
        taskId='run_dbt_job_task',
        jobId="{{{{ ti.xcom_pull(task_ids='{0}.get_dbt_job_id_from_name_task') |tojson }}}}".format(taskGroupId), 
        accountId=dbt_account_id,
        authorizationToken=dbt_authorisation_token,
        dataValue="{{{{ ti.xcom_pull(task_ids='{0}.update_dbt_job_with_vars_task') |tojson }}}}".format(taskGroupId),
        dag=dag
        )

      get_dbt_job_run_status_task = getDbtJobRun(
        taskId='get_dbt_job_run_status_task',
        jobRunId="{{{{ ti.xcom_pull(task_ids='{0}.run_dbt_job_task' )}}}}".format(taskGroupId),
        accountId=dbt_account_id,
        authorizationToken=dbt_authorisation_token,
        dag=dag
        )

      return_dbt_job_run_status_task = returnDbtJobRun(
        taskId='return_dbt_job_run_status_task',
        jobRunId="{{{{ ti.xcom_pull(task_ids='{0}.run_dbt_job_task' )}}}}".format(taskGroupId),
        accountId =dbt_account_id,
        authorizationToken=dbt_authorisation_token,
        dag=dag
        )

      dbt_job_status_response_task = PythonOperator(
        task_id='dbt_job_status_response_task',
        python_callable=task_to_fail,
        provide_context=True,
        do_xcom_push=True,
        op_kwargs={'task_id': taskGroupId + '.return_dbt_job_run_status_task'},
        dag=dag
        )

  list_dbt_jobs_in_environment_task >> get_dbt_job_id_from_name_task >> get_dbt_job_details_task >> update_dbt_job_with_vars_task >> run_dbt_job_task >> get_dbt_job_run_status_task >> return_dbt_job_run_status_task >> dbt_job_status_response_task
  return taskgroup