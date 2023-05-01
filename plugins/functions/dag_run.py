from airflow.utils.db import provide_session
from airflow.models.dag import get_last_dagrun

def get_execution_date_of_dag(dagId):

    @provide_session
    def _get_execution_date_of_dag(exec_date, session=None,  **kwargs):
        last_dag_run = get_last_dagrun(
            dagId,
            session
            )
        print(last_dag_run)
        print(f"EXEC DATE: {last_dag_run.execution_date}")
        return last_dag_run.execution_date

    return _get_execution_date_of_dag

