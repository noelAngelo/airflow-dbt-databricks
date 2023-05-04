from airflow.plugins_manager import AirflowPlugin


class WorkbenchDir(AirflowPlugin):
    name = "workbench_dir"
    dag_directory = "../dags/workbench"
