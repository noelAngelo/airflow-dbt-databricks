import yaml
from pathlib import Path
import logging

# File path is /usr/local/airflow/plugins/functions/dag_schedule.py
DAG_DIR = Path(__file__).parent.parent.parent
CONFIG_DIR = "dags/config"

def get_schedule(dag, environment):

    SCHEDULE_FILE_NAME = environment +'_schedule.yml'
    SCHEDULE_FILE_PATH = DAG_DIR / CONFIG_DIR / SCHEDULE_FILE_NAME
    schedule_config_file_path = Path(SCHEDULE_FILE_PATH)
    logging.info("Schedule config file path: {}".format(schedule_config_file_path))
    schedule = None

    if schedule_config_file_path.exists():
        with open(schedule_config_file_path, "r") as config_file:
            sources_config = yaml.safe_load(config_file)
        schedule = sources_config.get(dag, None)
        logging.info("Get Schedule for DAG {0}: {1}".format(dag, schedule))
    
    return schedule