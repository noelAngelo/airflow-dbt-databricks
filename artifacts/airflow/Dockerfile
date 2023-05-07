FROM apache/airflow:slim-2.5.1-python3.10

COPY requirements.txt /opt/airflow/requirements.txt
COPY connections.yml /opt/airflow/connections.yml
COPY variables.json /opt/airflow/variables.json

RUN pip install --no-cache-dir -r requirements.txt
RUN airflow info
RUN airflow db init
# RUN airflow connections import /opt/airflow/connections.yml
RUN airflow variables import /opt/airflow/variables.json