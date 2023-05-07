FROM apache/airflow:slim-2.5.1-python3.10

COPY requirements.txt /opt/airflow/requirements.txt
COPY connections.yml /opt/airflow/connections.yml

RUN pip install --no-cache-dir -r requirements.txt
RUN airflow db init
RUN airflow connections import /opt/airflow/connections.yml