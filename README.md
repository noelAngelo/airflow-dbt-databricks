# Airflow Docker Containers for dbt and Databricks

This repository contains Docker containers for spinning up Apache Airflow, along with plugins for triggering dbt jobs in dbt Cloud and Databricks jobs in Databricks Cloud using their respective providers in Airflow.

## Prerequisites

- Docker installed on your machine
- Access to dbt Cloud and Databricks Cloud

## Getting Started

Use the included Docker Compose file to run the following in order

1. Postgres:
   ```bash
   docker compose up postgres
   ```
2. Airflow (Initialization script):
   ```bash
   docker compose up airflow init
   ```
3. Airflow Webserver and Scheduler:
   ```bash
   docker compose up airflow-webserver airflow-scheduler
   ```
4. Open your browser and navigate to `localhost:8080` to access the Airflow web interface.

## Configuration

### dbt Cloud Connection

Create a new connection for dbt Cloud by navigating to the `Admin` → `Connections` page and clicking the "`+`" button. 

Use the following settings:
   - Connection ID: `dbt_default`
   - Connection Type: `dbt Cloud`
   - Host: `https://cloud.getdbt.com/`
   - Login: Your dbt Cloud account ID
   - Password: Your dbt Cloud API key
   - Schema: Your dbt project name
   - Port: `443`
   - Extra: `{"project_name": "your_project_name"}`

### Databricks Cloud Connection

Create a new connection for Databricks Cloud by navigating to the `Admin` → `Connections` page and clicking the "`+`" button.

Use the following settings:
   - Connection ID: `databricks_default`
   - Connection Type: `Databricks`
   - Host: `https://<databricks-instance>.cloud.databricks.com/`
   - Login: Leave blank
   - Password: Leave blank
   - Schema: Leave blank
   - Port: Leave blank
   - Extra: `{"token": "<PERSONAL-ACCESS-TOKEN>"}`

> To get your personal access token, follow this [link](https://docs.databricks.com/workflows/jobs/how-to/use-airflow-with-jobs.html#configure-a-databricks-connection).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.