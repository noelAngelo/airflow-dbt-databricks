# Managed Data Pipeline Airflow

This repository contains Airflow DAGs and plugins for triggering dbt jobs in dbt Cloud and Databricks jobs in Databricks. These DAGs can be used to automate your data pipeline and orchestrate your dbt and Databricks jobs.

## To-do
- Trigger Databricks notebook in AWS
- Trigger dbt job in dbt Cloud
- Send elementary data report file to S3
- Set up Amazon SES and configure SMTP in Airflow
- Send email

## Requirements
To use these DAGs and plugins, you will need the following:

### Databricks
- A running SQL Warehouse
- A job / all-purpose cluster
- A Databricks Notebook
- A Workflow Job

### dbt
- A dbt Job

Once you have all the information above, fill in the details:
- Create a `connections.yml` 
  - from the `connections.yml.sample` file
- Create a `profiles.yml` 
  - from the `profiles.yml.sample` file


## Usage
To use these DAGs and plugins, you will need to:

1. Clone this repository to your local machine.
2. Create artifacts in `artifacts/airflow` directory:
    - `connections.yml`: contains Databricks and dbt connection string details 
    - `profiles.yml`: contains Databricks details for dbt to connect to (_used by: elementary_)
3. Clone a `dbt project` under the `artifacts/elementary` directory.
4. Trigger the DAGs manually or configure them to run on a schedule.

The DAGs included in this repository use HTTP requests to trigger dbt jobs in dbt Cloud and Databricks jobs in Databricks. These DAGs can be modified to fit your specific use case and can be extended to include additional tasks and dependencies.

## Screenshots

### Airflow
![img-airflow](https://github.com/Deloitte/mdp-airflow/blob/feature/poc-adventureworks/artifacts/assets/img-airflow.png)

### Databricks
![img-databricks](https://github.com/Deloitte/mdp-airflow/blob/feature/poc-adventureworks/artifacts/assets/img-databricks.png)

### Elementary
![img-elementary](https://github.com/Deloitte/mdp-airflow/blob/feature/poc-adventureworks/artifacts/assets/img-elementary.png)

## References
- https://github.com/aws/aws-mwaa-local-runner
- https://repost.aws/knowledge-center/mwaa-environment-install-libraries
