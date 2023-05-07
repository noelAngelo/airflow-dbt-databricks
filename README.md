# Managed Data Pipeline Airflow

![img-airflow](https://github.com/Deloitte/mdp-airflow/blob/feature/poc-adventureworks/artifacts/assets/img-architecture.png)

The repository hosts Airflow DAGs and plugins that enable the triggering of dbt jobs in both dbt Cloud and Databricks. 
These DAGs facilitate the automation of the _adventureworks_ data pipeline and orchestration of the dbt and Databricks jobs under the `ddloa` asset.
Currently, the PoC employs a shared model in which Airflow and dbt have joint ownership of the dbt project. See more under
[Ownership](#ownership).

##  Table of Contents
1. [To-do](#to-do)
2. [Requirements](#requirements)
    - [Databricks](#databricks)
    -  [dbt](#dbt)
3. [Usage](#usage)
4. [Challenges](#challenges)
    - [elementary CLI](#elementary-data)
5. [Potential Mitigations](#potential-mitigations)
    - [GitHub Actions](#elementary-data-1)
6. [Screenshots](#screenshots)
7. [References](#references)

## To-do
- ~~Trigger Databricks notebook in AWS~~
- ~~Trigger dbt job in dbt Cloud~~
- ~~Generate elementary data report~~
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
    - `databricks`.`conn_type`: databricks
    - `databricks`.`host`: the host URL in databricks
    - `databricks`.`password`: the Personal Access token in databricks
    - `dbt`.`login`: the Account ID in dbt
    - `dbt`.`password`: the Personal Access Token in dbt
- Create a `profiles.yml` 
  - from the `profiles.yml.sample` file
    - `elementary`.`catalog`: catalog in databricks
    - `elementary`.`schema`: schema in databricks
    - `elementary`.`http_path`: sql warehouse `http_path`
    - `elementary`.`host`: the host url in databricks (without https://)
    - `elementary`.`token`: the Personal Access Token in databricks

## Usage
To use these DAGs and plugins, you will need to:

1. Clone this repository to your local machine.
2. Create artifacts in `artifacts/airflow` directory:
    - `connections.yml`: contains Databricks and dbt connection string details 
    - `profiles.yml`: contains Databricks details for dbt to connect to (_used by: elementary_)
3. Clone a `dbt project` under the `artifacts/elementary` directory.
4. Trigger the DAGs manually or configure them to run on a schedule.

The DAGs included in this repository use HTTP requests to trigger dbt jobs in dbt Cloud and Databricks jobs in Databricks. These DAGs can be modified to fit your specific use case and can be extended to include additional tasks and dependencies.

## Challenges

### Elementary Data

hoarding of ownership - repository contains both codebases from Airflow & dbt project. in both `managed_adventureworks` 
and `poc_adventureworks` DAG, the process to trigger elementary CLI are as follows:
  - install python dependencies 
  - clone repository
  - run `edr` using elementary CLI

it was done this way because:
  - to generate the report, we can only use elementary CLI (elementary Cloud is in _beta_ and is _invite-only_)
  - elementary CLI is dependent on having the dbt project stored locally

this poses challenges because of the following:
- Code duplication: both repositories have a copy of the dbt project, it can lead to code duplication and maintenance overhead. This only becomes an issue if a user updates the wrong repository - makes code changes in the _mdp-airflow_ repository instead of the _mdp-dbt-databricks_ repository
- Inconsistent Results: If the dbt project is used in both repositories but evolves independently, it may lead to inconsistent results. The dbt transformations performed in the airflow DAG may not align with the transformations performed in the dbt project if they are out of sync. This can lead to data inconsistencies and incorrect analysis.
- Dependency Management: If the airflow DAG depends on specific versions of the dbt project, managing these dependencies can become challenging. Updating the dbt project in one repository might require changes in the airflow DAG to accommodate the new version. This can introduce compatibility issues and potentially break the DAG execution.

## Potential Mitigations

### Elementary Data
1. use GitHub Actions - a GitHub [Action](https://github.com/elementary-data/run-elementary-action) already exists that can be used to trigger elementary CLI 
2. migrate dbt project / airflow repo - it would be better to have a single repository where both the airflow DAG files and the dbt project are stored. This approach ensures better code organization, easier synchronization, consistent results, and simplified dependency management (standard PoC)
3. store the dbt project as docker images - might be worth using an ECR to store the different versions of the dbt project and use Docker to run the pipeline instead of just the BashOperator. this also locks the changes that can be made to the dbt project hosted in the `mdp-airflow` repository

## Ownership

### Shared Model

Currently, the repository uses a shared model of ownership between Airflow and dbt

### Independent Model

lorem ipsum 

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
