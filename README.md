# Managed Data Pipeline Airflow

This repository contains Airflow DAGs and plugins for triggering dbt jobs in dbt Cloud and Databricks jobs in Databricks. These DAGs can be used to automate your data pipeline and orchestrate your dbt and Databricks jobs.

## Requirements
To use these DAGs and plugins, you will need the following:

- An Airflow instance running in the AWS Managed Workflows for Apache Airflow (MWAA) service.
- A dbt project deployed on dbt Cloud.
- A Databricks workspace.


## Usage
To use these DAGs and plugins, you will need to:

1. Clone this repository to your local machine.
2. Modify the DAGs and plugins to fit your specific use case. This may include updating variables, credentials, and task dependencies.
3. Copy the modified DAGs and plugins to your Airflow instance in the MWAA service.
4. Trigger the DAGs manually or configure them to run on a schedule.

The DAGs included in this repository use HTTP requests to trigger dbt jobs in dbt Cloud and Databricks jobs in Databricks. These DAGs can be modified to fit your specific use case and can be extended to include additional tasks and dependencies.

## Contributing
If you would like to contribute to this repository, please open a pull request with your changes. We welcome contributions that improve the functionality and usability of these DAGs and plugins.

## License
This repository is licensed under the MIT License. See the LICENSE file for more information.