from airflow.hooks.http_hook import HttpHook
from airflow.utils.decorators import apply_defaults

class DbtCloudJobOperator(BaseOperator):
    """
    Operator to trigger a dbt job in dbt Cloud.

    :param dbt_cloud_conn_id: The name of the Airflow connection to use for dbt Cloud authentication.
    :type dbt_cloud_conn_id: str
    :param dbt_cloud_account_id: The ID of the dbt Cloud account.
    :type dbt_cloud_account_id: str
    :param dbt_cloud_job_id: The ID of the dbt Cloud job to run.
    :type dbt_cloud_job_id: str
    :param http_conn_id: The name of the Airflow connection to use for the HTTP request.
    :type http_conn_id: str
    """
    
    template_fields = ('dbt_cloud_job_id',)
    ui_color = '#F0E68C'

    @apply_defaults
    def __init__(self,
                 dbt_cloud_conn_id,
                 dbt_cloud_account_id,
                 dbt_cloud_job_id,
                 http_conn_id='http_default',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.dbt_cloud_account_id = dbt_cloud_account_id
        self.dbt_cloud_job_id = dbt_cloud_job_id
        self.http_conn_id = http_conn_id
    
    def execute(self, context):
        http_hook = HttpHook(http_conn_id=self.http_conn_id)
        headers = {
            'Authorization': f'Token {self._get_dbt_cloud_token()}',
            'Content-Type': 'application/json'
        }
        endpoint = f'https://cloud.getdbt.com/api/v2/accounts/{self.dbt_cloud_account_id}/jobs/{self.dbt_cloud_job_id}/run/'
        response = http_hook.run(endpoint, headers=headers, json={})
        response_json = response.json()
        self.log.info(f"dbt job '{response_json['job']['name']}' (ID: {response_json['job']['id']}) has been triggered in dbt Cloud.")

    def _get_dbt_cloud_token(self):
        conn = self.get_connection(self.dbt_cloud_conn_id)
        return conn.password
