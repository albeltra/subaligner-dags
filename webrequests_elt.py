from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.decorators import task
from airflow.kubernetes.secret import Secret

# get the current Kubernetes namespace Airflow is running in
namespace = conf.get("kubernetes", "NAMESPACE")

# set the name that will be printed
name = "subaligner"
secrets = [Secret("env", "MONGO_PASSWORD", "mongo-password", "password")]

with DAG(
        start_date=datetime(2024, 11, 6),
        catchup=True,
        schedule_interval="*/2 * * * *",
        dag_id="WebRequests_ELT",
        render_template_as_native_obj=False,
        concurrency=1,
        max_active_runs=1
) as dag:
    @task(task_id="test_run")
    def test_run(**kwargs):
        import requests
        import json
        end_date = kwargs["dag_run"].execution_date
        start_date = end_date - timedelta(hours=1)

        print(kwargs["dag_run"].start_date)
        print(kwargs["dag_run"].execution_date)

        start = (start_date - timedelta(days=1)).replace(tzinfo=timezone.utc, microsecond=0)
        end = end_date.replace(tzinfo=timezone.utc, microsecond=0)
        zone = ""
        data = {
            "query": """query ListFirewallEvents($zoneTag: string, $filter: FirewallEventsAdaptiveFilter_InputObject) {
                    viewer {
                      zones(filter: { zoneTag: $zoneTag }) {
                        firewallEventsAdaptive(
                          filter: $filter
                          limit: 10000
                          orderBy: [datetime_DESC]
                        ) {
                          action
                          clientAsn
                          clientCountryName
                          clientRequestHTTPHost
                          clientIP
                          clientRequestPath
                          clientRequestHTTPHost
                          clientRequestScheme
                          clientRequestQuery
                          datetime
                          source
                          userAgent
                        }
                      }
                    }
                  }""",
            "variables": {
                "zoneTag": zone,
                "filter": {
                    "datetime_geq": start.isoformat().replace('+00:00', 'Z'),
                    "datetime_leq": end.isoformat().replace('+00:00', 'Z')
                }
            }}

        url = "https://api.cloudflare.com/client/v4/graphql/"
        headers = {
            "Content-Type": "application/json",
            "X-Auth-Email": "",
            "X-Auth-Key": ""
        }

        response = requests.post(url, headers=headers, data=json.dumps(data)).json()
        # ['data']['viewer']['zones'][0]['firewallEventsAdaptive']
        print(response)
        return True


    [test_run()]
