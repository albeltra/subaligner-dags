from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.decorators import task
from airflow.models import Variable

with DAG(
        start_date=datetime(2024, 11, 6),
        catchup=True,
        schedule_interval="*/2 * * * *",
        dag_id="WebRequests_ELT",
        render_template_as_native_obj=False,
        concurrency=1,
        max_active_runs=1
) as dag:
    @task(task_id="extract_data")
    def extract_data(args):
        import requests
        import json
        from time import sleep
        print(args)
        start = args["start"]
        end = args["end"]
        zone = args["zone"]
        sleep_time = args["sleep"]
        ips = args["ips"]
        website = args["website"]
        mongo_url = Variable.get("mongo_url")
        api_key = Variable.get("api_key")
        email = Variable.get("email")
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
            "X-Auth-Email": f"{email}",
            "X-Auth-Key": f"{api_key}"
        }
        sleep(sleep_time)
        response = requests.post(url, headers=headers, data=json.dumps(data)).json()
        print(response)
        # if 'data' in response:
        #     results = response['data']['viewer']['zones'][0]['firewallEventsAdaptive']
        #     final_results = []
        #     for result in results:
        #         if result["clientIP"] not in ips:
        #             result['datetime'] = parser.parse(result['datetime']).astimezone(timezone('America/Los_Angeles'))
        #             if all([cur_agent not in result["userAgent"] for cur_agent in bad_agents]):
        #                 ua = user_agent_parser.Parse(result["userAgent"])
        #                 for k in ["os", "device", "user_agent"]:
        #                     result["ua_" + k] = ua[k]["family"]
        #
        #                 rec = ast.literal_eval(str(database.get_all(result["clientIP"])))
        #                 rec['longitude'] = float(rec['longitude'])
        #                 rec['latitude'] = float(rec['latitude'])
        #
        #                 final_results.append(result | rec)
        # if len(final_results) > 0:
        #     getattr(client.logs, website).insert_many(final_results)
        return response


    @task(task_id="test_run")
    def test_run(**kwargs):
        import requests
        import json
        import socket
        end_date = kwargs["dag_run"].execution_date
        start_date = end_date - timedelta(hours=1)

        print(kwargs["dag_run"].start_date)
        print(kwargs["dag_run"].execution_date)

        start = (start_date - timedelta(days=1)).replace(tzinfo=timezone.utc, microsecond=0)
        end = end_date.replace(tzinfo=timezone.utc, microsecond=0)
        web_zones = Variable.get("zones", deserialize_json=True)
        hosts = Variable.get("hosts", deserialize_json=True)

        try:
            ips = [requests.get('https://checkip.amazonaws.com').text.strip()]
        except:
            ips = []
        for x in hosts:
            try:
                ips.append(socket.gethostbyname_ex(x)[2][0])
            except socket.gaierror:
                continue

        return [{"start": start, "end": end, "zone": zone, "ips": ips, "website": website, "sleep_time": i*30} for i,(website, zone) in enumerate(web_zones.items())]


    extract_data.expand(args=test_run())
