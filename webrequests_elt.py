from datetime import datetime, timezone, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

with DAG(
        start_date=datetime(2024, 11, 6),
        catchup=True,
        #schedule_interval="0 * * * *",
        dag_id="WebRequests_ELT",
        render_template_as_native_obj=False,
        concurrency=1,
        max_active_runs=1
) as dag:
    @task(task_id="extract_data")
    def extract_data(args):
        import requests
        import json
        import ast
        import IP2Location
        import IP2Proxy
        import pymongo
        from time import sleep
        from dateutil import parser
        from ua_parser import user_agent_parser
        from pytz import timezone as tz
        print(args)
        start = args["start"]
        end = args["end"]
        zone = args["zone"]
        ips = args["ips"]
        website = args["website"]
        mongo_url = Variable.get("mongo_url")
        api_key = Variable.get("api_key")
        email = Variable.get("email")
        client = pymongo.MongoClient(mongo_url)
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
        response = requests.post(url, headers=headers, data=json.dumps(data)).json()
        print(response)
        if 'data' in response:
            location_db = IP2Location.IP2Location("/opt/airflow/logs/IP2LOCATION-LITE-DB11.IPV6.BIN", "SHARED_MEMORY")
            proxy_db = IP2Proxy.IP2Proxy("/opt/airflow/logs/IP2PROXY-LITE-PX11.BIN")
            results = response['data']['viewer']['zones'][0]['firewallEventsAdaptive']
            final_results = []
            for result in results:
                if result["clientIP"] not in ips:
                    result['datetime'] = parser.parse(result['datetime']).astimezone(tz('America/Los_Angeles'))
                    ua = user_agent_parser.Parse(result["userAgent"])
                    for k in ["os", "device", "user_agent"]:
                        result["ua_" + k] = ua[k]["family"]

                    location_rec = ast.literal_eval(str(location_db.get_all(result["clientIP"])))
                    location_rec['longitude'] = float(location_rec['longitude'])
                    location_rec['latitude'] = float(location_rec['latitude'])

                    proxy_rec = ast.literal_eval(str(proxy_db.get_all(result["clientIP"])))

                    if proxy_rec["is_proxy"]:
                        new_proxy = {("" if k.startswith("proxy") or k == "is_proxy" else "proxy_") + k: v for k, v in proxy_rec.items()}
                    else:
                        new_proxy = {"is_proxy": False}

                    new_proxy["is_proxy"] = bool(new_proxy["is_proxy"])
                    final_results.append(result | location_rec | new_proxy)

            if len(final_results) > 0:
                operations = [pymongo.UpdateOne(record, {"$set": record}, upsert=True) for record in final_results]
                getattr(client.logs, website).bulk_write(operations)
        sleep(10)
        return response


    @task(task_id="fetch_variables")
    def fetch_variables(**kwargs):
        import requests
        import socket
        end_date = kwargs["dag_run"].execution_date
        start_date = end_date - timedelta(hours=1)

        print(kwargs["dag_run"].start_date)
        print(kwargs["dag_run"].execution_date)

        start = start_date.replace(tzinfo=timezone.utc, microsecond=0)
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

        return [{"start": start, "end": end, "zone": zone, "ips": ips, "website": website} for website, zone in web_zones.items()]


    extract_data.expand(args=fetch_variables())
