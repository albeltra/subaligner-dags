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
        schedule_interval="0 */5 * * *",
        dag_id="WebRequests_ELT",
        render_template_as_native_obj=False,
        concurrency=1,
        max_active_runs=1
) as dag:
    @task(task_id="test_run")
    def test_run(**kwargs):
        end_date = kwargs["dag_run"].execution_date
        start_date = end_date - timedelta(hours=1)

        print(kwargs["dag_run"].start_date)
        print(kwargs["dag_run"].execution_date)

        start = (start_date - timedelta(days=1)).replace(tzinfo=timezone.utc, microsecond=0)
        end = end_date.replace(tzinfo=timezone.utc, microsecond=0)
        print(start, end)
        return True


    [test_run()]
