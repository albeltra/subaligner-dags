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
        start_date=datetime(2024, 11, 3),
        catchup=True,
        schedule_interval="30 7 * * *",
        dag_id="WebRequests_ELT",
        render_template_as_native_obj=False,
        concurrency=1,
        max_active_runs=1
) as dag:
    @task(task_id="test_run")
    def test_run(**kwargs):
        execution_date = kwargs["dag_run"].execution_date
        start_date = kwargs["dag_run"].start_date
        start_time = start_date
        print(execution_date)
        print(start_date)
        print(start_date.replace(hour=0, minute=0, second=0, microsecond=0,
                                                     tzinfo=timezone.utc))

        cur_end_time = start_time + timedelta(seconds=28800)

        start = (cur_end_time - timedelta(days=1)).replace(tzinfo=timezone.utc, microsecond=0)
        end = cur_end_time.replace(tzinfo=timezone.utc, microsecond=0)
        print(start, end)

        # print(start_time - timedelta(days=1))
        return True


    [test_run()]
