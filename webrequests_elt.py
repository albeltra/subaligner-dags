from datetime import datetime

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
        start_date=datetime(2024, 9, 11),
        catchup=False,
        schedule_interval="30 7 * * *",
        dag_id="WebRequests_ELT",
        render_template_as_native_obj=False,
        concurrency=1,
        max_active_runs=1
) as dag:
    @task(task_id="test_run")
    def test_run(**kwargs):
        print(kwargs) 
        return kwargs["dag_run"].get_task_instance('start').start_date


    [test_run()]
