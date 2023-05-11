from datetime import datetime

from airflow import DAG
from airflow.configuration import conf
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s

# get the current Kubernetes namespace Airflow is running in
namespace = conf.get("kubernetes", "NAMESPACE")

# set the name that will be printed
name = "subaligner"
secrets = [Secret("env", "MONGO_PASSWORD", "mongo-password", "password")]
affinity = k8s.V1Affinity(
    node_affinity=k8s.V1NodeSelector(
            k8s.V1NodeSelectorTerm(match_expressions=[
                k8s.V1NodeSelectorRequirement(key="hostname", operator="In", values=["10.253.2.1"])]
                )
            )
    )

# instantiate the DAG
with DAG(
    start_date=datetime(2023, 5, 3),
    catchup=False,
    schedule=None,
    dag_id="Align_and_Score_New_Media",
) as dag:
    inspect_file = KubernetesPodOperator(
        # unique id of the task within the DAG
        task_id="inspect_file",
        # the Docker image to launch
        image="beltranalex928/subaligner-airflow-inspector",
        # launch the Pod on the same cluster as Airflow is running on
        in_cluster=True,
        # launch the Pod in the same namespace as Airflow is running in
        namespace=namespace,
        # Pod configuration
        # name the Pod
        name="subaligner_inspect_file",
        env_vars={'MEDIA_PATH': """"{{ dag_run.conf['MEDIA_PATH'] }}"""},
        # give the Pod name a random suffix, ensure uniqueness in the namespace
        random_name_suffix=True,
        # attach labels to the Pod, can be used for grouping
        labels={"app": "backend", "env": "dev"},
        # reattach to worker instead of creating a new Pod on worker failure
        reattach_on_restart=True,
        # delete Pod after the task is finished
        is_delete_operator_pod=True,
        # get log stdout of the container as task logs
        get_logs=True,
        # log events in case of Pod failure
        log_events_on_failure=True,
        do_xcom_push=True
    )
    predict_and_score = KubernetesPodOperator(
        # unique id of the task within the DAG
        task_id="subaligner_predict_and_score",
        # the Docker image to launch
        image="beltranalex928/subaligner-airflow-predictor",
        # launch the Pod on the same cluster as Airflow is running on
        in_cluster=True,
        # launch the Pod in the same namespace as Airflow is running in
        namespace=namespace,
        # Pod configuration
        # name the Pod
        name="subaligner_predict_and_score",
        cmds=["python3",
              "predict.py",
              "-m",
              "single",
              "-s",
              ":embedded:stream_index=" + """{{ ti.xcom_pull(task_ids='inspect_file', key='STREAM_INDEX') }}""",
              "-c",
              """{{ ti.xcom_pull(task_ids='inspect_file', key='AUDIO_CHANNEL') }}""",
              "-v",
              """{{ ti.xcom_pull(task_ids='inspect_file', key='MEDIA_PATH') }}"""],
        # give the Pod name a random suffix, ensure uniqueness in the namespace
        random_name_suffix=True,
        # reattach to worker instead of creating a new Pod on worker failure
        reattach_on_restart=True,
        # delete Pod after the task is finished
        is_delete_operator_pod=True,
        # get log stdout of the container as task logs
        get_logs=True,
        # log events in case of Pod failure
        log_events_on_failure=True,
        do_xcom_push=True
    )

    send_results_to_db = KubernetesPodOperator(
        # unique id of the task within the DAG
        task_id="send_results_to_db",
        # the Docker image to launch
        image="beltranalex928/subaligner-airflow-send-to-db",
        # launch the Pod on the same cluster as Airflow is running on
        in_cluster=True,
        # launch the Pod in the same namespace as Airflow is running in
        namespace=namespace,
        # Pod configuration
        # name the Pod
        name="send_results_to_db",
        # give the Pod name a random suffix, ensure uniqueness in the namespace
        random_name_suffix=True,
        # reattach to worker instead of creating a new Pod on worker failure
        reattach_on_restart=True,
        # delete Pod after the task is finished
        is_delete_operator_pod=True,
        # get log stdout of the container as task logs
        get_logs=True,
        # log events in case of Pod failure
        log_events_on_failure=True,
        secrets=secrets,
        # pass your name as an environment var
        env_vars={"SUBALIGNER_Accuracy": """{{ ti.xcom_pull(task_ids='predict_and_score', key='Accuracy') }}""",
                  "SUBALIGNER_Loss": """{{ ti.xcom_pull(task_ids='predict_and_score', key='Loss') }}""",
                  "SUBALIGNER_Shift": """{{ ti.xcom_pull(task_ids='predict_and_score', key='Shift') }}""",
                  "SUBALIGNER_Type": """{{ ti.xcom_pull(task_ids='predict_and_score', key='Type') }}""",
                  "SUBALIGNER_Duration": """{{ ti.xcom_pull(task_ids='predict_and_score', key='Duration') }}""",
                  "SUBALIGNER_Extension": """{{ ti.xcom_pull(task_ids='predict_and_score', key='Extension') }}""",
                  "SUBALIGNER_Codec": """{{ ti.xcom_pull(task_ids='predict_and_score', key='Codec') }}""",
                  "MONGO_HOST": "subaligner-analytics-mongodb",
                  "DB": "data",
                  "COLLECTION": "predictions"},
        do_xcom_push=True
    )
