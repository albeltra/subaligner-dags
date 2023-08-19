from datetime import datetime
import json

from airflow import DAG
from airflow.configuration import conf
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from base64 import b64encode

# get the current Kubernetes namespace Airflow is running in
namespace = conf.get("kubernetes", "NAMESPACE")

# set the name that will be printed
name = "subaligner"
secrets = [Secret("env", "MONGO_PASSWORD", "mongo-password", "password")]

affinity = k8s.V1Affinity(
    node_affinity=k8s.V1NodeAffinity(preferred_during_scheduling_ignored_during_execution=[
        k8s.V1PreferredSchedulingTerm(weight=1, preference=k8s.V1NodeSelectorTerm(match_expressions=[
            k8s.V1NodeSelectorRequirement(key="hostname", operator="In", values=["192.168.10.10"])])
        ),
        k8s.V1PreferredSchedulingTerm(weight=10, preference=k8s.V1NodeSelectorTerm(match_expressions=[
            k8s.V1NodeSelectorRequirement(key="hostname", operator="In", values=["192.168.10.11"])])
        ),
        k8s.V1PreferredSchedulingTerm(weight=1, preference=k8s.V1NodeSelectorTerm(match_expressions=[
            k8s.V1NodeSelectorRequirement(key="hostname", operator="In", values=["192.168.10.18"])])
        ),
    ]
    )
)

volume_names = ["movies", "movies-4k", "tv"]
volume_mounts = [k8s.V1VolumeMount(name=x, mount_path="/" + x, sub_path=None, read_only=True) for x in volume_names]
volumes = [k8s.V1Volume(name=x, host_path=k8s.V1HostPathVolumeSource(path="/" + x)) for x in volume_names]

volumes += [k8s.V1Volume(name="data", host_path=k8s.V1HostPathVolumeSource(path="/data"))]
volume_mounts += [k8s.V1VolumeMount(name="data", mount_path="/data", sub_path=None, read_only=False)]

volumes += [k8s.V1Volume(name="audio-subs", host_path=k8s.V1HostPathVolumeSource(path="/audio-subs"))]
volume_mounts += [k8s.V1VolumeMount(name="audio-subs", mount_path="/audio-subs", sub_path=None, read_only=False)]

# instantiate the DAG
with DAG(
        start_date=datetime(2023, 5, 3),
        catchup=False,
        schedule=None,
        dag_id="Align_and_Score_New_Media_Weighted",
        render_template_as_native_obj=False,
        user_defined_filters={"b64encode": b64encode},
        concurrency=8,
        max_active_runs=4
) as dag:
    extract_audio = KubernetesPodOperator(
        # unique id of the task within the DAG
        task_id="extract_audio",
        affinity=affinity,
        # the Docker image to launch
        image="beltranalex928/subaligner-airflow-extract-audio",
        # launch the Pod on the same cluster as Airflow is running on
        in_cluster=True,
        # launch the Pod in the same namespace as Airflow is running in
        namespace=namespace,
        volumes=volumes,
        volume_mounts=volume_mounts,
        # Pod configuration
        # name the Pod
        name="extract_audio",
        env_vars={"mediaFile": """{{dag_run.conf['mediaFile']}}""",
                  "mediaInfo": """{{dag_run.conf['mediaInfo']}}""",
                  "stream_index": """{{dag_run.conf.get('stream_index', '')}}""",
                  "audio_channel": """{{dag_run.conf.get('audio_channel', '')}}"""},
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
    extract_subtitles = KubernetesPodOperator(
        # unique id of the task within the DAG
        task_id="extract_subtitles",
        affinity=affinity,
        # the Docker image to launch
        image="beltranalex928/subaligner-airflow-extract-subtitles",
        # launch the Pod on the same cluster as Airflow is running on
        in_cluster=True,
        # launch the Pod in the same namespace as Airflow is running in
        namespace=namespace,
        volumes=volumes,
        volume_mounts=volume_mounts,
        # Pod configuration
        # name the Pod
        name="extract_subtitles",
        env_vars={"mediaFile": """{{dag_run.conf['mediaFile']}}""",
                  "mediaInfo": """{{dag_run.conf['mediaInfo']}}""",
                  "stream_index": """{{dag_run.conf.get('stream_index', '')}}""",
                  "audio_channel": """{{dag_run.conf.get('audio_channel', '')}}"""},
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
    predict_and_score = KubernetesPodOperator(
        # unique id of the task within the DAG
        task_id="predict_and_score",
        affinity=affinity,
        # the Docker image to launch
        image="beltranalex928/subaligner-airflow-predictor:weighted",
        # launch the Pod on the same cluster as Airflow is running on
        in_cluster=True,
        # launch the Pod in the same namespace as Airflow is running in
        namespace=namespace,
        volumes=volumes,
        volume_mounts=volume_mounts,
        # Pod configuration
        # name the Pod
        name="predict_and_score",
        env_vars={"mediaFile": """{{dag_run.conf['mediaFile']}}""",
                  "mediaInfo": """{{dag_run.conf.get('mediaInfo')}}"""},
        cmds=["python3", "/scripts/predict.py"],
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
        affinity=affinity,
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
        env_vars={"mediaFile": """{{dag_run.conf['mediaFile']}}""",
                  "mediaInfo": """{{dag_run.conf.get('mediaInfo')}}""",
                  "SUBALIGNER_loss":
                  "{{ task_instance.xcom_pull(task_ids='predict_and_score', key='return_value')['SUBALIGNER_loss'] }}",
                  "SUBALIGNER_loss_pre_shift":
                      "{{ task_instance.xcom_pull(task_ids='predict_and_score', key='return_value')['SUBALIGNER_loss_pre_shift'] }}",
                  "SUBALIGNER_subtitle_file_path":
                  "{{ task_instance.xcom_pull(task_ids='predict_and_score', key='return_value')['SUBALIGNER_subtitle_file_path'] }}",
                  "SUBALIGNER_time_load_dataset":
                  "{{ task_instance.xcom_pull(task_ids='predict_and_score', key='return_value')['SUBALIGNER_time_load_dataset'] }}",
                  "SUBALIGNER_X_shape":
                  "{{ task_instance.xcom_pull(task_ids='predict_and_score', key='return_value')['SUBALIGNER_X_shape'] }}",
                  "SUBALIGNER_time_predictions":
                  "{{ task_instance.xcom_pull(task_ids='predict_and_score', key='return_value')['SUBALIGNER_time_predictions'] }}",
                  "SUBALIGNER_seconds_to_shift":
                  "{{ task_instance.xcom_pull(task_ids='predict_and_score', key='return_value')['SUBALIGNER_seconds_to_shift'] }}",
                  "SUBALIGNER_original_start":
                  "{{ task_instance.xcom_pull(task_ids='predict_and_score', key='return_value')['SUBALIGNER_original_start'] }}",
                  "SUBALIGNER_time_sync":
                  "{{ task_instance.xcom_pull(task_ids='predict_and_score', key='return_value')['SUBALIGNER_time_sync'] }}",
                  "SUBALIGNER_Extension":
                  "{{ task_instance.xcom_pull(task_ids='predict_and_score', key='return_value')['SUBALIGNER_Extension'] }}",
                  "MONGO_HOST": "subaligner-mongodb.subaligner.svc.cluster.local",
                  "DB": """{{dag_run.conf['database']}}""",
                  "COLLECTION": """{{dag_run.conf['collection']}}""",
                  "DATA_TYPE": """{{dag_run.conf['data_type']}}""",
                  "WEIGHT": """{{dag_run.conf['weight']}}"""},

        do_xcom_push=True
    )
    [extract_audio, extract_subtitles] >> predict_and_score >> send_results_to_db
