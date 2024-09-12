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


io_selector = k8s.V1Affinity(
    node_affinity=k8s.V1NodeAffinity(required_during_scheduling_ignored_during_execution=
    k8s.V1NodeSelector(node_selector_terms=[k8s.V1NodeSelectorTerm(match_expressions=[
            k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="In", values=["compute-worker-io"])]
        )
        ]
        )
    )
)

anti_io_selector = k8s.V1Affinity(
    node_affinity=k8s.V1NodeAffinity(required_during_scheduling_ignored_during_execution=
    k8s.V1NodeSelector(node_selector_terms=[k8s.V1NodeSelectorTerm(match_expressions=[
            k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="NotIn", values=["compute-worker-io"])]
        )
        ]
        )
    )
)


prefer_io_affinity = k8s.V1Affinity(
    node_affinity=k8s.V1NodeAffinity(preferred_during_scheduling_ignored_during_execution=[
        k8s.V1PreferredSchedulingTerm(weight=4, preference=k8s.V1NodeSelectorTerm(match_expressions=[
            k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="In", values=["compute-worker-io"])])
        ),
        k8s.V1PreferredSchedulingTerm(weight=1, preference=k8s.V1NodeSelectorTerm(match_expressions=[
            k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="NotIn", values=["compute-worker-io"])])
        )
    ]
    )
)

network_weighted_prefer_compute_affinity = k8s.V1Affinity(
    node_affinity=k8s.V1NodeAffinity(
        required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
            node_selector_terms=[k8s.V1NodeSelectorTerm(match_expressions=[
                k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="NotIn", values=["compute-worker-io"])]
            )
            ]
        ),
        preferred_during_scheduling_ignored_during_execution=[
        k8s.V1PreferredSchedulingTerm(weight=100, preference=k8s.V1NodeSelectorTerm(match_expressions=[
            k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="In", values=["compute-worker-lean"])])
        ),
        k8s.V1PreferredSchedulingTerm(weight=25, preference=k8s.V1NodeSelectorTerm(match_expressions=[
            k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="In", values=["compute"])])
        ),
        k8s.V1PreferredSchedulingTerm(weight=50, preference=k8s.V1NodeSelectorTerm(match_expressions=[
            k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="In", values=["compute-worker-fentanyl"])])
        ),
        k8s.V1PreferredSchedulingTerm(weight=50, preference=k8s.V1NodeSelectorTerm(match_expressions=[
            k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="In", values=["compute-worker-fentanyl-3090"])])
        )
    ]
    )
)







volume_names = ["movies", "tv"]
nfs_names = ["Movies", "TV"] 
# volume_mounts = [k8s.V1VolumeMount(name=x, mount_path="/" + x, sub_path=None, read_only=True) for x in volume_names]
# volumes = [k8s.V1Volume(name=x, host_path=k8s.V1HostPathVolumeSource(path="/" + x)) for x in volume_names]

volume_mounts = [k8s.V1VolumeMount(name=x, mount_path="/" + x.title(), sub_path=None, read_only=True) for x in volume_names]
volumes = [k8s.V1Volume(name=x, nfs=k8s.V1NFSVolumeSource(path=f"/mnt/user/Media/{y}", server="192.168.10.6")) for x,y in zip(volume_names, nfs_names)]

volumes += [k8s.V1Volume(name="data", host_path=k8s.V1HostPathVolumeSource(path="/data"))]
volume_mounts += [k8s.V1VolumeMount(name="data", mount_path="/data", sub_path=None, read_only=False)]


volumes += [k8s.V1Volume(name="audio-subs", host_path=k8s.V1HostPathVolumeSource(path="/audio-subs"))]
volume_mounts += [k8s.V1VolumeMount(name="audio-subs", mount_path="/audio-subs", sub_path=None, read_only=False)]




# instantiate the DAG
with DAG(
        start_date=datetime(2023, 5, 3),
        catchup=False,
        schedule=None,
        dag_id="Batch_Process_Data",
        render_template_as_native_obj=False,
        user_defined_filters={"b64encode": b64encode},
        concurrency=1,

        max_active_runs=1
) as dag:
    extract_audio = KubernetesPodOperator.partial(
        # unique id of the task within the DAG
        task_id="extract_audio",
        affinity=network_weighted_prefer_compute_affinity,
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
    extract_audio.expand(arguments=[[str(x)] for x in range(1, 5)])
