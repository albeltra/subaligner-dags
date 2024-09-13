from datetime import datetime
import json

from airflow import DAG, task
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

NUM_DISKS = 14

# lean_selector = k8s.V1Affinity(
#     node_affinity=k8s.V1NodeAffinity(required_during_scheduling_ignored_during_execution=
#     k8s.V1NodeSelector(node_selector_terms=[k8s.V1NodeSelectorTerm(match_expressions=[
#             k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="In", values=["compute-worker-lean"])]
#         )
#         ]
#         )
#     )
# )

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
    node_affinity=k8s.V1NodeAffinity(
        required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
            node_selector_terms=[k8s.V1NodeSelectorTerm(match_expressions=[
                k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="NotIn",
                                              values=["compute-worker-lean"])]
            )
            ]
        ),
        preferred_during_scheduling_ignored_during_execution=[
            k8s.V1PreferredSchedulingTerm(weight=4, preference=k8s.V1NodeSelectorTerm(match_expressions=[
                k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="In",
                                              values=["compute-worker-io"])])
                                          ),
            k8s.V1PreferredSchedulingTerm(weight=1, preference=k8s.V1NodeSelectorTerm(match_expressions=[
                k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="NotIn",
                                              values=["compute-worker-io"])])
                                          )
        ]
    )
)

network_weighted_prefer_compute_affinity = k8s.V1Affinity(
    node_affinity=k8s.V1NodeAffinity(
        required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
            node_selector_terms=[k8s.V1NodeSelectorTerm(match_expressions=[
                k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="NotIn",
                                              values=["compute-worker-io"])]
            )
            ]
        ),
        preferred_during_scheduling_ignored_during_execution=[
            k8s.V1PreferredSchedulingTerm(weight=100, preference=k8s.V1NodeSelectorTerm(match_expressions=[
                k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="In",
                                              values=["compute-worker-lean"])])
                                          ),
            k8s.V1PreferredSchedulingTerm(weight=25, preference=k8s.V1NodeSelectorTerm(match_expressions=[
                k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="In", values=["compute"])])
                                          ),
            k8s.V1PreferredSchedulingTerm(weight=50, preference=k8s.V1NodeSelectorTerm(match_expressions=[
                k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="In",
                                              values=["compute-worker-fentanyl"])])
                                          ),
            k8s.V1PreferredSchedulingTerm(weight=50, preference=k8s.V1NodeSelectorTerm(match_expressions=[
                k8s.V1NodeSelectorRequirement(key="kubernetes.io/hostname", operator="In",
                                              values=["compute-worker-fentanyl-3090"])])
                                          )
        ]
    )
)

volume_names = ["movies", "tv"]
nfs_names = ["Movies", "TV"]

disk_volumes = [k8s.V1Volume(name=f"mnt", host_path=k8s.V1HostPathVolumeSource(path=f"/host"))]
disk_volume_mounts = [k8s.V1VolumeMount(name=f"mnt", mount_path=f"/mnt", sub_path=None, read_only=True)]

media_volumes = [k8s.V1Volume(name=x, host_path=k8s.V1HostPathVolumeSource(path="/" + x)) for x in volume_names]

media_volume_mounts = [k8s.V1VolumeMount(name=x, mount_path="/" + x, sub_path=None, read_only=True) for x in
                       volume_names]
disk_media_volume_mounts = [k8s.V1VolumeMount(name=x, mount_path="/mnt/user/Media/" + y, sub_path=None, read_only=True)
                            for x, y in zip(volume_names, nfs_names)]

nfs_media_volumes = [k8s.V1Volume(name=x, nfs=k8s.V1NFSVolumeSource(path=f"/mnt/user/Media/{y}", server="192.168.10.6"))
                     for x, y in zip(volume_names, nfs_names)]
nfs_media_volume_mounts = [k8s.V1VolumeMount(name=x, mount_path="/" + x, sub_path=None, read_only=True) for x in
                           volume_names]

nfs_data_volumes = [
    k8s.V1Volume(name="data", nfs=k8s.V1NFSVolumeSource(path=f"/mnt/user/subaligner-data", server="192.168.10.6"))]
data_volumes = [k8s.V1Volume(name="data", host_path=k8s.V1HostPathVolumeSource(path="/data"))]
data_volume_mounts = [k8s.V1VolumeMount(name="data", mount_path="/data", sub_path=None, read_only=False)]

nfs_data_volumes += [k8s.V1Volume(name="audio-subs", nfs=k8s.V1NFSVolumeSource(path=f"/mnt/user/subaligner-audio-subs",
                                                                               server="192.168.10.6"))]
data_volumes += [k8s.V1Volume(name="audio-subs", host_path=k8s.V1HostPathVolumeSource(path="/audio-subs"))]
data_volume_mounts += [k8s.V1VolumeMount(name="audio-subs", mount_path="/audio-subs", sub_path=None, read_only=False)]


def queue_failed_jobs(NUM_DISKS, redis_host="redis-master", redis_port="6379", max_attempts=3):
    from redis import Redis
    from rq import Queue
    from rq.job import Job

    redis = Redis(host=redis_host, port=redis_port)
    queues = {("disk" + str(i)): Queue(name="disk" + str(i),
                                       connection=Redis(
                                           host=redis_host,
                                           port=redis_port),
                                       default_timeout=100000) for i in range(1, NUM_DISKS + 1)}
    retry_queues = []
    for k, q in queues.items():
        registry = q.failed_job_registry

        # This is how to get jobs from FailedJobRegistry
        any_queued = False
        failed_jobs = registry.get_job_ids()
        for job_id in failed_jobs:
            job = Job.fetch(job_id, connection=redis)
            num_attempts = job.meta.get("num_attempts", 1)
            if num_attempts < max_attempts:
                # job.meta["num_attempts"] = num_attempts + 1
                # job.save_meta()
                # registry.requeue(job_id)
                any_queued = True

        if any_queued:
            retry_queues.append(k)
        # assert len(registry) == 0
    return retry_queues


with DAG(
        start_date=datetime(2024, 9, 11),
        catchup=False,
        schedule=None,
        dag_id="Batch_Process_Data",
        render_template_as_native_obj=False,
        user_defined_filters={"b64encode": b64encode},
        concurrency=14,
        max_active_runs=1
) as dag:
    queue_jobs = KubernetesPodOperator(
        # unique id of the task within the DAG
        task_id="queue_jobs",
        affinity=io_selector,
        # the Docker image to launch
        image="beltranalex928/subaligner-airflow-queue-jobs",
        image_pull_policy='Always',
        # launch the Pod on the same cluster as Airflow is running on
        in_cluster=True,
        # launch the Pod in the same namespace as Airflow is running in
        namespace=namespace,
        cmds=["/bin/bash", "-c"],
        arguments=["python queue_jobs.py"],
        volumes=data_volumes + disk_volumes + media_volumes,
        volume_mounts=data_volume_mounts + disk_volume_mounts + media_volume_mounts + disk_media_volume_mounts,
        # Pod configuration
        # name the Pod
        name="queue_jobs",
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
    run_extraction = KubernetesPodOperator.partial(
        # unique id of the task within the DAG
        task_id="extract_audio_subtitle",
        affinity=prefer_io_affinity,
        # the Docker image to launch
        image="beltranalex928/subaligner-airflow-extract-audio-subtitle",
        image_pull_policy='Always',
        # launch the Pod on the same cluster as Airflow is running on
        in_cluster=True,
        # launch the Pod in the same namespace as Airflow is running in
        namespace=namespace,
        volumes=data_volumes + media_volumes,
        volume_mounts=data_volume_mounts + media_volume_mounts,
        # Pod configuration
        # name the Pod
        name="extract_audio_subtitle",
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
    #
    # generate_features = KubernetesPodOperator.partial(
    #     # unique id of the task within the DAG
    #     task_id="generate_features",
    #     affinity=network_weighted_prefer_compute_affinity,
    #     # the Docker image to launch
    #     image="beltranalex928/subaligner-airflow-extract-audio",
    #     image_pull_policy='Always',
    #     # launch the Pod on the same cluster as Airflow is running on
    #     in_cluster=True,
    #     # launch the Pod in the same namespace as Airflow is running in
    #     namespace=namespace,
    #     volumes=volumes,
    #     volume_mounts=volume_mounts,
    #     # Pod configuration
    #     # name the Pod
    #     name="extract_audio",
    #     env_vars={"mediaFile": """{{dag_run.conf['mediaFile']}}""",
    #               "mediaInfo": """{{dag_run.conf['mediaInfo']}}""",
    #               "stream_index": """{{dag_run.conf.get('stream_index', '')}}""",
    #               "audio_channel": """{{dag_run.conf.get('audio_channel', '')}}"""},
    #     # give the Pod name a random suffix, ensure uniqueness in the namespace
    #     random_name_suffix=True,
    #     # reattach to worker instead of creating a new Pod on worker failure
    #     reattach_on_restart=True,
    #     # delete Pod after the task is finished
    #     is_delete_operator_pod=True,
    #     # get log stdout of the container as task logs
    #     get_logs=True,
    #     # log events in case of Pod failure
    #     log_events_on_failure=True,
    #     do_xcom_push=True
    # )

    run_extraction.expand(
        arguments=queue_failed_jobs(NUM_DISKS, redis_host="redis-master", redis_port="6379"
                                    )
    ) >> queue_jobs >> run_extraction.expand(
        arguments=[[f"rq worker --burst disk{str(x)} --with-scheduler --url redis://redis-master:6379"]
                   for x in range(1, 15)])
