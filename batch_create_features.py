from datetime import datetime
import json

from airflow import DAG, task
from airflow.configuration import conf
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from base64 import b64encode

from airflow.decorators import task
from helper import CustomFeatureEmbedder
# get the current Kubernetes namespace Airflow is running in
namespace = conf.get("kubernetes", "NAMESPACE")

# set the name that will be printed
name = "subaligner"
secrets = [Secret("env", "MONGO_PASSWORD", "mongo-password", "password")]

NUM_DISKS = 14

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

    scan_paths = KubernetesPodOperator(
        task_id="queue_jobs",
        affinity=io_selector,
        image="beltranalex928/subaligner-airflow-queue-jobs",
        image_pull_policy='Always',
        in_cluster=True,
        namespace=namespace,
        cmds=["/bin/bash", "-c"],
        arguments=["python queue_jobs.py"],
        volumes=data_volumes + disk_volumes + media_volumes,
        volume_mounts=data_volume_mounts + disk_volume_mounts + media_volume_mounts + disk_media_volume_mounts,
        name="queue_jobs",
        random_name_suffix=True,
        reattach_on_restart=True,
        is_delete_operator_pod=True,
        get_logs=True,
        log_events_on_failure=True,
        do_xcom_push=True
    )
    @task(task_id="create_features")
    def create(**kwargs):
        import gc
        import os
        from datetime import datetime, timedelta
        from pathlib import Path
        from typing import Tuple, Optional

        import h5py
        import librosa
        import numpy as np
        from pysrt import SubRipTime, SubRipFile
        from scipy.stats import laplace
        from subaligner.embedder import FeatureEmbedder
        from subaligner.subtitle import Subtitle
        CustomFeatureEmbedder().extract_data_and_label_from_audio(**kwargs)


    @task(task_id="add_features_to_db")
    def add_to_db(connection_string,
                  db,
                  audio_path,
                  out_path,
                  media_path,
                  subtitle_path,
                  kind,
                  codec,
                  version):
        import h5py
        import numpy as np
        import pymongo

        try:
            with h5py.File(out_path, 'r') as f:
                length = f['data'].shape[0]
                label_average = np.array(f['label_average'])
                og_label_average = np.array(f['og_label_average'])
        except FileNotFoundError:
            return
        client = pymongo.MongoClient(connection_string)
        col = client[db]["v" + str(version)]
        doc = {'audio_file_path': str(audio_path),
               'features_file_path': str(out_path),
               'media_file_path': media_path,
               'length': length,
               'subtitle_file_path': subtitle_path,
               'extension': audio_path.split('.')[-2],
               'subtitle': subtitle_path.split('.')[-3],
               'kind': kind,
               'codec': codec
               }
        col.update_one(doc, {
            "$set": doc | {"og_label_average": float(og_label_average), "label_average": float(label_average)}},
                       upsert=True)

        col = client[db]["data"]
        doc = {'audio_file_path': str(audio_path),
               'features_file_path': str(out_path),
               'media_file_path': media_path,
               'length': length,
               'subtitle_file_path': subtitle_path,
               'subtitle': subtitle_path.split('.')[-3],
               'extension': audio_path.split('.')[-2],
               'kind': kind,
               'codec': codec,
               'version': version
               }
        col.update_one(doc, {"$set": doc}, upsert=True)

    #

