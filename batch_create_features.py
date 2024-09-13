from datetime import datetime
import json

from airflow import DAG
from airflow.configuration import conf
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from base64 import b64encode

from airflow.decorators import task, task_group

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
        dag_id="Batch_Create_Features",
        render_template_as_native_obj=False,
        user_defined_filters={"b64encode": b64encode},
        concurrency=14,
        max_active_runs=1
) as dag:

    scan_paths = KubernetesPodOperator(
        task_id="scan_paths",
        affinity=io_selector,
        image="beltranalex928/subaligner-airflow-queue-jobs",
        image_pull_policy='Always',
        in_cluster=True,
        namespace=namespace,
        cmds=["/bin/bash", "-c"],
        arguments=["python scan_paths.py"],
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
    def create(kwargs):
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

        class CustomFeatureEmbedder(object):
            """Audio and subtitle feature embedding.
            """

            def __init__(
                    self,
                    n_mfcc: int = 13,
                    frequency: int = 16000,
                    hop_len: int = 512,
                    step_sample: float = 0.04,
                    len_sample: float = 0.075,
            ) -> None:
                """Feature embedder initialiser.

                Keyword Arguments:
                    n_mfcc {int} -- The number of MFCC components (default: {13}).
                    frequency {float} -- The sample rate  (default: {16000}).
                    hop_len {int} -- The number of samples per frame (default: {512}).
                    step_sample {float} -- The space (in seconds) between the beginning of each sample (default: 1s / 25 FPS = 0.04s).
                    len_sample {float} -- The length in seconds for the input samples (default: {0.075}).
                """

                self.__n_mfcc = n_mfcc  # number of MFCC components
                self.__frequency = frequency  # sample rate
                self.__hop_len = hop_len  # number of samples per frame
                self.__step_sample = (
                    step_sample
                )  # space (in seconds) between the beginning of each sample
                self.__len_sample = (
                    len_sample
                )  # length in seconds for the input samples
                self.__item_time = (
                                           1.0 / frequency
                                   ) * hop_len  # 1 item = 1/16000 seg = 32 ms

            @property
            def n_mfcc(self) -> int:
                """Get the number of MFCC components.

                Returns:
                    int -- The number of MFCC components.
                """

                return self.__n_mfcc

            @property
            def frequency(self) -> int:
                """Get the sample rate.

                Returns:
                    int -- The sample rate.
                """

                return self.__frequency

            @property
            def hop_len(self) -> int:
                """Get the number of samples per frame.

                Returns:
                    int -- The number of samples per frame.
                """

                return self.__hop_len

            @property
            def step_sample(self) -> float:
                """The space (in seconds) between the begining of each sample.

                Returns:
                    float -- The space (in seconds) between the begining of each sample.
                """

                return self.__step_sample

            @step_sample.setter
            def step_sample(self, step_sample: int) -> None:
                """Configure the step sample

                Arguments:
                    step_sample {float} -- the value of the step sample (1 / frame_rate)
                """

                self.__step_sample = step_sample

            @property
            def len_sample(self) -> float:
                """Get the length in seconds for the input samples.

                Returns:
                    float -- The length in seconds for the input samples.
                """

                return self.__item_time

            @classmethod
            def time_to_sec(cls, pysrt_time: SubRipTime) -> float:
                """Convert timestamp to seconds.

                Arguments:
                    pysrt_time {pysrt.SubRipTime} -- SubRipTime or coercible.

                Returns:
                    float -- The number of seconds.
                """
                # There is a weird bug in pysrt triggered by a programatically generated
                # subtitle with start time "00:00:00,000". When it occurs, .millisecond
                # will return 999.0 and .seconds will return 60.0 and .minutes will return
                # 60.0 and .hours will return -1.0. So force it return 0.0 on this occasion.
                if str(pysrt_time) == "00:00:00,000":
                    return float(0)

                total_sec = pysrt_time.milliseconds / float(1000)
                total_sec += int(pysrt_time.seconds)
                total_sec += int(pysrt_time.minutes) * 60
                total_sec += int(pysrt_time.hours) * 60 * 60

                return round(total_sec, 3)

            def get_len_mfcc(self) -> float:
                """Get the number of samples to get LEN_SAMPLE: LEN_SAMPLE/(HOP_LEN/FREQUENCY).

                Returns:
                    float -- The number of samples.
                """

                return self.__len_sample / (self.__hop_len / self.__frequency)

            def get_step_mfcc(self) -> float:
                """Get the number of samples to get STEP_SAMPLE: STEP_SAMPLE/(HOP_LEN/FREQUENCY).

                Returns:
                    float -- The number of samples.
                """

                return self.__step_sample / (self.__hop_len / self.__frequency)

            def time_to_position(self, pysrt_time: SubRipTime) -> int:
                """Return a cell position from timestamp.

                Arguments:
                    pysrt_time {pysrt.SubRipTime} -- SubRipTime or coercible.

                Returns:
                    int -- The cell position.
                """

                return int(
                    (
                            float(self.__frequency * FeatureEmbedder.time_to_sec(pysrt_time)) / self.__hop_len
                    ) / self.get_step_mfcc()
                )

            def duration_to_position(self, seconds: float) -> int:
                """Return the cell position from a time in seconds.

                Arguments:
                    seconds {float} -- The duration in seconds.

                Returns:
                    int -- The cell position.
                """

                return int(
                    (float(self.__frequency * seconds) / self.__hop_len) / self.get_step_mfcc()
                )

            def position_to_duration(self, position: int) -> float:
                """Return the time in seconds from a cell position.

                Arguments:
                    position {int} -- The cell position.

                Returns:
                    float -- The number of seconds.
                """

                return (
                        float(position) * self.get_step_mfcc() * self.__hop_len
                ) / self.__frequency

            def position_to_time_str(self, position: int) -> str:
                """Return the time string from a cell position.

                Arguments:
                    position {int} -- The cell position.

                Returns:
                    string -- The time string (e.g., 01:23:20,150).
                """

                td = timedelta(
                    seconds=(float(position) * self.get_step_mfcc() * self.__hop_len) / self.__frequency
                )
                dt = (
                        datetime(1, 1, 1) + td
                )  # TODO: Not working for subtitles longer than 24 hours.
                hh = (
                    str(dt.hour)
                    if len(str(dt.hour)) > 1
                    else "0{}".format(str(dt.hour))
                )
                mm = (
                    str(dt.minute)
                    if len(str(dt.minute)) > 1
                    else "0{}".format(str(dt.minute))
                )
                ss = (
                    str(dt.second)
                    if len(str(dt.second)) > 1
                    else "0{}".format(str(dt.second))
                )
                ms = int(dt.microsecond / 1000)
                if len(str(ms)) == 3:
                    fff = str(ms)
                elif len(str(ms)) == 2:
                    fff = "0{}".format(str(ms))
                else:
                    fff = "00{}".format(str(ms))
                return "{}:{}:{},{}".format(hh, mm, ss, fff)

            def extract_data_and_label_from_audio(
                    self,
                    audio_file_path: str,
                    subtitle_file_path: Optional[str],
                    subtitles: Optional[SubRipFile] = None,
                    sound_effect_start_marker: Optional[str] = None,
                    sound_effect_end_marker: Optional[str] = None,
            ) -> Tuple[np.ndarray, np.ndarray]:
                """Generate a train dataset from an audio file and its subtitles

                Arguments:
                    audio_file_path {string} -- The path to the audio file.
                    subtitle_file_path {string} -- The path to the subtitle file.

                Keyword Arguments:
                    subtitles {pysrt.SubRipFile} -- The SubRipFile object (default: {None}).
                    sound_effect_start_marker: {string} -- A string indicating the start of the ignored sound effect (default: {None}).
                    sound_effect_end_marker: {string} -- A string indicating the end of the ignored sound effect (default: {None}).

                Returns:
                    tuple -- The training data and the training lables.
                """

                len_mfcc = self.get_len_mfcc()
                step_mfcc = self.get_step_mfcc()

                total_time = datetime.now()
                og_subs = None

                # Load subtitles
                if subtitle_file_path is None and subtitles is not None:
                    subs = subtitles
                elif subtitle_file_path is not None:
                    subs = Subtitle.load(subtitle_file_path).subs
                    print("Subtitle file loaded: {}".format(subtitle_file_path))
                else:
                    print("Subtitles are missing")
                    raise Exception("Subtitles are missing")

                if sound_effect_start_marker is not None:
                    original_size = len(subs)
                    og_subs = subs.copy()
                    subs = Subtitle.remove_sound_effects_by_affixes(
                        subs, se_prefix="[", se_suffix="]"
                    )
                    subs = Subtitle.remove_sound_effects_by_affixes(
                        subs, se_prefix="(", se_suffix=")"
                    )
                    subs = Subtitle.remove_sound_effects_by_affixes(
                        subs, se_prefix="*", se_suffix="*"
                    )
                    # subs = Subtitle.remove_sound_effects_by_case(
                    #     subs, se_uppercase=True
                    # )
                    print(
                        "{} sound effects removed".format(original_size - len(subs))
                    )

                t = datetime.now()

                # Load audio file
                audio_time_series, sample_rate = librosa.load(
                    audio_file_path, sr=self.frequency
                )

                # Get MFCC features
                mfcc = librosa.feature.mfcc(
                    y=audio_time_series,
                    sr=sample_rate,
                    hop_length=int(self.__hop_len),
                    n_mfcc=self.__n_mfcc,
                )
                del audio_time_series
                gc.collect()

                print(
                    "Audio file loaded and embedded with sample rate {}: {}".format(
                        sample_rate, audio_file_path
                    )
                )

                # Group multiple MFCCs of 32 ms into a larger range for LSTM
                # and each stride will have an overlay with the previous one
                samples = []
                print(mfcc.shape)
                print(step_mfcc)
                # (13, 286652)
                # 1.25
                # 229320
                # (229320,)

                # (229320, 13, 2)
                for i in np.arange(0, mfcc.shape[1], step_mfcc):
                    samples.append(mfcc[:, int(i):int(i) + int(len_mfcc)])
                # Last element may not complete so remove it
                samples = samples[: int((mfcc.shape[1] - len_mfcc) / step_mfcc) + 1]
                print(len(samples))

                train_data = np.stack(samples)
                del samples
                gc.collect()

                mfcc_extration_time = datetime.now() - t

                t = datetime.now()

                # Create array of labels
                # NOTE: if the duration of subtitle is greater the length of video, the labels may be truncated
                og_labels = None
                if og_subs is not None:
                    og_labels = np.zeros(len(train_data))
                    for sub in og_subs:
                        for i in np.arange(
                                self.time_to_position(sub.start), self.time_to_position(sub.end) + 1
                        ):
                            if i < len(og_labels):
                                og_labels[i] = 1

                labels = np.zeros(len(train_data))
                for sub in subs:
                    for i in np.arange(
                            self.time_to_position(sub.start), self.time_to_position(sub.end) + 1
                    ):
                        if i < len(labels):
                            labels[i] = 1

                label_extraction_time = datetime.now() - t

                print(
                    "----- Feature Embedding Metrics --------"
                )
                print(
                    "| Audio file path: {}".format(audio_file_path)
                )
                print(
                    "| Subtitle file path: {}".format(subtitle_file_path)
                )
                print(
                    "| MFCC extration time: {}".format(str(mfcc_extration_time))
                )
                print(
                    "| Label extraction time: {}".format(str(label_extraction_time))
                )
                print(
                    "| Total time: {}".format(str(datetime.now() - total_time))
                )
                print(
                    "----------------------------------------"
                )
                print(labels.shape)
                print(train_data.shape)
                return train_data, labels, og_labels

            def extract_and_save_data_and_label_from_audio(self,
                                                           index,
                                                           out_dir,
                                                           codec,
                                                           kind,
                                                           audio_file_path: str,
                                                           subtitle_file_path: Optional[str],
                                                           subtitles: Optional[SubRipFile] = None,
                                                           sound_effect_start_marker: Optional[str] = None,
                                                           sound_effect_end_marker: Optional[str] = None,
                                                           force: bool = False,
                                                           ):
                file_name = Path(out_dir) / f'{Path(audio_file_path).name.replace(".wav", "")}.hdf5'
                with h5py.File(file_name, 'a') as f:
                    try:
                        del f['path']
                    except:
                        pass
                    try:
                        del f['subtitle_file_path']
                    except:
                        pass
                    try:
                        del f['audio_file_path']
                    except:
                        pass
                    try:
                        del f['extension']
                    except:
                        pass
                    try:
                        del f['index']
                    except:
                        pass
                    try:
                        del f['codec']
                    except:
                        pass
                    try:
                        del f['kind']
                    except:
                        pass
                    try:
                        del f['label_average']
                    except:
                        pass
                    try:
                        del f['og_label_average']
                    except:
                        pass
                    try:
                        del f['data_99']
                    except:
                        pass

                    if 'data' not in f.keys() or 'labels' not in f.keys() or 'og_labels' not in f.keys():

                        data = self.extract_data_and_label_from_audio(audio_file_path=audio_file_path,
                                                                      subtitle_file_path=subtitle_file_path,
                                                                      subtitles=subtitles,
                                                                      sound_effect_start_marker=sound_effect_start_marker,
                                                                      sound_effect_end_marker=sound_effect_end_marker),
                        x = np.array(data[0][0])
                        labels = np.array(data[0][1])
                        og_labels = np.array(data[0][2])

                    else:
                        x = np.array(f['data'])
                        labels = np.array(f['labels'])
                        og_labels = np.array(f['og_labels'])

                    assert x.shape[0] == labels.shape[0]
                    try:
                        del f['data']
                    except:
                        pass
                    try:
                        del f['labels']
                    except:
                        pass
                    try:
                        del f['og_labels']
                    except:
                        pass

                    f['data'] = x

                    quantile = np.quantile(x, .99, axis=0)
                    minimum = np.min(x, axis=0)
                    temp = (x - minimum) / (quantile - minimum)
                    temp[temp > 1] = 1.0

                    f['data_99'] = temp

                    sigmas = [4 + x * .5 for x in range(25)]
                    try:
                        for sigma in sigmas:
                            if force:
                                try:
                                    del f[f'{name}gauss_{sigma}']
                                except:
                                    pass
                            else:
                                for name, cur_labels in zip(["", "og_"], [labels, og_labels]):
                                    if (force or f'{name}gauss_{sigma}' not in f.keys()) and cur_labels is not None:
                                        start = np.where(cur_labels)[0][0]
                                        end = np.where(cur_labels)[0][-1]
                                        gauss = laplace.pdf(np.arange(0, len(cur_labels) - (end - start) + 1), start,
                                                            sigma)

                                        f[f'{name}gauss_{sigma}'] = gauss / np.max(gauss)
                        f['labels'] = labels
                        f['label_average'] = np.mean(labels)
                        if og_labels is not None:
                            f['og_labels'] = og_labels
                            f['og_label_average'] = np.mean(og_labels)
                        f['path'] = audio_file_path.replace('.wav', ''),
                        f['subtitle_file_path'] = subtitle_file_path,
                        f['audio_file_path'] = audio_file_path,
                        f['extension'] = audio_file_path.replace('.wav', '').split('.')[-1],
                        f['index'] = index
                        f['codec'] = codec
                        f['kind'] = kind
                        return
                    except IndexError:
                        print('Index Error')
                        pass
                os.remove(file_name)

        CustomFeatureEmbedder().extract_data_and_label_from_audio(**kwargs)


    @task(task_id="add_features_to_db")
    def add_to_db(args):
        connection_string = args["connection_string"]
        db = args["db"]
        audio_path = args["audio_path"]
        out_path = args["out_path"]
        media_path = args["media_path"]
        subtitle_path = args["subtitle_path"]
        kind = args["kind"]
        codec = args["codec"]
        version = args["version"]
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

    @task_group
    def create_and_add_to_db():
        arg = "{{ task_instance.xcom_pull(task_ids='scan_paths', key='return_value')['output'] }}"
        scan_paths >> create(arg[0]["create_features_kwargs"]) # >> add_to_db(arg["add_to_db_kwargs"])


    create_and_add_to_db