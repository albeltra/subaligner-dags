# subaligner-dags
This repo contains dags used in the evaluation of my Subaligner model in Airflow. There is currently on dag on use on a single-worker-node Airflow installation.

In summary, audio and subtitle extraction occur in parallel, subtitles are aligned, and finally statistics about the alignment process are uploaded to an in-cluster mongo db using Longhorn as the storage provider.
