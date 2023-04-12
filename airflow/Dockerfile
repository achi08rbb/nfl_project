FROM apache/airflow:2.5.3-python3.9
ENV AIRFLOW_HOME=/opt/airflow
# ENV AIRFLOW_UID=50000

USER root
RUN apt-get update -qq && apt-get install vim unzip -qqq

ARG CLOUD_SDK_VERSION=425.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

USER airflow

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary

COPY code ./code
COPY csv ./csv
#creates a folder in the airflow directory in docker

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]


WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID
