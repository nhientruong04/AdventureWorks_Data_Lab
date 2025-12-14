FROM apache/airflow:3.1.0

USER root

# Install Google Cloud SDK
RUN apt-get update && apt-get install -y apt-transport-https ca-certificates gnupg curl && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
    | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | gpg --dearmor > /usr/share/keyrings/cloud.google.gpg && \
    apt-get update && apt-get install -y google-cloud-cli && \
    rm -rf /var/lib/apt/lists/*

# install dbt
RUN apt-get update && apt-get install -y python3-venv
RUN python3 -m venv /opt/venvs/dbt-env
RUN /opt/venvs/dbt-env/bin/pip install --upgrade pip \
    && /opt/venvs/dbt-env/bin/pip install dbt-core dbt-bigquery

RUN chown -R airflow:root /opt/venvs

USER airflow

RUN pip install requests loguru pyyaml google-cloud-secret-manager
