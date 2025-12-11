FROM python:3.12-slim

# install dbt
RUN apt-get update

ENV VENV_PATH=/venv
ENV PATH="$VENV_PATH/bin:$PATH"

RUN python3 -m venv $VENV_PATH
RUN pip install --upgrade pip && \
    pip install dbt-core dbt-bigquery

# setup
WORKDIR /usr/app
COPY . .
ENV DBT-PROFILES-DIR=/usr/app

RUN dbt deps

CMD ["dbt", "run", "-s", "stg_sales__salesorderheader"]
