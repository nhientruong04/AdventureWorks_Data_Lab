from yaml import safe_load
from loguru import logger
import os
import requests
import json
from google.cloud import secretmanager

CONFIG_PATH = "config.yaml"
assert os.path.exists(CONFIG_PATH)
with open(CONFIG_PATH, "r") as stream:
    config = safe_load(stream)

USER_ID = config.get('AIRBYTE_USER_ID', os.environ['AIRBYTE_USER_ID'])
USER_SECRET = config.get('AIRBYTE_USER_SECRET',
                         os.environ['AIRBYTE_USER_SECRET'])
DATABASE_IP = config.get('DATABASE_HOST_IP', os.environ['DATABASE_HOST_IP'])
DATABASE_PASSWORD = config.get(
    'DATABASE_PASSWORD', os.environ['DATABASE_PASSWORD'])
BQ_PROJECT_ID = config.get('BQ_PROJECT_ID', os.environ['BQ_PROJECT_ID'])
BQ_PROJECT_REGION = config.get('BQ_PROJECT_REGION',
                               os.environ['BQ_PROJECT_REGION'])


def get_access_secret(project_id: str, secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()

    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    resp = client.access_secret_version(request={"name": name})

    return json.loads(resp.payload.data)


def get_access_token():
    url = f"{config['base_url']}/applications/token"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "client_id": USER_ID,
        "client_secret": USER_SECRET
    }

    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    resp.raise_for_status()
    return resp.json()['access_token']


def get_or_create_workspace(config, token, workspace_name='AdventureWorks2022'):
    url = f"{config['base_url']}/workspaces"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    workspaces = resp.json()['data']

    # no workspace found
    if len(workspaces) == 0:
        payload = {
            "name": workspace_name
        }
        resp = requests.post(url, headers=headers, data=json.dumps(payload))
        resp.raise_for_status()
        return resp.json()['workspaceId']
    else:
        # WARN:the workspace chosen to use will not be deterministic
        return workspaces[0]['workspaceId']


def get_or_create_source(config: dict, token: str, workspaceId: str):
    source_config = config['source']
    url = f"{config['base_url']}/sources"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    logger.info("Checking existing sources in workspace.")
    # find constructed sources first, return the first found mssql source
    resp = requests.get(url, headers=headers)
    sources = resp.json()['data']
    if len(sources) != 0:
        valid_source = None
        for source in sources:
            if source['sourceType'] == "mssql":
                valid_source = source
                break
        if valid_source is not None:
            return valid_source['sourceId']

    logger.info("No valid source found, constructing "
                "new source to OLTP database...")

    payload = {
        "name": source_config['name'],
        "workspaceId": workspaceId,
        "configuration": {
            "host": DATABASE_IP,
            "port": source_config['port'],
            "schemas": source_config['schemas'],
            "database": source_config['database'],
            "username": source_config['username'],
            "password": DATABASE_PASSWORD,
            "replication_method": {
                "method": "STANDARD"
            },
            "ssl_method": {
                "ssl_method": "encrypted_trust_server_certificate",
            },
            "tunnel_method": {
                "tunnel_method": "NO_TUNNEL"
            },
            "sourceType": "mssql"
        }
    }

    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        logger.error("Encountered an error during request, "
                     "raw error message is printed below.")
        print(resp.text)
        raise e

    logger.info("Source configured.")
    return resp.json()['sourceId']


def get_or_create_destination(config: dict, token: str, workspaceId: str):
    hmac_secret = get_access_secret(BQ_PROJECT_ID, 'hmac_secret')
    destination_config = config['destination']
    url = f"{config['base_url']}/destinations"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    logger.info("Checking existing destinations in workspace.")
    # find constructed destinations first, return the first found BQ destination
    resp = requests.get(url, headers=headers)
    destinations = resp.json()['data']
    if len(destinations) != 0:
        valid_destination = None
        for destination in destinations:
            if destination['destinationType'] == "bigquery":
                valid_destination = destination
                break
        if valid_destination is not None:
            return valid_destination['destinationId']

    logger.info("No valid destination found, constructing "
                "new destination to BigQuery warehouse...")

    payload = {
        "name": destination_config['name'],
        "workspaceId": workspaceId,
        "configuration": {
            "project_id": BQ_PROJECT_ID,
            "dataset_location": BQ_PROJECT_REGION,
            "dataset_id": destination_config['default_dataset_name'],
            "loading_method": {
                "method": "GCS Staging",
                "credential": {
                    "credential_type": "HMAC_KEY",
                    "hmac_key_access_id": hmac_secret['access_id'],
                    "hmac_key_secret": hmac_secret['key']
                },
                "gcs_bucket_name": destination_config['gcs_bucket'],
                "gcs_bucket_path": destination_config['gcs_bucket_data_folder']
            },
            "destinationType": "bigquery"
        }
    }

    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        logger.error("Encountered an error during request, "
                     "raw error message is printed below.")
        print(resp.text)
        raise e

    logger.info("Destination configured.")
    return resp.json()['destinationId']


def create_connection(config: dict, token: str, sourceId: str,
                      destinationId: str, workspaceId: str):
    connection_config = config['connection']
    url = f"{config['base_url']}/connections"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    logger.info(f"Using source {sourceId}, destination {destinationId}")
    logger.info("Constructing new connection...")

    payload = {
        "name": connection_config['name'],
        "sourceId": sourceId,
        "destinationId": destinationId,
        "workspaceId": workspaceId,
        "configurations": {
            "streams": connection_config['streams']
        },
        "namespaceDefinition": "custom_format",
        "namespaceFormat": "raw_${SOURCE_NAMESPACE}"
    }

    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        logger.error("Encountered an error during request, "
                     "raw error message is printed below.")
        print(resp.text)
        raise e

    logger.info("Source configured.")
    return resp.json()['sourceId']


if __name__ == "__main__":
    workspaceId = config['airbyte'].\
        get('workspaceId', get_or_create_workspace(config, get_access_token()))
    logger.info(f"Using workspaceId: {workspaceId}")
    sourceId = get_or_create_source(config, get_access_token(), workspaceId)
    destinationId = get_or_create_destination(config, get_access_token(),
                                              workspaceId)
    connectionId = create_connection(config, get_access_token(), sourceId,
                                     destinationId, workspaceId)
