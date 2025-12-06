from yaml import safe_load
from loguru import logger
import os
import requests
import json

CONFIG_PATH = "config.yaml"
assert os.path.exists(CONFIG_PATH)
with open(CONFIG_PATH, "r") as stream:
    config = safe_load(stream)

USER_ID = config.get('AIRBYTE_USER_ID', os.environ['AIRBYTE_USER_ID'])
USER_SECRET = config.get('AIRBYTE_USER_SECRET',
                         os.environ['AIRBYTE_USER_SECRET'])
DATABASE_IP = config.get('DATABASE_HOST_IP', os.environ['DATABASE_HOST_IP'])
DATABASE_PASSWORD = config.get(
    'DATABASE_HOST_PASSWORD', os.environ['DATABASE_HOST_PASSWORD'])


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

    logger.info("No valid source found, constructing\
    new source to OLTP database...")

    payload = {
        "name": "mssql-oltp-database",
        "workspaceId": workspaceId,
        "configuration": {
            "host": DATABASE_IP,
            "port": source_config['port'],
            "schemas": source_config['schemas'],
            "database": source_config['database'],
            "username": source_config["username"],
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
        logger.error("Encountered an error during request,\
            raw error message is printed below.")
        print(resp.text)
        raise e

    logger.info("Source configured.")
    return resp.json()['sourceId']


if __name__ == "__main__":
    workspaceId = get_or_create_workspace(config, get_access_token())
    logger.info(f"Using workspaceId: {workspaceId}")
    sourceId = get_or_create_source(config, get_access_token(), workspaceId)
    print(sourceId)
