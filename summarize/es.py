import json
import requests
import os
import base64
import logging

from datetime import date

# Configure logging
logger = logging.getLogger(__name__)


def init_session(username: str = None, password: str = None):
    """Initialize the session with basic authentication"""

    session = requests.Session()
    session.headers.update({
        "Content-Type": "application/json"
    })

    if username is not None and password is not None:
        auth = base64.b64encode((username + ":" + password).encode('utf-8')).decode('utf-8')
        session.headers.update({
            "Authorization": f"Basic {auth}"
        })

    return session


def create_index(host: str, index_name: str, username: str = None, password: str = None):
    """Create an index in Elasticsearch"""

    session = init_session(username, password)
    session.put(f"{host}/{index_name}")
    logger.info(f"Created index {index_name}")


def index_documents(documents, host: str, index_name: str, username: str = None, password: str = None):
    """Index documents into Elasticsearch"""

    session = init_session(username, password)

    body = ""
    for doc in documents:
        body += f'{{"index": {{"_index": "{index_name}"}}}}\n{json.dumps(doc)}\n'

    response = session.post(f"{host}/{index_name}/_doc/_bulk", data=body, headers={"Content-Type": "application/x-ndjson"})

    if response.status_code != 200 or response.json()['errors']:
        logger.error(f"Failed to index documents: {response.text}")
        raise Exception(f"Failed to index documents: {response.text}")

    logger.debug(f"Indexed {len(documents)} documents into {index_name}")


def search(query, host, index_name, username: str = None, password: str = None):
    """Query an index in Elasticsearch"""

    session = init_session(username, password)

    response = session.get(f"{host}/{index_name}/_search", json=query)

    if response.status_code != 200:
        logger.error(f"Failed to query index: {response.text}")
        raise Exception(f"Failed to query index: {response.text}")

    return response.json()


def delete_by_query(query: dict, host: str, index_name: str, username: str = None, password: str = None):

    session = init_session(username, password)

    response = session.post(f"{host}/{index_name}/_delete_by_query", json=query)

    if response.status_code != 200:
        logger.error(f"Failed to delete documents based on query: {response.text}")
        raise Exception(f"Failed to delete documents based on query: {response.text}")

