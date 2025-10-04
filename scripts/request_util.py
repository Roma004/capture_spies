from loguru import logger
from io import FileIO
import requests
import json
import os
from typing import Any, Callable
import pandas as pd

SERVICE_URL="http://localhost:8000"

def request_decorator(
    fn: Callable[[requests.Session, str], Any],
    uri: str,
    base_url: str = SERVICE_URL,
    headers: dict | None = None
) -> str | None:
    try:
        with requests.Session() as session:
            if headers is not None:
                session.headers.update(headers)
            session.stream = True
            session.verify = False
            res = fn(session, url=f"{base_url}{uri}")
    except Exception as e:
        logger.error("Error processing request: {err}", err=e)
        return None
    return res

def upload_file(
    uri: str,
    data: dict,
    file: FileIO,
    base_url: str = SERVICE_URL,
    headers: dict | None = None
) -> str | None:
    def process_request(
        session: requests.Session,
        url: str,
    ) -> requests.Response:
        resp = session.post(
            url,
            files={"upload_file": file},
            params=data
        )
        return resp
    resp: requests.Response = request_decorator(
        process_request,
        uri=uri, base_url=base_url, headers=headers
    )
    if resp is None:
        return None
    if resp.status_code != 200:
        logger.error(
            "Got non-200 response: {code} -- {body}",
            code=resp.status_code, body=resp.text[:500]
        )
        return None
    try:
        res = json.loads(resp.text)
    except Exception as e:
        logger.error("Error loading data from json: {err}", err=e)
        return None
    return res
