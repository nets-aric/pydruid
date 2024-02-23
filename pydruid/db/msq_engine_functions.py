import requests

import time
from functools import wraps


def retry(retry_on_exception, tries=4, delay=3, backoff=2):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except retry_on_exception as e:
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry

    return deco_retry


class TaskNotRunningException(Exception):
    pass


class TaskFailedException(Exception):
    pass


@retry(retry_on_exception=requests.exceptions.Timeout)
def get_msq_query_status(druid_base_url: str, task_id: str):
    response = requests.get(f"{druid_base_url}/druid/indexer/v1/task/{task_id}/status/")

    return response.json()["status"]["statusCode"]


@retry(retry_on_exception=requests.exceptions.Timeout)
def get_msq_result(druid_base_url: str, task_id: str):
    response = requests.get(f"{druid_base_url}/druid/indexer/v1/task/{task_id}/reports")

    return response.json()["multiStageQuery"]["payload"]["results"]


def get_msq_error_response(druid_base_url: str, task_id: str):
    response = requests.get(
        f"{druid_base_url}/druid/indexer/v1/task/{task_id}/reports"
    ).json()

    return response["multiStageQuery"]["payload"]["status"]["errorReport"]["error"]


@retry(retry_on_exception=requests.exceptions.Timeout)
def cancel_msq_query(druid_base_url: str, task_id: str):
    requests.post(url=f"{druid_base_url}/druid/indexer/v1/task/{task_id}/shutdown")
    return
