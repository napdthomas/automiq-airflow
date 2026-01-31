# /opt/airflow/dags/automiq_test.py
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.utils.state import State
from datetime import datetime, timedelta
import json
import logging
import requests
import redis
import time
import os

# Config
JENKINS_URL = "http://jenkins.local:30080"
JENKINS_USER = "admin"
JENKINS_TOKEN = os.getenv("JENKINS_TOKEN")
NETBOX_URL = "http://netbox.local:30080/api"
VAULT_ADDR = "http://vault.local:30080"
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
REDIS_HOST = "automiq-netbox-redis-1"
REDIS_PORT = 6379


def acquire_lock(lock_name: str, ttl: int = 300):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    if not r.set(lock_name, "locked", nx=True, ex=ttl):
        raise AirflowFailException(f"Lock already held for {lock_name}")
    logging.info(f"Lock acquired for {lock_name}")


def release_lock(lock_name: str):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    deleted = r.delete(lock_name)
    if deleted == 0:
        raise AirflowFailException(f"Failed to release lock {lock_name}")
    logging.info(f"Lock released for {lock_name}")


@task
def ward_lock(target: str):
    acquire_lock(f"{target}-lock", ttl=300)


@task(trigger_rule="all_done")
def ward_unlock(target: str):
    release_lock(f"{target}-lock")


@task_group
def call_netbox(target: str):

    @task
    def get_netbox_token(**context):
        resp = requests.get(
            f"{VAULT_ADDR}/v1/secret/data/netbox",
            headers={"X-Vault-Token": VAULT_TOKEN},
        )
        resp.raise_for_status()
        token = resp.json()["data"]["data"]["netbox"]
        context["ti"].xcom_push(key="netbox_token", value=token)

    @task
    def get_device_metadata(target: str, **context):
        token = context["ti"].xcom_pull(
            key="netbox_token",
            task_ids="call_netbox.get_netbox_token",
        )
        headers = {"Authorization": f"Token {token}"}
        resp = requests.get(
            f"{NETBOX_URL}/dcim/devices/?name={target}",
            headers=headers,
        )
        resp.raise_for_status()

        if resp.json()["count"] == 0:
            raise AirflowFailException(f"Device {target} not found")

        device = resp.json()["results"][0]
        role_obj = device.get("device_role")
        device_type = device.get("device_type") or {}
        manufacturer_obj = device_type.get("manufacturer") or {}

        meta = {
            "Site": device.get("site", {}).get("name"),
            "Role": role_obj["name"] if role_obj else None,
            "Manufacturer": manufacturer_obj.get("name"),
            "Type": device_type.get("model"),
        }

        context["ti"].xcom_push(key="device_metadata", value=meta)

    get_netbox_token() >> get_device_metadata(target)


# =========================
# Jenkins ping
# =========================
@task_group
def call_nornir_job(params: dict):

    @task
    def trigger_ping_job(params: dict, **context):
        normalized = {}
        for k, v in params.items():
            if isinstance(v, Param):
                if hasattr(v, "value") and v.value is not None:
                    normalized[k] = v.value
                elif hasattr(v, "default") and v.default is not None:
                    normalized[k] = v.default
                else:
                    normalized[k] = str(v)
            else:
                normalized[k] = v

        jenkins_params = {
            "TARGET": normalized.get("target"),
            "PLAYBOOK": "nornir_ping.py",
            "EXTRA_VARS": normalized.get("extra_vars", ""),
        }

        job_name = "run_nornir_job"
        url = f"{JENKINS_URL}/job/{job_name}/buildWithParameters"

        resp = requests.post(
            url,
            params=jenkins_params,
            auth=(JENKINS_USER, JENKINS_TOKEN),
        )
        resp.raise_for_status()

        queue_url = resp.headers.get("Location")
        if not queue_url:
            raise AirflowFailException("Failed to get Jenkins queue location")

        build_number = None
        for _ in range(30):
            q = requests.get(
                f"{queue_url}api/json",
                auth=(JENKINS_USER, JENKINS_TOKEN),
            )
            q.raise_for_status()
            data = q.json()
            if data.get("executable"):
                build_number = data["executable"]["number"]
                break
            time.sleep(2)

        if build_number is None:
            raise AirflowFailException("Failed to get Jenkins build number")

        return build_number

    @task
    def poll_ping_job(build_number: int, params: dict):
        job_name = "run_nornir_job"
        while True:
            r = requests.get(
                f"{JENKINS_URL}/job/{job_name}/{build_number}/api/json",
                auth=(JENKINS_USER, JENKINS_TOKEN),
            )
            r.raise_for_status()
            data = r.json()
            if not data.get("building"):
                if data.get("result") != "SUCCESS":
                    raise AirflowFailException(
                        f"Jenkins job failed: {data.get('result')}"
                    )
                break
            time.sleep(5)

    build = trigger_ping_job(params=params)
    poll_ping_job(build_number=build, params=params)


# =========================
# Jenkins ansible
# =========================
@task_group
def call_ansible_job(params: dict):

    @task
    def trigger_ansible_job(params: dict, **context):
        normalized = {}
        for k, v in params.items():
            if isinstance(v, Param):
                if hasattr(v, "value") and v.value is not None:
                    normalized[k] = v.value
                elif hasattr(v, "default") and v.default is not None:
                    normalized[k] = v.default
                else:
                    normalized[k] = str(v)
            else:
                normalized[k] = v

        jenkins_params = {
            "TARGET": normalized.get("target"),
            "PLAYBOOK": normalized.get("playbook"),
            "EXTRA_VARS": normalized.get("extra_vars", ""),
        }

        job_name = "run_ansible_job"
        url = f"{JENKINS_URL}/job/{job_name}/buildWithParameters"

        resp = requests.post(
            url,
            params=jenkins_params,
            auth=(JENKINS_USER, JENKINS_TOKEN),
        )
        resp.raise_for_status()

        queue_url = resp.headers.get("Location")
        if not queue_url:
            raise AirflowFailException("Failed to get Jenkins queue location")

        build_number = None
        for _ in range(30):
            q = requests.get(
                f"{queue_url}api/json",
                auth=(JENKINS_USER, JENKINS_TOKEN),
            )
            q.raise_for_status()
            data = q.json()
            if data.get("executable"):
                build_number = data["executable"]["number"]
                break
            time.sleep(2)

        if build_number is None:
            raise AirflowFailException("Failed to get Jenkins build number")

        return build_number

    @task
    def poll_ansible_job(build_number: int, params: dict):
        job_name = "run_ansible_job"
        while True:
            r = requests.get(
                f"{JENKINS_URL}/job/{job_name}/{build_number}/api/json",
                auth=(JENKINS_USER, JENKINS_TOKEN),
            )
            r.raise_for_status()
            data = r.json()
            if not data.get("building"):
                if data.get("result") != "SUCCESS":
                    raise AirflowFailException(
                        f"Jenkins job failed: {data.get('result')}"
                    )
                break
            time.sleep(5)

    build = trigger_ansible_job(params=params)
    poll_ansible_job(build_number=build, params=params)


# =========================
# Jenkins Slack Job
# =========================
@task_group
def call_slack_job():

    @task(trigger_rule="all_done")
    def trigger_slack_job(**context):
        dag_run = context["dag_run"]
        tis = dag_run.get_task_instances()

        failed_tis = [
            ti for ti in tis
            if ti.state == State.FAILED
            and ti.task_id not in {"call_slack_job.trigger_slack_job"}
        ]

        if not failed_tis:
            logging.info("No failures; skipping Slack Jenkins job.")
            return None

        ti = failed_tis[0]

        jenkins_params = {
            "DAG_ID": dag_run.dag_id,
            "RUN_ID": dag_run.run_id,
            "TASK_ID": ti.task_id,
        }

        job_name = "run_slack_job"
        url = f"{JENKINS_URL}/job/{job_name}/buildWithParameters"

        resp = requests.post(
            url,
            params=jenkins_params,
            auth=(JENKINS_USER, JENKINS_TOKEN),
        )
        resp.raise_for_status()

        queue_url = resp.headers.get("Location")
        if not queue_url:
            raise AirflowFailException("Failed to get Jenkins queue location")

        build_number = None
        for _ in range(30):
            q = requests.get(
                f"{queue_url}api/json",
                auth=(JENKINS_USER, JENKINS_TOKEN),
            )
            q.raise_for_status()
            data = q.json()
            if data.get("executable"):
                build_number = data["executable"]["number"]
                break
            time.sleep(2)

        if build_number is None:
            raise AirflowFailException("Failed to get Jenkins build number")

        return build_number

    @task(trigger_rule="all_done")
    def poll_slack_job(build_number: int):
        if build_number is None:
            return

        job_name = "run_slack_job"
        while True:
            r = requests.get(
                f"{JENKINS_URL}/job/{job_name}/{build_number}/api/json",
                auth=(JENKINS_USER, JENKINS_TOKEN),
            )
            r.raise_for_status()
            data = r.json()
            if not data.get("building"):
                if data.get("result") != "SUCCESS":
                    raise AirflowFailException(
                        f"Slack Jenkins job failed: {data.get('result')}"
                    )
                break
            time.sleep(5)

    build = trigger_slack_job()
    poll_slack_job(build_number=build)


# =========================
# Jenkins JIRA Job
# =========================
@task_group
def call_jira_job():

    @task(trigger_rule="all_done")
    def trigger_jira_job(**context):
        dag_run = context["dag_run"]
        tis = dag_run.get_task_instances()

        failed_tis = [
            ti for ti in tis
            if ti.state == State.FAILED
            and ti.task_id not in {"call_jira_job.trigger_jira_job"}
        ]

        if not failed_tis:
            logging.info("No failures; skipping JIRA Jenkins job.")
            return None

        ti = failed_tis[0]

        jenkins_params = {
            "DAG_ID": dag_run.dag_id,
            "RUN_ID": dag_run.run_id,
            "TASK_ID": ti.task_id,
        }

        job_name = "run_jira_job"
        url = f"{JENKINS_URL}/job/{job_name}/buildWithParameters"

        resp = requests.post(
            url,
            params=jenkins_params,
            auth=(JENKINS_USER, JENKINS_TOKEN),
        )
        resp.raise_for_status()

        queue_url = resp.headers.get("Location")
        if not queue_url:
            raise AirflowFailException("Failed to get Jenkins queue location")

        build_number = None
        for _ in range(30):
            q = requests.get(
                f"{queue_url}api/json",
                auth=(JENKINS_USER, JENKINS_TOKEN),
            )
            q.raise_for_status()
            data = q.json()
            if data.get("executable"):
                build_number = data["executable"]["number"]
                break
            time.sleep(2)

        if build_number is None:
            raise AirflowFailException("Failed to get Jenkins build number")

        return build_number

    @task(trigger_rule="all_done")
    def poll_jira_job(build_number: int):
        if build_number is None:
            return

        job_name = "run_jira_job"
        while True:
            r = requests.get(
                f"{JENKINS_URL}/job/{job_name}/{build_number}/api/json",
                auth=(JENKINS_USER, JENKINS_TOKEN),
            )
            r.raise_for_status()
            data = r.json()
            if not data.get("building"):
                if data.get("result") != "SUCCESS":
                    raise AirflowFailException(
                        f"JIRA Jenkins job failed: {data.get('result')}"
                    )
                break
            time.sleep(5)

    build = trigger_jira_job()
    poll_jira_job(build_number=build)


default_args = {
    "owner": "automiq",
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
}

with DAG(
    "automiq_test",
    default_args=default_args,
    schedule=None,
    catchup=False,
    params={
        "target": "clab-netauto-lab-ceos1",
        "playbook": "eos_system_baseline.yaml",
        "extra_vars": "{\"interface\":\"Ethernet1\",\"description\":\"automation test\"}",
    },
) as dag:

    lock = ward_lock("{{ params.target }}")
    netbox = call_netbox("{{ params.target }}")
    jenkins_ping = call_nornir_job(params=dag.params)
    jenkins = call_ansible_job(params=dag.params)
    unlock = ward_unlock("{{ params.target }}")
    slack = call_slack_job()
    jira = call_jira_job()

    lock >> netbox >> jenkins_ping >> jenkins >> unlock >> slack >> jira
