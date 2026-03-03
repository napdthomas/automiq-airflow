# dags/automiq_test.py
from airflow import DAG
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from typing import Optional, Set
import logging
import requests
import redis
import os

NETBOX_URL = "http://netbox.core.svc.cluster.local/api"
VAULT_ADDR = "http://vault.core.svc.cluster.local:8200"
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
REDIS_HOST = "netbox-valkey-primary.core.svc.cluster.local"
REDIS_PORT = 6379
REDIS_PASSWORD = os.getenv("VALKEY_PASSWORD")

CAMPAIGN_MANAGER_URL = "http://campaign-manager-service.campaign-manager.svc.cluster.local:8080"


def has_failed_tasks(context, exclude_tasks: Optional[Set[str]] = None) -> bool:
    """Check if any tasks in the DAG run have failed"""
    if exclude_tasks is None:
        exclude_tasks = {"final_status_check", "notify_success", "notify_failure"}

    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()
    failed_states = {"failed", "upstream_failed"}

    for ti in task_instances:
        if ti.task_id not in exclude_tasks and ti.state in failed_states:
            return True
    return False


def acquire_lock(lock_name: str, ttl: int = 300):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=0)
    if not r.set(lock_name, "locked", nx=True, ex=ttl):
        raise AirflowFailException(f"Lock already held for {lock_name}")
    logging.info(f"Lock acquired for {lock_name}")


def release_lock(lock_name: str):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=0)
    deleted = r.delete(lock_name)
    if deleted == 0:
        logging.warning(f"Lock {lock_name} was not held or already released")
    else:
        logging.info(f"Lock released for {lock_name}")


@task
def ward_lock(target: str):
    acquire_lock(f"{target}-lock", ttl=300)


@task
def ward_unlock(target: str):
    release_lock(f"{target}-lock")


@task
def get_netbox_metadata(target: str):
    resp = requests.get(
        f"{VAULT_ADDR}/v1/secret/data/netbox",
        headers={"X-Vault-Token": VAULT_TOKEN},
    )
    resp.raise_for_status()
    netbox_token = resp.json()["data"]["data"]["netbox"]

    headers = {"Authorization": f"Token {netbox_token}"}
    resp = requests.get(
        f"{NETBOX_URL}/dcim/devices/?name={target}",
        headers=headers,
    )
    resp.raise_for_status()

    if resp.json()["count"] == 0:
        raise AirflowFailException(f"Device {target} not found")

    device = resp.json()["results"][0]

    primary_ip = device.get("primary_ip4", {}).get("address", "")
    management_ip = primary_ip.split("/")[0] if primary_ip else ""

    if not management_ip:
        raise AirflowFailException(f"Device {target} has no management IP")

    return {
        "target": target,
        "management_ip": management_ip,
        "site": device.get("site", {}).get("name", ""),
        "role": device.get("role", {}).get("name", ""),
        "manufacturer": device.get("device_type", {}).get("manufacturer", {}).get("name", ""),
        "type": device.get("device_type", {}).get("model", ""),
    }


@task(trigger_rule=TriggerRule.ALL_DONE)
def final_status_check(**context):
    """Check if DAG had any failures and raise exception to mark DAG as failed"""
    if has_failed_tasks(context):
        raise AirflowFailException("DAG failed - automation tasks failed")

    logging.info("✓ All automation tasks completed successfully")
    return "success"


@task(trigger_rule=TriggerRule.ALL_SUCCESS)
def notify_success(**context):
    """
    Callback to Campaign Manager on full success.
    Fires only when every upstream task succeeded.
    """
    _notify_campaign_manager("SUCCESS", context)


@task(trigger_rule=TriggerRule.ONE_FAILED)
def notify_failure(**context):
    """
    Callback to Campaign Manager on any failure.
    Fires as soon as one upstream task has failed.
    """
    _notify_campaign_manager("FAILED", context)


def _notify_campaign_manager(status: str, context: dict):
    """Send status callback to Campaign Manager API."""
    dag_run   = context["dag_run"]
    conf      = dag_run.conf or {}
    campaign_id = conf.get("campaign_id")

    if not campaign_id:
        logging.warning("[CALLBACK] No campaign_id in dag_run.conf — skipping Campaign Manager notification")
        return

    candidate = conf.get("target", "unknown")
    url       = f"{CAMPAIGN_MANAGER_URL}/api/campaigns/{campaign_id}/status"

    payload = {
        "status":     status,
        "dag_run_id": dag_run.run_id,
        "candidate":  candidate,
        "message":    f"DAG {dag_run.dag_id} run {dag_run.run_id} finished with status {status}",
    }

    logging.info(f"[CALLBACK] Notifying Campaign Manager | campaign={campaign_id} | candidate={candidate} | status={status}")

    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logging.info(f"[CALLBACK] Campaign Manager acknowledged | response={resp.json()}")
    except requests.exceptions.ConnectionError as e:
        # Don't fail the DAG if Campaign Manager is unreachable
        logging.error(f"[CALLBACK] Could not reach Campaign Manager at {url}: {e}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"[CALLBACK] Campaign Manager returned error | status={resp.status_code} | body={resp.text}")
    except Exception as e:
        logging.error(f"[CALLBACK] Unexpected error notifying Campaign Manager: {e}")


default_args = {
    "owner": "automiq",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

with DAG(
    "automiq_test_k8s",
    default_args=default_args,
    schedule=None,
    catchup=False,
    params={
        "target": "clab-netauto-lab-juno1",
        "playbook": "junos_system_baseline.yaml",
        "extra_vars": '{}',
    },
) as dag:

    lock     = ward_lock("{{ params.target }}")
    metadata = get_netbox_metadata("{{ params.target }}")

    nornir = KubernetesPodOperator(
        task_id='run_nornir',
        namespace='ansible',
        image='docker.io/nthomas48/nornir-runner:latest',
        cmds=['/bin/bash', '-c'],
        arguments=['''
            set -euo pipefail
            mkdir -p /workspace && cd /workspace
            git clone http://gitea-http.gitea.svc.cluster.local:3000/admin/automiq-nornir.git repo
            cd repo
            python scripts/nornir_ping.py --target $TARGET_NAME
        '''],
        name='nornir-{{ ti.xcom_pull(task_ids="get_netbox_metadata")["target"] | replace(".", "-") | replace("_", "-") }}',
        env_vars={
            'TARGET_IP':   '{{ ti.xcom_pull(task_ids="get_netbox_metadata")["management_ip"] }}',
            'TARGET_NAME': '{{ ti.xcom_pull(task_ids="get_netbox_metadata")["target"] }}',
        },
        is_delete_operator_pod=False,
        get_logs=True,
    )

    ansible = KubernetesPodOperator(
        task_id='run_ansible',
        namespace='ansible',
        image='docker.io/nthomas48/ansible-ara-runner:latest',
        cmds=['/bin/bash', '-c'],
        arguments=['''
            set -euo pipefail
            mkdir -p /workspace && cd /workspace
            git clone http://gitea-http.gitea.svc.cluster.local:3000/admin/automiq-ansible.git repo
            cd repo
            ansible-playbook playbooks/{{ params.playbook }} -i "$TARGET_IP,"
        '''],
        name='ansible-{{ ti.xcom_pull(task_ids="get_netbox_metadata")["target"] | replace(".", "-") | replace("_", "-") }}',
        env_vars={
            'ANSIBLE_STDOUT_CALLBACK':  'ara_default',
            'ANSIBLE_HOST_KEY_CHECKING': 'False',
            'VAULT_TOKEN':   VAULT_TOKEN,
            'TARGET_IP':     '{{ ti.xcom_pull(task_ids="get_netbox_metadata")["management_ip"] }}',
            'TARGET_NAME':   '{{ ti.xcom_pull(task_ids="get_netbox_metadata")["target"] }}',
        },
        is_delete_operator_pod=False,
        get_logs=True,
    )

    # Teardown pattern ensures unlock always runs
    unlock = ward_unlock("{{ params.target }}").as_teardown(setups=lock)

    # Join point before final status check
    automation_join = EmptyOperator(
        task_id="automation_join",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    final_check      = final_status_check()
    notify_success_t = notify_success()
    notify_failure_t = notify_failure()

    # DAG flow
    lock >> metadata >> nornir >> ansible >> automation_join
    [nornir, ansible] >> unlock
    automation_join >> final_check

    # Callbacks fire after final_check resolves
    # notify_success only if final_check passed; notify_failure if it raised
    final_check >> [notify_success_t, notify_failure_t]