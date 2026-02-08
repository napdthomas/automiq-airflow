# dags/automiq_test.py
from airflow import DAG
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.exceptions import AirflowFailException
from airflow.models import State
from datetime import datetime, timedelta
import json
import logging
import requests
import redis
import os

# Config
NETBOX_URL = "http://netbox.core.svc.cluster.local/api"
VAULT_ADDR = "http://vault.core.svc.cluster.local:8200"
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
REDIS_HOST = "netbox-valkey-primary.core.svc.cluster.local"
REDIS_PORT = 6379
REDIS_PASSWORD = os.getenv("VALKEY_PASSWORD")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


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


@task(trigger_rule="all_done")
def ward_unlock(target: str):
    """Always release lock, even on failure"""
    release_lock(f"{target}-lock")


@task
def get_netbox_metadata(target: str):
    """Get device metadata from NetBox"""
    # Get NetBox token from Vault
    resp = requests.get(
        f"{VAULT_ADDR}/v1/secret/data/netbox",
        headers={"X-Vault-Token": VAULT_TOKEN},
    )
    resp.raise_for_status()
    netbox_token = resp.json()["data"]["data"]["netbox"]
    
    # Get device from NetBox
    headers = {"Authorization": f"Token {netbox_token}"}
    resp = requests.get(
        f"{NETBOX_URL}/dcim/devices/?name={target}",
        headers=headers,
    )
    resp.raise_for_status()

    if resp.json()["count"] == 0:
        raise AirflowFailException(f"Device {target} not found")

    device = resp.json()["results"][0]
    
    # Extract management IP (remove subnet mask)
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


@task(trigger_rule="all_done")
def send_slack_notification(**context):
    """Send Slack notification on task failure"""
    SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")
    
    if not SLACK_WEBHOOK:
        logging.error("SLACK_WEBHOOK_URL environment variable not set")
        return "Slack webhook not configured"
    
    dag_run = context["dag_run"]
    tis = dag_run.get_task_instances()
    
    # Find failed tasks (excluding this notification task and status check)
    failed_tis = [
        ti for ti in tis
        if ti.state == State.FAILED
        and ti.task_id not in {"send_slack_notification", "check_workflow_status"}
    ]
    
    if not failed_tis:
        logging.info("No failures; skipping Slack notification.")
        return None
    
    # Get first failed task for notification
    ti = failed_tis[0]
    
    # Build kubectl command to view logs
    log_command = (
        f"kubectl logs -n airflow airflow-scheduler-0 -c scheduler --tail=200 | "
        f"grep -A 50 '{ti.task_id}'"
    )
    
    # Create Slack message with kubectl command
    payload = {
        "text": f""":x: Airflow Task Failed
DAG: `{dag_run.dag_id}`
Run: `{dag_run.run_id}`
Task: `{ti.task_id}`
State: `{ti.state}`

View logs with:
```
{log_command}
```

Or view all task states:
```
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- \\
  airflow tasks states-for-dag-run {dag_run.dag_id} {dag_run.run_id}
```
"""
    }
    
    try:
        resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
        resp.raise_for_status()
        logging.info(f"Slack notification sent for failed task: {ti.task_id}")
        return f"Notified about {ti.task_id} failure"
    except Exception as e:
        logging.error(f"Failed to send Slack notification: {e}")
        # Don't fail the DAG if Slack notification fails
        return f"Slack notification failed: {e}"


@task(trigger_rule="all_done")
def check_workflow_status(**context):
    """Check if any upstream tasks failed and fail the DAG if so"""
    dag_run = context['dag_run']
    
    # Get all task instances for this DAG run
    failed_tasks = []
    for task_instance in dag_run.get_task_instances():
        if task_instance.state == 'failed' and task_instance.task_id not in ['ward_unlock', 'send_slack_notification', 'check_workflow_status']:
            failed_tasks.append(task_instance.task_id)
    
    if failed_tasks:
        raise AirflowFailException(f"Workflow failed due to task failures: {', '.join(failed_tasks)}")
    
    logging.info("All workflow tasks completed successfully!")


default_args = {
    "owner": "automiq",
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(seconds=20),
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

    lock = ward_lock("{{ params.target }}")
    metadata = get_netbox_metadata("{{ params.target }}")
    
    # Create Nornir task
    nornir = KubernetesPodOperator(
        task_id='run_nornir',
        namespace='ansible',
        image='docker.io/nthomas48/nornir-runner:latest',
        cmds=['/bin/bash', '-c'],
        arguments=['''
            set -euo pipefail
            
            echo "▶ Preparing workspace"
            mkdir -p /workspace && cd /workspace
            
            echo "▶ Cloning Nornir repo"
            git clone https://github.com/napdthomas/automiq-nornir.git repo
            cd repo
            
            echo "▶ Running Nornir script"
            echo "Target: $TARGET_NAME ($TARGET_IP)"
            python scripts/nornir_ping.py --target $TARGET_NAME
        '''],
        name='nornir-{{ ti.xcom_pull(task_ids="get_netbox_metadata")["target"] | replace(".", "-") | replace("_", "-") }}',
        env_vars={
            'TARGET_IP': '{{ ti.xcom_pull(task_ids="get_netbox_metadata")["management_ip"] }}',
            'TARGET_NAME': '{{ ti.xcom_pull(task_ids="get_netbox_metadata")["target"] }}',
        },
        is_delete_operator_pod=False,
        get_logs=True,
    )
    
    # Create Ansible task
    ansible = KubernetesPodOperator(
        task_id='run_ansible',
        namespace='ansible',
        image='docker.io/nthomas48/ansible-ara-runner:latest',
        cmds=['/bin/bash', '-c'],
        arguments=['''
            set -euo pipefail

            echo '{{ params.extra_vars }}' > /tmp/extra_vars.json

            mkdir -p /workspace && cd /workspace
            git clone https://github.com/napdthomas/automiq-ansible.git repo
            cd repo

            if [ ! -f "playbooks/{{ params.playbook }}" ]; then
              echo "Playbook not found" >&2
              exit 10
            fi

            echo "Running playbook: {{ params.playbook }}"
            echo "Target: $TARGET_IP"
            
            ansible-playbook playbooks/{{ params.playbook }} \
              -i "$TARGET_IP," \
              --extra-vars @/tmp/extra_vars.json
        '''],
        name='ansible-{{ ti.xcom_pull(task_ids="get_netbox_metadata")["target"] | replace(".", "-") | replace("_", "-") }}',
        env_vars={
            'ANSIBLE_STDOUT_CALLBACK': 'default',
            'ANSIBLE_HOST_KEY_CHECKING': 'False',
            'VAULT_TOKEN': VAULT_TOKEN,
            'TARGET_IP': '{{ ti.xcom_pull(task_ids="get_netbox_metadata")["management_ip"] }}',
            'TARGET_NAME': '{{ ti.xcom_pull(task_ids="get_netbox_metadata")["target"] }}',
        },
        is_delete_operator_pod=False,
        get_logs=True,
    )
    
    unlock = ward_unlock("{{ params.target }}")
    slack_notify = send_slack_notification()
    status_check = check_workflow_status()

    # DAG structure: Always unlock, send Slack notification, then check status
    lock >> metadata >> nornir >> ansible >> unlock >> slack_notify >> status_check
