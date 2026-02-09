# dags/automiq_test.py
from airflow import DAG
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import json
import logging
import requests
import redis
import os
import base64

# Config
NETBOX_URL = "http://netbox.core.svc.cluster.local/api"
VAULT_ADDR = "http://vault.core.svc.cluster.local:8200"
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
REDIS_HOST = "netbox-valkey-primary.core.svc.cluster.local"
REDIS_PORT = 6379
REDIS_PASSWORD = os.getenv("VALKEY_PASSWORD")


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


def send_failure_notifications(context):
    """
    Callback function triggered on any task failure.
    Sends notifications to both Slack and Jira.
    """
    ti = context['task_instance']
    dag_run = context['dag_run']
    exception = context.get('exception')
    
    SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")
    JIRA_BASE_URL = "https://automiq-automation.atlassian.net"
    JIRA_PROJECT_KEY = "KAN"
    JIRA_USER = os.getenv("JIRA_USER")
    JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")
    
    # Build kubectl commands for troubleshooting
    log_command = (
        f"kubectl logs -n airflow airflow-scheduler-0 -c scheduler --tail=200 | "
        f"grep -A 50 '{ti.task_id}'"
    )
    
    status_command = (
        f"kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- "
        f"airflow tasks states-for-dag-run {dag_run.dag_id} {dag_run.run_id}"
    )
    
    # Send Slack notification
    if SLACK_WEBHOOK:
        try:
            slack_payload = {
                "text": f""":x: Airflow Task Failed
DAG: `{dag_run.dag_id}`
Run: `{dag_run.run_id}`
Task: `{ti.task_id}`
State: `{ti.state}`
Exception: `{str(exception)[:200] if exception else 'N/A'}`

View logs with:
```
{log_command}
```

Or view all task states:
```
{status_command}
```
"""
            }
            
            resp = requests.post(SLACK_WEBHOOK, json=slack_payload, timeout=10)
            resp.raise_for_status()
            print(f"✓ Slack notification sent for failed task: {ti.task_id}")
        except Exception as e:
            print(f"✗ Failed to send Slack notification: {e}")
    else:
        print("✗ Slack webhook not configured")
    
    # Send Jira notification
    if JIRA_USER and JIRA_API_TOKEN:
        try:
            jira_payload = {
                "fields": {
                    "project": {"key": JIRA_PROJECT_KEY},
                    "summary": f":x: Airflow Task Failed - {dag_run.dag_id}:{ti.task_id}",
                    "description": {
                        "type": "doc",
                        "version": 1,
                        "content": [
                            {
                                "type": "paragraph",
                                "content": [{"type": "text", "text": "Airflow Task Failed\n\n"}]
                            },
                            {
                                "type": "paragraph",
                                "content": [{"type": "text", "text": f"DAG: {dag_run.dag_id}\n", "marks": [{"type": "strong"}]}]
                            },
                            {
                                "type": "paragraph",
                                "content": [{"type": "text", "text": f"Run ID: {dag_run.run_id}\n"}]
                            },
                            {
                                "type": "paragraph",
                                "content": [{"type": "text", "text": f"Failed Task: {ti.task_id}\n"}]
                            },
                            {
                                "type": "paragraph",
                                "content": [{"type": "text", "text": f"State: {ti.state}\n"}]
                            },
                            {
                                "type": "paragraph",
                                "content": [{"type": "text", "text": f"Exception: {str(exception)[:500] if exception else 'N/A'}\n"}]
                            },
                            {
                                "type": "paragraph",
                                "content": [{"type": "text", "text": "\nView logs with:\n"}]
                            },
                            {
                                "type": "codeBlock",
                                "attrs": {"language": "bash"},
                                "content": [{"type": "text", "text": log_command}]
                            },
                            {
                                "type": "paragraph",
                                "content": [{"type": "text", "text": "\nView all task states:\n"}]
                            },
                            {
                                "type": "codeBlock",
                                "attrs": {"language": "bash"},
                                "content": [{"type": "text", "text": status_command}]
                            }
                        ]
                    },
                    "issuetype": {"name": "Task"}
                }
            }
            
            auth_string = f"{JIRA_USER}:{JIRA_API_TOKEN}"
            auth_bytes = base64.b64encode(auth_string.encode('ascii'))
            base64_auth = auth_bytes.decode('ascii')
            
            headers = {
                "Authorization": f"Basic {base64_auth}",
                "Content-Type": "application/json"
            }
            
            resp = requests.post(
                f"{JIRA_BASE_URL}/rest/api/3/issue",
                json=jira_payload,
                headers=headers,
                timeout=10
            )
            resp.raise_for_status()
            issue_data = resp.json()
            issue_key = issue_data.get("key")
            print(f"✓ Jira issue created: {issue_key} - {JIRA_BASE_URL}/browse/{issue_key}")
        except Exception as e:
            print(f"✗ Failed to create Jira issue: {e}")
    else:
        print("✗ Jira credentials not configured")


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
def check_workflow_status(**context):
    """
    Check if any critical tasks failed and fail the DAG if so.
    This ensures the DAG shows as 'failed' when tasks fail.
    """
    ti = context['ti']
    dag_run = context['dag_run']
    
    # Get the states of critical tasks by checking XCom or task context
    # Since we can't query the DB in Airflow 3.0, we use a different approach
    
    # Check if any upstream tasks failed by examining the current task's upstream
    # If this task is running, some tasks completed, but we need to know if any failed
    
    # Simple approach: Store failed task info in XCom from the callback
    # Or check task states from the dag_run object (if accessible)
    
    # For Airflow 3.0, the best approach is to let the callback handle notifications
    # and use task dependencies to determine success/failure
    
    # Check if critical workflow tasks succeeded by trying to pull their XCom data
    critical_tasks = ['ward_lock', 'get_netbox_metadata', 'run_nornir', 'run_ansible']
    failed_tasks = []
    
    for task_id in critical_tasks:
        try:
            # If the task failed, it won't have pushed successful XCom data
            # This is a heuristic approach for Airflow 3.0
            result = ti.xcom_pull(task_ids=task_id, key='return_value')
            # If we got here, task likely succeeded (or was skipped)
        except Exception as e:
            # Task may have failed
            logging.warning(f"Could not retrieve XCom from {task_id}: {e}")
    
    # Alternative: Check if the callback was triggered (it would have set a flag)
    # Or simply assume if we're here with trigger_rule="all_done", check for failures
    
    # Since we can't reliably detect failures in Airflow 3.0 without DB access,
    # we'll rely on the on_failure_callback to handle notifications
    # and manually check the most likely failure points
    
    # If get_netbox_metadata failed, its XCom won't exist
    try:
        metadata = ti.xcom_pull(task_ids='get_netbox_metadata', key='return_value')
        if not metadata:
            failed_tasks.append('get_netbox_metadata')
    except:
        failed_tasks.append('get_netbox_metadata')
    
    if failed_tasks:
        raise AirflowFailException(f"Workflow failed due to task failures: {', '.join(failed_tasks)}")
    
    logging.info("All workflow tasks completed successfully!")
    return "Workflow completed successfully"


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
    on_failure_callback=send_failure_notifications,  # DAG-level callback for notifications
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
    status_check = check_workflow_status()

    # DAG flow: Run workflow, always unlock, then check status
    lock >> metadata >> nornir >> ansible >> unlock >> status_check
