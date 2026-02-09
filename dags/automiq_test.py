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
import time

# Config
NETBOX_URL = "http://netbox.core.svc.cluster.local/api"
VAULT_ADDR = "http://vault.core.svc.cluster.local:8200"
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
REDIS_HOST = "netbox-valkey-primary.core.svc.cluster.local"
REDIS_PORT = 6379
REDIS_PASSWORD = os.getenv("VALKEY_PASSWORD")
JENKINS_URL = "http://jenkins.jenkins.svc.cluster.local:8080"
JENKINS_USER = os.getenv("JENKINS_USER")
JENKINS_TOKEN = os.getenv("JENKINS_TOKEN")


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
def check_workflow_status(**context):
    """
    Check if any critical tasks failed.
    Store failure info in XCom for Jenkins notification.
    """
    ti = context['ti']
    dag_run = context['dag_run']
    
    # Check if critical workflow tasks succeeded by trying to pull their XCom data
    critical_tasks = ['ward_lock', 'get_netbox_metadata', 'run_nornir', 'run_ansible']
    failed_tasks = []
    
    for task_id in critical_tasks:
        try:
            result = ti.xcom_pull(task_ids=task_id, key='return_value')
            if result is None and task_id != 'ward_lock':  # ward_lock doesn't return anything
                failed_tasks.append(task_id)
        except Exception as e:
            logging.warning(f"Could not retrieve XCom from {task_id}: {e}")
            failed_tasks.append(task_id)
    
    # Store failure info in XCom for Jenkins task
    if failed_tasks:
        ti.xcom_push(key='failed_tasks', value=failed_tasks)
        ti.xcom_push(key='workflow_failed', value=True)
        raise AirflowFailException(f"Workflow failed due to task failures: {', '.join(failed_tasks)}")
    
    ti.xcom_push(key='workflow_failed', value=False)
    logging.info("All workflow tasks completed successfully!")
    return "Workflow completed successfully"


@task(trigger_rule="all_done")
def trigger_jenkins_slack_notification(**context):
    """
    Trigger Jenkins job to send Slack notification if workflow failed.
    """
    ti = context['ti']
    dag_run = context['dag_run']
    
    if not JENKINS_USER or not JENKINS_TOKEN:
        logging.error("Jenkins credentials not configured")
        return "Jenkins credentials not configured"
    
    # Check if workflow failed
    workflow_failed = ti.xcom_pull(task_ids='check_workflow_status', key='workflow_failed')
    
    if not workflow_failed:
        logging.info("No failures; skipping Slack notification.")
        return "No failures detected"
    
    # Get failed tasks
    failed_tasks = ti.xcom_pull(task_ids='check_workflow_status', key='failed_tasks') or []
    failed_task_id = failed_tasks[0] if failed_tasks else "unknown"
    
    # Build log command
    log_command = (
        f"kubectl logs -n airflow airflow-scheduler-0 -c scheduler --tail=200 | "
        f"grep -A 50 '{failed_task_id}'"
    )
    
    status_command = (
        f"kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- "
        f"airflow tasks states-for-dag-run {dag_run.dag_id} {dag_run.run_id}"
    )
    
    # Jenkins parameters
    jenkins_params = {
        "DAG_ID": dag_run.dag_id,
        "RUN_ID": dag_run.run_id,
        "TASK_ID": failed_task_id,
        "LOG_URL": status_command,
    }
    
    try:
        job_name = "run_slack_job_scm"
        url = f"{JENKINS_URL}/job/{job_name}/buildWithParameters"
        
        logging.info(f"Triggering Jenkins Slack job: {job_name}")
        logging.info(f"Parameters: {jenkins_params}")
        
        resp = requests.post(
            url,
            params=jenkins_params,
            auth=(JENKINS_USER, JENKINS_TOKEN),
            timeout=10
        )
        resp.raise_for_status()
        
        # Get queue location
        queue_url = resp.headers.get("Location")
        if not queue_url:
            logging.error("No queue location in Jenkins response")
            return "Jenkins job triggered but no queue URL"
        
        logging.info(f"Jenkins job queued at: {queue_url}")
        
        # Wait for job to start and get build number
        build_number = None
        for _ in range(30):
            try:
                q = requests.get(
                    f"{queue_url}api/json",
                    auth=(JENKINS_USER, JENKINS_TOKEN),
                    timeout=5
                )
                q.raise_for_status()
                data = q.json()
                if data.get("executable"):
                    build_number = data["executable"]["number"]
                    break
            except Exception as e:
                logging.warning(f"Waiting for Jenkins build to start: {e}")
            time.sleep(2)
        
        if build_number:
            logging.info(f"✓ Jenkins Slack notification job started: #{build_number}")
            return f"Slack notification job started: #{build_number}"
        else:
            logging.warning("Jenkins job queued but build number not retrieved")
            return "Slack notification job queued"
            
    except Exception as e:
        logging.error(f"Failed to trigger Jenkins Slack notification: {e}")
        return f"Failed to trigger Jenkins: {e}"


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
    status_check = check_workflow_status()
    slack_notify = trigger_jenkins_slack_notification()

    # DAG flow: Run workflow, always unlock, check status, send Slack notification via Jenkins
    lock >> metadata >> nornir >> ansible >> unlock >> status_check >> slack_notify
