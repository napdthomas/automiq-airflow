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
import subprocess

NETBOX_URL = "http://netbox.core.svc.cluster.local/api"
VAULT_ADDR = "http://vault.core.svc.cluster.local:8200"
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
REDIS_HOST = "netbox-valkey-primary.core.svc.cluster.local"
REDIS_PORT = 6379
REDIS_PASSWORD = os.getenv("VALKEY_PASSWORD")
JENKINS_URL = "http://jenkins.jenkins:8080"
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


def get_failed_task_from_dag_run(dag_id, run_id):
    """Execute kubectl to get task states and find the failed task"""
    try:
        result = subprocess.run(
            [
                "kubectl", "exec", "-n", "airflow", "airflow-scheduler-0", 
                "-c", "scheduler", "--",
                "airflow", "tasks", "states-for-dag-run", dag_id, run_id
            ],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if '| failed' in line:
                    parts = line.split('|')
                    if len(parts) >= 3:
                        task_id = parts[2].strip()
                        logging.info(f"Found failed task: {task_id}")
                        return task_id
        
        logging.warning("Could not find failed task in kubectl output")
        return "unknown"
    except Exception as e:
        logging.error(f"Error getting failed task: {e}")
        return "unknown"


@task(trigger_rule="one_failed")
def trigger_jenkins_jira_notification(**context):
    ti = context['ti']
    dag_run = context['dag_run']
    
    if not JENKINS_USER or not JENKINS_TOKEN:
        logging.error("Jenkins credentials not configured")
        return {"status": "error", "message": "Jenkins credentials not configured"}
    
    failed_task_id = get_failed_task_from_dag_run(dag_run.dag_id, dag_run.run_id)
    
    status_command = (
        f"kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- "
        f"airflow tasks states-for-dag-run {dag_run.dag_id} {dag_run.run_id}"
    )
    
    jenkins_params = {
        "DAG_ID": dag_run.dag_id,
        "RUN_ID": dag_run.run_id,
        "TASK_ID": failed_task_id,
        "LOG_URL": status_command,
    }
    
    try:
        job_name = "run_jira_job"
        url = f"{JENKINS_URL}/job/{job_name}/buildWithParameters"
        
        logging.info(f"Triggering Jenkins Jira job: {job_name}")
        logging.info(f"Failed task: {failed_task_id}")
        
        resp = requests.post(
            url,
            params=jenkins_params,
            auth=(JENKINS_USER, JENKINS_TOKEN),
            timeout=10
        )
        resp.raise_for_status()
        
        queue_url = resp.headers.get("Location")
        if not queue_url:
            logging.error("No queue location in Jenkins response")
            return {"status": "error", "message": "No queue URL", "failed_task": failed_task_id}
        
        logging.info(f"Jenkins job queued at: {queue_url}")
        
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
        
        if not build_number:
            logging.warning("Jenkins job queued but build number not retrieved")
            return {"status": "queued", "build_number": None, "jira_ticket": None, "failed_task": failed_task_id}
        
        logging.info(f"Jenkins Jira job started: #{build_number}")
        
        for _ in range(60):
            try:
                build_resp = requests.get(
                    f"{JENKINS_URL}/job/{job_name}/{build_number}/api/json",
                    auth=(JENKINS_USER, JENKINS_TOKEN),
                    timeout=5
                )
                build_resp.raise_for_status()
                build_data = build_resp.json()
                
                if not build_data.get("building"):
                    if build_data.get("result") == "SUCCESS":
                        console_resp = requests.get(
                            f"{JENKINS_URL}/job/{job_name}/{build_number}/consoleText",
                            auth=(JENKINS_USER, JENKINS_TOKEN),
                            timeout=10
                        )
                        console_text = console_resp.text
                        
                        import re
                        match = re.search(r'JIRA issue created.*?([A-Z]+-\d+)', console_text)
                        if match:
                            jira_ticket = match.group(1)
                            logging.info(f"✓ Jira ticket created: {jira_ticket}")
                            return {"status": "success", "build_number": build_number, "jira_ticket": jira_ticket, "failed_task": failed_task_id}
                        else:
                            logging.warning("Could not extract Jira ticket from console")
                            return {"status": "success", "build_number": build_number, "jira_ticket": None, "failed_task": failed_task_id}
                    else:
                        logging.error(f"Jenkins job failed: {build_data.get('result')}")
                        return {"status": "failed", "build_number": build_number, "jira_ticket": None, "failed_task": failed_task_id}
                    break
            except Exception as e:
                logging.warning(f"Waiting for Jenkins job to complete: {e}")
            time.sleep(2)
        
        return {"status": "timeout", "build_number": build_number, "jira_ticket": None, "failed_task": failed_task_id}
            
    except Exception as e:
        logging.error(f"Failed to trigger Jenkins Jira notification: {e}")
        return {"status": "error", "message": str(e), "failed_task": failed_task_id}


@task(trigger_rule="all_done")
def trigger_jenkins_slack_notification(**context):
    ti = context['ti']
    dag_run = context['dag_run']
    
    jira_result = ti.xcom_pull(task_ids='trigger_jenkins_jira_notification', key='return_value')
    
    if not jira_result or not isinstance(jira_result, dict):
        logging.info("No failures; skipping Slack notification.")
        return "No failures detected"
    
    if not JENKINS_USER or not JENKINS_TOKEN:
        logging.error("Jenkins credentials not configured")
        return "Jenkins credentials not configured"
    
    failed_task_id = jira_result.get('failed_task', 'unknown')
    jira_ticket = jira_result.get('jira_ticket')
    
    status_command = (
        f"kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- "
        f"airflow tasks states-for-dag-run {dag_run.dag_id} {dag_run.run_id}"
    )
    
    jenkins_params = {
        "DAG_ID": dag_run.dag_id,
        "RUN_ID": dag_run.run_id,
        "TASK_ID": failed_task_id,
        "LOG_URL": status_command,
    }
    
    if jira_ticket:
        jenkins_params["JIRA_TICKET"] = jira_ticket
    
    try:
        job_name = "run_slack_job"
        url = f"{JENKINS_URL}/job/{job_name}/buildWithParameters"
        
        logging.info(f"Triggering Jenkins Slack job: {job_name}")
        logging.info(f"Failed task: {failed_task_id}")
        if jira_ticket:
            logging.info(f"Jira ticket: {jira_ticket}")
        
        resp = requests.post(
            url,
            params=jenkins_params,
            auth=(JENKINS_USER, JENKINS_TOKEN),
            timeout=10
        )
        resp.raise_for_status()
        
        queue_url = resp.headers.get("Location")
        if not queue_url:
            logging.error("No queue location in Jenkins response")
            return "Jenkins job triggered but no queue URL"
        
        logging.info(f"Jenkins job queued at: {queue_url}")
        
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


@task(trigger_rule="all_done")
def ensure_dag_fails_on_error(**context):
    ti = context['ti']
    
    try:
        jira_result = ti.xcom_pull(task_ids='trigger_jenkins_jira_notification', key='return_value')
        if jira_result and isinstance(jira_result, dict) and jira_result.get('status') != 'error':
            logging.error("DAG failed - Jira notification was sent")
            raise AirflowFailException("Workflow failed - one or more critical tasks failed")
    except AirflowFailException:
        raise
    except Exception as e:
        logging.info(f"No failures detected (jira task was skipped): {e}")
    
    logging.info("All tasks completed successfully")
    return "DAG completed successfully"


default_args = {
    "owner": "automiq",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
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
    jira_notify = trigger_jenkins_jira_notification()
    slack_notify = trigger_jenkins_slack_notification()
    final_check = ensure_dag_fails_on_error()

    lock >> metadata >> nornir >> ansible
    
    [nornir, ansible] >> unlock
    [nornir, ansible] >> jira_notify >> slack_notify
    
    [unlock, slack_notify] >> final_check
