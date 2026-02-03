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

# Config
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
    
    return {
        "target": target,
        "site": device.get("site", {}).get("name"),
        "role": device.get("device_role", {}).get("name"),
        "manufacturer": device.get("device_type", {}).get("manufacturer", {}).get("name"),
        "type": device.get("device_type", {}).get("model"),
    }


def run_nornir_job(target: str, playbook: str, extra_vars: str):
    """Run Nornir job directly in Kubernetes"""
    return KubernetesPodOperator(
        task_id='run_nornir',
        namespace='ansible',
        image='docker.io/nthomas48/nornir-runner:latest',
        cmds=['/bin/bash', '-c'],
        arguments=[f'''
            set -euo pipefail
            
            echo "▶ Preparing workspace"
            mkdir -p /workspace
            cd /workspace
            
            echo "▶ Cloning Nornir repo"
            git clone https://github.com/napdthomas/automiq-nornir.git repo
            cd repo
            
            echo "▶ Running Nornir script"
            if [ ! -f "scripts/{playbook}" ]; then
              echo "❌ Script scripts/{playbook} not found" >&2
              exit 10
            fi
            
            python scripts/{playbook} {target}
        '''],
        name=f'nornir-{target.replace(".", "-").replace("_", "-")}',
        is_delete_operator_pod=True,
        get_logs=True,
    )


def run_ansible_job(target: str, playbook: str, extra_vars: str):
    """Run Ansible job directly in Kubernetes"""
    # Encode extra vars to base64
    import base64
    extra_vars_b64 = base64.b64encode(extra_vars.encode()).decode()
    
    return KubernetesPodOperator(
        task_id='run_ansible',
        namespace='ansible',
        image='docker.io/nthomas48/ansible-ara-runner:latest',
        cmds=['/bin/bash', '-c'],
        arguments=[f'''
            set -euo pipefail

            # decode extra vars
            if [ -n "{extra_vars_b64}" ]; then
              echo "{extra_vars_b64}" | base64 -d > /tmp/extra_vars.json
            else
              echo "{{}}" > /tmp/extra_vars.json
            fi

            # clone repo
            mkdir -p /workspace
            cd /workspace
            git clone https://github.com/napdthomas/automiq-ansible.git repo
            cd repo

            # ensure playbook exists
            if [ ! -f "playbooks/{playbook}" ]; then
              printf 'Playbook not found\\n' >&2
              exit 10
            fi

            # run playbook
            ansible-playbook playbooks/{playbook} \\
              -i "{target}," \\
              --extra-vars @/tmp/extra_vars.json
        '''],
        name=f'ansible-{target.replace(".", "-").replace("_", "-")}',
        is_delete_operator_pod=True,
        get_logs=True,
        env_vars={
            'ANSIBLE_STDOUT_CALLBACK': 'default',
            'ANSIBLE_HOST_KEY_CHECKING': 'False',
        }
    )


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
        "target": "clab-netauto-lab-ceos1",
        "playbook": "eos_system_baseline.yaml",
        "extra_vars": '{"interface":"Ethernet1","description":"automation test"}',
    },
) as dag:

    lock = ward_lock("{{ params.target }}")
    metadata = get_netbox_metadata("{{ params.target }}")
    
    nornir = run_nornir_job(
        target="{{ params.target }}",
        playbook="nornir_ping.py",
        extra_vars="{{ params.extra_vars }}"
    )
    
    ansible = run_ansible_job(
        target="{{ params.target }}",
        playbook="{{ params.playbook }}",
        extra_vars="{{ params.extra_vars }}"
    )
    
    unlock = ward_unlock("{{ params.target }}")

    lock >> metadata >> nornir >> ansible >> unlock
