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


@task
def run_nornir_job(metadata: dict):
    """Run Nornir job with metadata from NetBox"""
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    
    # Pass metadata as environment variables
    env_vars = {
        'TARGET_IP': metadata['management_ip'],
        'TARGET_NAME': metadata['target'],
        'SITE': metadata.get('site', ''),
        'ROLE': metadata.get('role', ''),
        'MANUFACTURER': metadata.get('manufacturer', ''),
        'TYPE': metadata.get('type', ''),
    }
    
    op = KubernetesPodOperator(
        task_id='run_nornir_k8s',
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
            echo "Metadata: site=$SITE, role=$ROLE, manufacturer=$MANUFACTURER, type=$TYPE"
            
            # Pass target name (script will use TARGET_IP env var)
            python scripts/nornir_ping.py --target $TARGET_NAME
        '''],
        name=f"nornir-{metadata['target'].replace('.', '-').replace('_', '-')}",
        env_vars=env_vars,
        is_delete_operator_pod=False,  # Keep pod for debugging
        get_logs=True,
    )
    
    return op.execute(context={})


@task
def run_ansible_job(metadata: dict, playbook: str, extra_vars: str):
    """Run Ansible job with metadata from NetBox"""
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    import json
    
    # Merge extra_vars with metadata
    try:
        extra = json.loads(extra_vars) if extra_vars else {}
    except:
        extra = {}
    
    extra.update({
        'target_ip': metadata['management_ip'],
        'target_name': metadata['target'],
        'site': metadata.get('site', ''),
        'role': metadata.get('role', ''),
    })
    
    extra_vars_json = json.dumps(extra)
    
    op = KubernetesPodOperator(
        task_id='run_ansible_k8s',
        namespace='ansible',
        image='docker.io/nthomas48/ansible-ara-runner:latest',
        cmds=['/bin/bash', '-c'],
        arguments=[f'''
            set -euo pipefail

            echo '{extra_vars_json}' > /tmp/extra_vars.json

            mkdir -p /workspace && cd /workspace
            git clone https://github.com/napdthomas/automiq-ansible.git repo
            cd repo

            if [ ! -f "playbooks/{playbook}" ]; then
              echo "Playbook not found" >&2
              exit 10
            fi

            echo "Running playbook: {playbook}"
            echo "Target: {metadata['management_ip']}"
            
            ansible-playbook playbooks/{playbook} \
              -i "{metadata['management_ip']}," \
              --extra-vars @/tmp/extra_vars.json
        '''],
        name=f"ansible-{metadata['target'].replace('.', '-').replace('_', '-')}",
        env_vars={
            'ANSIBLE_STDOUT_CALLBACK': 'default',
            'ANSIBLE_HOST_KEY_CHECKING': 'False',
        },
        is_delete_operator_pod=False,  # Keep pod for debugging
        get_logs=True,
    )
    
    return op.execute(context={})


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
        "playbook": "eos_system_baseline.yaml",
        "extra_vars": '{}',
    },
) as dag:

    lock = ward_lock("{{ params.target }}")
    metadata = get_netbox_metadata("{{ params.target }}")
    nornir = run_nornir_job(metadata)
    ansible = run_ansible_job(metadata, "{{ params.playbook }}", "{{ params.extra_vars }}")
    unlock = ward_unlock("{{ params.target }}")

    lock >> metadata >> nornir >> ansible >> unlock
