# dags/automiq_test.py
from airflow import DAG
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import logging
import requests
import redis
import os
from kafka import KafkaProducer
import json

NETBOX_URL = "http://netbox.core.svc.cluster.local/api"
VAULT_ADDR = "http://vault.core.svc.cluster.local:8200"
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
REDIS_HOST = "netbox-valkey-primary.core.svc.cluster.local"
REDIS_PORT = 6379
REDIS_PASSWORD = os.getenv("VALKEY_PASSWORD")
KAFKA_BOOTSTRAP = "automiq-kafka-bootstrap.kafka.svc.cluster.local:9092"


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


@task(trigger_rule="all_done")
def publish_completion_event(**context):
    """Publish DAG completion event to Kafka for AI agent to process"""
    dag_run = context['dag_run']
    ti = context['ti']
    
    # Check if any critical tasks failed using database query
    from airflow.models import TaskInstance as TI
    from airflow.settings import Session
    
    session = Session()
    failed_tasks = []
    
    try:
        task_instances = session.query(TI).filter(
            TI.dag_id == dag_run.dag_id,
            TI.run_id == dag_run.run_id,
            TI.task_id.in_(['ward_lock', 'get_netbox_metadata', 'run_nornir', 'run_ansible'])
        ).all()
        
        failed_tasks = [t.task_id for t in task_instances if t.state == 'failed']
        logging.info(f"Task states check: {[(t.task_id, t.state) for t in task_instances]}")
    finally:
        session.close()
    
    has_failures = len(failed_tasks) > 0
    
    event = {
        "dag_id": dag_run.dag_id,
        "run_id": dag_run.run_id,
        "state": "failed" if has_failures else "success",
        "failed_tasks": failed_tasks,
        "start_date": str(dag_run.start_date) if dag_run.start_date else None,
        "end_date": str(datetime.utcnow()),
        "target": dag_run.conf.get("target") if dag_run.conf else None,
        "playbook": dag_run.conf.get("playbook") if dag_run.conf else None,
    }
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        future = producer.send('airflow.dag.completed', event)
        result = future.get(timeout=10)
        producer.close()
        logging.info(f"✓ Published to Kafka: partition={result.partition}, offset={result.offset}")
        logging.info(f"✓ Event: {event}")
        return {"status": "success", "has_failures": has_failures}
    except Exception as e:
        logging.error(f"✗ Kafka publish failed: {type(e).__name__}: {e}")
        return {"status": "failed", "error": str(e), "has_failures": has_failures}


@task(trigger_rule="all_done")
def fail_if_upstream_failed(**context):
    """Final task to ensure DAG fails if any automation tasks failed"""
    ti = context['ti']
    
    # Get result from publish task
    publish_result = ti.xcom_pull(task_ids='publish_completion_event')
    
    if publish_result and publish_result.get('has_failures'):
        logging.error(f"DAG failed - automation tasks failed")
        raise AirflowFailException("DAG failed - one or more automation tasks failed")
    
    logging.info("✓ All automation tasks completed successfully")
    return "success"


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
            mkdir -p /workspace && cd /workspace
            git clone http://gitea-http.gitea.svc.cluster.local:3000/admin/automiq-nornir.git repo
            cd repo
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
            git clone http://gitea-http.gitea.svc.cluster.local:3000/admin/automiq-ansible.git repo
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
            'ANSIBLE_STDOUT_CALLBACK': 'ara_default',
            'ANSIBLE_HOST_KEY_CHECKING': 'False',
            'VAULT_TOKEN': VAULT_TOKEN,
            'TARGET_IP': '{{ ti.xcom_pull(task_ids="get_netbox_metadata")["management_ip"] }}',
            'TARGET_NAME': '{{ ti.xcom_pull(task_ids="get_netbox_metadata")["target"] }}',
        },
        is_delete_operator_pod=False,
        get_logs=True,
    )

    unlock = ward_unlock("{{ params.target }}")
    notify = publish_completion_event()
    final = fail_if_upstream_failed()

    lock >> metadata >> nornir >> ansible >> unlock >> notify >> final
