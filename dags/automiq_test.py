# dags/automiq_test.py - Without locking
from airflow import DAG
from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import logging
import requests
import os
from kafka import KafkaProducer
import json

NETBOX_URL = "http://netbox.core.svc.cluster.local/api"
VAULT_ADDR = "http://vault.core.svc.cluster.local:8200"
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
KAFKA_BOOTSTRAP = "automiq-kafka-bootstrap.kafka.svc.cluster.local:9092"


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
    
    # Simple state determination
    failed = any(t.state == 'failed' for t in ti.get_dagrun().get_task_instances())
    
    event = {
        "dag_id": dag_run.dag_id,
        "run_id": dag_run.run_id,
        "state": "failed" if failed else "success",
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
        return {"status": "success"}
    except Exception as e:
        logging.error(f"✗ Kafka publish failed: {e}")
        return {"status": "failed", "error": str(e)}


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
            mkdir -p /workspace && cd /workspace
            git clone http://gitea-http.gitea.svc.cluster.local:3000/admin/automiq-ansible.git repo
            cd repo
            ansible-playbook playbooks/{{ params.playbook }} -i "$TARGET_IP,"
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

    notify = publish_completion_event()

    metadata >> nornir >> ansible >> notify
