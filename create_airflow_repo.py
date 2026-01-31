#!/usr/bin/env python3
import os

# Directory structure
dirs = [
    "dags",
    "plugins/custom_operators",
    "config",
    "tests",
]

# Files to create with content
files = {
    "README.md": """# Automiq Airflow DAGs

This repository contains Airflow DAGs for network automation.

## Structure
- `dags/` - Airflow DAG definitions
- `plugins/` - Custom operators and hooks
- `config/` - Configuration files for connections and variables
- `tests/` - DAG validation tests

## Deployment
DAGs are automatically deployed via Jenkins pipeline.
""",
    "dags/automiq_test.py": """# Add your automiq_test.py content here
from airflow import DAG
from datetime import datetime

# Your DAG code goes here
""",
    "dags/network_provisioning.py": """# Network provisioning DAG
from airflow import DAG
from datetime import datetime

# Your provisioning DAG code goes here
""",
    "dags/backup_configs.py": """# Backup configurations DAG
from airflow import DAG
from datetime import datetime

# Your backup DAG code goes here
""",
    "plugins/custom_operators/__init__.py": """# Custom operators package
""",
    "config/connections.json": """{
  "connections": [
    {
      "conn_id": "netbox_default",
      "conn_type": "http",
      "host": "netbox",
      "port": 8080
    }
  ]
}
""",
    "config/variables.json": """{
  "variables": {
    "jenkins_url": "http://automiq-jenkins:8080",
    "vault_addr": "http://vault:8200"
  }
}
""",
    "tests/test_dags.py": """import pytest
from airflow.models import DagBag

def test_no_import_errors():
    dag_bag = DagBag()
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

def test_dags_load():
    dag_bag = DagBag()
    assert dag_bag.dag_ids, "No DAGs found"
""",
    "requirements.txt": """apache-airflow==3.0.2
redis
requests
""",
    ".gitignore": """__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
*.log
.DS_Store
"""
}

def create_structure():
    # Create subdirectories
    for dir_path in dirs:
        os.makedirs(dir_path, exist_ok=True)
        print(f"Created directory: {dir_path}")
    
    # Create files
    for file_path, content in files.items():
        # Create parent directory if needed
        parent_dir = os.path.dirname(file_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        
        with open(file_path, 'w') as f:
            f.write(content)
        print(f"Created file: {file_path}")
    
    print("\n✓ Directory structure created successfully!")
    print("\nNext steps:")
    print("1. git init")
    print("2. git add .")
    print("3. git commit -m 'Initial commit'")
    print("4. git remote add origin <your-repo-url>")
    print("5. git push -u origin main")

if __name__ == "__main__":
    create_structure()
