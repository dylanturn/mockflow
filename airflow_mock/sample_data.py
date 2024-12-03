"""
Sample data generator for Airflow mock API.
"""
from datetime import datetime, timedelta
from typing import List
import random
from .models import DAG, DAGRun, TaskInstance, Variable, Connection
from .store import InstanceStore

def generate_sample_dags(count: int = 5) -> List[DAG]:
    """Generate sample DAGs."""
    dags = []
    for i in range(count):
        dag_id = f"example_dag_{i}"
        dag = DAG(
            dag_id=dag_id,
            is_paused=random.choice([True, False]),
            is_active=True,
            fileloc=f"/tmp/dags/{dag_id}.py",
            owners=["admin"],
            description=f"Example DAG {i} for testing",
            schedule_interval={"type": "cron", "value": "0 0 * * *"},
            tags=[{"name": "example"}, {"name": "test"}]
        )
        dags.append(dag)
    return dags

def generate_sample_dag_runs(dag_id: str, count: int = 3) -> List[DAGRun]:
    """Generate sample DAG runs for a DAG."""
    dag_runs = []
    base_date = datetime.utcnow() - timedelta(days=count)
    
    for i in range(count):
        execution_date = base_date + timedelta(days=i)
        run_id = f"scheduled__{execution_date.strftime('%Y-%m-%dT%H:%M:%S')}"
        state = random.choice(["success", "failed", "running"])
        
        dag_run = DAGRun(
            dag_id=dag_id,
            run_id=run_id,
            execution_date=execution_date,
            start_date=execution_date,
            end_date=execution_date + timedelta(minutes=30) if state != "running" else None,
            state=state,
            external_trigger=False,
            conf={}
        )
        dag_runs.append(dag_run)
    return dag_runs

def generate_sample_task_instances(dag_id: str, run_id: str, count: int = 4) -> List[TaskInstance]:
    """Generate sample task instances for a DAG run."""
    task_instances = []
    base_date = datetime.utcnow() - timedelta(hours=1)
    
    for i in range(count):
        task_id = f"task_{i}"
        state = random.choice(["success", "failed", "running", "upstream_failed"])
        start_date = base_date + timedelta(minutes=i*5)
        
        task_instance = TaskInstance(
            task_id=task_id,
            dag_id=dag_id,
            run_id=run_id,
            state=state,
            try_number=random.randint(1, 3),
            max_tries=3,
            start_date=start_date,
            end_date=start_date + timedelta(minutes=random.randint(1, 10)) if state != "running" else None,
            duration=random.randint(60, 600) if state != "running" else None,
            pool="default_pool",
            queue="default",
            priority_weight=1
        )
        task_instances.append(task_instance)
    return task_instances

def generate_sample_variables() -> List[Variable]:
    """Generate sample variables."""
    return [
        Variable(key="env", value="dev", description="Environment name"),
        Variable(key="api_key", value="secret123", description="API key for external service"),
        Variable(key="batch_size", value="100", description="Default batch size for processing"),
    ]

def generate_sample_connections() -> List[Connection]:
    """Generate sample connections."""
    return [
        Connection(
            conn_id="postgres_default",
            conn_type="postgres",
            host="localhost",
            port=5432,
            login="airflow",
            password="airflow",
            schema="airflow"
        ),
        Connection(
            conn_id="aws_default",
            conn_type="aws",
            login="AKIAXXXXXXXXXXXXXXXX",
            password="secret",
            extra={"region": "us-east-1"}
        ),
        Connection(
            conn_id="http_default",
            conn_type="http",
            host="api.example.com",
            schema="https"
        )
    ]

def populate_instance(instance: InstanceStore) -> None:
    """Populate an instance with sample data."""
    # Generate and add DAGs
    dags = generate_sample_dags()
    for dag in dags:
        instance.add_dag(dag)
        
        # Generate and add DAG runs for each DAG
        dag_runs = generate_sample_dag_runs(dag.dag_id)
        for dag_run in dag_runs:
            instance.add_dag_run(dag_run)
            
            # Generate and add task instances for each DAG run
            task_instances = generate_sample_task_instances(dag.dag_id, dag_run.run_id)
            for task_instance in task_instances:
                instance.add_task_instance(task_instance)
    
    # Add variables
    for variable in generate_sample_variables():
        instance.variables[variable.key] = variable
    
    # Add connections
    for connection in generate_sample_connections():
        instance.connections[connection.conn_id] = connection
