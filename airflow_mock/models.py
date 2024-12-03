"""
Data models for the Airflow mock API.
"""
from datetime import datetime
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

class DAG(BaseModel):
    dag_id: str
    is_paused: bool = False
    is_active: bool = True
    last_parsed_time: datetime = Field(default_factory=datetime.utcnow)
    last_pickled: datetime = Field(default_factory=datetime.utcnow)
    last_expired: datetime = Field(default_factory=datetime.utcnow)
    scheduler_lock: Optional[bool] = None
    pickle_id: Optional[str] = None
    fileloc: str = "/tmp/dag.py"
    owners: List[str] = Field(default_factory=list)
    description: Optional[str] = None
    schedule_interval: Optional[dict] = None
    tags: List[dict] = Field(default_factory=list)

class DAGCollection(BaseModel):
    """Collection of DAGs with metadata."""
    dags: List[DAG]
    total_entries: int

class DAGRun(BaseModel):
    dag_id: str
    run_id: str
    execution_date: datetime = Field(default_factory=datetime.utcnow)
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    state: str = "running"
    external_trigger: bool = False
    conf: dict = Field(default_factory=dict)

class DAGRunCollection(BaseModel):
    """Collection of DAG runs with metadata."""
    dag_runs: List[DAGRun]
    total_entries: int

class TaskInstance(BaseModel):
    task_id: str
    dag_id: str
    run_id: str
    state: str = "none"
    try_number: int = 1
    max_tries: int = 0
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    duration: Optional[float] = None
    pool: str = "default_pool"
    queue: str = "default"
    priority_weight: int = 1
    operator: str = "PythonOperator"
    queued_when: Optional[datetime] = None
    queued_by_job_id: Optional[str] = None
    pid: Optional[int] = None
    executor_config: Dict[str, Any] = Field(default_factory=dict)
    sla_miss: Optional[datetime] = None
    rendered_fields: Dict[str, Any] = Field(default_factory=dict)

class TaskInstanceCollection(BaseModel):
    """Collection of task instances with metadata."""
    task_instances: List[TaskInstance]
    total_entries: int

class TaskInstanceActionResponse(BaseModel):
    """Response for task instance actions like clear, set state, etc."""
    task_id: str
    dag_id: str
    run_id: str
    state: str
    message: str

class TaskLog(BaseModel):
    """Task instance log entry."""
    try_number: int
    content: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class ClearTaskInstance(BaseModel):
    """Request to clear a task instance."""
    dry_run: bool = False
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    only_failed: bool = False
    only_running: bool = False
    include_subdags: bool = False
    include_parentdag: bool = False
    reset_dag_runs: bool = False

class Variable(BaseModel):
    key: str
    value: str
    description: Optional[str] = None

class VariableCollection(BaseModel):
    """Collection of variables with metadata."""
    variables: List[Variable]
    total_entries: int

class Connection(BaseModel):
    conn_id: str
    conn_type: str
    host: Optional[str] = None
    port: Optional[int] = None
    login: Optional[str] = None
    password: Optional[str] = None
    connection_schema: Optional[str] = None
    extra: Optional[dict] = None

class ConnectionCollection(BaseModel):
    """Collection of connections with metadata."""
    connections: List[Connection]
    total_entries: int

class XCom(BaseModel):
    """XCom value for cross-task communication."""
    key: str
    value: Any
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    execution_date: datetime = Field(default_factory=datetime.utcnow)
    task_id: str
    dag_id: str
    run_id: Optional[str] = None
    description: Optional[str] = None

class XComCollection(BaseModel):
    """Collection of XCom values with metadata."""
    xcom_entries: List[XCom]
    total_entries: int

class Pool(BaseModel):
    """Pool model for resource management."""
    name: str
    slots: int
    description: Optional[str] = None
    occupied_slots: int = 0
    queued_slots: int = 0
    running_slots: int = 0
    open_slots: Optional[int] = None
    
    def __init__(self, **data):
        super().__init__(**data)
        # Calculate open slots based on total slots and occupied slots
        self.open_slots = self.slots - self.occupied_slots

class PoolCollection(BaseModel):
    """Collection of pools with metadata."""
    pools: List[Pool]
    total_entries: int

class ProviderHook(BaseModel):
    """Hook information for a provider."""
    hook_class_name: str
    connection_type: str
    hook_name: str
    package_name: str
    description: Optional[str] = None

class Provider(BaseModel):
    """Provider model for external service integration."""
    package_name: str
    description: Optional[str] = None
    version: str
    provider_name: str
    hooks: List[ProviderHook] = []
    extra_links: List[str] = []
    connection_form: Optional[dict] = None

class ProviderCollection(BaseModel):
    """Collection of providers with metadata."""
    providers: List[Provider]
    total_entries: int
