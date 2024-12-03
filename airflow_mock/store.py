"""
Data store for managing multiple Airflow instance states.
"""
from typing import Dict, List, Optional, Any
from datetime import datetime
from .models import (
    DAG, DAGRun, TaskInstance, Variable,
    Connection, TaskLog, XCom, Pool,
    Provider, ProviderHook
)

class InstanceStore:
    """Manages the state for a single Airflow instance."""
    
    def __init__(self, instance_id: str):
        self.instance_id = instance_id
        self.dags: Dict[str, DAG] = {}
        self.dag_runs: Dict[str, Dict[str, DAGRun]] = {}  # dag_id -> run_id -> DAGRun
        self.task_instances: Dict[str, Dict[str, Dict[str, TaskInstance]]] = {}  # dag_id -> run_id -> task_id -> TaskInstance
        self.task_logs: Dict[str, Dict[str, Dict[str, Dict[int, TaskLog]]]] = {}  # dag_id -> run_id -> task_id -> try_number -> TaskLog
        self.variables: Dict[str, Variable] = {}
        self.connections: Dict[str, Connection] = {}
        self.xcoms: Dict[str, Dict[str, Dict[str, Dict[str, XCom]]]] = {}  # dag_id -> task_id -> key -> run_id -> XCom
        self.pools: Dict[str, Pool] = {}
        self.providers: Dict[str, Provider] = {}  # package_name -> Provider

    def add_dag(self, dag: DAG) -> None:
        """Add or update a DAG."""
        self.dags[dag.dag_id] = dag
        
    def get_dag(self, dag_id: str) -> Optional[DAG]:
        """Get a DAG by ID."""
        return self.dags.get(dag_id)
        
    def add_dag_run(self, dag_run: DAGRun) -> None:
        """Add or update a DAG run."""
        if dag_run.dag_id not in self.dag_runs:
            self.dag_runs[dag_run.dag_id] = {}
        self.dag_runs[dag_run.dag_id][dag_run.run_id] = dag_run
        
    def get_dag_run(self, dag_id: str, run_id: str) -> Optional[DAGRun]:
        """Get a DAG run by DAG ID and run ID."""
        return self.dag_runs.get(dag_id, {}).get(run_id)
        
    def add_task_instance(self, task_instance: TaskInstance) -> None:
        """Add or update a task instance."""
        if task_instance.dag_id not in self.task_instances:
            self.task_instances[task_instance.dag_id] = {}
        if task_instance.run_id not in self.task_instances[task_instance.dag_id]:
            self.task_instances[task_instance.dag_id][task_instance.run_id] = {}
        self.task_instances[task_instance.dag_id][task_instance.run_id][task_instance.task_id] = task_instance
        
    def get_task_instance(self, dag_id: str, run_id: str, task_id: str) -> Optional[TaskInstance]:
        """Get a task instance by DAG ID, run ID, and task ID."""
        return self.task_instances.get(dag_id, {}).get(run_id, {}).get(task_id)

    def get_task_instances(self, dag_id: str, run_id: str) -> List[TaskInstance]:
        """Get all task instances for a DAG run."""
        return list(self.task_instances.get(dag_id, {}).get(run_id, {}).values())

    def add_task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int, log: TaskLog) -> None:
        """Add a log entry for a task instance."""
        if dag_id not in self.task_logs:
            self.task_logs[dag_id] = {}
        if run_id not in self.task_logs[dag_id]:
            self.task_logs[dag_id][run_id] = {}
        if task_id not in self.task_logs[dag_id][run_id]:
            self.task_logs[dag_id][run_id][task_id] = {}
        self.task_logs[dag_id][run_id][task_id][try_number] = log

    def get_task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int) -> Optional[TaskLog]:
        """Get a log entry for a task instance."""
        return self.task_logs.get(dag_id, {}).get(run_id, {}).get(task_id, {}).get(try_number)

    def clear_task_instance(self, task_instance: TaskInstance) -> None:
        """Clear a task instance by resetting its state and try number."""
        task_instance.state = "none"
        task_instance.try_number = 1
        task_instance.start_date = None
        task_instance.end_date = None
        task_instance.duration = None
        self.add_task_instance(task_instance)

    def add_xcom_value(self, xcom: XCom) -> None:
        """Add or update an XCom value."""
        if xcom.dag_id not in self.xcoms:
            self.xcoms[xcom.dag_id] = {}
        if xcom.task_id not in self.xcoms[xcom.dag_id]:
            self.xcoms[xcom.dag_id][xcom.task_id] = {}
        if xcom.key not in self.xcoms[xcom.dag_id][xcom.task_id]:
            self.xcoms[xcom.dag_id][xcom.task_id][xcom.key] = {}
        if xcom.run_id:
            self.xcoms[xcom.dag_id][xcom.task_id][xcom.key][xcom.run_id] = xcom

    def get_xcom_value(self, dag_id: str, task_id: str, key: str, run_id: Optional[str] = None) -> Optional[XCom]:
        """Get an XCom value."""
        if run_id:
            return self.xcoms.get(dag_id, {}).get(task_id, {}).get(key, {}).get(run_id)
        # If run_id is not specified, return the most recent XCom value
        xcoms = self.xcoms.get(dag_id, {}).get(task_id, {}).get(key, {}).values()
        if not xcoms:
            return None
        return max(xcoms, key=lambda x: x.timestamp)

    def list_xcom_values(
        self,
        dag_id: Optional[str] = None,
        task_id: Optional[str] = None,
        run_id: Optional[str] = None,
        key: Optional[str] = None
    ) -> List[XCom]:
        """List XCom values with optional filtering."""
        result = []
        
        # Get all XComs if no DAG ID specified
        dag_ids = [dag_id] if dag_id else list(self.xcoms.keys())
        
        for d_id in dag_ids:
            if d_id not in self.xcoms:
                continue
                
            # Get all tasks if no task ID specified
            task_ids = [task_id] if task_id else list(self.xcoms[d_id].keys())
            
            for t_id in task_ids:
                if t_id not in self.xcoms[d_id]:
                    continue
                    
                # Get all keys if no key specified
                keys = [key] if key else list(self.xcoms[d_id][t_id].keys())
                
                for k in keys:
                    if k not in self.xcoms[d_id][t_id]:
                        continue
                        
                    # Get all run IDs if no run ID specified
                    run_ids = [run_id] if run_id else list(self.xcoms[d_id][t_id][k].keys())
                    
                    for r_id in run_ids:
                        if r_id in self.xcoms[d_id][t_id][k]:
                            result.append(self.xcoms[d_id][t_id][k][r_id])
                            
        return result

    def delete_xcom_value(self, dag_id: str, task_id: str, key: str, run_id: Optional[str] = None) -> bool:
        """Delete an XCom value. Returns True if deleted, False if not found."""
        try:
            if run_id:
                del self.xcoms[dag_id][task_id][key][run_id]
            else:
                del self.xcoms[dag_id][task_id][key]
            return True
        except KeyError:
            return False

    def add_pool(self, pool: Pool) -> None:
        """Add or update a pool."""
        self.pools[pool.name] = pool

    def get_pool(self, name: str) -> Optional[Pool]:
        """Get a pool by name."""
        return self.pools.get(name)

    def list_pools(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> List[Pool]:
        """List pools with optional pagination and ordering."""
        pools = list(self.pools.values())
        
        # Apply ordering if specified
        if order_by:
            reverse = order_by.startswith('-')
            key = order_by[1:] if reverse else order_by
            pools.sort(key=lambda x: getattr(x, key), reverse=reverse)
            
        # Apply pagination
        if offset is not None:
            pools = pools[offset:]
        if limit is not None:
            pools = pools[:limit]
            
        return pools

    def delete_pool(self, name: str) -> bool:
        """Delete a pool. Returns True if deleted, False if not found."""
        try:
            del self.pools[name]
            return True
        except KeyError:
            return False

    def update_pool_slots(self, name: str, occupied: int = 0, queued: int = 0, running: int = 0) -> Optional[Pool]:
        """Update pool slot usage."""
        pool = self.get_pool(name)
        if pool:
            pool.occupied_slots = occupied
            pool.queued_slots = queued
            pool.running_slots = running
            pool.open_slots = pool.slots - pool.occupied_slots
            return pool
        return None

    def add_variable(self, variable: Variable) -> None:
        """Add or update a variable."""
        self.variables[variable.key] = variable

    def get_variable(self, key: str) -> Optional[Variable]:
        """Get a variable by key."""
        return self.variables.get(key)

    def list_variables(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> List[Variable]:
        """List variables with optional pagination and ordering."""
        variables = list(self.variables.values())
        
        # Apply ordering if specified
        if order_by:
            reverse = order_by.startswith('-')
            key = order_by[1:] if reverse else order_by
            variables.sort(key=lambda x: getattr(x, key), reverse=reverse)
            
        # Apply pagination
        if offset is not None:
            variables = variables[offset:]
        if limit is not None:
            variables = variables[:limit]
            
        return variables

    def delete_variable(self, key: str) -> bool:
        """Delete a variable. Returns True if deleted, False if not found."""
        try:
            del self.variables[key]
            return True
        except KeyError:
            return False

    def add_connection(self, connection: Connection) -> None:
        """Add or update a connection."""
        self.connections[connection.conn_id] = connection

    def get_connection(self, conn_id: str) -> Optional[Connection]:
        """Get a connection by ID."""
        return self.connections.get(conn_id)

    def list_connections(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Connection]:
        """List connections with optional pagination."""
        connections = list(self.connections.values())
        if offset is not None:
            connections = connections[offset:]
        if limit is not None:
            connections = connections[:limit]
        return connections

    def delete_connection(self, conn_id: str) -> bool:
        """Delete a connection. Returns True if deleted, False if not found."""
        try:
            del self.connections[conn_id]
            return True
        except KeyError:
            return False

    def add_provider(self, provider: Provider) -> None:
        """Add or update a provider."""
        self.providers[provider.package_name] = provider

    def get_provider(self, package_name: str) -> Optional[Provider]:
        """Get a provider by package name."""
        return self.providers.get(package_name)

    def list_providers(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> List[Provider]:
        """List providers with optional pagination and ordering."""
        providers = list(self.providers.values())
        
        # Apply ordering if specified
        if order_by:
            reverse = order_by.startswith('-')
            key = order_by[1:] if reverse else order_by
            providers.sort(key=lambda x: getattr(x, key), reverse=reverse)
            
        # Apply pagination
        if offset is not None:
            providers = providers[offset:]
        if limit is not None:
            providers = providers[:limit]
            
        return providers

    def delete_provider(self, package_name: str) -> bool:
        """Delete a provider. Returns True if deleted, False if not found."""
        try:
            del self.providers[package_name]
            return True
        except KeyError:
            return False

    def get_provider_hooks(self, package_name: str) -> List[ProviderHook]:
        """Get hooks for a specific provider."""
        provider = self.get_provider(package_name)
        return provider.hooks if provider else []

class MockStore:
    """Global store managing multiple Airflow instance states."""
    
    def __init__(self):
        self.instances: Dict[str, InstanceStore] = {}
        
    def get_instance(self, instance_id: str) -> InstanceStore:
        """Get or create an instance store."""
        if instance_id not in self.instances:
            self.instances[instance_id] = InstanceStore(instance_id)
        return self.instances[instance_id]
        
    def list_instances(self) -> List[str]:
        """List all instance IDs."""
        return list(self.instances.keys())

# Global store instance
store = MockStore()
