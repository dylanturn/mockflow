"""
FastAPI application for the Airflow mock API.
"""
from datetime import datetime
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, Header, Query, Body, Path
from .models import (
    DAG, DAGCollection,
    DAGRun, DAGRunCollection,
    TaskInstance, TaskInstanceCollection,
    TaskInstanceActionResponse, TaskLog, ClearTaskInstance,
    Variable, Connection, XCom, XComCollection,
    ConnectionCollection, VariableCollection,
    Pool, PoolCollection,
    Provider, ProviderCollection, ProviderHook
)
from .store import store

def create_app(instance_id: str) -> FastAPI:
    """Create a FastAPI application for a specific Airflow instance."""
    app = FastAPI(title=f"Airflow Mock API - Instance {instance_id}")
    instance_store = store.get_instance(instance_id)

    @app.get("/health")
    async def health_check():
        """Check the health of the API."""
        return {
            "status": "healthy",
            "instance_id": instance_id,
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0"
        }

    @app.get("/api/v1/dags", response_model=DAGCollection)
    async def list_dags(
        limit: int = Query(100, ge=1),
        offset: int = Query(0, ge=0)
    ) -> DAGCollection:
        """List DAGs."""
        dags = list(instance_store.dags.values())
        total = len(dags)
        dags = dags[offset:offset + limit]
        return DAGCollection(dags=dags, total_entries=total)

    @app.get("/api/v1/dags/{dag_id}", response_model=DAG)
    async def get_dag(dag_id: str) -> DAG:
        """Get a specific DAG."""
        dag = instance_store.get_dag(dag_id)
        if not dag:
            raise HTTPException(status_code=404, detail="DAG not found")
        return dag

    @app.get("/api/v1/dags/{dag_id}/dagRuns", response_model=DAGRunCollection)
    async def list_dag_runs(
        dag_id: str,
        limit: int = Query(100, ge=1),
        offset: int = Query(0, ge=0),
        execution_date_gte: Optional[datetime] = None,
        execution_date_lte: Optional[datetime] = None,
        state: Optional[str] = None
    ) -> DAGRunCollection:
        """List DAG runs."""
        if dag_id not in instance_store.dag_runs:
            return DAGRunCollection(dag_runs=[], total_entries=0)
            
        dag_runs = list(instance_store.dag_runs[dag_id].values())
        
        # Apply filters
        if execution_date_gte:
            dag_runs = [dr for dr in dag_runs if dr.execution_date >= execution_date_gte]
        if execution_date_lte:
            dag_runs = [dr for dr in dag_runs if dr.execution_date <= execution_date_lte]
        if state:
            dag_runs = [dr for dr in dag_runs if dr.state == state]
            
        total = len(dag_runs)
        dag_runs = dag_runs[offset:offset + limit]
        return DAGRunCollection(dag_runs=dag_runs, total_entries=total)

    @app.post("/api/v1/dags/{dag_id}/dagRuns", response_model=DAGRun)
    async def create_dag_run(
        dag_id: str,
        dag_run: DAGRun = Body(...)
    ) -> DAGRun:
        """Create a DAG run."""
        if dag_id not in instance_store.dags:
            raise HTTPException(status_code=404, detail="DAG not found")
            
        # Ensure dag_id matches the URL
        dag_run.dag_id = dag_id
        
        # Generate run_id if not provided
        if not dag_run.run_id:
            dag_run.run_id = f"manual__{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')}"
            
        # Set defaults if not provided
        if not dag_run.execution_date:
            dag_run.execution_date = datetime.utcnow()
        if not dag_run.start_date:
            dag_run.start_date = datetime.utcnow()
            
        instance_store.add_dag_run(dag_run)
        return dag_run

    @app.get("/api/v1/dags/{dag_id}/dagRuns/{run_id}", response_model=DAGRun)
    async def get_dag_run(dag_id: str, run_id: str) -> DAGRun:
        """Get a specific DAG run."""
        dag_run = instance_store.get_dag_run(dag_id, run_id)
        if not dag_run:
            raise HTTPException(status_code=404, detail="DAG run not found")
        return dag_run

    @app.patch("/api/v1/dags/{dag_id}/dagRuns/{run_id}", response_model=DAGRun)
    async def update_dag_run(
        dag_id: str,
        run_id: str,
        dag_run: DAGRun = Body(...)
    ) -> DAGRun:
        """Update a DAG run."""
        existing_run = instance_store.get_dag_run(dag_id, run_id)
        if not existing_run:
            raise HTTPException(status_code=404, detail="DAG run not found")
            
        # Update fields while preserving dag_id and run_id
        dag_run.dag_id = dag_id
        dag_run.run_id = run_id
        instance_store.add_dag_run(dag_run)
        return dag_run

    @app.get("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances", response_model=TaskInstanceCollection)
    async def list_task_instances(
        dag_id: str,
        run_id: str,
        limit: int = Query(100, ge=1),
        offset: int = Query(0, ge=0),
        execution_date_gte: Optional[datetime] = None,
        execution_date_lte: Optional[datetime] = None,
        state: Optional[str] = None,
    ) -> TaskInstanceCollection:
        """List task instances for a DAG run."""
        task_instances = instance_store.get_task_instances(dag_id, run_id)
        
        # Apply filters
        if state:
            task_instances = [ti for ti in task_instances if ti.state == state]
            
        total = len(task_instances)
        task_instances = task_instances[offset:offset + limit]
        return TaskInstanceCollection(task_instances=task_instances, total_entries=total)

    @app.get("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}", response_model=TaskInstance)
    async def get_task_instance(
        dag_id: str,
        run_id: str,
        task_id: str
    ) -> TaskInstance:
        """Get a specific task instance."""
        task_instance = instance_store.get_task_instance(dag_id, run_id, task_id)
        if not task_instance:
            raise HTTPException(status_code=404, detail="Task instance not found")
        return task_instance

    @app.post("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/setTaskInstanceState", response_model=TaskInstanceActionResponse)
    async def set_task_instance_state(
        dag_id: str,
        run_id: str,
        task_id: str,
        state: str = Body(..., embed=True)
    ) -> TaskInstanceActionResponse:
        """Set the state of a task instance."""
        task_instance = instance_store.get_task_instance(dag_id, run_id, task_id)
        if not task_instance:
            raise HTTPException(status_code=404, detail="Task instance not found")
            
        # Update task instance state
        task_instance.state = state
        if state == "running":
            task_instance.start_date = datetime.utcnow()
            task_instance.end_date = None
        elif state in ["success", "failed", "skipped"]:
            if not task_instance.start_date:
                task_instance.start_date = datetime.utcnow()
            task_instance.end_date = datetime.utcnow()
            if task_instance.start_date:
                task_instance.duration = (task_instance.end_date - task_instance.start_date).total_seconds()
                
        instance_store.add_task_instance(task_instance)
        
        return TaskInstanceActionResponse(
            task_id=task_id,
            dag_id=dag_id,
            run_id=run_id,
            state=state,
            message=f"Task instance state set to {state}"
        )

    @app.post("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/clearTaskInstance", response_model=TaskInstanceActionResponse)
    async def clear_task_instance(
        dag_id: str,
        run_id: str,
        task_id: str,
        clear_request: ClearTaskInstance = Body(...)
    ) -> TaskInstanceActionResponse:
        """Clear a task instance."""
        task_instance = instance_store.get_task_instance(dag_id, run_id, task_id)
        if not task_instance:
            raise HTTPException(status_code=404, detail="Task instance not found")
            
        # Check if task should be cleared based on request parameters
        should_clear = True
        if clear_request.only_failed and task_instance.state != "failed":
            should_clear = False
        if clear_request.only_running and task_instance.state != "running":
            should_clear = False
            
        if should_clear and not clear_request.dry_run:
            instance_store.clear_task_instance(task_instance)
            message = "Task instance cleared"
        else:
            message = "Task instance not cleared (dry run or filter conditions not met)"
            
        return TaskInstanceActionResponse(
            task_id=task_id,
            dag_id=dag_id,
            run_id=run_id,
            state=task_instance.state,
            message=message
        )

    @app.get("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}", response_model=TaskLog)
    async def get_task_logs(
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int = Path(..., ge=1)
    ) -> TaskLog:
        """Get logs for a task instance."""
        task_log = instance_store.get_task_log(dag_id, run_id, task_id, try_number)
        if not task_log:
            # If no logs exist, create a default log message
            task_log = TaskLog(
                try_number=try_number,
                content=f"No logs found for task {task_id} (try {try_number})",
                timestamp=datetime.utcnow()
            )
            instance_store.add_task_log(dag_id, run_id, task_id, try_number, task_log)
        return task_log

    @app.get("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries", response_model=XComCollection)
    async def list_xcom_entries(
        dag_id: str,
        run_id: str,
        task_id: str,
        key: Optional[str] = None,
        instance_id: str = "default"
    ) -> XComCollection:
        """List XCom entries for a task instance."""
        store = get_store(instance_id)
        xcom_entries = store.list_xcom_values(dag_id=dag_id, task_id=task_id, run_id=run_id, key=key)
        return XComCollection(xcom_entries=xcom_entries, total_entries=len(xcom_entries))

    @app.get("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{key}", response_model=XCom)
    async def get_xcom_entry(
        dag_id: str,
        run_id: str,
        task_id: str,
        key: str,
        instance_id: str = "default"
    ) -> XCom:
        """Get a specific XCom entry."""
        store = get_store(instance_id)
        xcom = store.get_xcom_value(dag_id=dag_id, task_id=task_id, key=key, run_id=run_id)
        if not xcom:
            raise HTTPException(status_code=404, detail=f"XCom entry not found")
        return xcom

    @app.post("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries", response_model=XCom)
    async def create_xcom_entry(
        dag_id: str,
        run_id: str,
        task_id: str,
        xcom: XCom,
        instance_id: str = "default"
    ) -> XCom:
        """Create an XCom entry."""
        store = get_store(instance_id)
        # Ensure the provided IDs match the URL parameters
        xcom.dag_id = dag_id
        xcom.run_id = run_id
        xcom.task_id = task_id
        store.add_xcom_value(xcom)
        return xcom

    @app.delete("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{key}", status_code=204)
    async def delete_xcom_entry(
        dag_id: str,
        run_id: str,
        task_id: str,
        key: str,
        instance_id: str = "default"
    ) -> None:
        """Delete an XCom entry."""
        store = get_store(instance_id)
        if not store.delete_xcom_value(dag_id=dag_id, task_id=task_id, key=key, run_id=run_id):
            raise HTTPException(status_code=404, detail=f"XCom entry not found")

    @app.get("/api/v1/connections", response_model=ConnectionCollection)
    async def list_connections(
        limit: Optional[int] = 100,
        offset: Optional[int] = None,
        instance_id: str = "default"
    ) -> ConnectionCollection:
        """List connections."""
        store = get_store(instance_id)
        connections = store.list_connections(limit=limit, offset=offset)
        return ConnectionCollection(
            connections=connections,
            total_entries=len(store.list_connections())
        )

    @app.get("/api/v1/connections/{conn_id}", response_model=Connection)
    async def get_connection(
        conn_id: str,
        instance_id: str = "default"
    ) -> Connection:
        """Get a specific connection."""
        store = get_store(instance_id)
        connection = store.get_connection(conn_id)
        if not connection:
            raise HTTPException(status_code=404, detail=f"Connection {conn_id} not found")
        return connection

    @app.post("/api/v1/connections", response_model=Connection)
    async def create_connection(
        connection: Connection,
        instance_id: str = "default"
    ) -> Connection:
        """Create a connection."""
        store = get_store(instance_id)
        # Check if connection already exists
        if store.get_connection(connection.conn_id):
            raise HTTPException(
                status_code=409,
                detail=f"Connection {connection.conn_id} already exists"
            )
        store.add_connection(connection)
        return connection

    @app.patch("/api/v1/connections/{conn_id}", response_model=Connection)
    async def update_connection(
        conn_id: str,
        connection: Connection,
        instance_id: str = "default"
    ) -> Connection:
        """Update a connection."""
        store = get_store(instance_id)
        if not store.get_connection(conn_id):
            raise HTTPException(
                status_code=404,
                detail=f"Connection {conn_id} not found"
            )
        # Ensure conn_id matches URL parameter
        connection.conn_id = conn_id
        store.add_connection(connection)
        return connection

    @app.delete("/api/v1/connections/{conn_id}", status_code=204)
    async def delete_connection(
        conn_id: str,
        instance_id: str = "default"
    ) -> None:
        """Delete a connection."""
        store = get_store(instance_id)
        if not store.delete_connection(conn_id):
            raise HTTPException(
                status_code=404,
                detail=f"Connection {conn_id} not found"
            )

    @app.get("/api/v1/variables", response_model=VariableCollection)
    async def list_variables(
        limit: Optional[int] = 100,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
        instance_id: str = "default"
    ) -> VariableCollection:
        """List variables."""
        store = get_store(instance_id)
        variables = store.list_variables(limit=limit, offset=offset, order_by=order_by)
        return VariableCollection(
            variables=variables,
            total_entries=len(store.list_variables())
        )

    @app.get("/api/v1/variables/{key}", response_model=Variable)
    async def get_variable(
        key: str,
        instance_id: str = "default"
    ) -> Variable:
        """Get a specific variable."""
        store = get_store(instance_id)
        variable = store.get_variable(key)
        if not variable:
            raise HTTPException(status_code=404, detail=f"Variable {key} not found")
        return variable

    @app.post("/api/v1/variables", response_model=Variable)
    async def create_variable(
        variable: Variable,
        instance_id: str = "default"
    ) -> Variable:
        """Create a variable."""
        store = get_store(instance_id)
        # Check if variable already exists
        if store.get_variable(variable.key):
            raise HTTPException(
                status_code=409,
                detail=f"Variable {variable.key} already exists"
            )
        store.add_variable(variable)
        return variable

    @app.patch("/api/v1/variables/{key}", response_model=Variable)
    async def update_variable(
        key: str,
        variable: Variable,
        instance_id: str = "default"
    ) -> Variable:
        """Update a variable."""
        store = get_store(instance_id)
        if not store.get_variable(key):
            raise HTTPException(
                status_code=404,
                detail=f"Variable {key} not found"
            )
        # Ensure key matches URL parameter
        variable.key = key
        store.add_variable(variable)
        return variable

    @app.delete("/api/v1/variables/{key}", status_code=204)
    async def delete_variable(
        key: str,
        instance_id: str = "default"
    ) -> None:
        """Delete a variable."""
        store = get_store(instance_id)
        if not store.delete_variable(key):
            raise HTTPException(
                status_code=404,
                detail=f"Variable {key} not found"
            )

    @app.get("/api/v1/pools", response_model=PoolCollection)
    async def list_pools(
        limit: Optional[int] = 100,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
        instance_id: str = "default"
    ) -> PoolCollection:
        """List pools."""
        store = get_store(instance_id)
        pools = store.list_pools(limit=limit, offset=offset, order_by=order_by)
        return PoolCollection(
            pools=pools,
            total_entries=len(store.list_pools())
        )

    @app.get("/api/v1/pools/{pool_name}", response_model=Pool)
    async def get_pool(
        pool_name: str,
        instance_id: str = "default"
    ) -> Pool:
        """Get a specific pool."""
        store = get_store(instance_id)
        pool = store.get_pool(pool_name)
        if not pool:
            raise HTTPException(status_code=404, detail=f"Pool {pool_name} not found")
        return pool

    @app.post("/api/v1/pools", response_model=Pool)
    async def create_pool(
        pool: Pool,
        instance_id: str = "default"
    ) -> Pool:
        """Create a pool."""
        store = get_store(instance_id)
        # Check if pool already exists
        if store.get_pool(pool.name):
            raise HTTPException(
                status_code=409,
                detail=f"Pool {pool.name} already exists"
            )
        store.add_pool(pool)
        return pool

    @app.patch("/api/v1/pools/{pool_name}", response_model=Pool)
    async def update_pool(
        pool_name: str,
        pool: Pool,
        instance_id: str = "default"
    ) -> Pool:
        """Update a pool."""
        store = get_store(instance_id)
        if not store.get_pool(pool_name):
            raise HTTPException(
                status_code=404,
                detail=f"Pool {pool_name} not found"
            )
        # Ensure pool name matches URL parameter
        pool.name = pool_name
        store.add_pool(pool)
        return pool

    @app.delete("/api/v1/pools/{pool_name}", status_code=204)
    async def delete_pool(
        pool_name: str,
        instance_id: str = "default"
    ) -> None:
        """Delete a pool."""
        store = get_store(instance_id)
        if not store.delete_pool(pool_name):
            raise HTTPException(
                status_code=404,
                detail=f"Pool {pool_name} not found"
            )

    @app.patch("/api/v1/pools/{pool_name}/slots", response_model=Pool)
    async def update_pool_slots(
        pool_name: str,
        occupied_slots: Optional[int] = None,
        queued_slots: Optional[int] = None,
        running_slots: Optional[int] = None,
        instance_id: str = "default"
    ) -> Pool:
        """Update pool slot usage."""
        store = get_store(instance_id)
        pool = store.get_pool(pool_name)
        if not pool:
            raise HTTPException(
                status_code=404,
                detail=f"Pool {pool_name} not found"
            )
        
        # Update only provided slot counts
        occupied = occupied_slots if occupied_slots is not None else pool.occupied_slots
        queued = queued_slots if queued_slots is not None else pool.queued_slots
        running = running_slots if running_slots is not None else pool.running_slots
        
        updated_pool = store.update_pool_slots(pool_name, occupied, queued, running)
        if not updated_pool:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to update pool {pool_name} slots"
            )
        return updated_pool

    @app.get("/api/v1/providers", response_model=ProviderCollection)
    async def list_providers(
        limit: Optional[int] = 100,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
        instance_id: str = "default"
    ) -> ProviderCollection:
        """List providers."""
        store = get_store(instance_id)
        providers = store.list_providers(limit=limit, offset=offset, order_by=order_by)
        return ProviderCollection(
            providers=providers,
            total_entries=len(store.list_providers())
        )

    @app.get("/api/v1/providers/{provider_name}", response_model=Provider)
    async def get_provider(
        provider_name: str,
        instance_id: str = "default"
    ) -> Provider:
        """Get a specific provider."""
        store = get_store(instance_id)
        provider = store.get_provider(provider_name)
        if not provider:
            raise HTTPException(status_code=404, detail=f"Provider {provider_name} not found")
        return provider

    @app.post("/api/v1/providers", response_model=Provider)
    async def create_provider(
        provider: Provider,
        instance_id: str = "default"
    ) -> Provider:
        """Create a provider."""
        store = get_store(instance_id)
        # Check if provider already exists
        if store.get_provider(provider.package_name):
            raise HTTPException(
                status_code=409,
                detail=f"Provider {provider.package_name} already exists"
            )
        store.add_provider(provider)
        return provider

    @app.patch("/api/v1/providers/{provider_name}", response_model=Provider)
    async def update_provider(
        provider_name: str,
        provider: Provider,
        instance_id: str = "default"
    ) -> Provider:
        """Update a provider."""
        store = get_store(instance_id)
        if not store.get_provider(provider_name):
            raise HTTPException(
                status_code=404,
                detail=f"Provider {provider_name} not found"
            )
        # Ensure provider name matches URL parameter
        provider.package_name = provider_name
        store.add_provider(provider)
        return provider

    @app.delete("/api/v1/providers/{provider_name}", status_code=204)
    async def delete_provider(
        provider_name: str,
        instance_id: str = "default"
    ) -> None:
        """Delete a provider."""
        store = get_store(instance_id)
        if not store.delete_provider(provider_name):
            raise HTTPException(
                status_code=404,
                detail=f"Provider {provider_name} not found"
            )

    @app.get("/api/v1/providers/{provider_name}/hooks", response_model=List[ProviderHook])
    async def get_provider_hooks(
        provider_name: str,
        instance_id: str = "default"
    ) -> List[ProviderHook]:
        """Get hooks for a specific provider."""
        store = get_store(instance_id)
        provider = store.get_provider(provider_name)
        if not provider:
            raise HTTPException(status_code=404, detail=f"Provider {provider_name} not found")
        return provider.hooks

    return app
