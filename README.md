# Airflow API Mock

This is a mock API for Airflow. It is used to test Airflow UI and plugins. It is not intended for production use. While it uses HTTPS for security, it is self-signed and should not be used in production.

The API spec is available at [airflow.apache.org/docs/apache-airflow/stable/_api/openapi.yaml](https://airflow.apache.org/docs/apache-airflow/stable/_api/openapi.yaml).

## Features

- [x] Multiple instance support
- [x] DAGs
- [x] DAG Runs
- [x] Tasks
- [x] XComs
- [x] Connections
- [x] Variables
- [x] Pools
- [x] Providers

## Installation

```bash
pip install -r requirements.txt
```

## Usage

To start a mock Airflow instance:

```bash
python main.py --instance-id airflow1 --port 8080
```

To start an instance with sample data pre-populated:

```bash
python main.py --instance-id airflow1 --port 8080 --populate
```

The sample data includes:
- 5 example DAGs with random states
- 3 DAG runs per DAG with various states (success/failed/running)
- 4 task instances per DAG run
- Common variables (environment, API keys, etc.)
- Common connections (Postgres, AWS, HTTP)

You can start multiple instances by running the command multiple times with different instance IDs and ports:

```bash
# Terminal 1: Start first instance with sample data
python main.py --instance-id airflow1 --port 8080 --populate

# Terminal 2: Start second instance with clean state
python main.py --instance-id airflow2 --port 8081
```

## API Endpoints

The mock API implements the standard Airflow REST API endpoints as defined in the [official OpenAPI specification](https://airflow.apache.org/docs/apache-airflow/stable/_api/openapi.yaml).

Each instance exposes its endpoints under `/api/v1/`, for example:
- `GET /api/v1/dags` - List all DAGs
- `GET /api/v1/dags/{dag_id}` - Get a specific DAG
- `GET /api/v1/dags/{dag_id}/dagRuns` - List DAG runs
- `GET /api/v1/dags/{dag_id}/dagRuns/{run_id}` - Get a specific DAG run
- `GET /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}` - Get a specific task instance

### XComs

The mock API supports XCom (cross-communication) between tasks. XCom endpoints allow tasks to exchange data and metadata:

- `GET /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries` - List XCom entries
- `GET /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{key}` - Get specific XCom entry
- `POST /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries` - Create XCom entry
- `DELETE /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{key}` - Delete XCom entry

### Connections

The mock API supports managing Airflow connections for external systems:

- `GET /api/v1/connections` - List connections with optional pagination
- `GET /api/v1/connections/{conn_id}` - Get specific connection
- `POST /api/v1/connections` - Create new connection
- `PATCH /api/v1/connections/{conn_id}` - Update existing connection
- `DELETE /api/v1/connections/{conn_id}` - Delete connection

### Variables

The mock API supports managing Airflow variables for configuration and runtime parameters:

- `GET /api/v1/variables` - List variables with optional pagination and ordering
- `GET /api/v1/variables/{key}` - Get specific variable
- `POST /api/v1/variables` - Create new variable
- `PATCH /api/v1/variables/{key}` - Update existing variable
- `DELETE /api/v1/variables/{key}` - Delete variable

### Pools

The mock API supports managing Airflow resource pools for task concurrency control:

- `GET /api/v1/pools` - List pools with optional pagination and ordering
- `GET /api/v1/pools/{pool_name}` - Get specific pool
- `POST /api/v1/pools` - Create new pool
- `PATCH /api/v1/pools/{pool_name}` - Update existing pool
- `DELETE /api/v1/pools/{pool_name}` - Delete pool
- `PATCH /api/v1/pools/{pool_name}/slots` - Update pool slot usage

### Providers

The mock API supports managing Airflow providers and their hooks:

- `GET /api/v1/providers` - List providers with optional pagination and ordering
- `GET /api/v1/providers/{provider_name}` - Get specific provider
- `POST /api/v1/providers` - Create new provider
- `PATCH /api/v1/providers/{provider_name}` - Update existing provider
- `DELETE /api/v1/providers/{provider_name}` - Delete provider
- `GET /api/v1/providers/{provider_name}/hooks` - Get provider hooks

## Development

The project is structured as follows:
- `airflow_mock/models.py` - Data models for Airflow objects
- `airflow_mock/store.py` - State management for multiple instances
- `airflow_mock/api.py` - FastAPI application and endpoints
- `airflow_mock/server.py` - Server for managing multiple instances
- `main.py` - CLI entry point

## Security Note

While this mock API uses HTTPS, it uses self-signed certificates and is not intended for production use.