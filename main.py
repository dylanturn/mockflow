"""
Main entry point for the Airflow mock API server.
"""
import asyncio
import click
from airflow_mock.server import server

@click.command()
@click.option("--instance-id", required=True, help="ID of the Airflow instance to start")
@click.option("--port", default=8080, help="Port to run the server on")
@click.option("--populate", is_flag=True, help="Populate the instance with sample data")
def main(instance_id: str, port: int, populate: bool):
    """Start an Airflow mock instance."""
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(server.start_instance(instance_id, port, populate))
    except KeyboardInterrupt:
        loop.run_until_complete(server.stop_instance(instance_id))
    finally:
        loop.close()

if __name__ == "__main__":
    main()
