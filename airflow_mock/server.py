"""
Server for running multiple Airflow mock instances.
"""
import asyncio
import uvicorn
from typing import Dict, List
from .api import create_app
from .store import store
from .sample_data import populate_instance

class MockServer:
    """Server that manages multiple Airflow mock instances."""
    
    def __init__(self):
        self.servers: Dict[str, uvicorn.Server] = {}
        
    async def start_instance(self, instance_id: str, port: int, populate: bool = False) -> None:
        """Start a new Airflow mock instance."""
        app = create_app(instance_id)
        
        # Populate with sample data if requested
        if populate:
            instance = store.get_instance(instance_id)
            populate_instance(instance)
        
        config = uvicorn.Config(app, host="0.0.0.0", port=port)
        server = uvicorn.Server(config)
        self.servers[instance_id] = server
        await server.serve()
        
    async def stop_instance(self, instance_id: str) -> None:
        """Stop an Airflow mock instance."""
        if instance_id in self.servers:
            await self.servers[instance_id].shutdown()
            del self.servers[instance_id]
            
    def list_instances(self) -> List[str]:
        """List all running instance IDs."""
        return list(self.servers.keys())

# Global server instance
server = MockServer()
