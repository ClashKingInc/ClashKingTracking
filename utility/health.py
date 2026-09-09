import asyncio

from fastapi import FastAPI
from uvicorn import Config, Server


async def run_health_check_server() -> asyncio.Task:
    """
    Creates a FastAPI health check app
    :return: A FastAPI application instance.
    """
    health_app = FastAPI()
    config = Config(app=health_app, host="0.0.0.0", port=8027, log_level="warning")
    server = Server(config)

    @health_app.get('/health')
    async def health_check():
        return {'status': 'ok', 'details': 'Tracking is running and ready'}

    _server_task = asyncio.create_task(server.serve())

    return _server_task
