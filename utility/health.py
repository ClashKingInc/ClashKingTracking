import uvicorn
from fastapi import FastAPI



def create_health_app() -> FastAPI:
    """
    Creates a FastAPI health check app
    :return: A FastAPI application instance.
    """
    health_app = FastAPI()

    @health_app.get('/health')
    async def health_check():
        return {'status': 'ok', 'details': 'Tracking is running and ready'}

    return health_app


def run_health_check_server() -> None:
    """
    Starts the health check FastAPI server.

    :param bot: The bot instance to check readiness.
    """
    app = create_health_app()
    uvicorn.run(app, host='0.0.0.0', port=8027)
