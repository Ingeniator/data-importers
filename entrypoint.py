"""Main application entrypoint.

Configures logging, creates FastAPI app, and starts the server.
"""

from dataimporter.config import get_settings
from dataimporter.logging_config import setup_logging

settings = get_settings()
logger = setup_logging(debug=settings.server.debug, silence_probes=settings.server.silence_probes).bind(module=__name__)

from dataimporter.main import app  # noqa: E402, F401

if __name__ == "__main__":
    import uvicorn

    logger.info("Starting server", host=settings.server.host, port=settings.server.port)
    uvicorn.run(
        "entrypoint:app",
        workers=settings.server.workers,
        host=settings.server.host,
        port=settings.server.port,
        timeout_keep_alive=settings.server.timeout_keep_alive,
        reload=settings.server.debug,
        log_level="debug" if settings.server.debug else "info",
    )
