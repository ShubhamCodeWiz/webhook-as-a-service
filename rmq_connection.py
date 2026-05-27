import aio_pika
from urllib.parse import urlparse
from config import RABBITMQ_URL
from logger import get_logger

logger = get_logger(__name__)

async def get_connection() -> aio_pika.abc.AbstractRobustConnection:
    """
    Opens and returns a robust AMQP connection.
    Does NOT open a channel — channel creation is the caller's responsibility.
    """
    # Parse the URL before connecting so we can log failures safely
    parsed = urlparse(RABBITMQ_URL)
    host = parsed.hostname
    vhost = parsed.path.lstrip("/") or "/"

    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
    except Exception as e:
        logger.error(
            "Failed to connect to RabbitMQ",
            extra={"host": host, "vhost": vhost, "error": str(e)}
        )
        raise  # Re-raise the exception so the app knows it failed!

    logger.info(
        "Successfully connected to RabbitMQ",
        extra={"host": host, "vhost": vhost}
    )
    
    return connection


