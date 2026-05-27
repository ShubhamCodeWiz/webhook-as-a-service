import aio_pika
from config import RETRY_EXCHANGE, RETRY_EXCHANGE_TYPE, RETRY_WAIT_QUEUE, TOPIC_EXCHANGE, DLQ_NAME, DLQ_ROUTING_KEY, RETRY_TTL_MS


async def setup_retry_infrastructure(channel: aio_pika.abc.AbstractChannel):
    # declare a exchange
    retry_exchange = await channel.declare_exchange(
        RETRY_EXCHANGE,
        RETRY_EXCHANGE_TYPE,
        durable=True
    )


    # declare a retry queue
    retry_queue = await channel.declare_queue(
        RETRY_WAIT_QUEUE,
        durable=True,

        arguments = {
            "x-message-ttl": RETRY_TTL_MS,
            "x-dead-letter-exchange": TOPIC_EXCHANGE,
        }
    )


    # binding queue with exchange
    await retry_queue.bind(retry_exchange, routing_key="#")


    # declare DLQ
    dlq = await channel.declare_queue(
        DLQ_NAME,
        durable=True,

        arguments = {}
    )

    # binding dlq with exchange
    await dlq.bind(retry_exchange, routing_key=DLQ_ROUTING_KEY)

