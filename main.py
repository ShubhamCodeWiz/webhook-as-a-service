import sys
import asyncio
from rmq_connection import get_connection
from producer import WebhookProducer
from consumer import WebhookConsumer
from models import WebhookEvent
from config import PREFETCH_COUNT, WEBHOOK_AUDIT_QUEUE, WEBHOOK_CRITICAL_QUEUE, WEBHOOK_STANDARD_QUEUE, CRITICAL_BINDING_KEYS, STANDARD_BINDING_KEYS, AUDIT_BINDING_KEYS

from logger import get_logger
from topology import setup_retry_infrastructure
import signal

logger = get_logger("main")


async def run_producer():
    """Logic to fire a batch of messages and exit."""
    connection = await get_connection()
    async with connection:
        producer = WebhookProducer(connection)
        await producer.setup()
        
        # Creating a sample batch
        # Creating a sample batch
        events = [
            WebhookEvent(event_type="payment.failed", target_url="...", payload={"i": 1}),
            # WebhookEvent(event_type="payment.created", target_url="...", payload={"i": 2}),
            # WebhookEvent(event_type="order.created", target_url="...", payload={"i": 3}),
            # WebhookEvent(event_type="user.created", target_url="...", payload={"i": 4}),
            # WebhookEvent(event_type="notification.sent", target_url="...", payload={"i": 5}),
        ]
            
        await producer.publish_batch(events)
        logger.info("Producer task finished.")

async def run_consumer(queue_name: str, binding_keys: list[str], worker_id: str):
    shutdown_event = asyncio.Event()
    
    # Catch SIGINT/SIGTERM and set the event instead of hard killing
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)


    connection = await get_connection()

    async with connection:
        consumer = WebhookConsumer(
            connection=connection,
            prefetch_count=PREFETCH_COUNT,
            worker_id=worker_id,
            queue_name=queue_name

        )
        
        await consumer.setup(binding_keys)

        if consumer.channel is None:
            raise RuntimeError("Consumer channel was not initialized")
            
        # guarantee infrastructure exists
        await setup_retry_infrastructure(consumer.channel)

        await consumer.start(shutdown_event)
        

if __name__ == "__main__":
    # Basic Argument Parsing
    if len(sys.argv) < 2:
        print("Usage: python3 main.py [producer|consumer]")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode == "producer":
        asyncio.run(run_producer())
    
    elif mode == "consumer":
        # Get worker-id if provided, else default to worker-1
        # Example: python3 main.py consumer my-worker

        queue_type = sys.argv[2].lower()

        if queue_type == "critical":
            queue_name = WEBHOOK_CRITICAL_QUEUE
            binding_keys = CRITICAL_BINDING_KEYS
        elif queue_type == "standard":
            queue_name = WEBHOOK_STANDARD_QUEUE
            binding_keys = STANDARD_BINDING_KEYS
        elif queue_type == "audit":
            queue_name = WEBHOOK_AUDIT_QUEUE
            binding_keys = AUDIT_BINDING_KEYS

        else:
            print(f"Unknown queue type: {queue_type}")
            sys.exit(1)
    
        worker_id = sys.argv[3] if len(sys.argv) > 3 else f"{queue_type}-worker-1"
        asyncio.run(run_consumer(queue_name, binding_keys, worker_id))

    else:
        print(f"Unknown mode: {mode}. Use 'producer' or 'consumer'.")