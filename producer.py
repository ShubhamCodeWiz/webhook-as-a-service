import time
import asyncio
import aio_pika

from config import TOPIC_EXCHANGE, TOPIC_EXCHANGE_TYPE
from logger import get_logger

logger = get_logger(__name__)

class WebhookProducer:
    def __init__(self, connection: aio_pika.abc.AbstractRobustConnection, exchange_name: str = TOPIC_EXCHANGE):
        """
        Synchronous constructor. 
        Saves the robust connection and routing details, but defers network I/O.
        """
        self.connection = connection
        self.exchange_name = exchange_name
        
        # Placeholders for async initialization
        self.channel: aio_pika.abc.AbstractChannel | None = None
        self.exchange: aio_pika.abc.AbstractExchange | None = None

    async def setup(self):
        """
        Asynchronous initialization. 
        Idempotently declares the channel, exchange.
        """
        # The aio-pika way to ensure Publisher Confirms are ON
        self.channel = await self.connection.channel(publisher_confirms=True)


        # 2. Declare the Topic exchange (durable so it survives broker restarts)
        self.exchange = await self.channel.declare_exchange(
            self.exchange_name,
            TOPIC_EXCHANGE_TYPE,
            durable=True
        )

        logger.info(
            "RabbitMQ infrastructure declared successfully",
            extra={
                "exchange": self.exchange_name,
                "publisher_confirms": True,
            }
        )

    async def publish(self, event):
        """
        Serializes a WebhookEvent and publishes it to the exchange with full AMQP metadata.
        """
        if not self.exchange:
            raise RuntimeError("Producer setup() must be called before publishing.")

        # Serialize the body to bytes. 
        # (Assuming event.to_json() returns a string, we encode it to raw bytes)
        body_bytes = event.to_json().encode("utf-8")

        # Construct the AMQP Envelope
        message = aio_pika.Message(
            body=body_bytes,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  
            content_type="application/json",                 
            message_id=event.event_id,                       
            correlation_id=event.correlation_id,            
            timestamp=int(time.time())                       
        )

        # Await the publish call to ensure the broker accepted it
        try: 
            await self.exchange.publish(
                message,
                routing_key=event.routing_key
            )

            # Log the successful action with the extracted correlation data
            logger.info(
                "Published webhook event",
                extra={
                    "event_id": event.event_id,
                    "correlation_id": event.correlation_id,
                    "event_type": event.event_type,
                    "target_url": event.target_url,
                    "routing_key": event.routing_key
                }
            )

        except aio_pika.exceptions.DeliveryError as e:
            logger.error(
                "RabbitMQ rejected message — publish failed",
                extra={
                    "event_id": event.event_id,
                    "correlation_id": event.correlation_id,
                    "error": str(e)
                }
            )
            raise


    async def publish_batch(self, events: list):
        """
        Fires off multiple webhook events concurrently.
        """
        if not events:
            return

        # Create a list of coroutines
        tasks = [self.publish(event) for event in events]
        
        # Execute them all concurrently
        await asyncio.gather(*tasks)

        logger.info(
            "Published batch of webhook events",
            extra={"batch_size": len(events)}
        )