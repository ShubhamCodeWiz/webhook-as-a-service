import time
import json
import asyncio
import aio_pika
from aio_pika.abc import ConsumerTag
from datetime import datetime, timezone
from http_client import deliver_webhook

from config import TOPIC_EXCHANGE, TOPIC_EXCHANGE_TYPE, RETRY_EXCHANGE, MAX_RETRIES, RETRY_EXCHANGE_TYPE, DLQ_ROUTING_KEY
from logger import get_logger
from models import WebhookEvent

logger = get_logger(__name__)

class WebhookConsumer:
    def __init__(self, connection: aio_pika.abc.AbstractRobustConnection, prefetch_count: int, worker_id: str, queue_name: str, exchange_name:str = TOPIC_EXCHANGE):
        self.connection = connection
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.prefetch_count = prefetch_count
        self.worker_id = worker_id
        
        self.channel: aio_pika.abc.AbstractChannel | None = None
        self.queue: aio_pika.abc.AbstractQueue | None = None
        self._consumer_tag: ConsumerTag | None = None
        self._current_task: asyncio.Task | None = None  # ← ADDED
        self.retry_exchange: aio_pika.abc.AbstractExchange | None = None

    async def setup(self, binding_keys: list[str]):
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=self.prefetch_count)

        exchange = await self.channel.declare_exchange(
            self.exchange_name,
            TOPIC_EXCHANGE_TYPE,
            durable=True
        )

        self.retry_exchange = await self.channel.declare_exchange(
            RETRY_EXCHANGE,
            RETRY_EXCHANGE_TYPE,
            durable=True
        )


        self.queue = await self.channel.declare_queue(
            self.queue_name,
            durable=True,
            arguments= {
                "x-dead-letter-exchange" : RETRY_EXCHANGE,
                "x-dead-letter-routing-key": DLQ_ROUTING_KEY
            }
        )


        for key in binding_keys:
            await self.queue.bind(exchange, routing_key=key)

        logger.info(
            f"Worker {self.worker_id} infrastructure ready",
            extra={
                "queue": self.queue_name,
                "prefetch_count": self.prefetch_count,
                "worker_id": self.worker_id
            }
        )

    async def _handle_message(self, message: aio_pika.abc.AbstractIncomingMessage):
        self._current_task = asyncio.current_task()  # track this task
        start_time = time.time()

        event: WebhookEvent | None = None

            
        try:
            raw_body = message.body.decode()
            event_dict = json.loads(raw_body)
            event = WebhookEvent.from_dict(event_dict)
            
            if message.redelivered:
                logger.warning("Redelivered message detected", extra={
                    "worker_id": self.worker_id,
                    "event_id": event.event_id,
                    "note": "possible duplicate delivery to target"

                })


            headers = message.headers or {}
            raw_retry_count = headers.get("retry_count", 0)

            # 2. Type Guard to make Pylance happy
            if isinstance(raw_retry_count, int):
                current_retry_count = raw_retry_count
            else:
                current_retry_count = 0

            result = await deliver_webhook(
                target_url=event.target_url,
                payload=event.payload,
                event_id=event.event_id,
                event_type=event.event_type,

            )

            processing_time_ms = int((time.time() - start_time) * 1000)

                
            if result.success:
                await message.ack() # explicit ack on success
        
                logger.info(
                    "Successfully processed webhook",
                    extra={
                        "worker_id": self.worker_id,
                        "event_id": event.event_id,
                        "event_type": event.event_type,
                        "target_url": event.target_url,
                        "status_code": result.status_code,
                        "attempt_number": current_retry_count + 1,
                        "retry_count": current_retry_count,
                        "response_time_ms": result.response_time_ms,
                        "processing_time_ms": processing_time_ms,
                        "redelivered": message.redelivered
                    }
                )

            elif not result.is_transient:
                # 4xx -> dlq
                await self._send_to_dlq(message, event, result.error, current_retry_count)
                await message.ack()
                logger.error(
                "Permanent failure, send to DLQ",
                extra={
                    "worker_id": self.worker_id,
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "target_url": event.target_url,
                    "status_code": result.status_code,
                    "response_time_ms": result.response_time_ms,
                    "processing_time_ms": processing_time_ms,
                    "attempt_number": current_retry_count+1,
                    "retry_count": current_retry_count,
                    "error": result.error,
                    "redelivered": message.redelivered
                }
            )
                
            else:
                # 5xx -> waiting queue
                is_exhaust = current_retry_count >= MAX_RETRIES

                if is_exhaust:
                    # send to dlq
                    await self._send_to_dlq(message, event, result.error, current_retry_count)
                    await message.ack()

                    logger.error("Retry budget exhausted, sent to DLQ", extra={
                        "worker_id": self.worker_id,
                        "event_id": event.event_id,
                        "event_type": event.event_type,
                        "target_url": event.target_url,
                        "status_code": result.status_code,
                        "response_time_ms": result.response_time_ms,
                        "processing_time_ms": processing_time_ms,
                        "attempt_number": current_retry_count+1,
                        "retry_count": current_retry_count,
                        "error": result.error,
                        "redelivered": message.redelivered
                    })
                    

                else:
                    # send to waiting queue
                    await self._send_to_retry(message, event, result.error, current_retry_count+1)
                    await message.ack()
                    logger.warning("Transient failure, queued for retry", extra={
                        "event_id": event.event_id,
                        "target_url": event.target_url,
                        "status_code": result.status_code,
                        "response_time_ms": result.response_time_ms,
                        "processing_time_ms": processing_time_ms,
                        "attempt_number": current_retry_count+1,
                        "retry_count": current_retry_count,
                        "error": result.error,
                        "redelivered": message.redelivered
                    })

        except Exception as e:

            # poison message(invalid json)
            if event is None:
                await self._send_to_dlq(message, None, str(e), 0)
                await message.ack()
                logger.error(
                "Poison message rejected to DLQ",
                extra={
                    "worker_id": self.worker_id,
                    "error": str(e),
                    "message_id": message.message_id,
                    "routing_key": message.routing_key,
                    "raw_body_snippet": message.body.decode(errors="replace")[:200]
                }
            )

            else:
                headers = message.headers or {}
                raw_retry_count = headers.get("retry_count", 0)

                # 2. Type Guard to make Pylance happy
                if isinstance(raw_retry_count, int):
                    current_retry_count = raw_retry_count
                else:
                    current_retry_count = 0

                logger.error(
                    "Processing failed, handling failure...",
                    extra={
                        "worker_id": self.worker_id,
                        "error": str(e),
                        "raw_body": message.body.decode()[:100]
                    }
                )

        
                await self._send_to_retry(message, event, str(e), current_retry_count+1)
                await message.ack()

        finally:
            self._current_task = None  # clear after handler completes


    async def _send_to_dlq(self,
        message: aio_pika.abc.AbstractIncomingMessage,
        event: WebhookEvent|None,
        error: str|None,
        current_retry_count: int
    ):
        
        if self.retry_exchange is None:
            raise RuntimeError("Retry exchange is not initialized. Call setup() first.")
        
        if event is None:
            event_id = None
            correlation_id = None
            event_dict = None
        else:
            event_id = event.event_id
            correlation_id = event.correlation_id
            event_dict = event.to_dict()
        
        
        
        dlq_payload = {
                "original_event_id": event_id,
                "last_error": str(error),
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "retry_count": current_retry_count,
                "original_payload": event_dict,
                "original_routing_key": message.routing_key
            }



        # Construct the AMQP Envelope
        dlq_message = aio_pika.Message(
            body=json.dumps(dlq_payload).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,  
            content_type="application/json",
            correlation_id=correlation_id,            
        )

    
        # republish it
        await self.retry_exchange.publish(dlq_message, routing_key=DLQ_ROUTING_KEY)
        

    async def _send_to_retry(self,
        message: aio_pika.abc.AbstractIncomingMessage,
        event: WebhookEvent,
        error: str|None,
        next_retry_count: int
    ):     
        
        if self.retry_exchange is None:
            raise RuntimeError("Retry exchange is not initialized. Call setup() first.")
        
        headers = message.headers or {}
        
        new_headers = {
                **headers,
                "retry_count": next_retry_count,
                "max_retries": MAX_RETRIES,
                "last_failure": str(error), 
                "last_failed_at": datetime.now(timezone.utc).isoformat()
            }


        retry_message = aio_pika.Message(
                body=message.body, # Keep original body for retries
                headers=new_headers,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                correlation_id=message.correlation_id,
                message_id=message.message_id,
                content_type=message.content_type,
                timestamp = message.timestamp,
            )

        if message.routing_key is None:
            raise RuntimeError("no routing key")

        # Publish to Retry queue
        await self.retry_exchange.publish(
            retry_message,
            routing_key=message.routing_key
        ) 


    async def start(self, shutdown_event: asyncio.Event):
        if not self.queue:
            raise RuntimeError("Consumer setup() must be called before starting.")

        logger.info(f"Worker {self.worker_id} is starting consumption...")
        self._consumer_tag = await self.queue.consume(self._handle_message)
        logger.info(f"Worker {self.worker_id} waiting for messages. CTRL+C to stop.")
        await shutdown_event.wait() # wait until shutdown signal is received
        logger.info("Shutdown signal received, finishing current message...")
        await self.stop()
        logger.info(f"Worker {self.worker_id} stopped cleanly.")

    async def stop(self):
        tag = self._consumer_tag
        if self.queue and tag is not None:
            await self.queue.cancel(tag)        # stop new messages arriving

        if self._current_task and not self._current_task.done():
            await self._current_task            # ← ADDED: wait for in-flight handler

        if self.channel and not self.channel.is_closed:
            await self.channel.close()
            logger.info(f"Worker {self.worker_id} channel closed gracefully.")