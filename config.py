import os

# RabbitMQ Connection
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")


# Retry Infrastructure
RETRY_EXCHANGE = "retry.exchange"
RETRY_EXCHANGE_TYPE = "topic"

RETRY_WAIT_QUEUE = "webhooks.retry.wait"
RETRY_WAIT_ROUTING_KEY = "retry.wait"
RETRY_TTL_MS = 5_000



# Permanent Dead Letter Queue
DLQ_NAME = "webhooks.dlq"
DLQ_ROUTING_KEY = "dlq"

MAX_RETRIES = 3


# Main Topic Exchange
TOPIC_EXCHANGE = "webhooks.topic.exchange"
TOPIC_EXCHANGE_TYPE = "topic"



# Main Working Queues
WEBHOOK_CRITICAL_QUEUE = "webhook.critical"
WEBHOOK_STANDARD_QUEUE = "webhook.standard"
WEBHOOK_AUDIT_QUEUE = "webhook.audit"


# Topic Routing keys
CRITICAL_BINDING_KEYS = [
    "payment.#",
    "order.#"
]

STANDARD_BINDING_KEYS = [
    "user.#",
    "notification.#"
]

AUDIT_BINDING_KEYS = [
    "#"
]



# Consumer Setting
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", 1))

