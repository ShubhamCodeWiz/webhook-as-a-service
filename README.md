# webhook-delivery-system

Python • RabbitMQ • aio_pika • httpx • Async • Reliability Patterns

`python` `rabbitmq` `async` `httpx` `pydantic` `distributed-systems` `status: active`

A production-grade asynchronous webhook delivery engine built on RabbitMQ, implementing topic-based routing, TTL-driven retry queues, dead-letter handling, and real HTTP delivery with transient/permanent failure classification.

Built from scratch to understand — not just use — the delivery guarantees that power systems at Stripe, Shopify, and every major SaaS platform that sends webhooks.

---

## The Problem This Solves

Every system that delivers webhooks faces the same failure modes:

- A receiving server is temporarily down — the event should retry, not disappear
- A receiving endpoint no longer exists — retrying forever wastes resources and delays visibility into a real problem
- A slow or hanging receiver can quietly stall an entire queue if nothing enforces a timeout
- A consumer can crash mid-delivery — messages must survive that crash without being lost or silently duplicated
- Every failure needs to be classified correctly, the first time, or the whole retry strategy collapses

This system solves all of these with layered infrastructure:

**topic routing → retry wait queue (TTL) → dead-letter queue → real HTTP delivery → failure classification**

---

## Architecture

```
webhook_delivery_system/
├── config.py
├── logger.py
├── models.py
├── rmq_connection.py
├── topology.py
├── producer.py
├── consumer.py
├── http_client.py
├── docker-compose.yml
└── main.py
```

---

## Request Flow

```
Producer
   │
   ▼
Topic Exchange (webhook.events)
   │  routed by event_type (e.g. payment.completed)
   ▼
Main Queue (webhook.critical / webhook.standard)
   │
   ▼
Consumer picks up message
   │
   ▼
http_client.deliver_webhook() → real HTTP POST
   │
   ├── 2xx ─────────────► ACK, delivery complete
   │
   ├── 4xx ─────────────► permanent failure → DLQ immediately, no retry
   │
   └── 5xx / timeout ───► transient failure
                              │
                              ├── retry_count < MAX_RETRIES → retry wait queue (TTL)
                              │        │
                              │        ▼
                              │   TTL expires → back to main queue → re-attempt
                              │
                              └── retry_count exhausted → DLQ for inspection
```

---

## Failure Classification

The core design decision of this system — every HTTP outcome is classified once, correctly, and routed accordingly.

| Outcome | Classification | Action |
|---|---|---|
| `2xx` | Success | ACK, delivery complete |
| `400 / 401 / 403 / 404` | Permanent | DLQ immediately — retrying a bad request never helps |
| `500 / 502 / 503 / 504` | Transient | Retry wait queue — the server may recover |
| Connection timeout | Transient | Retry wait queue — server may become reachable |
| Read timeout | Transient | Retry wait queue — server may respond next attempt |

Permanent failures skip the retry budget entirely. Transient failures consume one retry attempt each, tracked via a `retry_count` header that survives the full round trip through the retry wait queue.

---

## Components

**config.py** — Central configuration: exchange names, routing keys, `MAX_RETRIES`, TTL values. No magic numbers scattered across the codebase.

**topology.py** — Declares all RabbitMQ infrastructure separately from business logic: topic exchange, retry exchange, retry wait queue with TTL and dead-letter binding, main queues, DLQ.

**models.py** — `WebhookEvent`, the structured representation of an event moving through the system, with `to_dict()` / `from_dict()` for consistent (de)serialization.

**http_client.py** — Makes exactly one HTTP attempt per call. Zero knowledge of RabbitMQ. Returns a structured `DeliveryResult` — never raises. All retry decisions are made by the caller, not by this module.

**producer.py** — Publishes webhook events onto the topic exchange with the correct routing key.

**consumer.py** — Owns the full decision tree: reads the HTTP result, classifies the failure, routes to ACK / retry wait queue / DLQ, and logs every outcome with full context. Also detects `message.redelivered` to flag possible duplicate deliveries after a consumer crash.

**logger.py** — Structured JSON logging. Every delivery log line carries `event_id`, `target_url`, `status_code`, `response_time_ms`, `attempt_number`, and `redelivered` — directly queryable in Datadog, ELK, or Splunk.

---

## Installation

```bash
git clone https://github.com/ShubhamCodeWiz/webhook-delivery-system
cd webhook-delivery-system
pip install -r requirements.txt
docker-compose up -d
```

---

## Usage

```bash
# Start a consumer for the critical queue
python3 main.py consumer critical critical-worker-1

# Publish webhook events
python3 main.py producer
```

```python
# http_client.py can also be used standalone
from http_client import deliver_webhook

result = await deliver_webhook(
    target_url="https://api.example.com/webhooks",
    payload={"order_id": "ord_001", "amount": 100},
    event_id="evt_abc123",
    event_type="payment.completed"
)
```

---

## Example Output

A webhook that hits a `503`, retries with backoff via TTL, and succeeds on the second attempt:

```json
{"level": "WARNING", "message": "Transient failure, queued for retry", "event_id": "c0076f2c-...", "status_code": 503, "attempt_number": 1, "retry_count": 0, "error": "Transient Failure: HTTP 503"}
{"level": "INFO", "message": "Successfully processed webhook", "event_id": "c0076f2c-...", "status_code": 200, "attempt_number": 2, "retry_count": 1, "response_time_ms": 1845}
```

A permanent failure, routed straight to the DLQ with zero retries:

```json
{"level": "ERROR", "message": "Permanent failure, send to DLQ", "event_id": "5d53c560-...", "status_code": 404, "attempt_number": 1, "error": "Permanent Failure: HTTP 404"}
```

---

## Design Decisions

This section explains the *why* behind each technical choice.

### Why a separate retry wait queue instead of retrying inline?

Retrying inline blocks the consumer and hammers the failing endpoint immediately. A TTL-based retry wait queue lets RabbitMQ hold the message and return it automatically after a fixed delay — no busy-waiting, no blocked consumer, no polling.

### Why classify 4xx as permanent and 5xx as transient?

A 4xx means the request itself is broken — bad payload, bad auth, deleted endpoint. Retrying sends the same broken request into the same wall. A 5xx means the *server* is having a bad moment — it may recover. Retrying only ever helps for the latter.

### Why is `retry_count` a header, not RabbitMQ's `x-death` count?

`x-death` tracks queue-level dead-letter events, which do not map cleanly onto "how many times have we attempted HTTP delivery" once a message moves back and forth between the main queue and retry wait queue. A self-managed `retry_count` header, incremented explicitly on every delivery attempt, is unambiguous and survives the full round trip intact.

### Why does `http_client.py` know nothing about RabbitMQ?

Single responsibility. `http_client.py` makes one HTTP attempt and reports back a structured result — success, status code, timing, and whether the failure is transient. It has zero imports from `aio_pika`. All retry, DLQ, and queue-routing logic belongs to the consumer, not the HTTP layer.

### Why log `redelivered` on every delivery attempt?

At-least-once delivery means the same message can reach a consumer twice if a crash happens after HTTP delivery but before ack. We cannot prevent the duplicate HTTP call — we don't know if the previous attempt succeeded before the crash — but we can flag it, so the receiving system knows to check its own idempotency key (`event_id`) before treating it as new.

### Why structured JSON logs instead of plain text?

Plain text cannot be queried efficiently. JSON logs allow structured search across:

- Datadog
- ELK Stack
- Splunk

### Why does the consumer always ACK, even on failure?

Every failure path — permanent or transient — ends in either a DLQ publish or a retry wait queue publish, followed by an ACK. The message always leaves the main queue deliberately, through a path *we* chose. We never rely on `nack`/requeue to the same queue, which would create an unthrottled retry loop with no backoff and no visibility.

---

## What I Would Add Next

**Exponential backoff with jitter**

Currently retries use a fixed TTL. Replacing this with exponential backoff and full jitter (as validated by AWS research) would space out retries more intelligently and avoid synchronized retry storms across many failing events.

**A resilient, retry-aware HTTP caller inside http_client.py's caller**

Wrapping outbound calls with a circuit breaker would stop hammering a target that is completely down, failing fast instead of waiting out a full timeout on every attempt.

**DLQ inspector**

A small CLI or dashboard to browse `webhooks.dlq`, inspect `original_payload` and `last_error`, and optionally republish a message back onto the main queue for a manual replay.

**Observability**

Prometheus counters for total deliveries, retries, DLQ sends, and response time histograms — to replace log-grepping with real dashboards.

**Full test suite**

Using `pytest` and `respx` for HTTP mocking, to test every branch of the failure classification tree without depending on `httpbin.org` being reachable.

---

## License

MIT
