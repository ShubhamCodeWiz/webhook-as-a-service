from pydantic import BaseModel

class DeliveryResult(BaseModel):
    success: bool
    status_code: int | None
    response_time_ms: int
    error: str | None
    is_transient: bool
    


import httpx
import time

async def deliver_webhook(
        target_url: str,
        payload: dict,
        event_id: str,
        event_type: str,
        timeout_seconds: float = 10.0
)-> DeliveryResult:
    
    headers = {
        "Content-Type": "application/json",
        "X-Correlation-ID": event_id,
        "X-Webhook-Event-Type": event_type
    }

    start = time.monotonic()


    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.post(url=target_url, json=payload, headers=headers)

        response_time_ms = int((time.monotonic() - start) * 1000)


        if response.status_code < 300:
            return DeliveryResult(
                success=True,
                status_code=response.status_code,
                response_time_ms=response_time_ms,
                error=None,
                is_transient=False
            )

        elif response.status_code < 500:
            return DeliveryResult(
                success=False,
                status_code=response.status_code,
                response_time_ms=response_time_ms,
                error=f"Permanent Failure: HTTP {response.status_code}",
                is_transient=False
            )

        else:
            return DeliveryResult(
                success=False,
                status_code=response.status_code,
                response_time_ms=response_time_ms,
                error=f"Transient Failure: HTTP {response.status_code}",
                is_transient=True
            )

    except (httpx.TimeoutException, httpx.ConnectError) as e:
        response_time_ms = int((time.monotonic() - start) * 1000)

        error_msg = (
            f"Request timed out after {timeout_seconds}s"
            if isinstance(e, httpx.TimeoutException)
            else f"Connection failed: {str(e) or 'unknown'}"
    )

        return DeliveryResult(
                success=False,
                status_code=None,
                response_time_ms=response_time_ms,
                error=error_msg,
                is_transient=True
            )



