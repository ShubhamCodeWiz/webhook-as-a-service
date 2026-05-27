from pydantic import BaseModel, Field
from typing import Dict, Any
from uuid import uuid4
from datetime import datetime, timezone
from pydantic import ConfigDict


class WebhookEvent(BaseModel):
    event_id: str = Field(
        default_factory=lambda: str(uuid4())
    )

    event_type: str
    target_url: str

    payload: Dict[str, Any]

    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    correlation_id: str = Field(
        default_factory=lambda: str(uuid4())
    )

    model_config = ConfigDict(frozen=True)

    def to_dict(self) -> dict:
        return self.model_dump()

    def to_json(self) -> str:
        return self.model_dump_json()

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)
    
    @property
    def routing_key(self):
        return self.event_type