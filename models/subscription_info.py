from typing import List, Optional

from pydantic import BaseModel


class SubscriptionCondition(BaseModel):
    broadcaster_user_id: Optional[str] = None


class SubscriptionTransport(BaseModel):
    method: Optional[str] = None
    callback: Optional[str] = None
    session_id: Optional[str] = None
    connected_at: Optional[str] = None
    disconnected_at: Optional[str] = None


class SubscriptionInfo(BaseModel):
    id: str
    status: str
    type: str
    version: str
    condition: SubscriptionCondition
    created_at: str
    transport: SubscriptionTransport
    cost: int


class Pagination(BaseModel):
    cursor: Optional[str] = None


class SubscriptionInfoResponse(BaseModel):
    data: List[SubscriptionInfo]
    total: int
    total_cost: int
    max_total_cost: int
    pagination: Pagination
