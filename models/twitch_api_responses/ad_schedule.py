from typing import List

from pydantic import BaseModel


class AdSchedule(BaseModel):
    snooze_count: int
    snooze_refresh_at: str
    next_ad_at: str
    duration: int
    last_ad_at: str
    preroll_free_time: int


class AdScheduleResponse(BaseModel):
    data: List[AdSchedule]
