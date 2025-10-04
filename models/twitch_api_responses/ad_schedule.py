from typing import List

from pydantic import BaseModel


class AdSchedule(BaseModel):
    snooze_count: int
    snooze_refresh_at: int
    next_ad_at: int
    duration: int
    last_ad_at: int
    preroll_free_time: int


class AdScheduleResponse(BaseModel):
    data: List[AdSchedule]
