import msgspec


class AdSchedule(msgspec.Struct):
    snooze_count: int
    snooze_refresh_at: int
    next_ad_at: int
    duration: int
    last_ad_at: int
    preroll_free_time: int


class AdScheduleResponse(msgspec.Struct):
    data: list[AdSchedule]
