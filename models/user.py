from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class UserType(str, Enum):
    admin = "admin"
    global_mod = "global_mod"
    staff = "staff"
    normal = ""


class BroadcasterType(str, Enum):
    affiliate = "affiliate"
    partner = "partner"
    normal = ""


class UserInfo(BaseModel):
    id: str
    login: str
    display_name: str
    type: UserType
    broadcaster_type: BroadcasterType
    description: str
    profile_image_url: str
    offline_image_url: str
    created_at: str


class UserInfoResponse(BaseModel):
    data: List[UserInfo]
