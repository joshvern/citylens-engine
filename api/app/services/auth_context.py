from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AuthContext:
    app_user_id: str
    auth_provider: str
    auth_subject: str
    email: Optional[str]
    email_verified: bool
    is_admin: bool
    plan_type: str
