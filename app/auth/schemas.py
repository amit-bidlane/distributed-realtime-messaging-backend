from pydantic import BaseModel, Field, field_validator


def normalize_email(value: str) -> str:
    """Canonicalize an email address for storage/lookup: trimmed, lowercased.

    Shared by registration/login (via CredentialsRequest below) and contact
    sync (app.contacts.service.match_contacts) - both need to turn a
    user-typed or client-submitted string into the exact form stored in
    `User.email`, so matching (registration uniqueness, contact lookup) is
    case/whitespace-insensitive without needing a second normalized column.
    Raises ValueError on anything that isn't a plausible email shape.
    """
    normalized = value.strip().lower()
    if normalized.count("@") != 1:
        raise ValueError("email must contain one @")
    local, domain = normalized.split("@")
    if not local or "." not in domain:
        raise ValueError("email is invalid")
    return normalized


class CredentialsRequest(BaseModel):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=12, max_length=128)
    device_label: str = Field(default="Unnamed device", min_length=1, max_length=128)

    @field_validator("email")
    @classmethod
    def _normalize_email(cls, value: str) -> str:
        return normalize_email(value)

    @field_validator("device_label")
    @classmethod
    def normalize_device_label(cls, value: str) -> str:
        return value.strip()


class RefreshRequest(BaseModel):
    refresh_token: str = Field(min_length=1)


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class UserResponse(BaseModel):
    id: str
