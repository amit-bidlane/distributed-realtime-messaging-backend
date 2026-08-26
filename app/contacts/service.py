import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.models import User
from app.auth.schemas import normalize_email


@dataclass(frozen=True)
class ContactMatchResult:
    identifier: str
    user_id: uuid.UUID


async def match_contacts(
    session: AsyncSession, requester_id: uuid.UUID, identifiers: list[str]
) -> list[ContactMatchResult]:
    """Match submitted contact identifiers against registered users.

    Stateless and read-only: nothing from `identifiers` is written anywhere,
    not even a normalized copy - see README's Contact sync section for why.
    An identifier that doesn't normalize to a valid email is silently
    skipped rather than rejecting the whole batch: this system has no
    identifier type other than email today, so a phone-only contact (or any
    other garbage a real address book contains) can never match by
    definition, and one bad entry shouldn't fail an otherwise-valid sync.

    Matches are looked up with a single batched query rather than one query
    per identifier, so no per-identifier timing difference exists for a
    caller to use as an oracle beyond "in the returned list or not" - which
    is the one signal this endpoint is explicitly allowed to reveal.
    `identifier` in each result is the caller's own original string (not the
    normalized form), so the client can line a match back up with its own
    contact list without re-normalizing; when several submitted identifiers
    normalize to the same registered email, whichever was seen first wins.
    The requester's own account is never returned as a match.
    """
    normalized_to_raw: dict[str, str] = {}
    for raw in identifiers:
        try:
            normalized = normalize_email(raw)
        except ValueError:
            continue
        normalized_to_raw.setdefault(normalized, raw)

    if not normalized_to_raw:
        return []

    rows = await session.execute(
        select(User.id, User.email).where(User.email.in_(normalized_to_raw.keys()))
    )
    return [
        ContactMatchResult(identifier=normalized_to_raw[email], user_id=user_id)
        for user_id, email in rows.all()
        if user_id != requester_id
    ]
