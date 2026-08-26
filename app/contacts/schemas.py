import uuid
from typing import Annotated

from pydantic import BaseModel, Field

# No format validation here on purpose: a real address book routinely
# contains phone numbers, empty entries, etc. alongside emails. Anything
# that doesn't normalize to a valid email is silently unmatched by
# app.contacts.service.match_contacts rather than rejecting the whole
# request - see that module's docstring.
Identifier = Annotated[str, Field(min_length=1, max_length=320)]


class ContactSyncRequest(BaseModel):
    identifiers: list[Identifier] = Field(min_length=1, max_length=500)


class ContactMatch(BaseModel):
    # Deliberately minimal - no email, no other profile fields. See
    # README's Contact sync section.
    identifier: str
    user_id: uuid.UUID


class ContactSyncResponse(BaseModel):
    matches: list[ContactMatch]
