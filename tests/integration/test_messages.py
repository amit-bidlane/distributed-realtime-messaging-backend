import asyncio
import uuid
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.auth.models import User
from app.auth.passwords import hash_password
from app.conversations.models import ConversationType
from app.conversations.schemas import ConversationCreate
from app.conversations.service import create_conversation
from app.core.config import Settings
from app.db.base import Base
from app.db.session import create_engine, create_session_factory
from app.messages.models import Message, MessageReceipt
from app.messages.service import PersistResult, mark_delivered, persist_message

TEST_JWT_SECRET = "test-jwt-secret-that-is-at-least-thirty-two-characters"


def _make_engine(tmp_path: Path, name: str) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / name}",
        jwt_secret=TEST_JWT_SECRET,
        refresh_token_pepper="test-refresh-pepper",
    )
    engine = create_engine(settings)
    return engine, create_session_factory(engine)


async def _seed_direct_conversation(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    async with session_factory() as session:
        owner = User(email="owner@example.com", password_hash=hash_password("x"))
        member = User(email="member@example.com", password_hash=hash_password("x"))
        session.add_all([owner, member])
        await session.flush()
        conversation, _ = await create_conversation(
            session,
            owner.id,
            ConversationCreate(kind=ConversationType.DIRECT, member_ids=[member.id]),
        )
        return conversation.id, owner.id, member.id


def test_concurrent_sends_get_distinct_sequential_sequence_numbers(tmp_path: Path) -> None:
    engine, session_factory = _make_engine(tmp_path, "concurrent.db")

    async def scenario() -> tuple[PersistResult, PersistResult]:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        conversation_id, owner_id, member_id = await _seed_direct_conversation(session_factory)

        async def send(sender_id: uuid.UUID, body: str) -> PersistResult:
            # Each call opens its own session/connection, standing in for a
            # message arriving at a different FastAPI instance.
            async with session_factory() as session:
                return await persist_message(
                    session,
                    conversation_id=conversation_id,
                    sender_id=sender_id,
                    client_message_id=uuid.uuid4(),
                    body=body,
                )

        results = await asyncio.gather(
            send(owner_id, "from instance 1"),
            send(member_id, "from instance 2"),
        )
        await engine.dispose()
        return results[0], results[1]

    result_a, result_b = asyncio.run(scenario())

    assert result_a.is_duplicate is False
    assert result_b.is_duplicate is False
    assert {result_a.message.sequence_number, result_b.message.sequence_number} == {1, 2}
    assert result_a.message.id != result_b.message.id


def test_message_history_is_ordered_by_sequence_number(tmp_path: Path) -> None:
    engine, session_factory = _make_engine(tmp_path, "ordering.db")

    async def scenario() -> list[Message]:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        conversation_id, owner_id, _ = await _seed_direct_conversation(session_factory)

        for body in ("first", "second", "third", "fourth", "fifth"):
            async with session_factory() as session:
                await persist_message(
                    session,
                    conversation_id=conversation_id,
                    sender_id=owner_id,
                    client_message_id=uuid.uuid4(),
                    body=body,
                )

        async with session_factory() as session:
            rows = await session.scalars(
                select(Message)
                .where(Message.conversation_id == conversation_id)
                .order_by(Message.sequence_number)
            )
            history = list(rows.all())
        await engine.dispose()
        return history

    history = asyncio.run(scenario())

    assert [message.sequence_number for message in history] == [1, 2, 3, 4, 5]
    assert [message.body for message in history] == [
        "first",
        "second",
        "third",
        "fourth",
        "fifth",
    ]


def test_concurrent_identical_retries_produce_exactly_one_message(tmp_path: Path) -> None:
    engine, session_factory = _make_engine(tmp_path, "race.db")

    async def scenario() -> tuple[PersistResult, PersistResult, int]:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        conversation_id, owner_id, _ = await _seed_direct_conversation(session_factory)
        client_message_id = uuid.uuid4()

        async def send() -> PersistResult:
            async with session_factory() as session:
                return await persist_message(
                    session,
                    conversation_id=conversation_id,
                    sender_id=owner_id,
                    client_message_id=client_message_id,
                    body="retry me",
                )

        results = await asyncio.gather(send(), send())

        async with session_factory() as session:
            rows = await session.scalars(
                select(Message).where(
                    Message.conversation_id == conversation_id,
                    Message.client_message_id == client_message_id,
                )
            )
            count = len(rows.all())
        await engine.dispose()
        return results[0], results[1], count

    result_a, result_b, row_count = asyncio.run(scenario())

    assert row_count == 1
    assert {result_a.is_duplicate, result_b.is_duplicate} == {True, False}
    assert result_a.message.id == result_b.message.id
    assert result_a.message.sequence_number == result_b.message.sequence_number == 1


def test_database_rejects_duplicate_client_message_id_even_bypassing_the_app_check(
    tmp_path: Path,
) -> None:
    engine, session_factory = _make_engine(tmp_path, "constraint.db")

    async def scenario() -> None:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        conversation_id, owner_id, _ = await _seed_direct_conversation(session_factory)
        client_message_id = uuid.uuid4()

        async with session_factory() as session:
            await persist_message(
                session,
                conversation_id=conversation_id,
                sender_id=owner_id,
                client_message_id=client_message_id,
                body="original",
            )

        # Bypass persist_message's own pre-check entirely: insert a raw
        # duplicate row directly, proving the constraint itself is what
        # protects the data, not just the application-level check.
        async with session_factory() as session:
            session.add(
                Message(
                    conversation_id=conversation_id,
                    sender_id=owner_id,
                    client_message_id=client_message_id,
                    sequence_number=999,
                    body="raw duplicate",
                )
            )
            with pytest.raises(IntegrityError):
                await session.flush()

        await engine.dispose()

    asyncio.run(scenario())


def test_second_direct_conversation_between_same_pair_reuses_counter(tmp_path: Path) -> None:
    """Idempotent direct-conversation creation must not orphan a message send."""
    engine, session_factory = _make_engine(tmp_path, "reuse.db")

    async def scenario() -> PersistResult:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        conversation_id, owner_id, _ = await _seed_direct_conversation(session_factory)

        async with session_factory() as session:
            owner = await session.get(User, owner_id)
            assert owner is not None
            member_row = await session.scalar(
                select(User).where(User.id != owner_id)
            )
            assert member_row is not None
            duplicate_conversation, created = await create_conversation(
                session,
                owner_id,
                ConversationCreate(kind=ConversationType.DIRECT, member_ids=[member_row.id]),
            )
            assert created is False
            assert duplicate_conversation.id == conversation_id

        async with session_factory() as session:
            result = await persist_message(
                session,
                conversation_id=conversation_id,
                sender_id=owner_id,
                client_message_id=uuid.uuid4(),
                body="still works",
            )
        await engine.dispose()
        return result

    result = asyncio.run(scenario())

    assert result.is_duplicate is False
    assert result.message.sequence_number == 1


def test_receipt_race_rollback_does_not_corrupt_other_loaded_objects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recovering from a lost receipt-creation race must not corrupt other,
    unrelated objects already loaded in the same session."""
    engine, session_factory = _make_engine(tmp_path, "receipt_race.db")

    async def scenario() -> None:
        try:
            async with engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)
            conversation_id, owner_id, member_id = await _seed_direct_conversation(
                session_factory
            )

            async with session_factory() as session:
                send_result = await persist_message(
                    session,
                    conversation_id=conversation_id,
                    sender_id=owner_id,
                    client_message_id=uuid.uuid4(),
                    body="hello",
                )
            message_id = send_result.message.id

            real_get = AsyncSession.get
            race_injected = False

            async def get_with_injected_race(
                self: AsyncSession, entity: object, ident: object, *a: object, **kw: object
            ) -> object:
                # Lets _get_or_create_receipt's own lookup miss for real
                # (nothing exists yet), then - before it can INSERT - has a
                # second, independent session win the race by committing
                # that exact row first. Two live connections could resolve
                # this race in either order; this just makes it
                # deterministic for the test.
                value = await real_get(self, entity, ident, *a, **kw)
                nonlocal race_injected
                if entity is MessageReceipt and not race_injected:
                    race_injected = True
                    async with session_factory() as other_session:
                        other_session.add(
                            MessageReceipt(message_id=message_id, user_id=member_id)
                        )
                        await other_session.commit()
                return value

            monkeypatch.setattr(AsyncSession, "get", get_with_injected_race)

            async with session_factory() as session:
                # Stands in for get_missing_messages loading Message rows
                # into this session, just before mark_delivered is called
                # on them in a loop over the same session.
                loaded_message = await session.get(Message, message_id)
                assert loaded_message is not None

                await mark_delivered(session, message=loaded_message, user_id=member_id)
                assert race_injected is True  # else this test proves nothing

                assert loaded_message.body == "hello"
                assert loaded_message.deleted_at is None

            async with session_factory() as session:
                rows = (
                    await session.scalars(
                        select(MessageReceipt).where(
                            MessageReceipt.message_id == message_id,
                            MessageReceipt.user_id == member_id,
                        )
                    )
                ).all()
                assert len(rows) == 1
                assert rows[0].delivered_at is not None
        finally:
            await engine.dispose()

    asyncio.run(scenario())
