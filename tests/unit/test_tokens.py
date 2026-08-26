import uuid

import jwt
import pytest

from app.auth.tokens import (
    ALGORITHM,
    TokenError,
    create_access_token,
    create_refresh_token,
    decode_token,
    digest_refresh_token,
)
from app.core.config import Settings

SETTINGS = Settings(
    jwt_secret="test-jwt-secret-that-is-at-least-thirty-two-characters",
    refresh_token_pepper="test-refresh-pepper",
)


def test_access_token_round_trips_to_its_original_claims() -> None:
    user_id, session_id = uuid.uuid4(), uuid.uuid4()
    token = create_access_token(user_id, session_id, SETTINGS)

    claims = decode_token(token, "access", SETTINGS)

    assert claims.user_id == user_id
    assert claims.session_id == session_id
    assert claims.token_type == "access"


def test_refresh_token_round_trips_to_its_original_claims() -> None:
    user_id, session_id = uuid.uuid4(), uuid.uuid4()
    token = create_refresh_token(user_id, session_id, SETTINGS)

    claims = decode_token(token, "refresh", SETTINGS)

    assert claims.user_id == user_id
    assert claims.session_id == session_id
    assert claims.token_type == "refresh"


def test_decoding_a_refresh_token_as_an_access_token_is_rejected() -> None:
    """A token's declared `typ` must match what the caller asked for - an
    endpoint that requires an access token must not accept a refresh token
    just because it was signed by the same secret."""
    token = create_refresh_token(uuid.uuid4(), uuid.uuid4(), SETTINGS)

    with pytest.raises(TokenError):
        decode_token(token, "access", SETTINGS)


def test_tampered_signature_is_rejected() -> None:
    token = create_access_token(uuid.uuid4(), uuid.uuid4(), SETTINGS)
    # Flip a character in the middle of the token rather than the very last
    # one: the last base64url character of any segment encodes some padding
    # bits alongside its real data bits, so some substitutions there (e.g.
    # 'a' <-> 'b', which differ only in a padding bit) leave the decoded
    # bytes unchanged and the "tampered" token still verifies. A middle
    # character's bits are all real data, so changing it is guaranteed to
    # alter the decoded payload/signature bytes.
    middle = len(token) // 2
    tampered = token[:middle] + ("A" if token[middle] != "A" else "B") + token[middle + 1 :]

    with pytest.raises(TokenError):
        decode_token(tampered, "access", SETTINGS)


def test_token_signed_with_a_different_secret_is_rejected() -> None:
    other_settings = Settings(
        jwt_secret="a-completely-different-secret-value-that-is-long-enough",
        refresh_token_pepper="test-refresh-pepper",
    )
    token = create_access_token(uuid.uuid4(), uuid.uuid4(), other_settings)

    with pytest.raises(TokenError):
        decode_token(token, "access", SETTINGS)


def test_token_missing_a_required_claim_is_rejected() -> None:
    """decode_token requires sub/sid/typ/exp/jti even though PyJWT alone
    would accept a token missing some of them - a hand-crafted or replayed
    partial token must not slip through."""
    incomplete = jwt.encode(
        {"sub": str(uuid.uuid4()), "typ": "access"},
        SETTINGS.jwt_secret.get_secret_value(),
        algorithm=ALGORITHM,
    )

    with pytest.raises(TokenError):
        decode_token(incomplete, "access", SETTINGS)


def test_digest_refresh_token_is_deterministic_and_distinguishes_tokens() -> None:
    token_a = create_refresh_token(uuid.uuid4(), uuid.uuid4(), SETTINGS)
    token_b = create_refresh_token(uuid.uuid4(), uuid.uuid4(), SETTINGS)

    assert digest_refresh_token(token_a, SETTINGS) == digest_refresh_token(token_a, SETTINGS)
    assert digest_refresh_token(token_a, SETTINGS) != digest_refresh_token(token_b, SETTINGS)
