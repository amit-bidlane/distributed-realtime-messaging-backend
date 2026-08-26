from app.auth.passwords import hash_password, verify_password


def test_correct_password_verifies_against_its_own_hash() -> None:
    password_hash = hash_password("correct-horse-battery-staple")

    assert verify_password("correct-horse-battery-staple", password_hash) is True


def test_wrong_password_does_not_verify() -> None:
    password_hash = hash_password("correct-horse-battery-staple")

    assert verify_password("wrong-password-value", password_hash) is False


def test_hash_never_stores_the_plaintext_password() -> None:
    password_hash = hash_password("correct-horse-battery-staple")

    assert "correct-horse-battery-staple" not in password_hash


def test_two_hashes_of_the_same_password_differ() -> None:
    """Argon2 salts each hash independently, so two hashes of the same
    password must never be equal - otherwise equal hashes would leak that
    two accounts share a password."""
    first = hash_password("correct-horse-battery-staple")
    second = hash_password("correct-horse-battery-staple")

    assert first != second
    assert verify_password("correct-horse-battery-staple", second) is True


def test_verify_against_a_malformed_hash_returns_false_instead_of_raising() -> None:
    """user.password_hash is always argon2-cffi's own output in practice, but
    verify_password must degrade to "not a match" rather than crashing the
    login endpoint if that ever isn't true (e.g. a corrupted row)."""
    assert verify_password("anything", "not-a-real-argon2-hash") is False
