from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
import random


ph = PasswordHasher()

def hash_password(password: str):
    """
    Hash a password using Argon2 with a generated salt.
    Returns the encoded hash string.
    """
    return ph.hash(password)

def verify_password(password: str, password_hash: str):
    """
    Verify a password against an Argon2 hash.
    Returns True if valid; raises on failure.
    """
    try:
        ph.verify(password_hash, password)
        return True
    except VerifyMismatchError:
        return False

def generate_access_key() -> bytes:
    # Generate 32 random bytes, each in range 0..254
    return bytes(random.randrange(0, 255) for _ in range(32))

def generate_token_string() -> str:
    CHARSET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
    # Pick 32 random characters from the charset
    return ''.join(random.choice(CHARSET) for _ in range(32))

def hex_to_bytes(s: str):
    # Length must be even
    if len(s) % 2 != 0:
        return None
    result = []
    for i in range(0, len(s), 2):
        sub = s[i:i+2]
        try:
            result.append(int(sub, 16))
        except ValueError:
            return None
    return result
