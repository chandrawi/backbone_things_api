from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
import random
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from Crypto.Hash import SHA256


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

def encrypt_message(message: str | bytes, public_key: bytes):
    public_key = RSA.import_key(public_key)
    sha = SHA256.new()
    chipper_rsa = PKCS1_OAEP.new(public_key, sha)
    if isinstance(message, str):
        message = bytes(message, "utf-8")
    return chipper_rsa.encrypt(message)

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
