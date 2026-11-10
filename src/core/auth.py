"""Authentication module"""

import hmac
import bcrypt
from typing import Optional
from fastapi import Header, HTTPException, Query, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from .config import config

security = HTTPBearer()
optional_security = HTTPBearer(auto_error=False)


def verify_admin_password(plain_password: str, stored_password: str) -> bool:
    """Verify admin password against a bcrypt hash or legacy plaintext value."""
    if plain_password is None or stored_password is None:
        return False
    if stored_password.startswith(("$2b$", "$2a$", "$2y$")):
        return AuthManager.verify_password(plain_password, stored_password)
    return hmac.compare_digest(
        plain_password.encode("utf-8"), stored_password.encode("utf-8")
    )


class AuthManager:
    """Authentication manager"""

    @staticmethod
    def verify_api_key(api_key: str) -> bool:
        """Verify API key"""
        if api_key is None or config.api_key is None:
            return False
        return hmac.compare_digest(
            api_key.encode("utf-8"), str(config.api_key).encode("utf-8")
        )

    @staticmethod
    def verify_admin_password(plain_password: str, stored_password: str) -> bool:
        """Verify admin password (supports bcrypt hashes and legacy plaintext)."""
        return verify_admin_password(plain_password, stored_password)

    @staticmethod
    def verify_admin(username: str, password: str) -> bool:
        """Verify admin credentials"""
        # Compare with current config (which may be from database or config file)
        if username is None or config.admin_username is None:
            return False
        username_ok = hmac.compare_digest(
            username.encode("utf-8"), config.admin_username.encode("utf-8")
        )
        password_ok = verify_admin_password(password, config.admin_password)
        return username_ok and password_ok

    @staticmethod
    def hash_password(password: str) -> str:
        """Hash password"""
        return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    @staticmethod
    def verify_password(password: str, hashed: str) -> bool:
        """Verify password"""
        try:
            return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))
        except ValueError:
            return False


async def verify_api_key_header(
    credentials: HTTPAuthorizationCredentials = Security(security),
) -> str:
    """Verify API key from Authorization header"""
    api_key = credentials.credentials
    if not AuthManager.verify_api_key(api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key


async def verify_api_key_flexible(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(optional_security),
    x_goog_api_key: Optional[str] = Header(None, alias="x-goog-api-key"),
    key: Optional[str] = Query(None),
) -> str:
    """Verify API key from Authorization header, x-goog-api-key header, or key query param."""
    api_key = None

    if credentials is not None:
        api_key = credentials.credentials
    elif x_goog_api_key:
        api_key = x_goog_api_key
    elif key:
        api_key = key

    if not api_key or not AuthManager.verify_api_key(api_key):
        raise HTTPException(status_code=401, detail="Invalid API key")

    return api_key
