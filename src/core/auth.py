"""Authentication module"""

import asyncio
from dataclasses import dataclass
from fnmatch import fnmatchcase
import hmac
import bcrypt
from typing import Any, FrozenSet, Iterable, Optional, Tuple
from fastapi import Header, HTTPException, Query, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from .config import config

security = HTTPBearer()
optional_security = HTTPBearer(auto_error=False)


@dataclass(frozen=True)
class ApiKeyPolicy:
    """Resolved access policy for a verified client API key."""

    key_id: Optional[int]
    name: str
    is_master: bool
    allow_all_models: bool
    allowed_models: FrozenSet[str]

    def _matches(self, candidate: Optional[str]) -> bool:
        text = str(candidate or "").strip()
        if not text:
            return False
        for pattern in self.allowed_models:
            if not pattern:
                continue
            if "*" in pattern or "?" in pattern or "[" in pattern:
                if fnmatchcase(text, pattern):
                    return True
            elif text == pattern:
                return True
        return False

    def allows_listed_model(self, model_id: str) -> bool:
        if self.allow_all_models:
            return True
        return self._matches(model_id)

    def allows_execution(
        self,
        resolved_model: str,
        *,
        raw_model: Optional[str] = None,
        upstream_model: Optional[str] = None,
        base_aliases: Optional[Iterable[str]] = None,
    ) -> bool:
        if self.allow_all_models:
            return True
        if self._matches(resolved_model):
            return True
        if upstream_model and self._matches(upstream_model):
            return True
        raw_clean = str(raw_model or "").strip()
        if (
            raw_clean
            and base_aliases is not None
            and raw_clean in base_aliases
            and self._matches(raw_clean)
        ):
            return True
        return False


_UNRESTRICTED_POLICY = ApiKeyPolicy(
    key_id=None,
    name="master",
    is_master=True,
    allow_all_models=True,
    allowed_models=frozenset(),
)

def verify_admin_password(plain_password: str, stored_password: str) -> bool:
    """Verify admin password against a bcrypt hash or legacy plaintext value."""
    if plain_password is None or stored_password is None:
        return False
    if stored_password.startswith(("$2b$", "$2a$", "$2y$")):
        return AuthManager.verify_password(plain_password, stored_password)
    return hmac.compare_digest(
        plain_password.encode("utf-8"), stored_password.encode("utf-8")
    )



async def verify_admin_password_async(
    plain_password: str, stored_password: str
) -> bool:
    """Verify admin password asynchronously, offloading bcrypt CPU work to a thread."""
    if plain_password is None or stored_password is None:
        return False
    if stored_password.startswith(("$2b$", "$2a$", "$2y$")):
        return await asyncio.to_thread(
            AuthManager.verify_password, plain_password, stored_password
        )
    return hmac.compare_digest(
        plain_password.encode("utf-8"), stored_password.encode("utf-8")
    )

class AuthManager:
    """Authentication manager"""

    _managed_api_keys: Tuple[Tuple[str, ApiKeyPolicy], ...] = ()

    @classmethod
    def set_api_keys(cls, records: Iterable[Any]) -> None:
        """Replace the in-memory managed API key policy snapshot."""
        entries = []
        for rec in records or ():
            if not bool(getattr(rec, "is_active", True)):
                continue
            key_val = str(getattr(rec, "key_value", "") or "").strip()
            if not key_val:
                continue
            allow_all = bool(getattr(rec, "allow_all_models", True))
            raw_allowed = getattr(rec, "allowed_models", None) or ()
            allowed = frozenset(
                str(m).strip() for m in raw_allowed if str(m or "").strip()
            )
            policy = ApiKeyPolicy(
                key_id=getattr(rec, "id", None),
                name=str(getattr(rec, "name", "") or ""),
                is_master=False,
                allow_all_models=allow_all,
                allowed_models=allowed,
            )
            entries.append((key_val, policy))
        cls._managed_api_keys = tuple(entries)

    @classmethod
    def clear_api_keys(cls) -> None:
        cls._managed_api_keys = ()

    @classmethod
    def resolve_api_key_policy(cls, api_key: Optional[str]) -> Optional[ApiKeyPolicy]:
        """Verify an API key against the master key and active managed keys."""
        if api_key is None:
            return None
        raw = str(api_key)
        if not raw:
            return None
        raw_bytes = raw.encode("utf-8")
        master_key = getattr(config, "api_key", None)
        if master_key is not None and hmac.compare_digest(
            raw_bytes, str(master_key).encode("utf-8")
        ):
            return _UNRESTRICTED_POLICY
        matched: Optional[ApiKeyPolicy] = None
        for key_val, policy in cls._managed_api_keys:
            if hmac.compare_digest(raw_bytes, key_val.encode("utf-8")):
                matched = policy
        return matched

    @classmethod
    def get_policy_for_verified_key(
        cls, api_key: Optional[str]
    ) -> ApiKeyPolicy:
        """Return the policy for an already-verified key (unrestricted fallback for test overrides)."""
        policy = cls.resolve_api_key_policy(api_key)
        return policy if policy is not None else _UNRESTRICTED_POLICY

    @classmethod
    def verify_api_key(cls, api_key: str) -> bool:
        """Verify API key against master key or active managed API keys."""
        return cls.resolve_api_key_policy(api_key) is not None
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
    async def verify_admin_async(username: str, password: str) -> bool:
        """Verify admin credentials asynchronously without blocking the event loop."""
        if username is None or config.admin_username is None:
            return False
        username_ok = hmac.compare_digest(
            username.encode("utf-8"), config.admin_username.encode("utf-8")
        )
        password_ok = await verify_admin_password_async(password, config.admin_password)
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


def assert_api_key_model_allowed(
    api_key: str,
    resolved_model: str,
    *,
    raw_model: Optional[str] = None,
    upstream_model: Optional[str] = None,
    base_aliases: Optional[Iterable[str]] = None,
) -> ApiKeyPolicy:
    """Ensure the verified API key is permitted to invoke the target model."""
    policy = AuthManager.get_policy_for_verified_key(api_key)
    if not policy.allows_execution(
        resolved_model,
        raw_model=raw_model,
        upstream_model=upstream_model,
        base_aliases=base_aliases,
    ):
        label = str(raw_model or resolved_model).strip()
        raise HTTPException(
            status_code=403,
            detail=f"该 API Key 未公开此模型: {label}",
        )
    return policy
