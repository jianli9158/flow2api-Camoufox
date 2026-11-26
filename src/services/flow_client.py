"""Flow API Client for VideoFX (Veo) and Imagen."""

import asyncio
import contextvars
from http.cookiejar import CookieJar
import inspect
import json
import random
import re
import time
from unittest.mock import NonCallableMock
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse
try:
    from curl_cffi import CurlInfo, CurlOpt
    from curl_cffi.requests import AsyncSession
except ImportError:
    CurlInfo = None
    CurlOpt = None
    AsyncSession = None

from ..core.config import config
from ..core.flow_errors import FlowErrorCategory, classify_flow_error
from ..core.logger import debug_logger
from ..core.prompt_tools import detect_image_mime_type, jitter_seconds
from .browser_cookie_utils import serialize_cookie_header_for_url
from .flow_cookies import FlowCookieJar
from .flow_current import CurrentFlowClientMixin, _safe_updated_flow_cookies
from .flow_frontend import FlowFrontendMixin
_PROXY_UNSET = object()

class _FlowClientBase(FlowFrontendMixin):
    """VideoFX API 客户端基础层"""

    FLOW_PUBLIC_API_KEY = "AIzaSyBtrm0o5ab1c-Ec8ZuLcGt3oJAA5VWt3pY"
    FLOW_BROWSER_CHANNEL_HEADER = "stable"
    FLOW_BROWSER_COPYRIGHT_HEADER = "Copyright 2026 Google LLC. All Rights Reserved."
    FLOW_BROWSER_VALIDATION_HEADER = "MRCPrt/rS3JY47x2Yiz9h3ag4U8="
    FLOW_BROWSER_YEAR_HEADER = "2026"
    FLOW_FRONTEND_EXPERIMENT_IDS = (
        "106184493,106256669,105798603,106281924,106259075,106262194,"
        "105993823,106104244,105484652,1714252,105928947,106238955,"
        "106225453,106131447,1706538,119157484,106243706,105746691,"
        "106151974,106125249,106001691,106077941"
    )

    def __init__(self, proxy_manager, db=None):
        self.proxy_manager = proxy_manager
        self.db = db
        self.labs_base_url = config.flow_labs_base_url
        self.api_base_url = config.flow_api_base_url
        self.timeout = config.flow_timeout
        self._user_agent_cache = {}
        self._http_sessions: Dict[Tuple[int, str, str, bool], Any] = {}
        self._http_session_locks: Dict[int, asyncio.Lock] = {}
        self._http_session_last_used: Dict[Tuple[int, str, str, bool], float] = {}
        self._frontend_bootstrap_cache: Dict[str, Tuple[float, Dict[str, str]]] = {}
        self._request_fingerprint_ctx: contextvars.ContextVar[
            Optional[Dict[str, Any]]
        ] = contextvars.ContextVar("flow_request_fingerprint", default=None)
        self._default_client_headers = {
            "sec-ch-ua-mobile": "?0",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
        }

    def _ensure_http_session_pool(
        self,
    ) -> Tuple[Dict[Tuple[int, str, str, bool], Any], Dict[int, asyncio.Lock]]:
        sessions = getattr(self, "_http_sessions", None)
        if sessions is None:
            sessions = {}
            self._http_sessions = sessions
        locks = getattr(self, "_http_session_locks", None)
        if locks is None:
            locks = {}
            self._http_session_locks = locks
        return sessions, locks

    async def _close_http_session(self, session: Any) -> None:
        if session is None:
            return
        aexit = getattr(session, "__aexit__", None)
        if callable(aexit):
            try:
                res = aexit(None, None, None)
                if inspect.isawaitable(res):
                    await res
                return
            except BaseException:
                pass
        close_fn = getattr(session, "close", None)
        if callable(close_fn):
            try:
                res = close_fn()
                if inspect.isawaitable(res):
                    await res
            except BaseException:
                pass

    async def _get_http_session(
        self,
        impersonate: str,
        proxy_url: Optional[str],
        flag: bool = False,
    ) -> Tuple[Tuple[int, str, str, bool], Any]:
        sessions, locks = self._ensure_http_session_pool()
        loop_id = id(asyncio.get_running_loop())
        normalized_impersonate = str(impersonate or "chrome")
        key: Tuple[int, str, str, bool] = (
            loop_id,
            normalized_impersonate,
            str(proxy_url or ""),
            bool(flag),
        )
        last_used_map = getattr(self, "_http_session_last_used", None)
        if last_used_map is None:
            last_used_map = {}
            self._http_session_last_used = last_used_map
        now = time.monotonic()
        existing = sessions.get(key)
        if existing is not None and (now - last_used_map.get(key, now)) < 300.0:
            last_used_map[key] = now
            return key, existing

        lock = locks.get(loop_id)
        if lock is None:
            lock = asyncio.Lock()
            locks[loop_id] = lock

        stale_sessions: list[Any] = []
        async with lock:
            now = time.monotonic()
            for k, sess in list(sessions.items()):
                if k[0] == loop_id and (now - last_used_map.get(k, now)) >= 300.0:
                    sessions.pop(k, None)
                    last_used_map.pop(k, None)
                    stale_sessions.append(sess)
            existing = sessions.get(key)
            if existing is not None:
                last_used_map[key] = now
                chosen = existing
            else:
                if len(sessions) >= 32:
                    oldest_k = min(
                        sessions.keys(),
                        key=lambda item_k: last_used_map.get(item_k, 0.0),
                    )
                    stale_sessions.append(sessions.pop(oldest_k))
                    last_used_map.pop(oldest_k, None)
                created = AsyncSession(
                    trust_env=False,
                    impersonate=normalized_impersonate,
                )
                aenter = getattr(created, "__aenter__", None)
                if callable(aenter):
                    entered = aenter()
                    if inspect.isawaitable(entered):
                        entered = await entered
                    if entered is not None:
                        created = entered
                sessions[key] = created
                last_used_map[key] = now
                chosen = created

        for stale in stale_sessions:
            await self._close_http_session(stale)
        return key, chosen
    async def _evict_http_session(
        self,
        key: Tuple[int, str, str, bool],
        session: Any,
    ) -> None:
        sessions, _ = self._ensure_http_session_pool()
        if sessions.get(key) is session:
            sessions.pop(key, None)
        if hasattr(self, "_http_session_last_used"):
            self._http_session_last_used.pop(key, None)
        await self._close_http_session(session)

    def _ensure_token_http_session_pool(
        self,
    ) -> Tuple[
        Dict[Tuple[int, int, str, str, bool], Any],
        Dict[Tuple[int, int, str, str, bool], asyncio.Lock],
        Dict[int, asyncio.Lock],
    ]:
        sessions = getattr(self, "_token_http_sessions", None)
        if sessions is None:
            sessions = {}
            self._token_http_sessions = sessions
        key_locks = getattr(self, "_token_http_session_locks", None)
        if key_locks is None:
            key_locks = {}
            self._token_http_session_locks = key_locks
        pool_locks = getattr(self, "_token_http_pool_locks", None)
        if pool_locks is None:
            pool_locks = {}
            self._token_http_pool_locks = pool_locks
        return sessions, key_locks, pool_locks

    async def _get_token_http_session(
        self,
        *,
        token_id: int,
        impersonate: str,
        proxy_url: Optional[str],
        verify: bool = True,
    ) -> Tuple[Tuple[int, int, str, str, bool], Any, asyncio.Lock]:
        sessions, key_locks, pool_locks = self._ensure_token_http_session_pool()
        loop_id = id(asyncio.get_running_loop())
        normalized_impersonate = str(impersonate or "chrome")
        key: Tuple[int, int, str, str, bool] = (
            loop_id,
            int(token_id),
            normalized_impersonate,
            str(proxy_url or ""),
            bool(verify),
        )
        last_used_map = getattr(self, "_token_http_session_last_used", None)
        if last_used_map is None:
            last_used_map = {}
            self._token_http_session_last_used = last_used_map

        now = time.monotonic()
        existing = sessions.get(key)
        key_lock = key_locks.get(key)
        if (
            existing is not None
            and key_lock is not None
            and (now - last_used_map.get(key, now)) < 300.0
        ):
            last_used_map[key] = now
            return key, existing, key_lock

        pool_lock = pool_locks.get(loop_id)
        if pool_lock is None:
            pool_lock = asyncio.Lock()
            pool_locks[loop_id] = pool_lock

        stale_sessions: list[Any] = []
        async with pool_lock:
            now = time.monotonic()
            for k, sess in list(sessions.items()):
                k_lock = key_locks.get(k)
                if (
                    k[0] == loop_id
                    and (now - last_used_map.get(k, now)) >= 300.0
                    and (k_lock is None or not k_lock.locked())
                ):
                    sessions.pop(k, None)
                    last_used_map.pop(k, None)
                    key_locks.pop(k, None)
                    stale_sessions.append(sess)

            existing = sessions.get(key)
            key_lock = key_locks.get(key)
            if existing is not None and key_lock is not None:
                last_used_map[key] = now
                chosen = existing
            else:
                if len(sessions) >= 64:
                    unlocked_keys = [
                        k
                        for k in sessions.keys()
                        if not (key_locks.get(k) and key_locks[k].locked())
                    ]
                    if unlocked_keys:
                        oldest_k = min(
                            unlocked_keys,
                            key=lambda item_k: last_used_map.get(item_k, 0.0),
                        )
                        stale_sessions.append(sessions.pop(oldest_k))
                        last_used_map.pop(oldest_k, None)
                        key_locks.pop(oldest_k, None)
                session_kwargs: Dict[str, Any] = {
                    "trust_env": False,
                    "impersonate": normalized_impersonate,
                    "curl_infos": [CurlInfo.COOKIELIST],
                    "discard_cookies": True,
                }
                try:
                    created = AsyncSession(**session_kwargs)
                except TypeError:
                    session_kwargs.pop("discard_cookies", None)
                    created = AsyncSession(**session_kwargs)
                aenter = getattr(created, "__aenter__", None)
                if callable(aenter):
                    entered = aenter()
                    if inspect.isawaitable(entered):
                        entered = await entered
                    if entered is not None:
                        created = entered
                sessions[key] = created
                last_used_map[key] = now
                key_lock = key_locks.setdefault(key, asyncio.Lock())
                chosen = created

        for stale in stale_sessions:
            await self._close_http_session(stale)
        return key, chosen, key_lock

    async def _evict_token_http_session(
        self,
        key: Tuple[int, int, str, str, bool],
        session: Any,
    ) -> None:
        sessions, key_locks, _ = self._ensure_token_http_session_pool()
        if sessions.get(key) is session:
            sessions.pop(key, None)
            key_locks.pop(key, None)
        if hasattr(self, "_token_http_session_last_used"):
            self._token_http_session_last_used.pop(key, None)
        await self._close_http_session(session)

    async def close(self) -> None:
        """Close all pooled curl_cffi AsyncSession instances."""
        sessions, locks = self._ensure_http_session_pool()
        pooled = list(sessions.values())
        sessions.clear()
        locks.clear()
        if hasattr(self, "_http_session_last_used"):
            self._http_session_last_used.clear()

        token_sessions, token_key_locks, token_pool_locks = (
            self._ensure_token_http_session_pool()
        )
        pooled.extend(token_sessions.values())
        token_sessions.clear()
        token_key_locks.clear()
        token_pool_locks.clear()
        if hasattr(self, "_token_http_session_last_used"):
            self._token_http_session_last_used.clear()

        for session in pooled:
            await self._close_http_session(session)
    def _generate_user_agent(self, account_id: str = None) -> str:
        if not hasattr(self, "_user_agent_cache"):
            self._user_agent_cache = {}
        if not account_id:
            account_id = f"random_{random.randint(1, 999999)}"
        if account_id in self._user_agent_cache:
            return self._user_agent_cache[account_id]

        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) "
            "Gecko/20100101 Firefox/152.0"
        )
        self._user_agent_cache[account_id] = user_agent
        return user_agent

    def _set_request_fingerprint(self, fingerprint: Optional[Dict[str, Any]]):
        if not hasattr(self, "_request_fingerprint_ctx"):
            self._request_fingerprint_ctx = contextvars.ContextVar(
                "flow_request_fingerprint",
                default=None,
            )
        self._request_fingerprint_ctx.set(dict(fingerprint) if fingerprint else None)

    def get_request_fingerprint(self) -> Optional[Dict[str, Any]]:
        if not hasattr(self, "_request_fingerprint_ctx"):
            return None
        fingerprint = self._request_fingerprint_ctx.get()
        if not isinstance(fingerprint, dict) or not fingerprint:
            return None
        return dict(fingerprint)

    def clear_request_fingerprint(self):
        self._set_request_fingerprint(None)

    def _set_captcha_request_context(self, context: Optional[Dict[str, Any]]):
        if not hasattr(self, "_captcha_request_ctx"):
            self._captcha_request_ctx = contextvars.ContextVar(
                "flow_captcha_request", default=None
            )
        self._captcha_request_ctx.set(dict(context) if context else None)

    def _get_captcha_request_context(self) -> Dict[str, Any]:
        context = (
            self._captcha_request_ctx.get()
            if hasattr(self, "_captcha_request_ctx")
            else None
        )
        return dict(context) if context else {}

    def _current_browser_ref(self, token_id: Optional[int]):
        context = self._get_captcha_request_context()
        if context.get("token_id") == token_id:
            return context.get("browser_ref")
        return None

    def _get_primary_accept_language(self, fallback: str = "zh-CN,zh;q=0.9") -> str:
        fingerprint = self.get_request_fingerprint()
        if isinstance(fingerprint, dict):
            accept_language = str(fingerprint.get("accept_language") or "").strip()
            if accept_language:
                return self._normalize_accept_language_header(
                    accept_language, fallback=fallback
                )
        return self._normalize_accept_language_header(fallback, fallback=fallback)

    def _infer_sec_ch_ua_from_user_agent(self, user_agent: Optional[str]) -> str:
        ua = str(user_agent or "").strip()
        if not ua or "firefox/" in ua.lower():
            return ""
        match = re.search(r"(?:Chrome|Chromium)/(\d+)", ua, re.IGNORECASE)
        major = match.group(1) if match else "124"
        return f'"Google Chrome";v="{major}", "Chromium";v="{major}", "Not)A;Brand";v="24"'

    def _normalize_sec_ch_ua_header(
        self,
        sec_ch_ua: Optional[str],
        *,
        user_agent: Optional[str] = None,
    ) -> str:
        raw = str(sec_ch_ua or "").strip()
        inferred = self._infer_sec_ch_ua_from_user_agent(user_agent)
        if not raw:
            return inferred
        ua_text = str(user_agent or "").lower()
        if "chrome/" in ua_text and "google chrome" not in raw.lower():
            return inferred
        return raw or inferred

    def _build_fingerprint_from_user_agent(
        self,
        user_agent: Optional[str],
        *,
        accept_language: Optional[str] = None,
        proxy_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        ua = str(user_agent or "").strip()
        if not ua:
            return {}
        ua_lower = ua.lower()
        is_firefox = "firefox/" in ua_lower
        platform = '"Windows"'
        mobile = "?0"
        if "android" in ua_lower:
            platform = '"Android"'
            mobile = "?1"
        elif "iphone" in ua_lower or "ipad" in ua_lower or "ios" in ua_lower:
            platform = '"iOS"'
            mobile = "?1"
        elif "mac" in ua_lower:
            platform = '"macOS"'
        elif "linux" in ua_lower or "x11" in ua_lower:
            platform = '"Linux"'
        fingerprint: Dict[str, Any] = {
            "user_agent": ua,
            "sec_ch_ua": ""
            if is_firefox
            else self._infer_sec_ch_ua_from_user_agent(ua),
            "sec_ch_ua_mobile": "" if is_firefox else mobile,
            "sec_ch_ua_platform": "" if is_firefox else platform,
        }
        normalized_accept_language = self._normalize_accept_language_header(
            accept_language
        )
        if normalized_accept_language:
            fingerprint["accept_language"] = normalized_accept_language
        if proxy_url:
            fingerprint["proxy_url"] = proxy_url
        return fingerprint

    def _normalize_accept_language_header(
        self,
        accept_language: Optional[str],
        fallback: str = "zh-CN,zh;q=0.9",
    ) -> str:
        raw = str(accept_language or "").strip()
        if not raw:
            return fallback
        if "," in raw:
            normalized_parts: list[str] = []
            for index, item in enumerate(raw.split(",")):
                candidate = str(item or "").strip()
                if not candidate:
                    continue
                language = candidate.split(";", 1)[0].strip()
                if not language:
                    continue
                if index == 0:
                    normalized_parts.append(language)
                    continue
                q_match = re.search(r";\s*q=([0-9.]+)", candidate, re.IGNORECASE)
                q_value = (
                    q_match.group(1)
                    if q_match
                    else f"{max(0.1, 1 - (index * 0.1)):.1f}"
                )
                normalized_parts.append(f"{language};q={q_value}")
            if normalized_parts:
                return ",".join(normalized_parts)
            return fallback
        if "-" in raw:
            primary = raw.split("-", 1)[0].strip()
            if len(primary) == 2 and primary.isalpha():
                return f"{raw},{primary};q=0.9"
        return raw

    def _get_effective_request_user_agent(
        self, account_id: Optional[str] = None
    ) -> str:
        fingerprint = self.get_request_fingerprint()
        if isinstance(fingerprint, dict):
            user_agent = str(fingerprint.get("user_agent") or "").strip()
            if user_agent:
                return user_agent
        return self._generate_user_agent(account_id)

    @staticmethod
    def _should_attach_runtime_session_cookies(url: str) -> bool:
        host = str(urlparse(str(url or "")).hostname or "").lower()
        if not host:
            return False
        return any(
            host == candidate or host.endswith(f".{candidate}")
            for candidate in (
                "google.com",
                "googleapis.com",
                "recaptcha.net",
            )
        )

    @staticmethod
    def _merge_cookie_header(
        existing_cookie_header: Optional[str],
        extra_cookies: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        cookie_items: Dict[str, str] = {}
        raw_existing = str(existing_cookie_header or "").strip()
        if raw_existing:
            for part in raw_existing.split(";"):
                item = str(part or "").strip()
                if not item or "=" not in item:
                    continue
                key, value = item.split("=", 1)
                key = str(key or "").strip()
                value = str(value or "").strip()
                if key:
                    cookie_items[key] = value

        if isinstance(extra_cookies, dict):
            for key, value in extra_cookies.items():
                normalized_key = str(key or "").strip()
                normalized_value = str(value or "").strip()
                if (
                    normalized_key
                    and normalized_value
                    and normalized_key not in cookie_items
                ):
                    cookie_items[normalized_key] = normalized_value

        if not cookie_items:
            return raw_existing or None
        return "; ".join(f"{key}={value}" for key, value in cookie_items.items())

    def _build_flow_project_page_url(self, project_id: str) -> str:
        return f"https://flow.google.com/project/{project_id}"

    def _compact_json_dumps(self, payload: Any) -> str:
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _extract_project_id_from_request_payload(
        payload: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        if not isinstance(payload, dict):
            return None
        client_context = payload.get("clientContext")
        if isinstance(client_context, dict):
            project_id = str(client_context.get("projectId") or "").strip()
            if project_id:
                return project_id
        requests = payload.get("requests")
        if isinstance(requests, list):
            for item in requests:
                if not isinstance(item, dict):
                    continue
                item_client_context = item.get("clientContext")
                if not isinstance(item_client_context, dict):
                    continue
                project_id = str(item_client_context.get("projectId") or "").strip()
                if project_id:
                    return project_id
        return None

    async def _make_text_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        raw_body: Optional[Union[str, bytes]] = None,
        use_st: bool = False,
        st_token: Optional[str] = None,
        use_at: bool = False,
        at_token: Optional[str] = None,
        timeout: Optional[int] = None,
        respect_fingerprint_proxy: bool = True,
        force_no_proxy: bool = False,
        apply_default_client_headers: bool = True,
        return_error_response: bool = False,
        redact_sensitive_logs: bool = False,
        impersonate: str = "chrome",
        cookie_jar: Optional[CookieJar] = None,
        proxy_url: Any = _PROXY_UNSET,
        token_id: Optional[int] = None,
    ) -> str:
        """执行原始文本请求（如 SSE / batchexecute），返回响应文本。"""
        fingerprint = self._request_fingerprint_ctx.get()

        resolved_proxy_url: Optional[str] = None
        if not force_no_proxy:
            if proxy_url is not _PROXY_UNSET:
                resolved_proxy_url = str(proxy_url or "").strip() or None
            else:
                if self.proxy_manager:
                    if hasattr(self.proxy_manager, "get_request_proxy_url"):
                        resolved_proxy_url = await self.proxy_manager.get_request_proxy_url()
                    else:
                        resolved_proxy_url = await self.proxy_manager.get_proxy_url()

                if (
                    respect_fingerprint_proxy
                    and isinstance(fingerprint, dict)
                    and "proxy_url" in fingerprint
                ):
                    resolved_proxy_url = fingerprint.get("proxy_url")
                    if resolved_proxy_url == "":
                        resolved_proxy_url = None
        proxy_url = resolved_proxy_url
        request_timeout = timeout or self.timeout
        headers = dict(headers) if headers else {}

        if use_st and st_token:
            headers["Cookie"] = f"__Secure-next-auth.session-token={st_token}"
        if use_at and at_token:
            headers["authorization"] = f"Bearer {at_token}"

        account_id = (
            st_token[:16] if st_token else (at_token[:16] if at_token else None)
        )
        fingerprint_user_agent = (
            fingerprint.get("user_agent") if isinstance(fingerprint, dict) else None
        )
        effective_user_agent = str(
            fingerprint_user_agent or self._generate_user_agent(account_id)
        ).strip()
        headers.setdefault("Content-Type", "application/json")
        headers.setdefault("User-Agent", effective_user_agent)
        headers.setdefault(
            "Accept-Language",
            self._get_primary_accept_language(fallback="zh-CN,zh;q=0.9"),
        )

        if isinstance(fingerprint, dict):
            if fingerprint.get("accept_language"):
                headers.setdefault("Accept-Language", fingerprint["accept_language"])
            if fingerprint.get("sec_ch_ua"):
                headers["sec-ch-ua"] = self._normalize_sec_ch_ua_header(
                    fingerprint["sec_ch_ua"],
                    user_agent=headers.get("User-Agent"),
                )
            if fingerprint.get("sec_ch_ua_mobile"):
                headers["sec-ch-ua-mobile"] = fingerprint["sec_ch_ua_mobile"]
            if fingerprint.get("sec_ch_ua_platform"):
                headers["sec-ch-ua-platform"] = fingerprint["sec_ch_ua_platform"]
            if self._should_attach_runtime_session_cookies(url):
                origin = (
                    str(fingerprint.get("origin") or "").strip()
                    or "https://flow.google.com"
                )
                referer = str(fingerprint.get("referer") or "").strip()
                if not referer:
                    fingerprint_project_id = str(
                        fingerprint.get("project_id") or ""
                    ).strip()
                    if fingerprint_project_id:
                        referer = self._build_flow_project_page_url(
                            fingerprint_project_id
                        )
                if origin:
                    headers.setdefault("Origin", origin)
                if referer:
                    headers.setdefault("Referer", referer)
                merged_cookie_header = self._merge_cookie_header(
                    headers.get("Cookie"),
                    fingerprint.get("session_cookies"),
                )
                if merged_cookie_header:
                    headers["Cookie"] = merged_cookie_header

        if apply_default_client_headers:
            for key, value in self._default_client_headers.items():
                headers.setdefault(key, value)

        if self._should_attach_runtime_session_cookies(url):
            derived_project_id = self._extract_project_id_from_request_payload(
                json_data
            )
            headers.setdefault("Origin", "https://flow.google.com")
            if derived_project_id:
                headers.setdefault(
                    "Referer", self._build_flow_project_page_url(derived_project_id)
                )

        inferred_fingerprint = self._build_fingerprint_from_user_agent(
            headers.get("User-Agent"),
            accept_language=headers.get("Accept-Language"),
            proxy_url=proxy_url,
        )
        if not headers.get("sec-ch-ua") and inferred_fingerprint.get("sec_ch_ua"):
            headers["sec-ch-ua"] = inferred_fingerprint["sec_ch_ua"]
        if not headers.get("sec-ch-ua-platform") and inferred_fingerprint.get(
            "sec_ch_ua_platform"
        ):
            headers["sec-ch-ua-platform"] = inferred_fingerprint["sec_ch_ua_platform"]
        if not headers.get("sec-ch-ua-mobile") and inferred_fingerprint.get(
            "sec_ch_ua_mobile"
        ):
            headers["sec-ch-ua-mobile"] = inferred_fingerprint["sec_ch_ua_mobile"]

        if isinstance(cookie_jar, FlowCookieJar):
            cookie_jar.add_runtime_cookies(
                (fingerprint or {}).get("session_cookies"), url
            )

        request_body_for_log = (
            "<redacted>"
            if redact_sensitive_logs
            else (raw_body if raw_body is not None else json_data)
        )
        if config.debug_enabled:
            debug_logger.log_request(
                method=method,
                url=url,
                headers={} if redact_sensitive_logs else headers,
                body=request_body_for_log,
                proxy=proxy_url,
            )

        start_time = time.time()

        async def _send_with_ephemeral_cookie_session() -> Any:
            ephemeral_headers = dict(headers)
            ephemeral_headers.pop("Cookie", None)
            session_kwargs: Dict[str, Any] = {
                "trust_env": False,
                "cookies": cookie_jar,
                "curl_infos": [CurlInfo.COOKIELIST],
            }
            if isinstance(cookie_jar, FlowCookieJar):
                extra = cookie_jar.extra_cookie_header(url)
                session_kwargs.update(
                    curl_options={CurlOpt.COOKIE: extra},
                    allow_redirects=not bool(extra),
                )
            async with AsyncSession(**session_kwargs) as session:
                if method.upper() == "GET":
                    resp = await session.get(
                        url,
                        headers=ephemeral_headers,
                        proxy=proxy_url,
                        timeout=request_timeout,
                        impersonate=impersonate,
                    )
                else:
                    request_kwargs = {
                        "headers": ephemeral_headers,
                        "proxy": proxy_url,
                        "timeout": request_timeout,
                        "impersonate": impersonate,
                    }
                    if raw_body is not None:
                        request_kwargs["data"] = raw_body
                    else:
                        request_kwargs["json"] = json_data
                    resp = await session.post(url, **request_kwargs)
                updater = getattr(self, "_update_flow_cookie_jar", None)
                if updater is not None:
                    updater(cookie_jar, resp)
                return resp

        try:
            if cookie_jar is not None:
                if token_id is not None:
                    pool_key, session, key_lock = await self._get_token_http_session(
                        token_id=int(token_id),
                        impersonate=impersonate,
                        proxy_url=proxy_url,
                        verify=True,
                    )
                    if key_lock.locked():
                        response = await _send_with_ephemeral_cookie_session()
                    else:
                        async with key_lock:
                            try:
                                cookies_obj = getattr(session, "cookies", None)
                                clear_fn = getattr(cookies_obj, "clear", None)
                                if (
                                    callable(clear_fn)
                                    and not isinstance(cookies_obj, NonCallableMock)
                                ):
                                    clear_fn()
                                session_curl_opts = getattr(session, "curl_options", None)
                                if isinstance(session_curl_opts, dict):
                                    session_curl_opts.pop(CurlOpt.COOKIE, None)

                                extra = (
                                    cookie_jar.extra_cookie_header(url)
                                    if isinstance(cookie_jar, FlowCookieJar)
                                    else ""
                                )
                                pooled_headers = dict(headers)
                                if isinstance(cookie_jar, FlowCookieJar):
                                    scoped_cookie_header = serialize_cookie_header_for_url(
                                        cookie_jar.export_storage(), url
                                    )
                                    if scoped_cookie_header:
                                        pooled_headers["Cookie"] = scoped_cookie_header
                                    else:
                                        pooled_headers.pop("Cookie", None)
                                else:
                                    pooled_headers.pop("Cookie", None)

                                request_kwargs = {
                                    "headers": pooled_headers,
                                    "proxy": proxy_url,
                                    "timeout": request_timeout,
                                    "impersonate": impersonate,
                                    "cookies": cookie_jar,
                                    "discard_cookies": True,
                                    "allow_redirects": not bool(extra),
                                }
                                if method.upper() == "GET":
                                    try:
                                        response = await session.get(url, **request_kwargs)
                                    except TypeError:
                                        request_kwargs.pop("discard_cookies", None)
                                        request_kwargs.pop("cookies", None)
                                        request_kwargs.pop("allow_redirects", None)
                                        response = await session.get(url, **request_kwargs)
                                else:
                                    if raw_body is not None:
                                        request_kwargs["data"] = raw_body
                                    else:
                                        request_kwargs["json"] = json_data
                                    try:
                                        response = await session.post(url, **request_kwargs)
                                    except TypeError:
                                        request_kwargs.pop("discard_cookies", None)
                                        request_kwargs.pop("cookies", None)
                                        request_kwargs.pop("allow_redirects", None)
                                        response = await session.post(url, **request_kwargs)
                                updater = getattr(self, "_update_flow_cookie_jar", None)
                                if updater is not None:
                                    updater(cookie_jar, response)
                            except BaseException:
                                await self._evict_token_http_session(pool_key, session)
                                raise
                            finally:
                                if isinstance(getattr(session, "curl_options", None), dict):
                                    session.curl_options.pop(CurlOpt.COOKIE, None)
                else:
                    response = await _send_with_ephemeral_cookie_session()
            else:
                pool_key, session = await self._get_http_session(
                    impersonate=impersonate,
                    proxy_url=proxy_url,
                )
                try:
                    if method.upper() == "GET":
                        response = await session.get(
                            url,
                            headers=headers,
                            proxy=proxy_url,
                            timeout=request_timeout,
                            impersonate=impersonate,
                        )
                    else:
                        request_kwargs = {
                            "headers": headers,
                            "proxy": proxy_url,
                            "timeout": request_timeout,
                            "impersonate": impersonate,
                        }
                        if raw_body is not None:
                            request_kwargs["data"] = raw_body
                        else:
                            request_kwargs["json"] = json_data
                        response = await session.post(url, **request_kwargs)
                except BaseException:
                    await self._evict_http_session(pool_key, session)
                    raise

            duration_ms = (time.time() - start_time) * 1000
            response_text = response.text

            if config.debug_enabled:
                debug_logger.log_response(
                    status_code=response.status_code,
                    headers={}
                    if redact_sensitive_logs
                    else dict(response.headers),
                    body="<redacted>" if redact_sensitive_logs else response_text,
                    duration_ms=duration_ms,
                )

            if response.status_code >= 400 and return_error_response:
                return response_text

            if response.status_code >= 400:
                error_reason = (
                    f"HTTP Error {response.status_code}: {response_text[:500]}"
                )
                try:
                    error_body = response.json()
                    if "error" in error_body:
                        error_info = error_body["error"]
                        error_message = error_info.get("message", "")
                        details = error_info.get("details", [])
                        for detail in details:
                            if detail.get("reason"):
                                error_reason = detail.get("reason")
                                break
                        if error_message:
                            error_reason = f"{error_reason}: {error_message}"
                except Exception:
                    pass
                raise Exception(error_reason)

            return response_text

        except Exception as e:
            raise Exception(f"Flow API text request failed: {str(e)}")

    def _is_proxy_connection_error(self, error: Exception) -> bool:
        error_lower = str(error).lower()
        return any(
            keyword in error_lower
            for keyword in [
                "failed to connect to 127.0.0.1 port",
                "failed to connect to localhost port",
                "proxyerror",
                "proxy error",
                "failed to connect to proxy",
                "couldn't connect to server",
                "curl: (7)",
            ]
        )

    def _is_retryable_network_error(self, error_str: str) -> bool:
        error_lower = (error_str or "").lower()
        return any(
            keyword in error_lower
            for keyword in [
                "curl: (35)",
                "curl: (52)",
                "curl: (56)",
                "ssl_error_syscall",
                "tls connect error",
                "ssl connect error",
                "connection reset",
                "connection aborted",
                "connection was reset",
                "connection timed out",
                "curl: (28)",
                "timed out",
                "timeout",
                "unexpected eof",
                "empty reply from server",
                "recv failure",
                "send failure",
                "connection refused",
                "network is unreachable",
                "remote host closed connection",
            ]
        )

    def _get_control_plane_timeout(self) -> int:
        return max(5, min(int(self.timeout or 0) or 120, 10))

    def _get_video_submit_timeout(self) -> int:
        return max(30, min(int(self.timeout or 0) or 120, 75))

    def _get_video_poll_timeout(self) -> int:
        return max(10, min(int(self.timeout or 0) or 120, 45))

    def _resolve_generation_retry_budget(
        self, base_max_retries: int, error: Optional[Union[Exception, str]] = None
    ) -> int:
        effective_max_retries = max(1, int(base_max_retries or 1))
        if error is None:
            return effective_max_retries
        error_str = str(error)
        error_lower = error_str.lower()
        if (
            "recaptcha evaluation failed" in error_lower
            or "recaptcha 验证失败" in error_str
        ):
            return max(
                effective_max_retries,
                int(config.browser_captcha_generation_retries or 6),
            )
        return effective_max_retries

    def _resolve_runtime_impersonate(self, fallback: str = "firefox") -> str:
        resolved = self._resolve_impersonate_from_fingerprint(fallback=fallback)
        return resolved or fallback

    def _resolve_impersonate_from_fingerprint(self, fallback: str = "firefox") -> str:
        fingerprint = self.get_request_fingerprint()
        if not isinstance(fingerprint, dict):
            return fallback
        ua = str(fingerprint.get("user_agent") or "").strip().lower()
        if not ua:
            return fallback
        if "firefox/" in ua:
            return "firefox"
        if "chrome/" in ua or "chromium/" in ua:
            return "chrome"
        return fallback

    async def _acquire_video_launch_gate(
        self,
        token_id: Optional[int],
        token_video_concurrency: Optional[int],
    ) -> tuple[bool, int, int]:
        return True, 0, 0

    async def _release_video_launch_gate(self, token_id: Optional[int]):
        return

    def _detect_image_mime_type(self, image_bytes: bytes) -> str:
        return detect_image_mime_type(image_bytes, fallback="image/jpeg")

    async def _handle_retryable_generation_error(
        self,
        error: Exception,
        retry_attempt: int,
        max_retries: int,
        browser_id: Optional[Union[int, str]],
        project_id: str,
        log_prefix: str,
        defer_browser_error_notification: bool = False,
    ) -> bool:
        error_str = str(error)
        retry_reason = self._get_retry_reason(error_str)
        retry_delay = self._get_retry_delay_seconds(error_str, retry_attempt)
        effective_max_retries = self._resolve_generation_retry_budget(
            max_retries, error_str
        )
        is_terminal_attempt = retry_attempt >= effective_max_retries - 1
        should_notify_browser = (
            not defer_browser_error_notification
            or not retry_reason
            or is_terminal_attempt
            or "recaptcha" in error_str.lower()
        )
        if should_notify_browser:
            notify_reason = retry_reason or error_str[:120] or type(error).__name__
            await self._notify_browser_captcha_error(
                browser_id=getattr(error, "browser_ref", None) or browser_id,
                project_id=project_id,
                error_reason=notify_reason,
                error_message=error_str,
            )

        if not retry_reason or getattr(error, "submission_uncertain", False):
            return False
        if is_terminal_attempt:
            return False

        debug_logger.log_warning(
            f"{log_prefix}遇到{retry_reason}，将在 {retry_delay} 秒后重试 ({retry_attempt + 2}/{effective_max_retries})..."
        )
        await asyncio.sleep(retry_delay)
        return True

    def _get_retry_reason(self, error_str: str) -> Optional[str]:
        error_lower = str(error_str or "").lower()
        classified = classify_flow_error(error_str)
        if classified.category in (
            FlowErrorCategory.CONTENT_POLICY,
            FlowErrorCategory.TIER_PERMISSION,
            FlowErrorCategory.AUTH_EXPIRED,
            FlowErrorCategory.INVALID_INPUT,
            FlowErrorCategory.NOT_FOUND,
        ):
            return None
        if "browser captcha queue wait timeout" in error_lower:
            return None
        if "public_error_unusual_activity" in error_lower or "too much traffic" in error_lower:
            return None
        if "403" in error_lower:
            return "403错误"
        if "429" in error_lower or "too many requests" in error_lower:
            return "429限流"
        if self._is_retryable_network_error(error_str):
            return "网络/TLS错误"
        if "recaptcha evaluation failed" in error_lower:
            return "reCAPTCHA 验证失败"
        if "recaptcha" in error_lower:
            return "reCAPTCHA 错误"
        if classified.category == FlowErrorCategory.TRANSIENT_UPSTREAM or "public_error" in error_lower:
            return "500/内部错误"
        return None

    def _get_retry_delay_seconds(self, error_str: str, retry_attempt: int) -> float:
        _ = retry_attempt
        error_lower = str(error_str or "").lower()
        if "recaptcha" in error_lower:
            return round(jitter_seconds(2.0, low_ratio=0.75, high_ratio=1.35), 2)
        return round(jitter_seconds(1.0, low_ratio=0.7, high_ratio=1.4), 2)

    async def _notify_browser_captcha_error(
        self,
        browser_id: Optional[Union[int, str]] = None,
        project_id: Optional[str] = None,
        error_reason: Optional[str] = None,
        error_message: Optional[str] = None,
    ):
        _ = project_id
        if not browser_id:
            return
        try:
            from .browser_captcha import BrowserCaptchaService

            service = await BrowserCaptchaService.get_instance(self.db)
            await service.report_error(
                browser_id,
                error_reason=error_message or error_reason or "upstream_error",
            )
        except Exception:
            pass

    async def _notify_browser_captcha_request_finished(
        self,
        browser_id: Optional[Union[int, str]] = None,
        *,
        success: Optional[bool] = None,
    ):
        if browser_id:
            try:
                from .browser_captcha import BrowserCaptchaService

                service = await BrowserCaptchaService.get_instance(self.db)
                if success is None:
                    await service.report_request_finished(browser_id)
                else:
                    await service.report_request_finished(browser_id, success=success)
            except Exception:
                pass
        context = self._get_captcha_request_context()
        if context.get("browser_ref") == browser_id:
            self._set_captcha_request_context(None)

    async def _get_recaptcha_token(
        self,
        project_id: str,
        action: str = "IMAGE_GENERATION",
        token_id: Optional[int] = None,
        website_url: Optional[str] = None,
        method_override: Optional[str] = None,
    ) -> tuple[Optional[str], Optional[Union[int, str]]]:
        """Obtain a reCAPTCHA Enterprise token via the Camoufox + BiDi runtime."""
        _ = website_url
        _ = method_override
        previous = self._get_captcha_request_context()
        if (
            previous.get("browser_ref")
            and previous.get("owner") is asyncio.current_task()
        ):
            await self._notify_browser_captcha_request_finished(
                previous.get("browser_ref")
            )
        self._set_captcha_request_context(
            {
                "method": "camoufox",
                "token_id": token_id,
                "project_id": project_id,
                "owner": asyncio.current_task(),
            }
        )
        try:
            from .browser_captcha import (
                BrowserCaptchaQueueTimeout,
                BrowserCaptchaService,
            )

            service = await BrowserCaptchaService.get_instance(self.db)
            token, browser_id, fingerprint = await service.get_token(
                project_id, action, token_id=token_id
            )
            self._set_request_fingerprint(fingerprint if token else None)
            if token:
                self._set_captcha_request_context(
                    {
                        "method": "camoufox",
                        "browser_ref": browser_id,
                        "token_id": token_id,
                        "project_id": project_id,
                        "owner": asyncio.current_task(),
                    }
                )
            return token, browser_id
        except Exception as e:
            from .browser_captcha import BrowserCaptchaQueueTimeout

            err_msg = str(e)
            if (
                isinstance(e, BrowserCaptchaQueueTimeout)
                or "登录态已失效" in err_msg
                or "登录已失效" in err_msg
            ):
                raise
            debug_logger.log_error(f"[reCAPTCHA Camoufox] 错误: {str(e)}")
            self._set_request_fingerprint(None)
            return None, None


class FlowClient(CurrentFlowClientMixin, _FlowClientBase):
    """Flow client backed exclusively by the current flow.google.com RPC layer."""

    @staticmethod
    def _get_runtime_config():
        return config
