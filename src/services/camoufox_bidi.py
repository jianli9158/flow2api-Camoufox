"""Camoufox + WebDriver BiDi native runtime (ported from AIStudio2API camoufoxnative).

Provides:
- Zero-Playwright / Zero-nodriver native Camoufox browser process management
- Pure asyncio WebDriver BiDi client over localhost WebSocket
- Persistent per-account/per-slot Windows Firefox fingerprint (`camoufox-fingerprint.json` + `CAMOU_CONFIG_*`)
- 1 FPS idle frame rate (`gfx.display.frame-rate-divisor = 60`) + `renderLayers = False` CPU/GPU optimization
- Trusted BiDi `input.performActions` UI gesture + in-page `XMLHttpRequest` / `ProtectedSend` (`bidiChannel` 64KB chunked streaming)
- In-page `batchexecute` & `StreamChat` RPC execution sharing 100% browser TLS/H2/Cookie/WIZ_global_data context
- Local unauthenticated SOCKS5 proxy relay forwarding to authenticated upstream HTTP/HTTPS/SOCKS5 proxies
"""

from __future__ import annotations

import asyncio
import base64
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, AsyncIterator, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union
from ..core.prompt_tools import jitter_sleep
from urllib.parse import parse_qsl, quote, urlencode, urlparse
import uuid
import zipfile

from ..core.config import config
from ..core.logger import debug_logger
from .browser_cookie_utils import (
    build_browser_cookie_targets,
    build_cookie_signature,
    is_partitioned_cookie,
)
from .recaptcha_runtime import (
    TRUSTED_RECAPTCHA_HOOK_SOURCE,
    build_recaptcha_execute_script,
)

from .bidi_transport import (
    BIDI_ENDPOINT_PATTERN,
    BiDiChannel,
    BiDiClient,
    LocalSocks5Relay,
    _from_bidi_remote_value,
    _to_bidi_local_value,
    wait_for_bidi_endpoint,
)
from .camoufox_fingerprint import (
    CAMOUFOX_FIREFOX_MAJOR,
    CAMOUFOX_RELEASE_VERSION,
    FIREFOX_VERSION_PATTERN,
    _firefox_pref_literal,
    _start_stdout_drain,
    add_windows_substitute_names,
    build_camoufox_env,
    build_camoufox_fingerprint_config,
    ensure_camoufox_executable,
    find_camoufox_executable,
    load_or_create_account_camoufox_config,
    windows_fontconfig,
    write_camoufox_user_js,
)
from .camoufox_login import (
    FLOW_LOGIN_DETECT_SCRIPT,
    WEBDRIVER_KEYS,
    CamoufoxLiveView,
    CamoufoxLoginManager,
    CamoufoxLoginSession,
    has_display,
)
from .camoufox_profile import (
    PROFILE_KERNEL_LOCK_FILENAME,
    PROFILE_STARTUP_DIR_NAME,
    ProfileLease,
    ProfileLeaseBusyError,
    STARTUP_CACHE_ENTRIES,
    _clear_stale_firefox_profile_locks,
    _copy_startup_cache_entries,
    acquire_profile_lease,
    acquire_profile_login_lease,
    clear_site_cookies_in_profile,
    get_active_profile_lease_holder,
    get_profile_login_holder,
    is_token_in_login_session,
    list_browser_profile_dirs,
    normalize_browser_profile_dir,
    persist_account_login_artifacts,
    prune_camoufox_profile_bloat,
    release_profile_login_lease,
    resolve_account_profile_dir,
    restore_startup_cache,
    save_startup_cache,
)

FLOW_ORIGIN = "https://flow.google.com"
FLOW_PROJECTS_URL = "https://flow.google.com/projects"
FLOW_WEBSITE_KEY = "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV"
BROWSER_WINDOW_URL = "chrome://browser/content/browser.xhtml"

IDLE_FRAME_DIVISOR = 60
INTERACTIVE_FRAME_RATE = 60
PROTECTED_GRANT_BATCH = 64

PROTECTED_SEND_KEYS = [
    {"type": "keyDown", "value": "?"},
    {"type": "keyUp", "value": "?"},
]
# ============================================================================
# 7. CamoufoxWorker: Per-Slot Persistent Browser & BiDi Session
# ============================================================================


class CamoufoxWorker:
    """Single Camoufox browser instance controlled via WebDriver BiDi (AIStudio2API style).

    Compatible with `TokenBrowser` interface expected by `BrowserCaptchaService`.
    """

    def __init__(self, token_id: int, user_data_dir: str, db: Any = None):
        self.token_id = token_id
        self.user_data_dir = Path(user_data_dir)
        self.user_data_dir.mkdir(parents=True, exist_ok=True)
        self.db = db

        self._semaphore = asyncio.Semaphore(1)
        self._shared_browser_lock = asyncio.Lock()
        self._frame_lock = asyncio.Lock()
        self._frame_boosts = 0
        self._frame_divisor_set_to_1: bool = False

        self._process: Optional[asyncio.subprocess.Process] = None
        self._stdout_drain_task: Optional[asyncio.Task] = None
        self._bidi: Optional[BiDiClient] = None
        self._context_id: Optional[str] = None
        self._captcha_context_id: Optional[str] = None
        self._captcha_context_dirty: bool = False
        self._relay: Optional[LocalSocks5Relay] = None
        self._profile_dir: Optional[Path] = None
        self._account_dir: Optional[Path] = None
        self._shared_browser_pid: Optional[int] = None
        self._shared_bound_token_id: Optional[int] = None
        self._shared_bound_cookie_signature: Optional[str] = None
        self._browser_proxy_active: bool = False

        self._runtime_generation: int = 0
        self._solve_inflight: int = 0
        self._solve_count: int = 0
        self._error_count: int = 0
        self._consecutive_browser_failures: int = 0
        self._shared_launch_count: int = 0
        self._shared_reuse_count: int = 0
        self._last_idle_since: float = time.monotonic()

        self._last_fingerprint: Optional[Dict[str, Any]] = None
        self._last_harvest_session_id: Optional[str] = None
        self._harvest_streamchat_post: Optional[str] = None
        self._headless: bool = True
        self._is_persistent_profile: bool = False
        self._warmed_projects: set[str] = set()
        self._last_stream_chat_at: float = 0.0
        self._cached_wiz: Dict[str, str] = {}
        self._consecutive_risk_failures: int = 0
        self._profile_lease: Optional[ProfileLease] = None
    def is_busy(self) -> bool:
        return self._solve_inflight > 0

    def note_idle(self) -> None:
        if self._solve_inflight <= 0:
            self._last_idle_since = time.monotonic()

    def idle_seconds(self) -> float:
        if self.is_busy():
            return 0.0
        return max(0.0, time.monotonic() - self._last_idle_since)

    def has_shared_browser(self) -> bool:
        return bool(
            self._process is not None
            and self._process.returncode is None
            and self._bidi is not None
            and not self._bidi._closed
            and self._context_id
        )

    def get_last_fingerprint(self) -> Optional[Dict[str, Any]]:
        if not self._last_fingerprint:
            return None
        return dict(self._last_fingerprint)

    def get_last_harvest_session_id(self) -> Optional[str]:
        return self._last_harvest_session_id

    def note_recaptcha_rejected(self) -> None:
        """Mark resident captcha tab & warmup dirty so next solve rebuilds a clean Captcha Tab without disturbing the API Tab."""
        self._consecutive_risk_failures += 1
        self._captcha_context_dirty = True
        self._warmed_projects.clear()

    async def _clear_recaptcha_cookies(self) -> None:
        """Clear poisoned _GRECAPTCHA cookies inside the browser without touching Google login cookies."""
        if not self._bidi or self._bidi._closed:
            return
        try:
            await self._bidi.command(
                "storage.deleteCookies",
                {"filter": {"name": "_GRECAPTCHA"}},
                timeout=5.0,
            )
        except Exception:
            pass

    async def _ensure_captcha_context_id(
        self,
        *,
        force_recreate: bool = False,
        fallback_context_id: Optional[str] = None,
    ) -> str:
        """Return an isolated Captcha Tab context distinct from the resident API Tab (`self._context_id`)."""
        if not self._bidi or getattr(self._bidi, "_closed", False):
            fallback = fallback_context_id or self._context_id
            if fallback:
                return fallback
            raise RuntimeError("Camoufox BiDi 未连接")
        if force_recreate:
            await self._clear_recaptcha_cookies()
            self._warmed_projects.clear()
        if force_recreate and self._captcha_context_id:
            old_ctx = self._captcha_context_id
            self._captcha_context_id = None
            try:
                await self._bidi.evaluate(
                    old_ctx,
                    '(() => { try { localStorage.removeItem("_grecaptcha"); } catch (e) {} return true; })()',
                    await_promise=False,
                    timeout=3.0,
                )
            except Exception:
                pass
            if old_ctx != self._context_id:
                try:
                    await self._bidi.command(
                        "browsingContext.close",
                        {"context": old_ctx},
                        timeout=5.0,
                    )
                except Exception:
                    pass
        if self._captcha_context_id:
            try:
                alive = await self._bidi.evaluate(
                    self._captcha_context_id,
                    "1",
                    await_promise=False,
                    timeout=3.0,
                )
                if alive == 1:
                    return self._captcha_context_id
            except Exception:
                self._captcha_context_id = None
        try:
            created = await self._bidi.command(
                "browsingContext.create",
                {"type": "tab"},
                timeout=15.0,
            )
            new_ctx = str((created or {}).get("context") or "").strip() if isinstance(created, dict) else ""
            if new_ctx:
                self._captcha_context_id = new_ctx
                debug_logger.log_info(
                    f"[Camoufox] Slot-{self.token_id} 已创建独立打码 Tab (captcha_ctx={new_ctx[:12]}, api_ctx={str(self._context_id or '')[:12]})"
                )
                return new_ctx
        except Exception as exc:
            debug_logger.log_warning(
                f"[Camoufox] Slot-{self.token_id} 创建独立打码 Tab 失败，回退默认 context: {exc}"
            )
        fallback = fallback_context_id or self._context_id
        if not fallback:
            raise RuntimeError("Camoufox BiDi context 不可用")
        return fallback
    @staticmethod
    def _normalize_token_key(token_id: Optional[int]) -> Optional[int]:
        try:
            normalized = int(token_id or 0)
        except Exception:
            normalized = 0
        return normalized if normalized > 0 else None

    async def _resolve_proxy_runtime_config(
        self, token_proxy_url: Optional[str] = None
    ) -> Tuple[Optional[str], str]:
        raw_proxy_url = str(token_proxy_url or "").strip()
        if raw_proxy_url:
            if "://" not in raw_proxy_url:
                raw_proxy_url = f"http://{raw_proxy_url}"
            return raw_proxy_url, "token_captcha_proxy"

        if self.db and hasattr(self.db, "get_captcha_config"):
            try:
                captcha_cfg = await self.db.get_captcha_config()
                if getattr(captcha_cfg, "browser_proxy_enabled", False):
                    cfg_url = str(
                        getattr(captcha_cfg, "browser_proxy_url", "") or ""
                    ).strip()
                    if cfg_url:
                        if "://" not in cfg_url:
                            cfg_url = f"http://{cfg_url}"
                        return cfg_url, "global_browser_proxy"
            except Exception:
                pass
        return None, "none"

    async def _chrome_evaluate(self, expression: str) -> Any:
        """Execute privileged chrome JS inside `browser.xhtml` (for 1fps divisor & renderLayers)."""
        if not self._bidi:
            return None
        tree = await self._bidi.command(
            "browsingContext.getTree", {"maxDepth": 0, "moz:scope": "chrome"}
        )
        for item in tree.get("contexts") or []:
            if isinstance(item, dict) and item.get("url") == BROWSER_WINDOW_URL:
                win_ctx = str(item.get("context") or "")
                if win_ctx:
                    return await self._bidi.evaluate(win_ctx, expression)
        return None

    @asynccontextmanager
    async def _boost_frames(self):
        """Ensure Camoufox frame rate is at 60fps during interactive operations without polluting prefs.js with 1fps."""
        if not self._headless or not self._bidi:
            yield
            return
        async with self._frame_lock:
            if self._frame_boosts == 0 and not self._frame_divisor_set_to_1:
                try:
                    await self._chrome_evaluate(
                        "Services.prefs.setIntPref('gfx.display.frame-rate-divisor', 1)"
                    )
                    self._frame_divisor_set_to_1 = True
                except Exception:
                    pass
            self._frame_boosts += 1
        try:
            yield
        finally:
            async with self._frame_lock:
                self._frame_boosts = max(0, self._frame_boosts - 1)
    def _get_user_data_root(self) -> Path:
        if re.match(r"^browser_\d+$", self.user_data_dir.name):
            return self.user_data_dir.parent
        if self.user_data_dir.name.startswith("token_"):
            return self.user_data_dir.parent
        return self.user_data_dir

    def _resolve_account_dir(
        self,
        token_id: Optional[int],
        *,
        profile_dir_name: Optional[str] = None,
    ) -> Path:
        """Resolve the fixed per-account directory under `browser_data_rt/`."""
        if self.user_data_dir.name.startswith("token_") and not profile_dir_name:
            return self.user_data_dir
        root_dir = self._get_user_data_root()
        if token_id or (profile_dir_name and str(profile_dir_name).strip()):
            account_dir, _ = resolve_account_profile_dir(
                root_dir,
                token_id=token_id,
                profile_dir_name=profile_dir_name,
            )
            legacy_dir = (
                self.user_data_dir / f"token_{int(token_id)}"
                if token_id
                else None
            )
            if legacy_dir is not None and legacy_dir != account_dir and legacy_dir.is_dir():
                account_dir.mkdir(parents=True, exist_ok=True)
                for sub_name in ("camoufox-cache", "storage-state.json"):
                    src_item = legacy_dir / sub_name
                    dst_item = account_dir / sub_name
                    if src_item.exists() and not dst_item.exists():
                        try:
                            if src_item.is_dir():
                                shutil.copytree(src_item, dst_item, dirs_exist_ok=True)
                            else:
                                shutil.copy2(src_item, dst_item)
                        except Exception:
                            pass
            return account_dir
        return self.user_data_dir / "shared"

    async def _resolve_account_dir_async(
        self,
        token_id: Optional[int],
        profile_dir_name: Optional[str] = None,
    ) -> Path:
        return await asyncio.to_thread(
            self._resolve_account_dir, token_id, profile_dir_name=profile_dir_name
        )

    async def _get_token_profile_dir_name(self, token_id: Optional[int]) -> Optional[str]:
        token_key = self._normalize_token_key(token_id)
        if token_key is None or not self.db or not hasattr(self.db, "get_token"):
            return None
        try:
            token = await self.db.get_token(token_key)
        except Exception:
            return None
        return getattr(token, "browser_profile_dir", None) if token else None

    async def _launch_camoufox(
        self, token_proxy_url: Optional[str] = None, token_id: Optional[int] = None
    ) -> None:
        exe_path = await ensure_camoufox_executable()
        raw_proxy_url, _ = await self._resolve_proxy_runtime_config(token_proxy_url)
        self._browser_proxy_active = bool(raw_proxy_url)

        profile_dir_name = await self._get_token_profile_dir_name(token_id)
        account_dir = await self._resolve_account_dir_async(
            token_id, profile_dir_name=profile_dir_name
        )
        login_holder = get_profile_login_holder(account_dir)
        if login_holder:
            raise RuntimeError(
                f"账号浏览器资料目录 {account_dir.name} 正在进行登录会话 ({login_holder})，请先完成或关闭登录"
            )
        account_dir.mkdir(parents=True, exist_ok=True)
        if token_id:
            if self._profile_lease is not None:
                self._profile_lease.release()
                self._profile_lease = None
            self._profile_lease = acquire_profile_lease(
                account_dir,
                holder=f"camoufox_slot_{self.token_id}_token_{token_id}",
                wait_seconds=2.0,
            )
        fp_config = load_or_create_account_camoufox_config(
            account_dir, CAMOUFOX_FIREFOX_MAJOR, "en-US", "America/New_York"
        )
        try:
            relay_addr: Optional[Tuple[str, int]] = None
            if raw_proxy_url:
                parsed_proxy = urlparse(raw_proxy_url)
                if parsed_proxy.username or parsed_proxy.password or "socks" in (
                    parsed_proxy.scheme or ""
                ).lower():
                    self._relay = LocalSocks5Relay(raw_proxy_url)
                    relay_addr = await self._relay.start()

            if token_id:
                self._is_persistent_profile = True
                profile_dir = account_dir / "firefox-profile"
                profile_dir.mkdir(parents=True, exist_ok=True)
                _clear_stale_firefox_profile_locks(profile_dir)
            else:
                self._is_persistent_profile = False
                profile_dir = Path(
                    tempfile.mkdtemp(prefix=f"flow2api-camoufox-{self.token_id}-")
                )
            self._profile_dir = profile_dir
            self._account_dir = account_dir
            self._warmed_projects.clear()
            self._frame_divisor_set_to_1 = False
            cache_dir = account_dir / "camoufox-cache"
            # Headless by default (like AIStudio2API Worker), unless explicitly disabled
            self._headless = bool(
                os.environ.get("CAMOUFOX_HEADLESS", "1").strip() not in ("0", "false", "False")
            )
            write_camoufox_user_js(
                profile_dir,
                fp_config,
                proxy_url=raw_proxy_url,
                relay_addr=relay_addr,
                headless=self._headless,
                live_view=False,
                cache_dir=cache_dir,
                executable_path=exe_path,
            )

            args = [exe_path]
            if self._headless:
                args.append("--headless")
            args.extend(["--remote-debugging-port=0", "-wait-for-browser", "-profile", str(profile_dir)])

            env = build_camoufox_env(
                fp_config,
                headless=self._headless,
                live_view=False,
                profile_dir=profile_dir,
                executable_path=exe_path,
            )
            self._process = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
            )
            self._shared_browser_pid = self._process.pid

            # Wait for `WebDriver BiDi listening on ws://...`
            endpoint = await self._wait_for_bidi_endpoint(
                self._process, timeout=45.0
            )
            self._stdout_drain_task = self._start_stdout_drain(self._process)
            self._bidi = await BiDiClient.connect(endpoint, timeout=30.0)
            await self._bidi.command("session.new", {"capabilities": {}})
            try:
                await self._bidi.command(
                    "session.subscribe", {"events": ["script.message"]}
                )
            except Exception:
                pass

            tree = await self._bidi.command(
                "browsingContext.getTree", {"maxDepth": 0}
            )
            contexts = tree.get("contexts") or []
            if not contexts or not isinstance(contexts[0], dict):
                raise RuntimeError("Camoufox BiDi 未返回初始标签页")
            self._context_id = str(contexts[0].get("context") or "")
            if not self._context_id:
                raise RuntimeError("Camoufox BiDi 初始 context ID 为空")

            ua = str(
                fp_config.get("navigator.userAgent")
                or fp_config.get("headers.User-Agent")
                or ""
            )
            accept_lang = str(
                fp_config.get("headers.Accept-Language") or "en-US,en;q=0.9"
            )
            self._last_fingerprint = {
                "user_agent": ua,
                "accept_language": accept_lang,
                "proxy_url": raw_proxy_url,
                "platform": str(fp_config.get("navigator.platform") or "Win32"),
                "language": str(fp_config.get("navigator.language") or "en-US"),
                "languages": list(
                    fp_config.get("navigator.languages") or ["en-US", "en"]
                ),
                "timezone": str(fp_config.get("timezone") or "America/New_York"),
            }
            self._shared_proxy_url = raw_proxy_url
            self._shared_launch_count += 1
            self._shared_reuse_count = 0
            debug_logger.log_info(
                f"[Camoufox] Slot-{self.token_id} 进程已启动 (PID={self._shared_browser_pid}, BiDi={endpoint}, UA={ua[:80]})"
            )
        except BaseException:
            drain_task = self._stdout_drain_task
            proc = self._process
            bidi = self._bidi
            relay = self._relay
            profile_dir_cleanup = self._profile_dir
            self._stdout_drain_task = None
            self._process = None
            self._bidi = None
            self._context_id = None
            self._captcha_context_id = None
            self._captcha_context_dirty = False
            self._relay = None
            self._profile_dir = None
            self._shared_browser_pid = None
            if drain_task is not None and not drain_task.done():
                drain_task.cancel()
            if bidi is not None:
                try:
                    await bidi.close()
                except Exception:
                    pass
            if proc is not None and proc.returncode is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(proc.wait(), timeout=3.0)
                except Exception:
                    try:
                        proc.kill()
                        await asyncio.wait_for(proc.wait(), timeout=2.0)
                    except Exception:
                        pass
            if relay is not None:
                try:
                    await relay.close()
                except Exception:
                    pass
            if (
                profile_dir_cleanup is not None
                and profile_dir_cleanup.exists()
                and not self._is_persistent_profile
            ):
                shutil.rmtree(profile_dir_cleanup, ignore_errors=True)
            if self._profile_lease is not None:
                self._profile_lease.release()
                self._profile_lease = None
            raise

    @staticmethod
    def _start_stdout_drain(
        proc: asyncio.subprocess.Process,
    ) -> Optional[asyncio.Task]:
        return _start_stdout_drain(proc)

    @staticmethod
    async def _wait_for_bidi_endpoint(
        proc: asyncio.subprocess.Process, timeout: float = 45.0
    ) -> str:
        return await wait_for_bidi_endpoint(proc, timeout=timeout)

    async def _install_cookies_via_bidi(self, cookies: List[Dict[str, Any]]) -> None:
        if not self._bidi or not cookies:
            return
        for cookie in cookies:
            name = str(cookie.get("name") or "").strip()
            value = str(cookie.get("value") or "")
            if not name or name == "_GRECAPTCHA":
                continue
            domain = str(cookie.get("domain") or "").strip()
            url = str(cookie.get("url") or "").strip()
            if not domain and url:
                domain = urlparse(url).hostname or "flow.google.com"
            if not domain:
                domain = "flow.google.com"
            host_only = bool(cookie.get("hostOnly", False)) or (
                bool(url) and not str(cookie.get("domain") or "").startswith(".")
            )
            if host_only or name.startswith("__Host-"):
                domain = domain.lstrip(".")

            path = str(cookie.get("path") or "/")
            if name.startswith("__Host-"):
                path = "/"
            secure = bool(
                cookie.get("secure", True)
                or name.startswith(("__Secure-", "__Host-"))
            )
            http_only = bool(cookie.get("httpOnly", False))
            same_site_raw = str(cookie.get("sameSite") or "").strip().lower()
            same_site = (
                same_site_raw
                if same_site_raw in ("strict", "lax", "none")
                else ("none" if secure else "lax")
            )

            bidi_cookie: Dict[str, Any] = {
                "name": name,
                "value": {"type": "string", "value": value},
                "domain": domain,
                "path": path,
                "secure": secure,
                "httpOnly": http_only,
                "sameSite": same_site,
            }
            expires = cookie.get("expires")
            if isinstance(expires, (int, float)) and expires > 0:
                bidi_cookie["expiry"] = int(expires)

            params: Dict[str, Any] = {"cookie": bidi_cookie}
            partition_key = str(cookie.get("partitionKey") or "").strip()
            if partition_key:
                params["partition"] = {
                    "type": "storageKey",
                    "sourceOrigin": partition_key,
                }
            try:
                await self._bidi.command("storage.setCookie", params, timeout=10.0)
            except Exception as exc:
                if "partition" in params:
                    params.pop("partition", None)
                    try:
                        await self._bidi.command(
                            "storage.setCookie", params, timeout=10.0
                        )
                    except Exception:
                        debug_logger.log_warning(
                            f"[Camoufox] 设置 Cookie {name} 失败: {exc}"
                        )
                else:
                    debug_logger.log_warning(
                        f"[Camoufox] 设置 Cookie {name} 失败: {exc}"
                    )

    async def _export_live_google_cookies(self) -> List[Dict[str, Any]]:
        """Export current Google/Flow cookies from Camoufox via `storage.getCookies`."""
        if not self._bidi or self._bidi._closed:
            return []
        try:
            cookie_res = await asyncio.wait_for(
                self._bidi.command("storage.getCookies", {}), timeout=6.0
            )
        except Exception:
            return []
        raw_cookies = cookie_res.get("cookies") or []
        exported: List[Dict[str, Any]] = []
        for item in raw_cookies:
            if not isinstance(item, dict):
                continue
            domain = str(item.get("domain") or "").strip()
            if not (
                "google." in domain
                or domain.endswith("youtube.com")
            ):
                continue
            val_obj = item.get("value")
            val_str = (
                str(val_obj.get("value") or "")
                if isinstance(val_obj, dict)
                else str(val_obj or "")
            )
            name = str(item.get("name") or "").strip()
            if not name or name == "_GRECAPTCHA":
                continue
            same_site_raw = str(item.get("sameSite") or "lax").strip().lower()
            same_site = (
                "None"
                if same_site_raw == "none"
                else ("Strict" if same_site_raw == "strict" else "Lax")
            )
            entry: Dict[str, Any] = {
                "name": name,
                "value": val_str,
                "domain": domain,
                "hostOnly": not domain.startswith("."),
                "path": str(item.get("path") or "/"),
                "secure": bool(item.get("secure", True)),
                "httpOnly": bool(item.get("httpOnly", False)),
                "sameSite": same_site,
            }
            expiry = item.get("expiry")
            if isinstance(expiry, (int, float)) and expiry > 0:
                entry["expires"] = int(expiry)
            exported.append(entry)
        return exported

    async def _sync_live_cookies_to_db(
        self, token_id: Optional[int] = None, *, update_db: bool = True
    ) -> Optional[str]:
        """Persist rotated Google/Flow cookies (`__Secure-1PSIDTS`, `SIDCC`, `OSID`, etc.) back to `storage-state.json` and SQLite."""
        bound_id = self._normalize_token_key(token_id) or self._shared_bound_token_id
        exported = await self._export_live_google_cookies()
        if not exported:
            return None
        cookie_names = {c.get("name") for c in exported}
        if not (
            cookie_names.intersection({"SID", "__Secure-1PSID", "__Secure-3PSID"})
            and cookie_names.intersection({"OSID", "__Secure-OSID"})
        ):
            return None
        serialized = json.dumps(exported, ensure_ascii=False, separators=(",", ":"))
        new_sig = (
            build_cookie_signature(serialized, default_url=FLOW_ORIGIN)
            or hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        )

        # Update storage-state.json in active bound account_dir
        dirs_to_update: List[Path] = []
        account_dir = getattr(self, "_account_dir", None)
        if isinstance(account_dir, Path) and account_dir.exists():
            dirs_to_update.append(account_dir)
        elif bound_id is not None:
            fallback_dir = await self._resolve_account_dir_async(bound_id)
            if fallback_dir.exists():
                dirs_to_update.append(fallback_dir)

        primary_ss_exists = bool(
            dirs_to_update and (dirs_to_update[0] / "storage-state.json").is_file()
        )
        if (
            new_sig
            and new_sig == self._shared_bound_cookie_signature
            and primary_ss_exists
        ):
            return serialized

        for d in dirs_to_update:
            ss_path = d / "storage-state.json"
            try:
                state_obj: Dict[str, Any] = {"cookies": exported, "origins": []}
                if ss_path.is_file():
                    old_obj = json.loads(ss_path.read_text(encoding="utf-8"))
                    if isinstance(old_obj, dict) and isinstance(old_obj.get("origins"), list):
                        state_obj["origins"] = old_obj["origins"]
                ss_path.write_text(
                    json.dumps(state_obj, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            except Exception:
                pass

        if update_db and bound_id is not None and self.db is not None:
            try:
                now_utc = datetime.now(timezone.utc)
                interval_minutes = 30
                if hasattr(self.db, "get_token"):
                    tok_snapshot = await self.db.get_token(bound_id)
                    if tok_snapshot and getattr(tok_snapshot, "refresh_interval_minutes", None):
                        try:
                            interval_minutes = max(1, int(tok_snapshot.refresh_interval_minutes))
                        except Exception:
                            interval_minutes = 30
                new_st = "frontend-" + hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:32]
                await self.db.update_token(
                    bound_id,
                    google_cookies=serialized,
                    st=new_st,
                    at_expires=now_utc + timedelta(minutes=max(55, interval_minutes + 15)),
                    last_st_refresh_at=now_utc,
                    last_st_refresh_result="success",
                )
            except Exception as exc:
                debug_logger.log_warning(
                    f"[Camoufox] 同步 Token-{bound_id} 轮换 Cookie 到数据库失败: {exc}"
                )
        self._shared_bound_cookie_signature = new_sig
        return serialized

    async def _load_token_session_binding(
        self, token_id: Optional[int]
    ) -> Tuple[Optional[int], Optional[str], Optional[str], Optional[str]]:
        token_key = self._normalize_token_key(token_id)
        if token_key is None:
            return None, None, None, None
        token = None
        if self.db and hasattr(self.db, "get_token"):
            try:
                token = await self.db.get_token(token_key)
            except Exception as exc:
                debug_logger.log_warning(
                    f"[Camoufox] Slot-{self.token_id} 读取 token({token_key}) 失败: {exc}"
                )
        session_token = str(getattr(token, "st", "") or "").strip() if token else ""
        google_cookies = (
            str(getattr(token, "google_cookies", "") or "").strip() if token else ""
        )
        profile_dir_name = (
            str(getattr(token, "browser_profile_dir", "") or "").strip() if token else ""
        )
        if not google_cookies and (
            session_token.startswith("{") or "=" in session_token
        ):
            google_cookies = session_token
        if not (google_cookies or session_token):
            ss_path = (
                await self._resolve_account_dir_async(
                    token_key, profile_dir_name=profile_dir_name
                )
                / "storage-state.json"
            )
            if ss_path.is_file():
                try:
                    google_cookies = ss_path.read_text(encoding="utf-8").strip()
                except Exception:
                    pass
        if google_cookies:
            cookie_signature = (
                build_cookie_signature(google_cookies)
                or hashlib.sha256(google_cookies.encode("utf-8")).hexdigest()
            )
        elif session_token:
            cookie_signature = (
                build_cookie_signature(
                    f"__Secure-next-auth.session-token={session_token}"
                )
                or hashlib.sha256(session_token.encode("utf-8")).hexdigest()
            )
        else:
            cookie_signature = None
        return token_key, session_token, google_cookies, cookie_signature

    async def _ensure_shared_token_binding(
        self, token_id: Optional[int]
    ) -> bool:
        (
            token_key,
            session_token,
            google_cookies,
            cookie_signature,
        ) = await self._load_token_session_binding(token_id)

        if token_key is None:
            self._shared_bound_token_id = None
            self._shared_bound_cookie_signature = None
            return True

        if not (google_cookies or session_token) or not cookie_signature:
            return False

        if (
            self._shared_bound_token_id == token_key
            and self._shared_bound_cookie_signature == cookie_signature
        ):
            return True

        prev_signature = self._shared_bound_cookie_signature
        signature_refreshed = bool(
            prev_signature is not None and prev_signature != cookie_signature
        )
        if google_cookies:
            browser_cookies = build_browser_cookie_targets(
                google_cookies,
                default_url="https://flow.google.com/",
                fallback_urls=[
                    "https://flow.google.com/",
                    "https://www.google.com/",
                ],
            )
        else:
            browser_cookies = [
                {
                    "name": "__Secure-next-auth.session-token",
                    "value": session_token,
                    "url": target_url,
                    "secure": True,
                    "httpOnly": True,
                    "sameSite": "None",
                }
                for target_url in (
                    "https://flow.google.com/",
                    "https://www.google.com/",
                    "https://www.recaptcha.net/",
                )
            ]

        if not browser_cookies:
            return False

        try:
            if self._bidi:
                if not self._is_persistent_profile:
                    try:
                        await self._bidi.command("storage.deleteCookies", {})
                    except Exception:
                        pass
                account_dir = getattr(self, "_account_dir", None)
                has_native_session_cookies = False
                if self._is_persistent_profile and not signature_refreshed:
                    try:
                        existing_live = await self._export_live_google_cookies()
                        existing_names = {c.get("name") for c in existing_live}
                        if (
                            existing_names.intersection({"SID", "__Secure-1PSID", "__Secure-3PSID"})
                            and existing_names.intersection({"OSID", "__Secure-OSID"})
                        ):
                            has_native_session_cookies = True
                    except Exception:
                        pass
                if signature_refreshed:
                    self._cached_wiz = {"bl": "", "fSid": "", "at": ""}
                    if browser_cookies:
                        try:
                            await self._bidi.command("storage.deleteCookies", {})
                        except Exception:
                            pass
                        await self._install_cookies_via_bidi(browser_cookies)
                elif not has_native_session_cookies:
                    ss_cookies_loaded = False
                    if isinstance(account_dir, Path):
                        ss_path = account_dir / "storage-state.json"
                        if ss_path.is_file():
                            try:
                                ss_obj = json.loads(ss_path.read_text(encoding="utf-8"))
                                raw_ss_cookies = ss_obj.get("cookies") if isinstance(ss_obj, dict) else None
                                if isinstance(raw_ss_cookies, list) and raw_ss_cookies:
                                    await self._install_cookies_via_bidi(raw_ss_cookies)
                                    ss_cookies_loaded = True
                            except Exception:
                                pass
                    if not ss_cookies_loaded and browser_cookies:
                        await self._install_cookies_via_bidi(browser_cookies)
            self._shared_bound_token_id = token_key
            self._shared_bound_cookie_signature = cookie_signature
            return True
        except Exception as exc:
            debug_logger.log_warning(
                f"[Camoufox] Slot-{self.token_id} 绑定账号 Cookie 失败: {exc}"
            )
            return False

    async def _get_or_create_shared_browser(
        self,
        token_proxy_url: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Tuple[Any, Any, Any]:
        expected_proxy_url, _ = await self._resolve_proxy_runtime_config(
            token_proxy_url=token_proxy_url
        )
        expected_token_key = self._normalize_token_key(token_id)

        async with self._shared_browser_lock:
            alive = self.has_shared_browser()
            if alive and self._shared_proxy_url != expected_proxy_url:
                await self._recycle_browser_locked(
                    reason="proxy_changed", rotate_profile=False
                )
                alive = False

            if alive:
                current_bound = self._shared_bound_token_id
                if expected_token_key != current_bound and not (
                    expected_token_key is None and current_bound is None
                ):
                    await self._recycle_browser_locked(
                        reason=f"token_binding_changed:{current_bound}->{expected_token_key}",
                        rotate_profile=False,
                    )
                    alive = False

            if alive:
                if not await self._ensure_shared_token_binding(token_id):
                    await self._recycle_browser_locked(
                        reason="token_binding_failed", rotate_profile=False
                    )
                    alive = False

            if alive:
                self._shared_reuse_count += 1
                return self._process, self._bidi, self._context_id

            await self._launch_camoufox(
                token_proxy_url=token_proxy_url, token_id=token_id
            )
            if not await self._ensure_shared_token_binding(token_id):
                await self._recycle_browser_locked(
                    reason="initial_token_binding_failed", rotate_profile=False
                )
                raise RuntimeError("Camoufox 账号登录态绑定失败")

            self.note_idle()
            return self._process, self._bidi, self._context_id

    async def _recycle_browser_locked(
        self, reason: str = "unknown", rotate_profile: bool = True
    ) -> None:
        _ = rotate_profile
        proc = self._process
        drain_task = self._stdout_drain_task
        bidi = self._bidi
        relay = self._relay
        profile_dir = self._profile_dir
        account_dir = getattr(self, "_account_dir", None)
        bound_token_id = self._shared_bound_token_id

        reason_lower = str(reason or "").lower()
        if (
            bidi is not None
            and not bidi._closed
            and bound_token_id is not None
            and self._consecutive_browser_failures == 0
            and "relogin" not in reason_lower
            and "failed" not in reason_lower
            and "error" not in reason_lower
        ):
            try:
                await self._sync_live_cookies_to_db(bound_token_id)
            except Exception:
                pass
        self._process = None
        self._stdout_drain_task = None
        self._bidi = None
        self._context_id = None
        self._captcha_context_id = None
        self._captcha_context_dirty = False
        self._warmed_projects.clear()
        self._cached_wiz = {}
        self._relay = None
        self._profile_dir = None
        self._account_dir = None
        self._shared_browser_pid = None
        self._shared_proxy_url = None
        self._shared_bound_token_id = None
        self._shared_bound_cookie_signature = None
        self._frame_divisor_set_to_1 = False
        self._runtime_generation += 1
        self._consecutive_browser_failures = 0
        self._shared_reuse_count = 0

        if drain_task is not None and not drain_task.done():
            drain_task.cancel()

        if bidi is not None:
            try:
                await asyncio.wait_for(
                    bidi.command("browser.close", {}), timeout=4.0
                )
            except Exception:
                pass
            close_fn = getattr(bidi, "close", None)
            if callable(close_fn):
                await close_fn()

        if proc is not None:
            if proc.returncode is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(proc.wait(), timeout=4.0)
                except Exception:
                    try:
                        proc.kill()
                        await asyncio.wait_for(proc.wait(), timeout=2.0)
                    except Exception:
                        pass

        if relay is not None:
            await relay.close()
        if profile_dir is not None and profile_dir.exists():
            if account_dir is not None and account_dir.exists():
                await asyncio.to_thread(save_startup_cache, profile_dir, account_dir)
            if self._is_persistent_profile:
                _clear_stale_firefox_profile_locks(profile_dir)
            else:
                shutil.rmtree(profile_dir, ignore_errors=True)
        if self._profile_lease is not None:
            self._profile_lease.release()
            self._profile_lease = None
        if proc is not None or bidi is not None:
            debug_logger.log_info(
                f"[Camoufox] Slot-{self.token_id} 实例已回收 (reason={reason})"
            )

    async def recycle_browser(
        self, reason: str = "unknown", rotate_profile: bool = True
    ) -> None:
        async with self._shared_browser_lock:
            await self._recycle_browser_locked(
                reason=reason, rotate_profile=rotate_profile
            )

    async def force_close_pending_browser(
        self, request_ref: Optional[str] = None, close_all: bool = False
    ) -> None:
        if close_all:
            await self.recycle_browser(
                reason="force_close_all", rotate_profile=False
            )

    async def _send_trusted_gesture(self, context_id: str) -> None:
        """Dispatch a trusted BiDi keyboard/mouse gesture (like AIStudio2API `sendProtectedKey`)."""
        if not self._bidi:
            return
        try:
            await self._bidi.command(
                "input.performActions",
                {
                    "context": context_id,
                    "actions": [
                        {
                            "type": "pointer",
                            "id": "mouse",
                            "parameters": {"pointerType": "mouse"},
                            "actions": [
                                {"type": "pointerMove", "x": 380, "y": 280},
                                {"type": "pointerMove", "x": 540, "y": 360},
                            ],
                        },
                        {
                            "type": "key",
                            "id": "keyboard",
                            "actions": PROTECTED_SEND_KEYS,
                        },
                    ],
                },
                timeout=10.0,
            )
        except Exception as exc:
            debug_logger.log_warning(
                f"[Camoufox] 发送 BiDi trusted 输入动作失败: {exc}"
            )

    async def _dismiss_flow_overlays(self, context_id: str) -> None:
        """Dismiss Flow onboarding / consent dialogs (e.g. '体验并参与塑造 AI 创意工具') so the project editor and reCAPTCHA anchor operate normally."""
        if not self._bidi:
            return
        dismiss_expr = r"""
        (async () => {
            const keywords = [
                '下一步', '继续', '同意', '接受', '开始', '知道了', '跳过', '完成',
                'next', 'continue', 'start', 'got it', 'agree', 'accept', 'skip', 'done'
            ];
            const roots = Array.from(document.querySelectorAll(
                '.cdk-overlay-container, [role="dialog"], [role="alertdialog"], [aria-modal="true"]'
            ));
            let scrolledAny = false;
            for (const root of roots) {
                const allEls = [root, ...Array.from(root.querySelectorAll('*'))];
                for (const el of allEls) {
                    if (el.scrollHeight > el.clientHeight + 4 && el.scrollTop + el.clientHeight < el.scrollHeight - 2) {
                        el.scrollTop = el.scrollHeight;
                        el.dispatchEvent(new Event('scroll', { bubbles: true }));
                        el.dispatchEvent(new WheelEvent('wheel', { deltaY: 800, bubbles: true }));
                        scrolledAny = true;
                    }
                }
            }
            if (scrolledAny) {
                await new Promise(r => setTimeout(r, 280));
            }
            for (const root of roots) {
                const buttons = Array.from(root.querySelectorAll('button, [role="button"]'));
                for (const btn of buttons) {
                    if (btn.disabled || btn.getAttribute('aria-disabled') === 'true') continue;
                    const text = String(btn.innerText || btn.textContent || '').trim().toLowerCase();
                    if (!text || text.length > 24) continue;
                    if (keywords.some(k => text === k || text.includes(k))) {
                        btn.click();
                        return 'clicked:' + text;
                    }
                }
            }
            return scrolledAny ? 'scrolled' : '';
        })()
        """
        for _ in range(6):
            try:
                clicked = await self._bidi.evaluate(
                    context_id, dismiss_expr, await_promise=True
                )
                if not clicked:
                    break
                debug_logger.log_info(
                    f"[Camoufox] Slot-{self.token_id} 已自动处理 Flow 引导弹窗: {clicked}"
                )
                await asyncio.sleep(0.45)
            except Exception:
                break

    async def _perform_human_warmup(
        self, context_id: str, project_id: Optional[str], action: str = ""
    ) -> None:
        """Simulate realistic multi-point Bézier mouse trajectories, wheel scroll, and focus inside the Flow page so reCAPTCHA Enterprise scores >= 0.7-0.9."""
        if not self._bidi:
            return
        project_key = str(project_id or "__projects__").strip() or "__projects__"
        action_key = str(action or "").strip().upper()
        warmup_key = f"{project_key}:{action_key}" if action_key else project_key
        first_warmup = warmup_key not in self._warmed_projects
        rounds = 2 if first_warmup else 1
        for r in range(rounds):
            x0, y0 = random.randint(240, 480), random.randint(220, 420)
            x1, y1 = random.randint(520, 920), random.randint(260, 560)
            cx, cy = (x0 + x1) // 2 + random.randint(-90, 90), (y0 + y1) // 2 + random.randint(-70, 70)
            steps = 8 if first_warmup else 5
            pointer_actions: List[Dict[str, Any]] = [
                {"type": "pointerMove", "x": x0, "y": y0, "duration": 0}
            ]
            for i in range(1, steps + 1):
                t = i / steps
                inv = 1.0 - t
                px = int(inv * inv * x0 + 2 * inv * t * cx + t * t * x1)
                py = int(inv * inv * y0 + 2 * inv * t * cy + t * t * y1)
                pointer_actions.append(
                    {
                        "type": "pointerMove",
                        "x": max(40, min(1360, px)),
                        "y": max(40, min(780, py)),
                        "duration": random.randint(18, 36),
                    }
                )
            try:
                await self._bidi.command(
                    "input.performActions",
                    {
                        "context": context_id,
                        "actions": [
                            {
                                "type": "pointer",
                                "id": "mouse",
                                "parameters": {"pointerType": "mouse"},
                                "actions": pointer_actions,
                            },
                            {
                                "type": "wheel",
                                "id": "wheel",
                                "actions": [
                                    {
                                        "type": "scroll",
                                        "x": x1,
                                        "y": y1,
                                        "deltaX": 0,
                                        "deltaY": random.choice([-48, 36, 64, -32]),
                                        "duration": 25,
                                    }
                                ],
                            },
                            {
                                "type": "key",
                                "id": "keyboard",
                                "actions": PROTECTED_SEND_KEYS,
                            },
                        ],
                    },
                    timeout=10.0,
                )
            except Exception:
                await self._send_trusted_gesture(context_id)
            if first_warmup:
                await jitter_sleep(1.1 if r == 0 else 0.9, low_ratio=0.75, high_ratio=1.35)
            else:
                await jitter_sleep(0.25, low_ratio=0.7, high_ratio=1.5)
        if first_warmup:
            self._warmed_projects.add(warmup_key)

    async def _ensure_flow_page_ready(
        self,
        context_id: str,
        project_id: Optional[str],
        action: str = "IMAGE_GENERATION",
        force_reload: bool = False,
    ) -> bool:
        """Navigate or reuse the resident Flow page, warm up reCAPTCHA Enterprise, and suspend renderLayers when done."""
        if not self._bidi:
            return False

        target_url = (
            f"{FLOW_ORIGIN}/project/{project_id}"
            if project_id
            else FLOW_PROJECTS_URL
        )
        current_url = ""
        try:
            current_url = str(
                await self._bidi.evaluate(
                    context_id, "location.href", await_promise=False
                )
                or ""
            )
        except Exception:
            current_url = ""

        needs_nav = force_reload or not current_url.startswith(f"{FLOW_ORIGIN}/") or (
            bool(project_id) and f"/project/{project_id}" not in current_url
        )

        async with self._boost_frames():
            if force_reload:
                try:
                    await self._bidi.evaluate(
                        context_id,
                        """(() => { try { localStorage.removeItem("_grecaptcha"); } catch (e) {} return true; })()""",
                        await_promise=False,
                    )
                except Exception:
                    pass
            if needs_nav:
                try:
                    await self._bidi.command(
                        "browsingContext.navigate",
                        {
                            "context": context_id,
                            "url": target_url,
                            "wait": "complete" if force_reload else "interactive",
                        },
                        timeout=45.0,
                    )
                except Exception as nav_exc:
                    if "NS_ERROR_ABORT" not in str(nav_exc):
                        debug_logger.log_warning(
                            f"[Camoufox] Slot-{self.token_id} 导航 {target_url} 异常: {nav_exc}"
                        )
                if force_reload:
                    await asyncio.sleep(1.5)
            for _ in range(30):
                try:
                    state = await self._bidi.evaluate(
                        context_id, "document.readyState", await_promise=False
                    )
                    if state in ("interactive", "complete"):
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.3)
            # Verify we did not get redirected to signed-out /about or accounts.google.com
            if needs_nav:
                await asyncio.sleep(0.4)
            try:
                page_check = await self._bidi.evaluate(
                    context_id,
                    """({
                        href: String(location.href || ''),
                        hasSNlM0e: Boolean(window.WIZ_global_data && window.WIZ_global_data.SNlM0e),
                        hasEmail: Boolean(window.WIZ_global_data && window.WIZ_global_data.oPEP7c)
                    })""",
                    await_promise=False,
                )
                if isinstance(page_check, dict):
                    href_now = str(page_check.get("href") or "")
                    # If Flow automatically bounced to accounts.google.com SSO, wait for redirect chain to settle
                    if "accounts.google.com" in href_now:
                        for _ in range(18):
                            await asyncio.sleep(0.5)
                            page_check = await self._bidi.evaluate(
                                context_id,
                                """({
                                    href: String(location.href || ''),
                                    hasSNlM0e: Boolean(window.WIZ_global_data && window.WIZ_global_data.SNlM0e),
                                    hasEmail: Boolean(window.WIZ_global_data && window.WIZ_global_data.oPEP7c),
                                    hasLoginInput: Boolean(document.querySelector('input[type="email"], input[type="password"]'))
                                })""",
                                await_promise=False,
                            )
                            href_now = str((page_check or {}).get("href") or "")
                            if "accounts.google.com" not in href_now or (page_check or {}).get("hasLoginInput"):
                                break
                    is_unauthed_flow = (
                        "accounts.google.com" in href_now
                        or href_now.rstrip("/").endswith("/about")
                        or (
                            href_now.startswith(f"{FLOW_ORIGIN}/")
                            and not (page_check or {}).get("hasSNlM0e")
                            and not (page_check or {}).get("hasEmail")
                            and "/about" in href_now
                        )
                    )
                    if is_unauthed_flow and "accounts.google.com" not in href_now:
                        # Attempt passive OSID cookie refresh via accounts.google.com
                        passive_url = (
                            "https://accounts.google.com/ServiceLogin?passive=1209600&osid=1"
                            f"&continue={quote(target_url, safe='')}"
                            f"&followup={quote(target_url, safe='')}"
                        )
                        try:
                            await self._bidi.command(
                                "browsingContext.navigate",
                                {
                                    "context": context_id,
                                    "url": passive_url,
                                    "wait": "complete",
                                },
                                timeout=25.0,
                            )
                            for _ in range(20):
                                await asyncio.sleep(0.5)
                                page_check = await self._bidi.evaluate(
                                    context_id,
                                    """({
                                        href: String(location.href || ''),
                                        hasSNlM0e: Boolean(window.WIZ_global_data && window.WIZ_global_data.SNlM0e),
                                        hasEmail: Boolean(window.WIZ_global_data && window.WIZ_global_data.oPEP7c),
                                        hasLoginInput: Boolean(document.querySelector('input[type="email"], input[type="password"]'))
                                    })""",
                                    await_promise=False,
                                )
                                href_now = str((page_check or {}).get("href") or "")
                                if href_now.startswith(f"{FLOW_ORIGIN}/") and (
                                    (page_check or {}).get("hasSNlM0e") or (page_check or {}).get("hasEmail")
                                ):
                                    break
                                if "accounts.google.com" in href_now and (page_check or {}).get("hasLoginInput"):
                                    break
                            is_unauthed_flow = (
                                "accounts.google.com" in href_now
                                or href_now.rstrip("/").endswith("/about")
                                or (
                                    href_now.startswith(f"{FLOW_ORIGIN}/")
                                    and not (page_check or {}).get("hasSNlM0e")
                                    and not (page_check or {}).get("hasEmail")
                                    and "/about" in href_now
                                )
                            )
                        except Exception:
                            pass
                    if is_unauthed_flow:
                        if href_now.rstrip("/").endswith("/about") or "/about" in href_now:
                            raise RuntimeError(
                                "Flow 账号登录态已失效（identity_recheck_required：页面持续跳转 /about，需要 Google 二次身份核验），请在后台点击该账号的「重登」并完成“验证是您本人”"
                            )
                        raise RuntimeError(
                            "Flow 账号登录态已失效（页面已跳转至未登录页），请在后台点击该账号的「重登」重新登录"
                        )
            except Exception as unauth_exc:
                if "登录态已失效" in str(unauth_exc):
                    raise
            await self._dismiss_flow_overlays(context_id)
            try:
                cookie_bar_rect = await self._bidi.evaluate(
                    context_id,
                    """(() => {
                        const el = document.querySelector(".glue-cookie-notification-bar__accept");
                        if (!el) return null;
                        const r = el.getBoundingClientRect();
                        const vw = window.innerWidth || document.documentElement.clientWidth || 1280;
                        const vh = window.innerHeight || document.documentElement.clientHeight || 720;
                        if (r.width <= 0 || r.height <= 0 || r.left < 0 || r.top < 0 || r.right > vw || r.bottom > vh) return null;
                        return { x: Math.round(r.left + r.width / 2), y: Math.round(r.top + r.height / 2) };
                    })()""",
                    await_promise=False,
                )
                if isinstance(cookie_bar_rect, dict):
                    await self._bidi.command(
                        "input.performActions",
                        {
                            "context": context_id,
                            "actions": [
                                {
                                    "type": "pointer",
                                    "id": "mouse",
                                    "parameters": {"pointerType": "mouse"},
                                    "actions": [
                                        {
                                            "type": "pointerMove",
                                            "x": int(cookie_bar_rect["x"]),
                                            "y": int(cookie_bar_rect["y"]),
                                            "duration": 160,
                                        },
                                        {"type": "pointerDown", "button": 0},
                                        {"type": "pointerUp", "button": 0},
                                    ],
                                }
                            ],
                        },
                    )
                    await asyncio.sleep(0.2)
            except Exception:
                pass
            await self._send_trusted_gesture(context_id)
            ready = False
            for _ in range(30):
                try:
                    ok = await self._bidi.evaluate(
                        context_id,
                        "Boolean(window.grecaptcha && window.grecaptcha.enterprise && typeof window.grecaptcha.enterprise.execute === 'function')",
                        await_promise=False,
                    )
                    if ok is True:
                        ready = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.5)

            if not ready:
                # Inject enterprise.js if Flow page deferred it
                primary_host = (
                    "https://www.recaptcha.net"
                    if self._browser_proxy_active
                    else "https://www.google.com"
                )
                inject_expr = f"""
                (() => {{
                    if (window.grecaptcha && window.grecaptcha.enterprise && typeof window.grecaptcha.enterprise.execute === 'function') return true;
                    const script = document.createElement('script');
                    script.src = {json.dumps(f"{primary_host}/recaptcha/enterprise.js?render={FLOW_WEBSITE_KEY}")};
                    script.async = true;
                    (document.head || document.documentElement).appendChild(script);
                    return false;
                }})()
                """
                try:
                    await self._bidi.evaluate(
                        context_id, inject_expr, await_promise=False
                    )
                except Exception:
                    pass
                for _ in range(25):
                    try:
                        ok = await self._bidi.evaluate(
                            context_id,
                            "Boolean(window.grecaptcha && window.grecaptcha.enterprise && typeof window.grecaptcha.enterprise.execute === 'function')",
                            await_promise=False,
                        )
                        if ok is True:
                            ready = True
                            break
                    except Exception:
                        pass
                    await asyncio.sleep(0.5)

            return ready

    async def get_token(
        self,
        project_id: str,
        website_key: str = FLOW_WEBSITE_KEY,
        action: str = "IMAGE_GENERATION",
        token_proxy_url: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Acquire a reCAPTCHA Enterprise token inside the Camoufox Flow page with trusted BiDi activation."""
        async with self._semaphore:
            self._solve_inflight += 1
            max_retries = max(
                1, int(getattr(config, "browser_captcha_max_retries", 5) or 5)
            )
            try:
                for attempt in range(max_retries):
                    try:
                        start_ts = time.time()
                        _, bidi, _api_ctx = await self._get_or_create_shared_browser(
                            token_proxy_url=token_proxy_url, token_id=token_id
                        )
                        should_recreate_captcha_tab = bool(
                            attempt > 0
                            or self._consecutive_risk_failures > 0
                            or self._captcha_context_dirty
                        )
                        context_id = await self._ensure_captcha_context_id(
                            force_recreate=should_recreate_captcha_tab,
                            fallback_context_id=_api_ctx,
                        )
                        used_force_reload = bool(should_recreate_captcha_tab)
                        ready = await self._ensure_flow_page_ready(
                            context_id,
                            project_id,
                            action=action,
                            force_reload=used_force_reload,
                        )
                        if not ready:
                            raise RuntimeError("Camoufox 页面 grecaptcha.enterprise 未就绪")

                        async with self._boost_frames():
                            await self._perform_human_warmup(
                                context_id, project_id, action=action
                            )
                            script = build_recaptcha_execute_script(
                                website_key or FLOW_WEBSITE_KEY,
                                str(action or "IMAGE_GENERATION").strip().upper(),
                                25000,
                            )
                            result = await bidi.evaluate(
                                context_id,
                                script,
                                await_promise=True,
                                user_activation=True,
                                timeout=35.0,
                            )

                        if isinstance(result, dict) and result.get("ok"):
                            token = str(result.get("token") or "").strip()
                            if token:
                                self._solve_count += 1
                                self._consecutive_browser_failures = 0
                                self._captcha_context_dirty = False
                                if used_force_reload:
                                    self._consecutive_risk_failures = 0
                                session_id = (
                                    str(uuid.uuid4())
                                    if str(action or "").upper() == "CHAT_GENERATION"
                                    else None
                                )
                                self._last_harvest_session_id = session_id
                                await self._sync_live_cookies_to_db(token_id)
                                debug_logger.log_info(
                                    f"[Camoufox] Slot-{self.token_id} token 获取成功 "
                                    f"({(time.time() - start_ts) * 1000:.0f}ms, action={action}, reuse={self._shared_reuse_count})"
                                )
                                return token, session_id
                        err_detail = (
                            result.get("error")
                            if isinstance(result, dict)
                            else str(result)
                        )
                        raise RuntimeError(f"execute 未返回有效 token: {err_detail}")
                    except Exception as exc:
                        self._error_count += 1
                        self._consecutive_browser_failures += 1
                        debug_logger.log_warning(
                            f"[Camoufox] Slot-{self.token_id} 第 {attempt + 1}/{max_retries} 次取码失败: {exc}"
                        )
                        if "登录态已失效" in str(exc) or "登录已失效" in str(exc):
                            raise
                        if self._consecutive_browser_failures >= 2 or (
                            self._bidi and self._bidi._closed
                        ):
                            await self.recycle_browser(
                                reason=f"camoufox_solve_failed_{attempt + 1}",
                                rotate_profile=False,
                            )
                        if attempt < max_retries - 1:
                            await asyncio.sleep(1.0)
                return None, None
            finally:
                self._solve_inflight = max(0, self._solve_inflight - 1)
                self.note_idle()

    async def execute_flow_frontend_rpc(
        self,
        *,
        rpc_id: str,
        argument: List[Any],
        project_id: Optional[str] = None,
        source_path: Optional[str] = None,
        timeout: int = 90,
        token_id: Optional[int] = None,
        token_proxy_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute a Flow `batchexecute` RPC directly inside the Camoufox page via `XMLHttpRequest`."""
        async with self._semaphore:
            self._solve_inflight += 1
            try:
                _, bidi, context_id = await self._get_or_create_shared_browser(
                    token_proxy_url=token_proxy_url, token_id=token_id
                )
                current_url = ""
                try:
                    current_url = str(
                        await bidi.evaluate(
                            context_id, "String(location.href || '')", await_promise=False
                        )
                        or ""
                    )
                except Exception:
                    current_url = ""
                if not current_url.startswith(f"{FLOW_ORIGIN}/"):
                    await bidi.command(
                        "browsingContext.navigate",
                        {
                            "context": context_id,
                            "url": FLOW_PROJECTS_URL,
                            "wait": "interactive",
                        },
                        timeout=25.0,
                    )
                resolved_source_path = source_path or (
                    f"/project/{project_id}" if project_id else "/projects"
                )
                f_req = json.dumps(
                    [[[rpc_id, json.dumps(argument, ensure_ascii=False, separators=(",", ":")), None, "generic"]]],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )

                from .bidi_scripts import FLOW_BATCHEXECUTE_XHR_FN

                res = await bidi.call_function(
                    context_id,
                    FLOW_BATCHEXECUTE_XHR_FN,
                    arguments=[
                        _to_bidi_local_value(
                            {
                                "rpcId": rpc_id,
                                "sourcePath": resolved_source_path,
                                "fReq": f_req,
                                "timeoutMs": max(10000, int(timeout * 1000)),
                                "cachedBl": self._cached_wiz.get("bl") or "",
                                "cachedFSid": self._cached_wiz.get("fSid") or "",
                                "cachedAt": self._cached_wiz.get("at") or "",
                            }
                        ),
                    ],
                    await_promise=True,
                    user_activation=True,
                    timeout=max(30.0, float(timeout) + 15.0),
                )
                if not isinstance(res, dict) or not res.get("ok"):
                    err_msg = (
                        res.get("error")
                        if isinstance(res, dict)
                        else f"unexpected response: {res}"
                    )
                    raise RuntimeError(f"Camoufox 页面内 RPC 失败: {err_msg}")
                if res.get("bl") and res.get("fSid"):
                    self._cached_wiz = {
                        "bl": str(res.get("bl") or ""),
                        "fSid": str(res.get("fSid") or ""),
                        "at": str(res.get("at") or ""),
                    }
                await self._sync_live_cookies_to_db(token_id, update_db=False)
                return {
                    "status": int(res.get("status") or 0),
                    "text": str(res.get("text") or ""),
                }
            finally:
                self._solve_inflight = max(0, self._solve_inflight - 1)
                self.note_idle()

    async def stream_flow_agent_chat(
        self,
        *,
        project_id: str,
        request_builder: Callable[[str], str],
        timeout: int = 180,
        token_id: Optional[int] = None,
        token_proxy_url: Optional[str] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Execute Flow Creation Agent WebChannel (`streamCreationAgentChat/channel`) in-page and yield chunks."""
        async with self._semaphore:
            self._solve_inflight += 1
            stream_id = f"agent_{uuid.uuid4().hex[:12]}"
            active_bidi = None
            active_context_id: Optional[str] = None
            try:
                cookie_fallback: Dict[str, str] = {}
                try:
                    storage_path = self.user_data_dir / "storage-state.json"
                    if storage_path.exists():
                        st_data = json.loads(storage_path.read_text(encoding="utf-8"))
                        for c in st_data.get("cookies") or []:
                            if isinstance(c, dict) and c.get("name") in {
                                "SAPISID",
                                "__Secure-1PAPISID",
                                "__Secure-3PAPISID",
                            } and c.get("value"):
                                cookie_fallback[c["name"]] = str(c["value"])
                except Exception:
                    cookie_fallback = {}

                for attempt in range(2):
                    used_force_reload = bool(
                        attempt > 0 or self._consecutive_risk_failures > 0
                    )
                    _, bidi, context_id = await self._get_or_create_shared_browser(
                        token_proxy_url=token_proxy_url, token_id=token_id
                    )
                    active_bidi = bidi
                    active_context_id = context_id
                    ready = await self._ensure_flow_page_ready(
                        context_id,
                        project_id,
                        action="CHAT_GENERATION",
                        force_reload=used_force_reload,
                    )
                    if not ready and not used_force_reload:
                        ready = await self._ensure_flow_page_ready(
                            context_id,
                            project_id,
                            action="CHAT_GENERATION",
                            force_reload=True,
                        )
                    if not ready:
                        raise RuntimeError(
                            "Camoufox 页面 grecaptcha.enterprise 未就绪 (CHAT_GENERATION)"
                        )
                    async with self._boost_frames():
                        await self._perform_human_warmup(context_id, project_id)
                        rec_script = build_recaptcha_execute_script(
                            FLOW_WEBSITE_KEY, "CHAT_GENERATION", 25000
                        )
                        rec_res = await bidi.evaluate(
                            context_id,
                            rec_script,
                            await_promise=True,
                            user_activation=True,
                            timeout=35.0,
                        )
                        if not isinstance(rec_res, dict) or not rec_res.get("ok"):
                            err_detail = (
                                rec_res.get("error")
                                if isinstance(rec_res, dict)
                                else str(rec_res)
                            )
                            raise RuntimeError(
                                f"CHAT_GENERATION reCAPTCHA 获取失败: {err_detail}"
                            )
                        recaptcha_token = str(rec_res.get("token") or "").strip()
                        if not recaptcha_token:
                            raise RuntimeError("CHAT_GENERATION reCAPTCHA 返回空 token")
                        if used_force_reload:
                            self._consecutive_risk_failures = 0

                        req_json = request_builder(recaptcha_token)
                        start_fn = r"""
                        async (cfg) => {
                            let wiz = window.WIZ_global_data || {};
                            const fetchBootstrap = async () => {
                                const targetPath = `/project/${encodeURIComponent(cfg.projectId)}`;
                                const pageResp = await fetch(targetPath, { credentials: 'include' });
                                const html = await pageResp.text();
                                const mY = html.match(/"YmyN0b"\s*:\s*"((?:\\.|[^"\\])+)"/);
                                const mK = html.match(/"K21R3e"\s*:\s*"((?:\\.|[^"\\])+)"/);
                                const mS = html.match(/"y83d5b"\s*:\s*"((?:\\.|[^"\\])+)"/);
                                const dec = (v) => { try { return JSON.parse('"' + v + '"'); } catch (_) { return v; } };
                                return {
                                    YmyN0b: mY ? dec(mY[1]) : '',
                                    K21R3e: mK ? dec(mK[1]) : '',
                                    y83d5b: mS ? dec(mS[1]) : '',
                                };
                            };

                            let endpointBase = wiz.YmyN0b || '';
                            let apiKey = wiz.K21R3e || '';
                            let serverToken = wiz.y83d5b || '';
                            if (!endpointBase || !apiKey) {
                                const boot = await fetchBootstrap();
                                endpointBase = endpointBase || boot.YmyN0b;
                                apiKey = apiKey || boot.K21R3e;
                                serverToken = serverToken || boot.y83d5b;
                            }
                            endpointBase = (endpointBase || 'https://aisandbox-pa-webchannel.clients6.google.com').replace(/\/+$/, '');
                            apiKey = apiKey || 'AIzaSyAMHMKiLZOSJmkvoi7q3r9m4rgvP1x10e8';
                            const channelPath = `${endpointBase}/v1/flowCreationAgent/streamCreationAgentChat/channel`;

                            const readCookie = (name) => {
                                const m = document.cookie.match(new RegExp('(?:^|;\\s*)' + name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '=([^;]*)'));
                                return m ? decodeURIComponent(m[1]) : '';
                            };
                            const sha1Hex = async (str) => {
                                const buf = new TextEncoder().encode(str);
                                const hash = await crypto.subtle.digest('SHA-1', buf);
                                return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2, '0')).join('');
                            };
                            const ts = Math.floor(Date.now() / 1000);
                            const origin = location.origin || 'https://flow.google.com';
                            const authParts = [];
                            for (const [ck, scheme] of [['SAPISID', 'SAPISIDHASH'], ['__Secure-1PAPISID', 'SAPISID1PHASH'], ['__Secure-3PAPISID', 'SAPISID3PHASH']]) {
                                const val = readCookie(ck) || (cfg.cookieFallback && cfg.cookieFallback[ck]) || '';
                                if (val) {
                                    authParts.push(`${scheme} ${ts}_${await sha1Hex(`${ts} ${val} ${origin}`)}`);
                                }
                            }

                            const hdrLines = [];
                            if (authParts.length) hdrLines.push(`Authorization:${authParts.join(' ')}`);
                            if (apiKey) hdrLines.push(`X-Goog-Api-Key:${apiKey}`);
                            hdrLines.push('X-Goog-AuthUser:0');
                            if (serverToken) hdrLines.push(`X-Server-Token:${serverToken}`);
                            hdrLines.push('X-WebChannel-Content-Type:application/json+protobuf');
                            const httpHeadersParam = hdrLines.join('\r\n') + '\r\n';

                            const randZx = () => Math.random().toString(36).slice(2, 14);
                            const baseRid = Math.floor(10000 + Math.random() * 80000);

                            const ac = new AbortController();
                            if (!window.__flow2apiAgentStreams) {
                                window.__flow2apiAgentStreams = {};
                            }
                            const entry = {
                                status: 0,
                                text: '',
                                offset: 0,
                                done: false,
                                aborted: false,
                                error: '',
                                sid: '',
                                gsid: '',
                                channelPath,
                                _waiters: [],
                                notify: () => {
                                    const ws = entry._waiters.splice(0);
                                    for (const w of ws) {
                                        try { w(); } catch (_) {}
                                    }
                                },
                                abort: () => {
                                    if (entry.aborted) return;
                                    entry.aborted = true;
                                    entry.done = true;
                                    entry.notify();
                                    try { ac.abort(); } catch (_) {}
                                    if (entry.sid) {
                                        try {
                                            const termParams = new URLSearchParams();
                                            termParams.set('VER', '8');
                                            if (entry.gsid) termParams.set('gsessionid', entry.gsid);
                                            termParams.set('SID', entry.sid);
                                            termParams.set('TYPE', 'terminate');
                                            termParams.set('zx', randZx());
                                            fetch(`${entry.channelPath}?${termParams.toString()}`, {
                                                method: 'POST',
                                                credentials: 'include',
                                                keepalive: true,
                                            }).catch(() => {});
                                        } catch (_) {}
                                    }
                                },
                            };
                            window.__flow2apiAgentStreams[cfg.streamId] = entry;

                            const runWebChannel = async () => {
                                try {
                                    const hsUrl = `${channelPath}?VER=8&RID=${baseRid}&CVER=22&X-HTTP-Session-Id=gsessionid&%24httpHeaders=${encodeURIComponent(httpHeadersParam)}&zx=${randZx()}&t=1`;
                                    const hsResp = await fetch(hsUrl, {
                                        method: 'POST',
                                        credentials: 'include',
                                        signal: ac.signal,
                                        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                                        body: 'count=0',
                                    });
                                    const hsText = await hsResp.text();
                                    if (!hsResp.ok) {
                                        entry.status = hsResp.status;
                                        entry.error = `WebChannel handshake HTTP ${hsResp.status}: ${hsText.slice(0, 300)}`;
                                        entry.done = true;
                                        entry.notify();
                                        return;
                                    }
                                    const gsid = hsResp.headers.get('x-http-session-id') || '';
                                    const sidMatch = hsText.match(/\["c"\s*,\s*"([^"]+)"/);
                                    const sid = sidMatch ? sidMatch[1] : '';
                                    if (!sid) {
                                        entry.status = hsResp.status;
                                        entry.error = `WebChannel handshake missing SID: ${hsText.slice(0, 300)}`;
                                        entry.done = true;
                                        entry.notify();
                                        return;
                                    }
                                    entry.sid = sid;
                                    entry.gsid = gsid;

                                    const rpcParams = new URLSearchParams();
                                    if (gsid) rpcParams.set('gsessionid', gsid);
                                    rpcParams.set('VER', '8');
                                    rpcParams.set('RID', 'rpc');
                                    rpcParams.set('SID', sid);
                                    rpcParams.set('AID', '0');
                                    rpcParams.set('CI', '0');
                                    rpcParams.set('TYPE', 'xmlhttp');
                                    rpcParams.set('zx', randZx());
                                    rpcParams.set('t', '1');
                                    const rpcUrl = `${channelPath}?${rpcParams.toString()}`;

                                    const rpcPromise = (async () => {
                                        const rpcResp = await fetch(rpcUrl, {
                                            method: 'GET',
                                            credentials: 'include',
                                            signal: ac.signal,
                                        });
                                        entry.status = rpcResp.status;
                                        if (!rpcResp.ok) {
                                            const errBody = await rpcResp.text();
                                            entry.error = `WebChannel GET HTTP ${rpcResp.status}: ${errBody.slice(0, 300)}`;
                                            entry.done = true;
                                            entry.notify();
                                            return;
                                        }
                                        if (!rpcResp.body || !rpcResp.body.getReader) {
                                            entry.text = await rpcResp.text();
                                            entry.done = true;
                                            entry.notify();
                                            return;
                                        }
                                        const reader = rpcResp.body.getReader();
                                        const decoder = new TextDecoder('utf-8');
                                        while (true) {
                                            const { value, done } = await reader.read();
                                            if (value) {
                                                entry.text += decoder.decode(value, { stream: !done });
                                                entry.notify();
                                            }
                                            if (done) {
                                                const tail = decoder.decode();
                                                if (tail) entry.text += tail;
                                                entry.done = true;
                                                entry.notify();
                                                break;
                                            }
                                        }
                                    })();

                                    const fwdParams = new URLSearchParams();
                                    fwdParams.set('VER', '8');
                                    if (gsid) fwdParams.set('gsessionid', gsid);
                                    fwdParams.set('SID', sid);
                                    fwdParams.set('RID', String(baseRid + 1));
                                    fwdParams.set('AID', '0');
                                    fwdParams.set('zx', randZx());
                                    fwdParams.set('t', '1');
                                    const fwdUrl = `${channelPath}?${fwdParams.toString()}`;
                                    const fwdBody = new URLSearchParams();
                                    fwdBody.set('count', '1');
                                    fwdBody.set('ofs', '0');
                                    fwdBody.set('req0___data__', cfg.reqJson);

                                    const fwdResp = await fetch(fwdUrl, {
                                        method: 'POST',
                                        credentials: 'include',
                                        signal: ac.signal,
                                        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                                        body: fwdBody.toString(),
                                    });
                                    if (!fwdResp.ok) {
                                        const fwdErr = await fwdResp.text();
                                        entry.status = fwdResp.status;
                                        entry.error = `WebChannel POST HTTP ${fwdResp.status}: ${fwdErr.slice(0, 300)}`;
                                        entry.done = true;
                                        entry.notify();
                                        return;
                                    }
                                    await rpcPromise;
                                } catch (err) {
                                    if (entry.aborted) {
                                        entry.done = true;
                                        entry.notify();
                                        return;
                                    }
                                    entry.error = String((err && err.message) || err || 'WebChannel fetch error');
                                    entry.done = true;
                                    entry.notify();
                                }
                            };

                            runWebChannel();
                            return { ok: true };
                        }
                        """
                        init_res = await bidi.call_function(
                            context_id,
                            start_fn,
                            arguments=[
                                _to_bidi_local_value(
                                    {
                                        "streamId": stream_id,
                                        "projectId": project_id,
                                        "reqJson": req_json,
                                        "cookieFallback": cookie_fallback,
                                    }
                                )
                            ],
                            await_promise=True,
                            user_activation=True,
                            timeout=30.0,
                        )
                        if not isinstance(init_res, dict) or not init_res.get("ok"):
                            err_msg = (
                                init_res.get("error")
                                if isinstance(init_res, dict)
                                else str(init_res)
                            )
                            raise RuntimeError(f"Camoufox WebChannel 启动失败: {err_msg}")

                        deadline = time.monotonic() + max(30.0, float(timeout or 180))
                        buffer_all = ""
                        sent_nonempty_delta = False
                        should_retry_risk = False
                        while time.monotonic() < deadline:
                            rem_ms = max(100, min(1500, int((deadline - time.monotonic()) * 1000)))
                            poll = await bidi.call_function(
                                context_id,
                                """(sid, waitMs) => new Promise((resolve) => {
                                    const flush = () => {
                                        const store = window.__flow2apiAgentStreams || {};
                                        const entry = store[sid];
                                        if (!entry) return { missing: true, done: true };
                                        const delta = entry.text.slice(entry.offset);
                                        entry.offset = entry.text.length;
                                        return {
                                            status: entry.status,
                                            delta,
                                            done: Boolean(entry.done),
                                            error: entry.error || '',
                                        };
                                    };
                                    const store = window.__flow2apiAgentStreams || {};
                                    const entry = store[sid];
                                    if (!entry || entry.done || entry.error || entry.text.length > entry.offset) {
                                        resolve(flush());
                                        return;
                                    }
                                    let settled = false;
                                    const onWake = () => {
                                        if (settled) return;
                                        settled = true;
                                        clearTimeout(timer);
                                        resolve(flush());
                                    };
                                    const timer = setTimeout(() => {
                                        if (settled) return;
                                        settled = true;
                                        const idx = entry._waiters ? entry._waiters.indexOf(onWake) : -1;
                                        if (idx >= 0) entry._waiters.splice(idx, 1);
                                        resolve(flush());
                                    }, Math.max(50, Math.min(2000, Number(waitMs) || 1500)));
                                    (entry._waiters || (entry._waiters = [])).push(onWake);
                                })""",
                                arguments=[
                                    _to_bidi_local_value(stream_id),
                                    _to_bidi_local_value(rem_ms),
                                ],
                                await_promise=True,
                                timeout=15.0,
                            )
                            if not isinstance(poll, dict) or poll.get("missing"):
                                raise RuntimeError("Camoufox Agent WebChannel 状态丢失")
                            delta = str(poll.get("delta") or "")
                            done = bool(poll.get("done"))
                            err = str(poll.get("error") or "")
                            status = int(poll.get("status") or 0)
                            if delta:
                                buffer_all += delta
                                if (
                                    attempt == 0
                                    and not sent_nonempty_delta
                                    and (
                                        "PUBLIC_ERROR_UNUSUAL_ACTIVITY" in buffer_all
                                        or "reCAPTCHA evaluation failed" in buffer_all
                                    )
                                ):
                                    should_retry_risk = True
                                    self.note_recaptcha_rejected()
                                    break
                                # Defer emitting pure noop heartbeat frames until payload/error/done arrives
                                has_non_noop = bool(
                                    re.sub(r'\d+\s*\[\[\s*\d+\s*,\s*\[\s*"noop"\s*\]\s*\]\]', "", buffer_all).strip()
                                )
                                if has_non_noop or done:
                                    emit_chunk = buffer_all if not sent_nonempty_delta else delta
                                    sent_nonempty_delta = True
                                    yield {
                                        "transport": "webchannel",
                                        "status": status,
                                        "delta": emit_chunk,
                                        "done": done,
                                        "error": err,
                                    }
                            elif done:
                                if (
                                    attempt == 0
                                    and not sent_nonempty_delta
                                    and (
                                        "PUBLIC_ERROR_UNUSUAL_ACTIVITY" in buffer_all
                                        or "reCAPTCHA evaluation failed" in buffer_all
                                    )
                                ):
                                    should_retry_risk = True
                                    self.note_recaptcha_rejected()
                                    break
                                emit_chunk = (
                                    buffer_all
                                    if (buffer_all and not sent_nonempty_delta)
                                    else ""
                                )
                                yield {
                                    "transport": "webchannel",
                                    "status": status,
                                    "delta": emit_chunk,
                                    "done": True,
                                    "error": err,
                                }
                            if err:
                                raise RuntimeError(f"Camoufox Agent WebChannel 异常: {err}")
                            if done:
                                debug_logger.log_info(
                                    f"[Camoufox Agent] WebChannel done status={status} len={len(buffer_all)} preview={buffer_all[:350]!r}"
                                )
                                return
                        if should_retry_risk:
                            try:
                                await bidi.call_function(
                                    context_id,
                                    """(sid) => {
                                        const store = window.__flow2apiAgentStreams;
                                        if (!store) return;
                                        const entry = store[sid];
                                        if (entry && typeof entry.abort === 'function') entry.abort();
                                        delete store[sid];
                                    }""",
                                    arguments=[_to_bidi_local_value(stream_id)],
                                    await_promise=False,
                                    timeout=5.0,
                                )
                            except Exception:
                                pass
                            await asyncio.sleep(1.2)
                            continue
                        raise RuntimeError(
                            f"Camoufox Agent WebChannel 超时 ({int(timeout or 180)}s)"
                        )
            finally:
                if active_bidi is not None and active_context_id:
                    try:
                        await active_bidi.call_function(
                            active_context_id,
                            """(sid) => {
                                const store = window.__flow2apiAgentStreams;
                                if (!store) return;
                                const entry = store[sid];
                                if (entry && typeof entry.abort === 'function') entry.abort();
                                delete store[sid];
                            }""",
                            arguments=[_to_bidi_local_value(stream_id)],
                            await_promise=False,
                            timeout=5.0,
                        )
                    except Exception:
                        pass
                try:
                    await self._sync_live_cookies_to_db(token_id, update_db=True)
                except Exception:
                    pass
                self._solve_inflight = max(0, self._solve_inflight - 1)
                self.note_idle()

    async def submit_flow_request(
        self,
        project_id: str,
        website_key: str,
        action: str,
        url: str,
        at_token: str,
        json_data: Dict[str, Any],
        timeout: int,
        token_proxy_url: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Compatible with `TokenBrowser.submit_flow_request`."""
        token, _ = await self.get_token(
            project_id=project_id,
            website_key=website_key,
            action=action,
            token_proxy_url=token_proxy_url,
            token_id=token_id,
        )
        if not token:
            raise RuntimeError("Camoufox 未获取到有效 reCAPTCHA token")

        async with self._semaphore:
            self._solve_inflight += 1
            try:
                _, bidi, context_id = await self._get_or_create_shared_browser(
                    token_proxy_url=token_proxy_url, token_id=token_id
                )
                fn_decl = r"""
                async (cfg) => {
                    const patchToken = (value, tok) => {
                        if (!value) return;
                        if (Array.isArray(value)) { value.forEach(i => patchToken(i, tok)); return; }
                        if (typeof value !== 'object') return;
                        if (value.recaptchaContext && typeof value.recaptchaContext === 'object') {
                            value.recaptchaContext.token = tok;
                            if (!value.recaptchaContext.applicationType) {
                                value.recaptchaContext.applicationType = 'RECAPTCHA_APPLICATION_TYPE_WEB';
                            }
                        }
                        Object.values(value).forEach(i => patchToken(i, tok));
                    };
                    const body = JSON.parse(cfg.payloadJson);
                    patchToken(body, cfg.token);
                    const resp = await fetch(cfg.url, {
                        method: 'POST',
                        headers: {
                            'authorization': `Bearer ${cfg.atToken}`,
                            'content-type': 'text/plain;charset=UTF-8'
                        },
                        credentials: 'include',
                        body: JSON.stringify(body)
                    });
                    const text = await resp.text();
                    return { ok: resp.ok, status: resp.status, text, token: cfg.token };
                }
                """
                res = await bidi.call_function(
                    context_id,
                    fn_decl,
                    arguments=[
                        _to_bidi_local_value(
                            {
                                "url": url,
                                "atToken": at_token,
                                "token": token,
                                "payloadJson": json.dumps(
                                    json_data, ensure_ascii=False
                                ),
                            }
                        )
                    ],
                    await_promise=True,
                    user_activation=True,
                    timeout=max(35.0, float(timeout) + 10.0),
                )
                if not isinstance(res, dict):
                    raise RuntimeError("Camoufox 页面内提交返回格式异常")
                return res
            finally:
                self._solve_inflight = max(0, self._solve_inflight - 1)
                self.note_idle()

    async def generate_native_image(
        self,
        *,
        project_id: str,
        prompt: str,
        aspect_ratio: str = "IMAGE_ASPECT_RATIO_LANDSCAPE",
        model_name: str = "BELUGA",
        image_inputs: Optional[List[Dict[str, Any]]] = None,
        timeout: int = 120,
        token_id: Optional[int] = None,
        token_proxy_url: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Execute native Flow image generation via in-page UI + StreamChat in headless Camoufox."""
        if image_inputs:
            return None
        return await self._execute_native_stream_chat_generation(
            project_id=project_id,
            prompt=prompt,
            aspect_ratio=aspect_ratio,
            model_name=model_name,
            media_kind="image",
            image_inputs=image_inputs,
            timeout=timeout,
            token_id=token_id,
            token_proxy_url=token_proxy_url,
        )
    async def generate_native_video(
        self,
        *,
        project_id: str,
        prompt: str,
        aspect_ratio: str = "VIDEO_ASPECT_RATIO_LANDSCAPE",
        model_key: str = "",
        timeout: int = 180,
        token_id: Optional[int] = None,
        token_proxy_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute native Flow video generation via in-page UI + StreamChat in headless Camoufox."""
        return await self._execute_native_stream_chat_generation(
            project_id=project_id,
            prompt=prompt,
            aspect_ratio=aspect_ratio,
            model_name=model_key or "VEO_3_1",
            media_kind="video",
            image_inputs=None,
            timeout=timeout,
            token_id=token_id,
            token_proxy_url=token_proxy_url,
        )

    async def _execute_native_stream_chat_generation(
        self,
        *,
        project_id: str,
        prompt: str,
        aspect_ratio: str,
        model_name: str,
        media_kind: str = "image",
        image_inputs: Optional[List[Dict[str, Any]]] = None,
        timeout: int = 120,
        token_id: Optional[int] = None,
        token_proxy_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Unified single-pass headless Camoufox UI + StreamChat pipeline for image and video generation."""
        from .flow_frontend import FlowFrontendMixin

        if not project_id:
            raise ValueError("Camoufox 原生生成必须提供 project_id")
        async with self._semaphore:
            self._solve_inflight += 1
            try:
                _, bidi, context_id = await self._get_or_create_shared_browser(
                    token_proxy_url=token_proxy_url, token_id=token_id
                )
                target_url = f"{FLOW_ORIGIN}/project/{project_id}"
                ar_upper = str(aspect_ratio or "").upper()
                if "PORTRAIT" in ar_upper:
                    ar_desc = "vertical portrait 9:16"
                elif "SQUARE" in ar_upper:
                    ar_desc = "square 1:1"
                else:
                    ar_desc = "landscape 16:9"
                if media_kind == "video":
                    ar_prefix = f"Generate 1 {ar_desc} video of "
                else:
                    model_hint = (
                        "using Nano Banana Pro "
                        if str(model_name or "").upper() == "GEM_PIX_2"
                        else ""
                    )
                    ar_prefix = f"Generate 1 {ar_desc} image {model_hint}of "
                clean_prompt = " ".join(str(prompt or "").split())
                full_prompt = ar_prefix + clean_prompt

                try:
                    last_attempt_err: Optional[Exception] = None
                    frames: List[Any] = []
                    media_ids: List[str] = []
                    full_chunks = ""
                    sc_status = 0

                    for attempt_idx in range(2):
                        try:
                            if attempt_idx == 1 or self._consecutive_risk_failures > 0:
                                try:
                                    await bidi.evaluate(
                                        context_id,
                                        """(() => {
                                            try {
                                                localStorage.removeItem("_grecaptcha");
                                            } catch (e) {}
                                            return true;
                                        })()""",
                                        await_promise=False,
                                    )
                                except Exception:
                                    pass
                                await bidi.command(
                                    "browsingContext.navigate",
                                    {
                                        "context": context_id,
                                        "url": target_url,
                                        "wait": "complete",
                                    },
                                    timeout=45.0,
                                )
                                await asyncio.sleep(2.0)
                                await self._perform_human_warmup(context_id, project_id)
                            else:
                                current_url = ""
                                try:
                                    current_url = str(
                                        await bidi.evaluate(
                                            context_id,
                                            "String(location.href || '')",
                                            await_promise=False,
                                        )
                                        or ""
                                    )
                                except Exception:
                                    current_url = ""
                                if f"/project/{project_id}" not in current_url:
                                    await bidi.command(
                                        "browsingContext.navigate",
                                        {
                                            "context": context_id,
                                            "url": target_url,
                                            "wait": "interactive",
                                        },
                                        timeout=45.0,
                                    )

                            await bidi.evaluate(
                                context_id,
                                """(() => {
                                    window.__flow2apiStreamChatReqs = [];
                                    if (!window.__flow2apiOrigFetch) {
                                        window.__flow2apiOrigFetch = window.fetch;
                                        window.fetch = async function(input, init) {
                                            const url = input instanceof Request ? input.url : String(input || "");
                                            if (url.includes("StreamChat")) {
                                                const ac = new AbortController();
                                                const origSignal = (init && init.signal) || (input instanceof Request ? input.signal : null);
                                                if (origSignal) {
                                                    if (origSignal.aborted) ac.abort();
                                                    else origSignal.addEventListener("abort", () => { try { ac.abort(); } catch (_) {} }, { once: true });
                                                }
                                                const entry = {
                                                    url,
                                                    status: 0,
                                                    chunks: "",
                                                    done: false,
                                                    error: "",
                                                    _waiters: [],
                                                    notify: () => {
                                                        const ws = entry._waiters.splice(0);
                                                        for (const w of ws) {
                                                            try { w(); } catch (_) {}
                                                        }
                                                    },
                                                    abort: () => {
                                                        try { ac.abort(); } catch (_) {}
                                                        entry.done = true;
                                                        entry.notify();
                                                    },
                                                };
                                                window.__flow2apiStreamChatReqs.push(entry);
                                                try {
                                                    const nextInit = Object.assign({}, init || {}, { signal: ac.signal });
                                                    const resp = await window.__flow2apiOrigFetch.call(this, input, nextInit);
                                                    entry.status = resp.status;
                                                    const clone = resp.clone();
                                                    (async () => {
                                                        try {
                                                            const reader = clone.body.getReader();
                                                            const dec = new TextDecoder();
                                                            while (true) {
                                                                const { done, value } = await reader.read();
                                                                if (done) break;
                                                                entry.chunks += dec.decode(value, { stream: true });
                                                            }
                                                        } catch (e) {
                                                            entry.error = String(e);
                                                        } finally {
                                                            entry.done = true;
                                                            entry.notify();
                                                        }
                                                    })();
                                                    return resp;
                                                } catch (e) {
                                                    entry.error = String(e);
                                                    entry.done = true;
                                                    entry.notify();
                                                    throw e;
                                                }
                                            }
                                            return window.__flow2apiOrigFetch.apply(this, arguments);
                                        };
                                    }
                                    return true;
                                })()""",
                                await_promise=False,
                            )

                            cookie_bar_rect = await bidi.evaluate(
                                context_id,
                                """(() => {
                                    const el = document.querySelector(".glue-cookie-notification-bar__accept");
                                    if (!el) return null;
                                    const r = el.getBoundingClientRect();
                                    const vw = window.innerWidth || document.documentElement.clientWidth || 1280;
                                    const vh = window.innerHeight || document.documentElement.clientHeight || 720;
                                    if (r.width <= 0 || r.height <= 0 || r.left < 0 || r.top < 0 || r.right > vw || r.bottom > vh) return null;
                                    return { x: Math.round(r.left + r.width / 2), y: Math.round(r.top + r.height / 2) };
                                })()""",
                                await_promise=False,
                            )
                            if isinstance(cookie_bar_rect, dict):
                                await bidi.command(
                                    "input.performActions",
                                    {
                                        "context": context_id,
                                        "actions": [
                                            {
                                                "type": "pointer",
                                                "id": "mouse",
                                                "parameters": {"pointerType": "mouse"},
                                                "actions": [
                                                    {
                                                        "type": "pointerMove",
                                                        "x": int(cookie_bar_rect["x"]),
                                                        "y": int(cookie_bar_rect["y"]),
                                                        "duration": 180,
                                                    },
                                                    {"type": "pointerDown", "button": 0},
                                                    {"type": "pointerUp", "button": 0},
                                                ],
                                            }
                                        ],
                                    },
                                )
                                await asyncio.sleep(0.2)

                            pm_rect = None
                            for _ in range(40):
                                pm_rect = await bidi.evaluate(
                                    context_id,
                                    """(() => {
                                        const wiz = window.WIZ_global_data || {};
                                        const pm = document.querySelector(".ProseMirror");
                                        if (!pm) return null;
                                        const r = pm.getBoundingClientRect();
                                        const vw = window.innerWidth || document.documentElement.clientWidth || 1280;
                                        const vh = window.innerHeight || document.documentElement.clientHeight || 720;
                                        if (r.width <= 0 || r.height <= 0 || r.left < 0 || r.top < 0 || r.right > vw || r.bottom > vh) return null;
                                        const cx = Math.max(4, Math.min(vw - 4, Math.round(r.left + r.width / 2)));
                                        const cy = Math.max(4, Math.min(vh - 4, Math.round(r.top + r.height / 2)));
                                        return {
                                            x: cx,
                                            y: cy,
                                            bl: String(wiz.cfb2h || ""),
                                            fSid: String(wiz.FdrFJe || ""),
                                            at: String(wiz.SNlM0e || "")
                                        };
                                    })()""",
                                    await_promise=False,
                                )
                                if isinstance(pm_rect, dict):
                                    if pm_rect.get("bl") and pm_rect.get("fSid"):
                                        self._cached_wiz = {
                                            "bl": str(pm_rect.get("bl") or ""),
                                            "fSid": str(pm_rect.get("fSid") or ""),
                                            "at": str(pm_rect.get("at") or ""),
                                        }
                                    break
                                await asyncio.sleep(0.2)
                            if not isinstance(pm_rect, dict):
                                raise RuntimeError("Camoufox 页面未找到 .ProseMirror 输入框")

                            await bidi.command(
                                "input.performActions",
                                {
                                    "context": context_id,
                                    "actions": [
                                        {
                                            "type": "pointer",
                                            "id": "mouse",
                                            "parameters": {"pointerType": "mouse"},
                                            "actions": [
                                                {
                                                    "type": "pointerMove",
                                                    "x": int(pm_rect["x"]),
                                                    "y": int(pm_rect["y"]),
                                                    "duration": 180,
                                                },
                                                {"type": "pointerDown", "button": 0},
                                                {"type": "pointerUp", "button": 0},
                                            ],
                                        }
                                    ],
                                },
                            )
                            await asyncio.sleep(0.15)

                            await bidi.evaluate(
                                context_id,
                                """(() => {
                                    const pm = document.querySelector('.ProseMirror');
                                    if (pm && (pm.textContent || '').trim()) {
                                        pm.focus();
                                        document.execCommand('selectAll', false, null);
                                        document.execCommand('delete', false, null);
                                    }
                                })()""",
                                await_promise=False,
                            )

                            bulk_inserted = False
                            if len(full_prompt) > 8:
                                bulk_text = full_prompt[:-4]
                                try:
                                    inserted_len = await bidi.call_function(
                                        context_id,
                                        """(bulkText) => {
                                            const pm = document.querySelector(".ProseMirror");
                                            if (!pm) return 0;
                                            pm.focus();
                                            document.execCommand("insertText", false, bulkText);
                                            return (pm.textContent || "").length;
                                        }""",
                                        arguments=[_to_bidi_local_value(bulk_text)],
                                        await_promise=False,
                                    )
                                    if isinstance(inserted_len, (int, float)) and int(inserted_len) >= len(bulk_text):
                                        bulk_inserted = True
                                except Exception:
                                    bulk_inserted = False

                            key_actions: List[Dict[str, Any]] = []
                            if bulk_inserted:
                                for ch in full_prompt[-4:]:
                                    key_actions.append({"type": "keyDown", "value": ch})
                                    key_actions.append({"type": "pause", "duration": 8})
                                    key_actions.append({"type": "keyUp", "value": ch})
                                    key_actions.append({"type": "pause", "duration": 12})
                            else:
                                for ch in full_prompt:
                                    key_actions.append({"type": "keyDown", "value": ch})
                                    key_actions.append({"type": "pause", "duration": 2})
                                    key_actions.append({"type": "keyUp", "value": ch})
                                    key_actions.append({"type": "pause", "duration": 3})
                            await bidi.command(
                                "input.performActions",
                                {
                                    "context": context_id,
                                    "actions": [
                                        {
                                            "type": "key",
                                            "id": "keyboard",
                                            "actions": key_actions,
                                        }
                                    ],
                                },
                            )
                            await asyncio.sleep(0.25)

                            gen_rect = None
                            for _ in range(10):
                                gen_rect = await bidi.evaluate(
                                    context_id,
                                    """(() => {
                                        const btn = document.querySelector("button.generate-icon-button:not([disabled])");
                                        if (!btn || btn.disabled) return null;
                                        const r = btn.getBoundingClientRect();
                                        const vw = window.innerWidth || document.documentElement.clientWidth || 1280;
                                        const vh = window.innerHeight || document.documentElement.clientHeight || 720;
                                        if (r.width <= 0 || r.height <= 0 || r.left < 0 || r.top < 0 || r.right > vw || r.bottom > vh) return null;
                                        return {
                                            x: Math.max(4, Math.min(vw - 4, Math.round(r.left + r.width / 2))),
                                            y: Math.max(4, Math.min(vh - 4, Math.round(r.top + r.height / 2)))
                                        };
                                    })()""",
                                    await_promise=False,
                                )
                                if isinstance(gen_rect, dict):
                                    break
                                await asyncio.sleep(0.15)
                            if not isinstance(gen_rect, dict):
                                raise RuntimeError(
                                    "Camoufox 页面未找到可点击的 generate-icon-button"
                                )
                            self._last_stream_chat_at = time.monotonic()
                            await bidi.command(
                                "input.performActions",
                                {
                                    "context": context_id,
                                    "actions": [
                                        {
                                            "type": "pointer",
                                            "id": "mouse",
                                            "parameters": {"pointerType": "mouse"},
                                            "actions": [
                                                {
                                                    "type": "pointerMove",
                                                    "x": int(gen_rect["x"]),
                                                    "y": int(gen_rect["y"]),
                                                    "duration": 180,
                                                },
                                                {"type": "pointerDown", "button": 0},
                                                {"type": "pointerUp", "button": 0},
                                            ],
                                        }
                                    ],
                                },
                            )

                            stream_deadline = time.monotonic() + max(30.0, float(timeout or 90))
                            sc_status = 0
                            full_chunks = ""
                            wait_sc_fn = """(minLen, waitMs) => new Promise((resolve) => {
                                const check = () => {
                                    const reqs = window.__flow2apiStreamChatReqs || [];
                                    if (reqs.length <= minLen) return null;
                                    const last = reqs[reqs.length - 1];
                                    if (!last.done) return { pending: true, entry: last };
                                    return { status: last.status, done: true, chunks: last.chunks || "", error: last.error || "" };
                                };
                                const cur = check();
                                if (cur && cur.done) {
                                    resolve(cur);
                                    return;
                                }
                                let settled = false;
                                const timer = setTimeout(() => {
                                    if (settled) return;
                                    settled = true;
                                    const latest = check();
                                    resolve(latest && latest.done ? latest : null);
                                }, Math.max(80, Math.min(2000, Number(waitMs) || 1500)));
                                if (cur && cur.entry && Array.isArray(cur.entry._waiters)) {
                                    cur.entry._waiters.push(() => {
                                        if (settled) return;
                                        settled = true;
                                        clearTimeout(timer);
                                        const latest = check();
                                        resolve(latest && latest.done ? latest : null);
                                    });
                                }
                            })"""
                            while time.monotonic() < stream_deadline:
                                rem_ms = max(100, min(1500, int((stream_deadline - time.monotonic()) * 1000)))
                                st = await bidi.call_function(
                                    context_id,
                                    wait_sc_fn,
                                    arguments=[_to_bidi_local_value(0), _to_bidi_local_value(rem_ms)],
                                    await_promise=True,
                                    timeout=15.0,
                                )
                                if isinstance(st, dict) and st.get("done"):
                                    sc_status = int(st.get("status") or 0)
                                    full_chunks = str(st.get("chunks") or "")
                                    break
                            if full_chunks and (
                                "ask_for_permission" in full_chunks
                                or not FlowFrontendMixin._extract_stream_chat_media_ids(
                                    FlowFrontendMixin._parse_batchexecute_frames(full_chunks)
                                )
                            ) and "PUBLIC_ERROR_" not in full_chunks:
                                perm_rect = None
                                for _ in range(12):
                                    await asyncio.sleep(0.5)
                                    perm_rect = await bidi.evaluate(
                                        context_id,
                                        """(() => {
                                            const msgs = Array.from(document.querySelectorAll("flow-permission-message"));
                                            if (!msgs.length) return null;
                                            const lastMsg = msgs[msgs.length - 1];
                                            lastMsg.scrollIntoView({ block: "center" });
                                            const rows = Array.from(lastMsg.querySelectorAll(".option-row"));
                                            const target = rows[1] || rows[0];
                                            if (!target) return null;
                                            const r = target.getBoundingClientRect();
                                            const vw = window.innerWidth || document.documentElement.clientWidth || 1280;
                                            const vh = window.innerHeight || document.documentElement.clientHeight || 720;
                                            if (r.width <= 0 || r.height <= 0 || r.left < 0 || r.top < 0 || r.right > vw || r.bottom > vh) return null;
                                            return {
                                                x: Math.max(4, Math.min(vw - 4, Math.round(r.left + r.width / 2))),
                                                y: Math.max(4, Math.min(vh - 4, Math.round(r.top + r.height / 2)))
                                            };
                                        })()""",
                                        await_promise=False,
                                    )
                                    if isinstance(perm_rect, dict):
                                        break
                                if isinstance(perm_rect, dict):
                                    prev_count = int(
                                        await bidi.evaluate(
                                            context_id,
                                            "(window.__flow2apiStreamChatReqs || []).length",
                                            await_promise=False,
                                        )
                                        or 1
                                    )
                                    await bidi.command(
                                        "input.performActions",
                                        {
                                            "context": context_id,
                                            "actions": [
                                                {
                                                    "type": "pointer",
                                                    "id": "mouse",
                                                    "parameters": {"pointerType": "mouse"},
                                                    "actions": [
                                                        {
                                                            "type": "pointerMove",
                                                            "x": int(perm_rect["x"]),
                                                            "y": int(perm_rect["y"]),
                                                            "duration": 250,
                                                        },
                                                        {"type": "pointerDown", "button": 0},
                                                        {"type": "pause", "duration": 80},
                                                        {"type": "pointerUp", "button": 0},
                                                    ],
                                                }
                                            ],
                                        },
                                    )
                                    full_chunks = ""
                                    perm_deadline = time.monotonic() + max(30.0, float(timeout or 90))
                                    while time.monotonic() < perm_deadline:
                                        rem_ms = max(100, min(1500, int((perm_deadline - time.monotonic()) * 1000)))
                                        st2 = await bidi.call_function(
                                            context_id,
                                            wait_sc_fn,
                                            arguments=[
                                                _to_bidi_local_value(prev_count),
                                                _to_bidi_local_value(rem_ms),
                                            ],
                                            await_promise=True,
                                            timeout=15.0,
                                        )
                                        if isinstance(st2, dict) and st2.get("done"):
                                            sc_status = int(st2.get("status") or 0)
                                            full_chunks = str(st2.get("chunks") or "")
                                            break

                            if not full_chunks:
                                raise RuntimeError("Camoufox StreamChat 未返回响应流")
                            if "PUBLIC_ERROR_" in full_chunks:
                                err_match = re.search(
                                    r"PUBLIC_ERROR_[A-Z0-9_]+", full_chunks
                                )
                                err_code = (
                                    err_match.group(0)
                                    if err_match
                                    else "PUBLIC_ERROR_UNKNOWN"
                                )
                                self._consecutive_risk_failures += 1
                                raise RuntimeError(
                                    f"Flow StreamChat 返回错误: {err_code} (status={sc_status})"
                                )

                            frames = FlowFrontendMixin._parse_batchexecute_frames(
                                full_chunks
                            )
                            media_ids = FlowFrontendMixin._extract_stream_chat_media_ids(
                                frames
                            )
                            if not media_ids and media_kind == "video":
                                for rec in FlowFrontendMixin._extract_frontend_video_records(
                                    frames, project_id
                                ):
                                    if rec and FlowFrontendMixin._is_uuid(rec[0]):
                                        media_ids.append(str(rec[0]))
                            if not media_ids:
                                debug_logger.log_warning(
                                    f"[Camoufox] StreamChat ({media_kind}) 未提取到 media_id, preview={full_chunks[:600]!r}"
                                )
                                self._consecutive_risk_failures += 1
                                raise RuntimeError(
                                    f"Camoufox StreamChat 未提取到 media_id (len={len(full_chunks)})"
                                )
                            last_attempt_err = None
                            break
                        except Exception as attempt_exc:
                            last_attempt_err = attempt_exc
                            if attempt_idx == 0:
                                debug_logger.log_warning(
                                    f"[Camoufox] StreamChat ({media_kind}) 首次尝试失败 ({attempt_exc}), 执行预热重载重试"
                                )
                                continue
                            raise
                    if last_attempt_err is not None:
                        raise last_attempt_err

                    resolved_media: List[Dict[str, Any]] = []
                    for mid in media_ids:
                        as29s_res = await bidi.call_function(
                            context_id,
                            """async function(cfg) {
                                const wiz = window.WIZ_global_data || {};
                                const bl = wiz.cfb2h || cfg.cachedBl || "boq_labs-ai-sandbox-frontend_20261105.10_p0";
                                const fSid = wiz.FdrFJe || cfg.cachedFSid || "";
                                const at = wiz.SNlM0e || cfg.cachedAt || "";
                                const q = new URLSearchParams({
                                    rpcids: "as29s",
                                    "source-path": `/project/${cfg.projectId}/edit/${cfg.mediaId}`,
                                    bl: bl,
                                    "f.sid": fSid,
                                    hl: "zh-CN",
                                    _reqid: String(Math.floor(100000 + Math.random() * 900000)),
                                    rt: "c"
                                });
                                const body = new URLSearchParams({
                                    "f.req": JSON.stringify([[["as29s", JSON.stringify([cfg.mediaId]), null, "generic"]]]),
                                    "at": at
                                });
                                const resp = await fetch(`https://flow.google.com/_/AiSandboxAngularFrontend/data/batchexecute?${q.toString()}`, {
                                    method: "POST",
                                    credentials: "include",
                                    headers: {
                                        "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
                                        "x-same-domain": "1"
                                    },
                                    body: body.toString()
                                });
                                return { status: resp.status, text: await resp.text() };
                            }""",
                            arguments=[
                                _to_bidi_local_value(
                                    {
                                        "projectId": project_id,
                                        "mediaId": mid,
                                        "cachedBl": self._cached_wiz.get("bl") or "",
                                        "cachedFSid": self._cached_wiz.get("fSid") or "",
                                        "cachedAt": self._cached_wiz.get("at") or "",
                                    }
                                )
                            ],
                            await_promise=True,
                            timeout=30.0,
                        )
                        as29s_text = (
                            str(as29s_res.get("text") or "")
                            if isinstance(as29s_res, dict)
                            else ""
                        )
                        as29s_frames = (
                            FlowFrontendMixin._parse_batchexecute_frames(
                                as29s_text
                            )
                            if as29s_text
                            else []
                        )
                        fife_url = (
                            FlowFrontendMixin._find_flow_content_url(
                                as29s_frames,
                                "video" if media_kind == "video" else "image",
                            )
                            if as29s_frames
                            else ""
                        )
                        if not fife_url and as29s_text:
                            kind_token = "video" if media_kind == "video" else "image"
                            url_match = re.search(
                                rf"https://(?:flow-content\.google|storage\.googleapis\.com/ai-sandbox-videofx)/{kind_token}/[^\s\"'\\]+",
                                as29s_text,
                            )
                            fife_url = (
                                url_match.group(0) if url_match else ""
                            )
                        if fife_url:
                            if media_kind == "video":
                                resolved_media.append(
                                    {
                                        "name": mid,
                                        "projectId": project_id,
                                        "video": {
                                            "fifeUrl": fife_url,
                                            "mediaGenerationId": mid,
                                        },
                                    }
                                )
                            else:
                                resolved_media.append(
                                    {
                                        "name": mid,
                                        "projectId": project_id,
                                        "image": {
                                            "generatedImage": {
                                                "fifeUrl": fife_url,
                                                "mediaGenerationId": mid,
                                            }
                                        },
                                    }
                                )
                    self._consecutive_risk_failures = 0
                    return {
                        "media": resolved_media,
                        "mediaIds": media_ids,
                        "frames": frames,
                        "responseText": full_chunks,
                        "rawLength": len(full_chunks),
                        "session_id": f";{int(time.time() * 1000)}",
                        "fingerprint": self.get_last_fingerprint(),
                    }
                except asyncio.CancelledError:
                    try:
                        await bidi.evaluate(
                            context_id,
                            """(() => {
                                const reqs = window.__flow2apiStreamChatReqs || [];
                                for (const item of reqs) {
                                    if (item && !item.done && typeof item.abort === "function") {
                                        item.abort();
                                    }
                                }
                                return true;
                            })()""",
                            await_promise=False,
                            timeout=3.0,
                        )
                    except Exception:
                        pass
                    raise
                finally:
                    try:
                        await self._sync_live_cookies_to_db(token_id)
                    except Exception:
                        pass
            finally:
                self._solve_inflight = max(0, self._solve_inflight - 1)
                self.note_idle()

    async def refresh_google_cookies(
        self, *, token_id: int, token_proxy_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """Refresh bound account's Google/Flow cookies using Camoufox."""
        if not token_id:
            raise ValueError("Cookie 刷新必须指定 Token ID")
        async with self._semaphore:
            self._solve_inflight += 1
            try:
                _, bidi, context_id = await self._get_or_create_shared_browser(
                    token_proxy_url=token_proxy_url, token_id=token_id
                )
                eval_meta_js = r"""(() => {
                    const wiz = window.WIZ_global_data || {};
                    let email = String(wiz.oPEP7c || '');
                    if (!email && document.documentElement) {
                        const m = document.documentElement.innerHTML.match(/"oPEP7c"\s*:\s*"([^"@]+@[^"\\]+)"/);
                        if (m) email = m[1];
                    }
                    return {
                        url: String(location.href || ''),
                        email: email,
                        bl: String(wiz.cfb2h || ''),
                        fSid: String(wiz.FdrFJe || ''),
                        at: String(wiz.SNlM0e || '')
                    };
                })()"""
                async with self._boost_frames():
                    await bidi.command(
                        "browsingContext.navigate",
                        {
                            "context": context_id,
                            "url": FLOW_PROJECTS_URL,
                            "wait": "complete",
                        },
                        timeout=35.0,
                    )
                    page_meta = await bidi.evaluate(
                        context_id, eval_meta_js, await_promise=False
                    )
                    current_url = str((page_meta or {}).get("url") or "")
                    email = str((page_meta or {}).get("email") or "")
                    if not email:
                        if "accounts.google.com" not in current_url:
                            passive_url = (
                                "https://accounts.google.com/ServiceLogin?passive=1209600&osid=1"
                                f"&continue={quote(FLOW_PROJECTS_URL, safe='')}"
                                f"&followup={quote(FLOW_PROJECTS_URL, safe='')}"
                            )
                            try:
                                await bidi.command(
                                    "browsingContext.navigate",
                                    {
                                        "context": context_id,
                                        "url": passive_url,
                                        "wait": "complete",
                                    },
                                    timeout=25.0,
                                )
                            except Exception:
                                pass
                        for _ in range(20):
                            await asyncio.sleep(0.5)
                            try:
                                page_meta = await bidi.evaluate(
                                    context_id, eval_meta_js, await_promise=False
                                )
                                current_url = str((page_meta or {}).get("url") or "")
                                email = str((page_meta or {}).get("email") or "")
                                if urlparse(current_url).hostname == "flow.google.com" and email:
                                    break
                                has_input = await bidi.evaluate(
                                    context_id,
                                    "Boolean(document.querySelector('input[type=\"email\"], input[type=\"password\"]'))",
                                    await_promise=False,
                                )
                                if "accounts.google.com" in current_url and has_input:
                                    break
                            except Exception:
                                pass
                    if urlparse(current_url).hostname != "flow.google.com":
                        raise RuntimeError(
                            "Flow 登录已失效，请重新同步 Google Cookies"
                        )
                    if (page_meta or {}).get("bl") and (page_meta or {}).get("fSid"):
                        self._cached_wiz = {
                            "bl": str(page_meta.get("bl") or ""),
                            "fSid": str(page_meta.get("fSid") or ""),
                            "at": str(page_meta.get("at") or ""),
                        }
                serialized = await self._sync_live_cookies_to_db(
                    token_id, update_db=False
                )
                if not serialized:
                    exported = await self._export_live_google_cookies()
                    serialized = json.dumps(
                        exported, ensure_ascii=False, separators=(",", ":")
                    )
                return {
                    "google_cookies": serialized,
                    "email": email,
                }
            finally:
                self._solve_inflight = max(0, self._solve_inflight - 1)
                self.note_idle()

    async def get_custom_token(
        self,
        website_url: str,
        website_key: str,
        action: str = "homepage",
        enterprise: bool = False,
    ) -> Optional[str]:
        res = await self.get_custom_score(
            website_url=website_url,
            website_key=website_key,
            verify_url="",
            action=action,
            enterprise=enterprise,
        )
        return str((res or {}).get("token") or "").strip() or None

    async def get_custom_score(
        self,
        website_url: str,
        website_key: str,
        verify_url: str,
        action: str = "homepage",
        enterprise: bool = False,
    ) -> Dict[str, Any]:
        """Run score detector or custom reCAPTCHA verification inside Camoufox."""
        async with self._semaphore:
            self._solve_inflight += 1
            try:
                started_at = time.time()
                _, bidi, _api_ctx = await self._get_or_create_shared_browser()
                context_id = await self._ensure_captcha_context_id()
                async with self._boost_frames():
                    await bidi.command(
                        "browsingContext.navigate",
                        {
                            "context": context_id,
                            "url": website_url,
                            "wait": "complete",
                        },
                        timeout=35.0,
                    )
                    await self._send_trusted_gesture(context_id)
                    await asyncio.sleep(2.0)

                    execute_target = (
                        "grecaptcha.enterprise.execute"
                        if enterprise
                        else "grecaptcha.execute"
                    )
                    ready_target = (
                        "grecaptcha.enterprise.ready"
                        if enterprise
                        else "grecaptcha.ready"
                    )
                    token_expr = f"""
                    new Promise((resolve, reject) => {{
                        const timer = setTimeout(() => reject(new Error('timeout')), 25000);
                        try {{
                            {ready_target}(() => {{
                                {execute_target}({json.dumps(website_key)}, {{ action: {json.dumps(action)} }})
                                    .then(t => {{ clearTimeout(timer); resolve(t); }})
                                    .catch(e => {{ clearTimeout(timer); reject(e); }});
                            }});
                        }} catch (e) {{
                            clearTimeout(timer);
                            reject(e);
                        }}
                    }})
                    """
                    token = await bidi.evaluate(
                        context_id,
                        token_expr,
                        await_promise=True,
                        user_activation=True,
                        timeout=30.0,
                    )
                    token_elapsed_ms = int((time.time() - started_at) * 1000)

                    verify_result: Dict[str, Any] = {}
                    verify_elapsed_ms = 0
                    if verify_url:
                        v_start = time.time()
                        for _ in range(20):
                            dom_info = await bidi.evaluate(
                                context_id,
                                r"""
                                (() => {
                                    const bodyText = (document.body && document.body.innerText) || '';
                                    const match = bodyText.match(/Your score is\s*:\s*([0-9.]+)/i) || bodyText.match(/score\s*[:=]\s*([0-9.]+)/i);
                                    return {
                                        score: match ? parseFloat(match[1]) : null,
                                        raw_text: bodyText.slice(0, 2000),
                                        url: location.href,
                                        title: document.title
                                    };
                                })()
                                """,
                                await_promise=False,
                            )
                            if isinstance(dom_info, dict) and isinstance(
                                dom_info.get("score"), (int, float)
                            ):
                                verify_elapsed_ms = int(
                                    (time.time() - v_start) * 1000
                                )
                                verify_result = {
                                    "success": True,
                                    "score": dom_info["score"],
                                    "source": "camoufox_dom",
                                    "raw_text": dom_info.get("raw_text") or "",
                                    "page_url": dom_info.get("url") or "",
                                    "page_title": dom_info.get("title") or "",
                                }
                                break
                            await asyncio.sleep(0.5)
                return {
                    "token": str(token or "") or None,
                    "token_elapsed_ms": token_elapsed_ms,
                    "verify_mode": "browser_page_dom",
                    "verify_elapsed_ms": verify_elapsed_ms,
                    "verify_http_status": 200 if verify_result.get("success") else None,
                    "verify_result": verify_result,
                }
            except Exception as exc:
                debug_logger.log_warning(
                    f"[Camoufox] 自定义分数测试失败: {exc}"
                )
                return {
                    "token": None,
                    "verify_mode": "browser_page",
                    "verify_elapsed_ms": 0,
                    "verify_http_status": None,
                    "verify_result": {"success": False, "error": str(exc)},
                }
            finally:
                self._solve_inflight = max(0, self._solve_inflight - 1)
                self.note_idle()


