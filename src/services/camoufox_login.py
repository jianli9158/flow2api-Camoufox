"""Interactive Camoufox browser login sessions and LiveView WebSocket frame/input streaming."""

from __future__ import annotations

import asyncio
from collections import deque
import json
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse
import uuid

from ..core.logger import debug_logger
from .bidi_transport import BiDiClient, LocalSocks5Relay, wait_for_bidi_endpoint
from .camoufox_fingerprint import (
    CAMOUFOX_FIREFOX_MAJOR,
    _start_stdout_drain,
    build_camoufox_env,
    ensure_camoufox_executable,
    load_or_create_account_camoufox_config,
    write_camoufox_user_js,
)
from .camoufox_profile import (
    ProfileLease,
    _clear_stale_firefox_profile_locks,
    acquire_profile_lease,
    acquire_profile_login_lease,
    clear_site_cookies_in_profile,
    normalize_browser_profile_dir,
    persist_account_login_artifacts,
    release_profile_login_lease,
    resolve_account_profile_dir,
    restore_startup_cache,
    save_startup_cache,
)


def has_display() -> bool:
    if sys.platform in ("darwin", "win32"):
        return True
    return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))


WEBDRIVER_KEYS: Dict[str, str] = {
    "Cancel": "\uE001",
    "Help": "\uE002",
    "Backspace": "\uE003",
    "Tab": "\uE004",
    "Clear": "\uE005",
    "Enter": "\uE007",
    "Shift": "\uE008",
    "Control": "\uE009",
    "Alt": "\uE00A",
    "Pause": "\uE00B",
    "Escape": "\uE00C",
    "PageUp": "\uE00E",
    "PageDown": "\uE00F",
    "End": "\uE010",
    "Home": "\uE011",
    "ArrowLeft": "\uE012",
    "ArrowUp": "\uE013",
    "ArrowRight": "\uE014",
    "ArrowDown": "\uE015",
    "Insert": "\uE016",
    "Delete": "\uE017",
    "F1": "\uE031",
    "F2": "\uE032",
    "F3": "\uE033",
    "F4": "\uE034",
    "F5": "\uE035",
    "F6": "\uE036",
    "F7": "\uE037",
    "F8": "\uE038",
    "F9": "\uE039",
    "F10": "\uE03A",
    "F11": "\uE03B",
    "F12": "\uE03C",
    "Meta": "\uE03D",
    "ZenkakuHankaku": "\uE040",
}


class CamoufoxLiveView:
    """Streams live browser screenshots and forwards mouse/keyboard/wheel/paste inputs over WebSocket (AIStudio2API liveview.go)."""

    def __init__(self) -> None:
        self.status: str = "starting"
        self.message: str = "正在启动 Camoufox 浏览器..."
        self.frame_meta: Optional[Dict[str, Any]] = None
        self.frame_b64: str = ""
        self.frame_seq: int = 0
        self.dialog: Optional[Dict[str, Any]] = None
        self.dialog_seq: int = 0
        self.contexts: List[str] = []
        self._bidi: Optional[BiDiClient] = None
        self._inputs: deque[Dict[str, Any]] = deque(maxlen=256)
        self._subscribers: Set[Any] = set()
        self._last_frame_requested_at: float = time.monotonic()
        self._input_event = asyncio.Event()
        self._wake_event = asyncio.Event()
        self._closed = False
        self._tasks: List[asyncio.Task] = []

    def add_subscriber(self, subscriber: Any = None) -> Any:
        token = subscriber if subscriber is not None else object()
        self._subscribers.add(token)
        self._last_frame_requested_at = time.monotonic()
        self._wake_event.set()
        return token

    def remove_subscriber(self, subscriber: Any) -> None:
        self._subscribers.discard(subscriber)

    def snapshot(self, *, include_frame: bool = True) -> Dict[str, Any]:
        if include_frame:
            self._last_frame_requested_at = time.monotonic()
        payload: Dict[str, Any] = {
            "status": self.status,
            "message": self.message,
            "frame_seq": self.frame_seq,
            "dialog": self.dialog,
            "dialog_seq": self.dialog_seq,
        }
        if include_frame and self.frame_b64:
            payload["frame_b64"] = self.frame_b64
            payload["frame_meta"] = self.frame_meta
        return payload

    def set_status(self, status: str, message: str = "") -> None:
        self.status = status
        if message:
            self.message = message
        self._wake_event.set()

    async def attach(self, bidi: BiDiClient, root_context_id: str) -> None:
        self._bidi = bidi
        self.contexts = [root_context_id]
        bidi.add_listener(self._on_bidi_event)
        try:
            await bidi.command(
                "session.subscribe",
                {
                    "events": [
                        "browsingContext.contextCreated",
                        "browsingContext.contextDestroyed",
                        "browsingContext.userPromptOpened",
                        "browsingContext.userPromptClosed",
                    ]
                },
                timeout=10.0,
            )
        except Exception:
            pass
        self._tasks = [
            asyncio.create_task(self._capture_loop()),
            asyncio.create_task(self._input_loop()),
        ]

    async def close(self) -> None:
        self._closed = True
        self._wake_event.set()
        self._input_event.set()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._bidi is not None:
            self._bidi.remove_listener(self._on_bidi_event)
            self._bidi = None

    def active_context(self) -> str:
        return self.contexts[-1] if self.contexts else ""

    def _on_bidi_event(
        self, method: str, params: Optional[Dict[str, Any]] = None
    ) -> None:
        params = params or {}
        ctx_id = str(params.get("context") or "")
        if method == "browsingContext.contextCreated":
            if params.get("parent") is None and ctx_id and ctx_id not in self.contexts:
                self.contexts.append(ctx_id)
        elif method == "browsingContext.contextDestroyed":
            if ctx_id in self.contexts and len(self.contexts) > 1:
                self.contexts.remove(ctx_id)
        elif method == "browsingContext.userPromptOpened":
            self.dialog = {
                "context": ctx_id,
                "kind": str(params.get("type") or "alert"),
                "message": str(params.get("message") or ""),
                "value": str(params.get("defaultValue") or ""),
            }
            self.dialog_seq += 1
            self._wake_event.set()
        elif method == "browsingContext.userPromptClosed":
            self.dialog = None
            self.dialog_seq += 1
            self._wake_event.set()

    def enqueue_input(self, item: Dict[str, Any]) -> None:
        if not isinstance(item, dict):
            return
        itype = str(item.get("type") or "")
        if itype == "dialog":
            asyncio.create_task(
                self._answer_dialog(
                    bool(item.get("accept", True)), str(item.get("text") or "")
                )
            )
            return
        if self._inputs:
            last = self._inputs[-1]
            if last.get("type") == itype:
                if (
                    itype == "pointer"
                    and item.get("action") == "move"
                    and last.get("action") == "move"
                ):
                    self._inputs[-1] = item
                    self._input_event.set()
                    return
                if (
                    itype == "wheel"
                    and last.get("x") == item.get("x")
                    and last.get("y") == item.get("y")
                ):
                    last["dx"] = float(last.get("dx") or 0) + float(item.get("dx") or 0)
                    last["dy"] = float(last.get("dy") or 0) + float(item.get("dy") or 0)
                    self._input_event.set()
                    return
        self._inputs.append(item)
        self._input_event.set()

    async def _answer_dialog(self, accept: bool, text: str) -> None:
        dialog = self.dialog
        bidi = self._bidi
        if not dialog or not bidi:
            return
        params: Dict[str, Any] = {
            "context": dialog["context"],
            "accept": accept,
        }
        if dialog.get("kind") == "prompt" and accept:
            params["userText"] = text
        try:
            await bidi.command("browsingContext.handleUserPrompt", params, timeout=5.0)
        except Exception:
            pass

    async def _capture_loop(self) -> None:
        while not self._closed:
            now = time.monotonic()
            if len(self._subscribers) > 0:
                self._last_frame_requested_at = now
            elif (
                self.frame_seq > 0
                and (now - self._last_frame_requested_at) > 5.0
            ):
                await asyncio.sleep(0.4)
                continue
            started = now
            bidi = self._bidi
            ctx_id = self.active_context()
            if bidi and ctx_id and not bidi._closed:
                try:
                    shot = await bidi.command(
                        "browsingContext.captureScreenshot",
                        {
                            "context": ctx_id,
                            "format": {"type": "image/jpeg", "quality": 0.78},
                        },
                        timeout=4.0,
                    )
                    b64_data = str(shot.get("data") or "")
                    if b64_data and b64_data != self.frame_b64:
                        meta = await bidi.evaluate(
                            ctx_id,
                            "(() => [location.href, document.title || '', innerWidth || 1280, innerHeight || 800, devicePixelRatio || 1])()",
                            await_promise=False,
                            timeout=2.5,
                        )
                        if isinstance(meta, list) and len(meta) >= 5:
                            self.frame_meta = {
                                "type": "frame",
                                "context": ctx_id,
                                "url": str(meta[0] or ""),
                                "title": str(meta[1] or ""),
                                "width": float(meta[2] or 1280),
                                "height": float(meta[3] or 800),
                                "scale": float(meta[4] or 1),
                            }
                        elif self.frame_meta is None:
                            self.frame_meta = {
                                "type": "frame",
                                "context": ctx_id,
                                "url": "",
                                "title": "",
                                "width": 1280.0,
                                "height": 800.0,
                                "scale": 1.0,
                            }
                        self.frame_b64 = b64_data
                        self.frame_seq += 1
                        self._wake_event.set()
                except Exception:
                    pass
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.05, 0.14 - elapsed))

    async def _input_loop(self) -> None:
        while not self._closed:
            await self._input_event.wait()
            self._input_event.clear()
            if self._closed:
                break
            bidi = self._bidi
            if not bidi or bidi._closed:
                continue
            batch: List[Dict[str, Any]] = []
            while self._inputs:
                batch.append(self._inputs.popleft())
            for item in batch:
                ctx_id = str(item.get("context") or "") or self.active_context()
                if not ctx_id:
                    continue
                source = self._build_action_source(item)
                if not source:
                    continue
                try:
                    await bidi.command(
                        "input.performActions",
                        {"context": ctx_id, "actions": [source]},
                        timeout=5.0,
                    )
                except Exception:
                    pass

    def _build_action_source(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        itype = str(item.get("type") or "")
        meta = self.frame_meta or {}
        max_w = max(1, int(meta.get("width") or 1280))
        max_h = max(1, int(meta.get("height") or 800))
        x = min(max(int(round(float(item.get("x") or 0))), 0), max_w - 1)
        y = min(max(int(round(float(item.get("y") or 0))), 0), max_h - 1)
        action = str(item.get("action") or "")

        if itype == "pointer":
            move = {"type": "pointerMove", "x": x, "y": y}
            button = int(item.get("button") or 0)
            if action == "move":
                acts = [move]
            elif action == "down":
                acts = [move, {"type": "pointerDown", "button": button}]
            elif action == "up":
                acts = [move, {"type": "pointerUp", "button": button}]
            elif action == "click":
                acts = [
                    move,
                    {"type": "pointerDown", "button": button},
                    {"type": "pointerUp", "button": button},
                ]
            else:
                return None
            return {
                "type": "pointer",
                "id": "mouse",
                "parameters": {"pointerType": "mouse"},
                "actions": acts,
            }
        if itype == "wheel":
            dx = int(round(float(item.get("dx") or 0)))
            dy = int(round(float(item.get("dy") or 0)))
            return {
                "type": "wheel",
                "id": "wheel",
                "actions": [{"type": "scroll", "x": x, "y": y, "deltaX": dx, "deltaY": dy}],
            }
        if itype == "key":
            key_name = str(item.get("key") or "")
            key_val = WEBDRIVER_KEYS.get(key_name) or (
                key_name if len(key_name) == 1 else ""
            )
            if not key_val or action not in ("down", "up"):
                return None
            return {
                "type": "key",
                "id": "keyboard",
                "actions": [
                    {
                        "type": "keyDown" if action == "down" else "keyUp",
                        "value": key_val,
                    }
                ],
            }
        if itype == "text":
            text = str(item.get("text") or "")
            key_actions: List[Dict[str, Any]] = []
            for ch in text:
                if ch == "\n":
                    val = WEBDRIVER_KEYS["Enter"]
                elif ch == "\t":
                    val = WEBDRIVER_KEYS["Tab"]
                elif ord(ch) < 0x20 or ord(ch) == 0x7F:
                    continue
                else:
                    val = ch
                key_actions.append({"type": "keyDown", "value": val})
                key_actions.append({"type": "keyUp", "value": val})
            if not key_actions:
                return None
            return {"type": "key", "id": "keyboard", "actions": key_actions}
        return None


FLOW_LOGIN_DETECT_SCRIPT = r"""
(async () => {
    const href = String(location.href || '');
    const host = String(location.hostname || '').toLowerCase();
    const result = {
        url: href,
        host: host,
        title: String(document.title || ''),
        is_flow_origin: host === 'flow.google.com' || host === 'labs.google',
        is_auth_page: host.includes('accounts.google') || href.includes('/about') || href.includes('ServiceLogin'),
        has_onboarding_dialog: false,
        workspace_ready: false,
        xsrf_token: '',
        email: '',
        project_id: '',
        local_storage: []
    };
    try {
        result.local_storage = Object.keys(localStorage).sort().map(name => ({
            name,
            value: String(localStorage.getItem(name) ?? '')
        }));
    } catch (e) {}

    const projMatch = href.match(/\/project\/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})/);
    if (projMatch) {
        result.project_id = projMatch[1];
    }

    if (result.is_flow_origin && !result.is_auth_page) {
        try {
            const consentHints = [
                '隐私权政策', 'privacy policy', '体验并参与塑造', '您的数据和', 'your data and',
                '下一步', '上一步', '同意', '接受'
            ];
            const keywords = [
                '下一步', '继续', '同意', '接受', '开始', '知道了', '跳过', '完成',
                'next', 'continue', 'start', 'got it', 'agree', 'accept', 'skip', 'done'
            ];
            const dialogs = Array.from(
                document.querySelectorAll('[role="dialog"], [role="alertdialog"], [aria-modal="true"], .cdk-overlay-pane')
            ).filter(el => {
                if (el.offsetWidth <= 50 || el.offsetHeight <= 50) return false;
                const t = String(el.innerText || '').toLowerCase();
                return consentHints.some(h => t.includes(h)) || Array.from(el.querySelectorAll('button, [role="button"]')).some(
                    b => keywords.some(k => String(b.innerText || b.textContent || '').trim().toLowerCase() === k)
                );
            });
            if (dialogs.length > 0) {
                result.has_onboarding_dialog = true;
                let didScroll = false;
                for (const dlg of dialogs) {
                    const scrollables = [dlg, ...Array.from(dlg.querySelectorAll('*'))];
                    for (const s of scrollables) {
                        if (s.scrollHeight > s.clientHeight + 4 && s.scrollTop + s.clientHeight < s.scrollHeight - 2) {
                            s.scrollTop = s.scrollHeight;
                            s.dispatchEvent(new Event('scroll', { bubbles: true }));
                            s.dispatchEvent(new WheelEvent('wheel', { deltaY: 800, bubbles: true }));
                            didScroll = true;
                        }
                    }
                }
                if (didScroll) {
                    await new Promise(r => setTimeout(r, 280));
                }
                for (const dlg of dialogs) {
                    const btns = Array.from(dlg.querySelectorAll('button, [role="button"]'));
                    for (const btn of btns) {
                        if (btn.disabled || btn.getAttribute('aria-disabled') === 'true') continue;
                        const txt = String(btn.innerText || btn.textContent || '').trim().toLowerCase();
                        if (txt && txt.length <= 24 && keywords.some(k => txt === k || txt.includes(k))) {
                            btn.click();
                            break;
                        }
                    }
                }
            }
            const bodyText = String(document.body?.innerText || '');
            const isLoading = bodyText.includes('正在加载') || bodyText.includes('Loading...');
            const hasProjectUI = Boolean(
                result.project_id
                || document.querySelector('a[href*="/project/"]')
                || bodyText.includes('新建项目')
                || bodyText.includes('New project')
            );
            result.workspace_ready = !result.has_onboarding_dialog && !isLoading && hasProjectUI;
        } catch (e) {}
    }
    try {
        const wiz = window.WIZ_global_data;
        if (wiz && typeof wiz === 'object') {
            if (typeof wiz.SNlM0e === 'string' && wiz.SNlM0e) {
                result.xsrf_token = wiz.SNlM0e;
            }
            for (const v of Object.values(wiz)) {
                if (typeof v === 'string' && /^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$/i.test(v.trim())) {
                    result.email = v.trim().toLowerCase();
                    break;
                }
            }
        }
    } catch (e) {}

    if (!result.email) {
        try {
            const values = [];
            for (const el of document.querySelectorAll('[aria-label],[data-email]')) {
                values.push(el.getAttribute('data-email') || '');
                values.push(el.getAttribute('aria-label') || '');
            }
            values.push(document.body?.innerText || '');
            const m = values.join('\n').match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
            if (m) {
                result.email = m[0].toLowerCase();
            }
        } catch (e) {}
    }

    if (result.is_flow_origin && !result.is_auth_page && !result.email) {
        try {
            const resp = await fetch('/fx/api/auth/session', { credentials: 'include' });
            if (resp.ok) {
                const data = await resp.json();
                const sessionEmail = data?.user?.email;
                if (typeof sessionEmail === 'string' && sessionEmail.includes('@')) {
                    result.email = sessionEmail.trim().toLowerCase();
                }
            }
        } catch (e) {}
    }
    return result;
})()
"""


class CamoufoxLoginSession:
    """Isolated Camoufox login session that waits for Google Flow authentication and extracts cookies, storageState, fingerprint, and startupCache."""

    def __init__(
        self,
        session_id: str,
        *,
        mode: str = "auto",
        proxy_url: Optional[str] = None,
        token_id: Optional[int] = None,
        browser_profile_dir: Optional[str] = None,
        clean_login: bool = False,
        auto_save_token: bool = True,
        remark: Optional[str] = None,
        project_id: Optional[str] = None,
        project_name: Optional[str] = None,
        timeout_seconds: float = 300.0,
        user_data_root: Optional[Path] = None,
    ) -> None:
        self.session_id = session_id
        self.mode = mode
        self.proxy_url = (proxy_url or "").strip() or None
        self.token_id = token_id
        self.browser_profile_dir = normalize_browser_profile_dir(browser_profile_dir)
        self.clean_login = bool(clean_login)
        self.auto_save_token = auto_save_token
        self.remark = remark
        self.project_id = project_id
        self.project_name = project_name
        self.timeout_seconds = max(30.0, float(timeout_seconds or 300.0))
        self.user_data_root = user_data_root or (
            Path(__file__).resolve().parent.parent.parent / "browser_data_rt"
        )
        self.live_view = CamoufoxLiveView()
        self.status: str = "starting"
        self.message: str = "正在启动 Camoufox 浏览器..."
        self.error: Optional[str] = None
        self.result: Optional[Dict[str, Any]] = None
        self.created_at: float = time.time()
        self.finished_at: Optional[float] = None
        self.headless: bool = False
        self._process: Optional[asyncio.subprocess.Process] = None
        self._stdout_drain_task: Optional[asyncio.Task] = None
        self._bidi: Optional[BiDiClient] = None
        self._relay: Optional[LocalSocks5Relay] = None
        self._temp_account_dir: Optional[Path] = None
        self._is_ephemeral_staging: bool = False
        self._is_explicit_profile_dir: bool = bool(self.browser_profile_dir)
        self._reused_existing_profile: bool = False
        self._lease_account_dir: Optional[Path] = None
        self._profile_lease: Optional[ProfileLease] = None
        self._profile_dir: Optional[Path] = None
        self._task: Optional[asyncio.Task] = None
        self._cancel_event = asyncio.Event()

    def set_state(self, status: str, message: str, error: Optional[str] = None) -> None:
        self.status = status
        self.message = message
        if error is not None:
            self.error = error
        if status in ("completed", "failed", "cancelled") and self.finished_at is None:
            self.finished_at = time.time()
        self.live_view.set_status(status, message)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "status": self.status,
            "message": self.message,
            "error": self.error,
            "mode": "liveview" if self.headless else "window",
            "headless": self.headless,
            "has_display": has_display(),
            "token_id": self.token_id,
            "browser_profile_dir": self.browser_profile_dir,
            "reused_existing_profile": self._reused_existing_profile,
            "clean_login": self.clean_login,
            "auto_save_token": self.auto_save_token,
            "result": self.result,
            "frame_seq": self.live_view.frame_seq,
            "page_url": (self.live_view.frame_meta or {}).get("url") or "",
            "page_title": (self.live_view.frame_meta or {}).get("title") or "",
            "dialog": self.live_view.dialog,
        }

    async def start(
        self,
        on_complete: Optional[Callable[["CamoufoxLoginSession", Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None,
    ) -> None:
        self._task = asyncio.create_task(self._run(on_complete))

    async def cancel(self) -> None:
        self._cancel_event.set()
        if self.status not in ("completed", "failed"):
            self.set_state("cancelled", "浏览器登录已取消")
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        await self._cleanup(save_cache=False)

    async def _run(
        self,
        on_complete: Optional[Callable[["CamoufoxLoginSession", Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None,
    ) -> None:
        save_cache = False
        try:
            exe_path = await ensure_camoufox_executable()
            if self.mode == "liveview":
                self.headless = True
            elif self.mode == "window":
                self.headless = not has_display()
            else:
                self.headless = not has_display()

            if self.token_id or self.browser_profile_dir:
                account_dir, canonical_profile_dir = resolve_account_profile_dir(
                    self.user_data_root,
                    token_id=self.token_id,
                    profile_dir_name=self.browser_profile_dir,
                )
                self.browser_profile_dir = canonical_profile_dir
                self._is_ephemeral_staging = False
            else:
                account_dir = self.user_data_root / "pending" / f"login_{self.session_id}"
                self._is_ephemeral_staging = True

            try:
                from .browser_captcha import BrowserCaptchaService
                svc = BrowserCaptchaService._instance
                if svc is not None:
                    if self.token_id:
                        await svc.release_token_browsers(
                            int(self.token_id), reason="camoufox_relogin_start"
                        )
                    if hasattr(svc, "release_profile_browsers"):
                        await svc.release_profile_browsers(
                            account_dir, reason="camoufox_relogin_start"
                        )
            except Exception:
                pass

            acquire_profile_login_lease(
                account_dir, self.session_id, token_id=self.token_id
            )
            self._lease_account_dir = account_dir
            self._profile_lease = acquire_profile_lease(
                account_dir, holder=f"camoufox_login_{self.session_id}"
            )
            self._temp_account_dir = account_dir
            account_dir.mkdir(parents=True, exist_ok=True)

            fp_config = load_or_create_account_camoufox_config(
                account_dir,
                CAMOUFOX_FIREFOX_MAJOR,
                "en-US",
                "America/New_York",
            )

            relay_addr: Optional[Tuple[str, int]] = None
            if self.proxy_url:
                parsed = urlparse(self.proxy_url)
                if parsed.username or parsed.password or "socks" in (
                    parsed.scheme or ""
                ).lower():
                    self._relay = LocalSocks5Relay(self.proxy_url)
                    relay_addr = await self._relay.start()

            self._profile_dir = account_dir / "firefox-profile"
            self._reused_existing_profile = (
                self._profile_dir / "cookies.sqlite"
            ).is_file() or (self._profile_dir / "prefs.js").is_file()
            self._profile_dir.mkdir(parents=True, exist_ok=True)
            _clear_stale_firefox_profile_locks(self._profile_dir)
            if self.clean_login:
                clear_site_cookies_in_profile(account_dir)
                self._reused_existing_profile = False
            if not (self._profile_dir / "startupCache").exists():
                await asyncio.to_thread(
                    restore_startup_cache, account_dir, self._profile_dir
                )
            cache_dir = account_dir / "camoufox-cache"
            write_camoufox_user_js(
                self._profile_dir,
                fp_config,
                proxy_url=self.proxy_url,
                relay_addr=relay_addr,
                headless=self.headless,
                live_view=True,
                cache_dir=cache_dir,
                executable_path=exe_path,
            )

            args = [exe_path, "--remote-debugging-port=0"]
            if self.headless:
                args.extend(["-no-remote", "-headless"])
            else:
                args.append("-wait-for-browser")
            args.extend(["-profile", str(self._profile_dir)])

            env = build_camoufox_env(
                fp_config,
                headless=self.headless,
                live_view=True,
                profile_dir=self._profile_dir,
                executable_path=exe_path,
            )
            self._process = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
            )
            endpoint = await wait_for_bidi_endpoint(
                self._process, timeout=45.0
            )
            self._stdout_drain_task = _start_stdout_drain(self._process)
            self._bidi = await BiDiClient.connect(endpoint, timeout=30.0)
            capabilities: Dict[str, Any] = {
                "alwaysMatch": {
                    "unhandledPromptBehavior": {
                        "alert": "ignore",
                        "confirm": "ignore",
                        "prompt": "ignore",
                    }
                }
            }
            await self._bidi.command("session.new", {"capabilities": capabilities})

            tree = await self._bidi.command("browsingContext.getTree", {"maxDepth": 0})
            contexts = tree.get("contexts") or []
            if not contexts or not isinstance(contexts[0], dict):
                raise RuntimeError("Camoufox BiDi 未返回初始标签页")
            root_ctx = str(contexts[0].get("context") or "")
            if not root_ctx:
                raise RuntimeError("Camoufox BiDi 初始标签页无效")

            await self.live_view.attach(self._bidi, root_ctx)

            profile_note = (
                f"（已绑定并复用资料目录 {self.browser_profile_dir}）"
                if self._reused_existing_profile and self.browser_profile_dir
                else (
                    f"（已绑定资料目录 {self.browser_profile_dir}）"
                    if self.browser_profile_dir
                    else ""
                )
            )
            if self.headless:
                self.set_state(
                    "waiting",
                    f"请在下方远程浏览器画面中登录 Google 账号并进入 Flow 页面{profile_note}，登录成功后将自动拉取 Cookie 与缓存...",
                )
            else:
                self.set_state(
                    "waiting",
                    f"已弹出 Camoufox 浏览器窗口{profile_note}（也可在下方画面直接操作），请完成 Google 登录进入 Flow，成功后将自动抓取 Cookie 与缓存...",
                )
            try:
                await self._bidi.command(
                    "browsingContext.navigate",
                    {
                        "context": root_ctx,
                        "url": "https://flow.google.com/projects",
                        "wait": "none",
                    },
                    timeout=20.0,
                )
            except Exception as nav_exc:
                if "NS_ERROR_ABORT" not in str(nav_exc):
                    debug_logger.log_warning(f"[CamoufoxLogin] 初始导航提示: {nav_exc}")

            login_data = await self._wait_for_login()
            save_cache = True
            self.set_state(
                "saving",
                f"检测到 Flow 登录成功 ({login_data.get('email') or 'Google Account'})，正在导出 Cookie、StorageState 与浏览器缓存...",
            )

            await asyncio.sleep(1.2)
            exported = await self._export_login_state(login_data)

            await self._cleanup(save_cache=True)

            if on_complete is not None:
                saved_info = await on_complete(self, exported)
                exported.update(saved_info or {})

            final_token_id = exported.get("token_id") or self.token_id
            target_profile_hint = (
                exported.get("browser_profile_dir") or self.browser_profile_dir
            )
            if final_token_id and self._temp_account_dir and self._temp_account_dir.exists():
                try:
                    from .browser_captcha import BrowserCaptchaService
                    svc = BrowserCaptchaService._instance
                    if svc is not None:
                        await svc.release_token_browsers(
                            int(final_token_id), reason="camoufox_relogin_complete"
                        )
                except Exception:
                    pass
                target_dir, final_canonical = resolve_account_profile_dir(
                    self.user_data_root,
                    token_id=int(final_token_id),
                    profile_dir_name=target_profile_hint,
                )
                if self._temp_account_dir.resolve() != target_dir.resolve():
                    if self._is_ephemeral_staging and not target_dir.exists():
                        target_dir.parent.mkdir(parents=True, exist_ok=True)
                        await asyncio.to_thread(
                            shutil.move, str(self._temp_account_dir), str(target_dir)
                        )
                        _clear_stale_firefox_profile_locks(target_dir / "firefox-profile")
                        self._temp_account_dir = target_dir
                        self._is_ephemeral_staging = False
                    else:
                        await asyncio.to_thread(
                            persist_account_login_artifacts,
                            self._temp_account_dir,
                            target_dir,
                        )
                self.browser_profile_dir = final_canonical
                exported["browser_profile_dir"] = final_canonical
                exported["cache_persisted_dir"] = str(target_dir)
            elif not self._is_ephemeral_staging and self._temp_account_dir:
                exported["browser_profile_dir"] = self.browser_profile_dir
                exported["cache_persisted_dir"] = str(self._temp_account_dir)
            self.result = exported
            self.set_state(
                "completed",
                f"浏览器登录并拉取缓存完成！账号: {exported.get('email') or '已同步'}",
            )
        except asyncio.CancelledError:
            if self.status not in ("completed", "cancelled"):
                self.set_state("cancelled", "浏览器登录已取消")
        except Exception as exc:
            debug_logger.log_error(f"[CamoufoxLogin] 登录流程失败: {exc}")
            self.set_state("failed", f"浏览器登录失败: {exc}", error=str(exc))
        finally:
            await self._cleanup(save_cache=save_cache)
            if self._profile_lease is not None:
                self._profile_lease.release()
                self._profile_lease = None
            release_profile_login_lease(
                self._lease_account_dir, self.session_id, token_id=self.token_id
            )
            self._lease_account_dir = None
            if (
                self._is_ephemeral_staging
                and self._temp_account_dir
                and self._temp_account_dir.exists()
                and self._temp_account_dir.parent.name == "pending"
            ):
                shutil.rmtree(self._temp_account_dir, ignore_errors=True)

    async def _wait_for_login(self) -> Dict[str, Any]:
        deadline = time.monotonic() + self.timeout_seconds
        origins: Dict[str, Any] = {}
        session_cookie_names = {
            "SID",
            "__Secure-1PSID",
            "__Secure-3PSID",
            "SAPISID",
            "__Secure-1PAPISID",
            "__Secure-3PAPISID",
        }
        while time.monotonic() < deadline and not self._cancel_event.is_set():
            if self._process and self._process.returncode is not None:
                raise RuntimeError("Camoufox 登录窗口已被关闭，未完成登录")
            bidi = self._bidi
            ctx_id = self.live_view.active_context()
            if not bidi or bidi._closed or not ctx_id:
                await asyncio.sleep(0.5)
                continue
            try:
                info = await bidi.evaluate(
                    ctx_id,
                    FLOW_LOGIN_DETECT_SCRIPT,
                    await_promise=True,
                    timeout=6.0,
                )
            except Exception:
                await asyncio.sleep(0.5)
                continue

            if isinstance(info, dict):
                page_url = str(info.get("url") or "")
                parsed = urlparse(page_url)
                host = (parsed.hostname or "").lower()
                if parsed.scheme == "https" and (
                    host == "google.com"
                    or host.endswith(".google.com")
                    or host == "labs.google"
                ):
                    ls_items = info.get("local_storage")
                    if isinstance(ls_items, list) and ls_items:
                        origin_key = f"{parsed.scheme}://{parsed.netloc}"
                        origins[origin_key] = {
                            "origin": origin_key,
                            "localStorage": ls_items,
                        }

                is_about_landing = parsed.path.rstrip("/").endswith("/about")
                if info.get("is_flow_origin") and not info.get("is_auth_page"):
                    cookie_res = await bidi.command("storage.getCookies", {}, timeout=8.0)
                    raw_cookies = cookie_res.get("cookies") or []
                    found_session_cookies = {
                        str(c.get("name") or "")
                        for c in raw_cookies
                        if isinstance(c, dict)
                        and str(c.get("name") or "") in session_cookie_names
                    }
                    if is_about_landing:
                        if len(found_session_cookies) >= 2:
                            self.set_state(
                                "waiting",
                                "检测到账号停留在 Flow /about 页面（可能需要 Google 二次身份核验），请在浏览器中点击页面主按钮并完成“验证是您本人”...",
                            )
                        await asyncio.sleep(0.7)
                        continue
                    has_auth_signal = bool(
                        info.get("xsrf_token") or info.get("email")
                    )
                    if len(found_session_cookies) >= 2 and has_auth_signal:
                        if info.get("has_onboarding_dialog") or not info.get("workspace_ready"):
                            self.set_state(
                                "waiting",
                                "已登录进入 Flow，正在自动滚动并同意《隐私权政策》与引导条款（完成后自动保存）...",
                            )
                            await asyncio.sleep(0.7)
                            continue
                        return {
                            "email": str(info.get("email") or "").strip().lower(),
                            "xsrf_token": str(info.get("xsrf_token") or ""),
                            "project_id": str(info.get("project_id") or ""),
                            "page_url": page_url,
                            "origins": origins,
                        }
            await asyncio.sleep(0.6)
        raise RuntimeError("等待 Google Flow 登录超时，请重试")

    async def _export_login_state(self, login_data: Dict[str, Any]) -> Dict[str, Any]:
        assert self._bidi is not None
        in_page_meta: Dict[str, Any] = {}
        try:
            ctx = self.live_view.active_context()
            if ctx:
                meta_res = await self._bidi.evaluate(
                    ctx,
                    r"""(async () => {
                        const out = { email: '', project_id: '', credits: null, tier: '' };
                        try {
                            const wiz = window.WIZ_global_data || {};
                            const at = String(wiz.SNlM0e || '');
                            const bl = String(wiz.cfb2h || 'boq_labs-ai-sandbox-frontend_20260415.05_p0');
                            const fSid = String(wiz.FdrFJe || '');
                            const hl = String(document.documentElement.lang || navigator.language || 'en');
                            if (typeof wiz.oPEP7c === 'string' && wiz.oPEP7c.includes('@')) {
                                out.email = wiz.oPEP7c.trim().toLowerCase();
                            }
                            for (let w = 0; w < 8; w++) {
                                if (document.querySelector('a[href*="/project/"]') || /\/project\/[0-9a-f-]{36}/i.test(location.pathname)) break;
                                await new Promise(r => setTimeout(r, 400));
                            }
                            for (let i = 0; i < 4; i++) {
                                const btns = Array.from(document.querySelectorAll('.cdk-overlay-container button, [role="dialog"] button'));
                                const hit = btns.find(b => !b.disabled && ['下一步','继续','同意','接受','开始','知道了','跳过','next','continue','start','got it','agree','accept','skip'].some(k => (b.innerText || '').trim().toLowerCase().includes(k)));
                                if (!hit) break;
                                hit.click();
                                await new Promise(r => setTimeout(r, 350));
                            }
                            const m = location.pathname.match(/\/project\/([0-9a-f-]{36})/i);
                            if (m) out.project_id = m[1];
                            if (!out.project_id) {
                                for (const a of document.querySelectorAll('a[href*="/project/"]')) {
                                    const am = (a.getAttribute('href') || '').match(/\/project\/([0-9a-f-]{36})/i);
                                    if (am) { out.project_id = am[1]; break; }
                                }
                            }
                            if (at) {
                                const callRpc = async (rpcId, argJson) => {
                                    const qs = new URLSearchParams({
                                        'rpcids': rpcId,
                                        'source-path': location.pathname || '/projects',
                                        'bl': bl,
                                        'hl': hl,
                                        'soc-app': '1',
                                        'soc-platform': '1',
                                        'soc-device': '1',
                                        'rt': 'c'
                                    });
                                    if (fSid) qs.set('f.sid', fSid);
                                    const body = new URLSearchParams({
                                        'f.req': JSON.stringify([[[rpcId, argJson, null, 'generic']]]),
                                        'at': at
                                    });
                                    const resp = await fetch(`/_/LabsTailwindUi/data/batchexecute?${qs.toString()}`, {
                                        method: 'POST',
                                        credentials: 'include',
                                        headers: {
                                            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
                                            'X-Same-Domain': '1'
                                        },
                                        body: body.toString()
                                    });
                                    return await resp.text();
                                };
                                try {
                                    const credText = await callRpc('i945dd', '["projects"]');
                                    const tm = credText.match(/PAYGATE_TIER_[A-Z_]+/);
                                    if (tm) out.tier = tm[0];
                                    const numMatch = credText.match(/\\?"\[(\d+),\\?"PAYGATE_TIER/);
                                    if (numMatch) out.credits = parseInt(numMatch[1], 10);
                                } catch (e) {}
                                if (!out.project_id) {
                                    try {
                                        const projText = await callRpc('jHPbke', JSON.stringify(['projects/*', [null, ['Flow Project']], [null, 22]]));
                                        const pm = projText.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i);
                                        if (pm) out.project_id = pm[0];
                                    } catch (e) {}
                                }
                            }
                        } catch (e) {}
                        return out;
                    })()""",
                    await_promise=True,
                    timeout=15.0,
                )
                if isinstance(meta_res, dict):
                    in_page_meta = meta_res
        except Exception as exc:
            debug_logger.log_warning(f"[CamoufoxLogin] 页面内预取项目与积分提示: {exc}")

        cookie_res = await self._bidi.command("storage.getCookies", {}, timeout=10.0)
        raw_cookies = cookie_res.get("cookies") or []
        flow_cookies: List[Dict[str, Any]] = []
        storage_cookies: List[Dict[str, Any]] = []

        for item in raw_cookies:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            domain = str(item.get("domain") or "").strip()
            path = str(item.get("path") or "/")
            if not name or not domain:
                continue
            clean_domain = domain.lstrip(".").lower()
            if not (
                clean_domain == "google.com"
                or clean_domain.endswith(".google.com")
                or clean_domain == "labs.google"
                or clean_domain.endswith(".labs.google")
            ):
                continue
            val_obj = item.get("value")
            value = (
                str(val_obj.get("value") or "")
                if isinstance(val_obj, dict)
                else str(val_obj or "")
            )
            expiry = item.get("expiry")
            expires = (
                float(expiry)
                if isinstance(expiry, (int, float)) and expiry > 0
                else -1.0
            )
            same_site_raw = str(item.get("sameSite") or "lax").strip().lower()
            same_site = (
                same_site_raw.capitalize()
                if same_site_raw in ("none", "lax", "strict")
                else "Lax"
            )
            http_only = bool(item.get("httpOnly", False))
            secure = bool(item.get("secure", True))
            host_only = not domain.startswith(".") and (
                clean_domain == "flow.google.com" or name.startswith("__Host-")
            )
            cookie_entry: Dict[str, Any] = {
                "name": name,
                "value": value,
                "domain": domain,
                "path": path,
                "expires": expires,
                "httpOnly": http_only,
                "secure": secure,
                "sameSite": same_site,
            }
            if host_only:
                cookie_entry["hostOnly"] = True
            flow_cookies.append(cookie_entry)
            storage_cookies.append(cookie_entry)

        if not flow_cookies:
            raise RuntimeError("未从 Camoufox 浏览器导出到有效的 Google/Flow Cookie")

        flow_cookies.sort(
            key=lambda c: (c.get("domain") or "", c.get("name") or "", c.get("path") or "")
        )
        origins_map = login_data.get("origins") or {}
        storage_origins = sorted(
            origins_map.values(), key=lambda o: str(o.get("origin") or "")
        )
        storage_state = {
            "cookies": storage_cookies,
            "origins": storage_origins,
        }
        if self._temp_account_dir and self._temp_account_dir.exists():
            (self._temp_account_dir / "storage-state.json").write_text(
                json.dumps(storage_state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        google_cookies_json = json.dumps(
            flow_cookies, ensure_ascii=False, separators=(",", ":")
        )
        return {
            "email": login_data.get("email") or in_page_meta.get("email") or "",
            "project_id": (
                self.project_id
                or login_data.get("project_id")
                or in_page_meta.get("project_id")
                or ""
            ),
            "credits": in_page_meta.get("credits"),
            "user_paygate_tier": in_page_meta.get("tier") or "",
            "page_url": login_data.get("page_url") or "https://flow.google.com/projects",
            "cookie_count": len(flow_cookies),
            "google_cookies": google_cookies_json,
        }

    async def _cleanup(self, save_cache: bool = False) -> None:
        await self.live_view.close()
        drain_task = self._stdout_drain_task
        bidi = self._bidi
        proc = self._process
        relay = self._relay
        profile_dir = self._profile_dir
        temp_account_dir = self._temp_account_dir

        self._stdout_drain_task = None
        self._bidi = None
        self._process = None
        self._relay = None
        self._profile_dir = None

        if drain_task is not None and not drain_task.done():
            drain_task.cancel()

        if bidi is not None:
            try:
                await asyncio.wait_for(bidi.command("browser.close", {}), timeout=4.0)
            except Exception:
                pass
            await bidi.close()

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
            if save_cache and temp_account_dir is not None and temp_account_dir.exists():
                await asyncio.to_thread(
                    save_startup_cache, profile_dir, temp_account_dir
                )
                _clear_stale_firefox_profile_locks(profile_dir)
            elif self._is_ephemeral_staging:
                shutil.rmtree(profile_dir, ignore_errors=True)
            else:
                _clear_stale_firefox_profile_locks(profile_dir)


class CamoufoxLoginManager:
    """Manages active Camoufox interactive login sessions for the Admin UI."""

    _instance: Optional["CamoufoxLoginManager"] = None

    def __init__(self) -> None:
        self._sessions: Dict[str, CamoufoxLoginSession] = {}
        self._lock = asyncio.Lock()

    @classmethod
    def get_instance(cls) -> "CamoufoxLoginManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _prune_sessions(self) -> None:
        now = time.time()
        for sid, sess in list(self._sessions.items()):
            if sess.status in ("completed", "failed", "cancelled"):
                ended_at = sess.finished_at or sess.created_at
                age = now - ended_at
                if age > 60.0 and sess.live_view.frame_b64:
                    sess.live_view.frame_b64 = ""
                if age > 300.0:
                    self._sessions.pop(sid, None)
            elif now - sess.created_at > max(600.0, sess.timeout_seconds + 300.0):
                self._sessions.pop(sid, None)

    def get_session(self, session_id: str) -> Optional[CamoufoxLoginSession]:
        self._prune_sessions()
        sess = self._sessions.get(session_id)
        if sess is not None and sess.status in ("starting", "waiting", "saving"):
            sess.live_view._last_frame_requested_at = time.monotonic()
        return sess

    def get_active_session(self) -> Optional[CamoufoxLoginSession]:
        self._prune_sessions()
        for sess in list(self._sessions.values()):
            if sess.status in ("starting", "waiting", "saving"):
                return sess
        return None

    async def start_session(
        self,
        *,
        mode: str = "auto",
        proxy_url: Optional[str] = None,
        token_id: Optional[int] = None,
        browser_profile_dir: Optional[str] = None,
        clean_login: bool = False,
        auto_save_token: bool = True,
        remark: Optional[str] = None,
        project_id: Optional[str] = None,
        project_name: Optional[str] = None,
        timeout_seconds: float = 300.0,
        on_complete: Optional[Callable[[CamoufoxLoginSession, Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None,
    ) -> CamoufoxLoginSession:
        async with self._lock:
            for _, old_sess in list(self._sessions.items()):
                if old_sess.status in ("starting", "waiting"):
                    await old_sess.cancel()
            self._prune_sessions()
            session_id = uuid.uuid4().hex[:16]
            session = CamoufoxLoginSession(
                session_id,
                mode=mode,
                proxy_url=proxy_url,
                token_id=token_id,
                browser_profile_dir=browser_profile_dir,
                clean_login=clean_login,
                auto_save_token=auto_save_token,
                remark=remark,
                project_id=project_id,
                project_name=project_name,
                timeout_seconds=timeout_seconds,
            )
            self._sessions[session_id] = session
            await session.start(on_complete=on_complete)
            return session

    async def cancel_session(self, session_id: str) -> bool:
        sess = self._sessions.get(session_id)
        if not sess:
            return False
        await sess.cancel()
        return True
