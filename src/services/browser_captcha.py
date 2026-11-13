"""Camoufox + WebDriver BiDi browser pool service."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass
import os
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid

from ..core.config import config
from ..core.logger import debug_logger
from .camoufox_bidi import CamoufoxWorker

# Compatibility alias for callers/tests referencing TokenBrowser
TokenBrowser = CamoufoxWorker


@dataclass
class _BrowserRequest:
    ref: str
    slot_id: int
    browser: Any
    token_id: Optional[int]
    generation: int
    owner: Any
    borrower: Any = None
    owner_callback: Any = None
    hold: bool = False
    in_browser_op: bool = True
    failed: bool = False
    risk_reported: bool = False
    fingerprint: Optional[Dict[str, Any]] = None


_ActiveRequest = _BrowserRequest

@dataclass
class _SlotRecovery:
    phase: str = "ready"
    failure_streak: int = 0
    cooldown_until: float = 0.0
    recovery_count: int = 0
    reason: str = ""
    needs_recycle: bool = False
    retiring: bool = False
    task: Optional[asyncio.Task] = None


class BrowserCaptchaQueueTimeout(RuntimeError):
    """No browser slot became available before the admission deadline."""


class BrowserCaptchaService:
    """Camoufox + WebDriver BiDi multi-slot browser pool service (singleton)."""

    _instance: Optional["BrowserCaptchaService"] = None
    _lock = asyncio.Lock()
    QUEUE_WAIT_SECONDS = 30.0
    DRAIN_WAIT_SECONDS = 120.0
    RECOVERY_TIMEOUT_SECONDS = 60.0
    RECOVERY_BACKOFF_SECONDS = (5.0, 15.0, 30.0, 60.0)
    MAINTENANCE_INTERVAL_SECONDS = 15.0

    def __init__(self, db=None):
        self.db = db
        self.website_key = "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV"
        self.base_user_data_dir = os.path.join(os.getcwd(), "browser_data_rt")
        self._browsers: Dict[int, CamoufoxWorker] = {}
        self._browsers_lock = asyncio.Lock()
        self._slot_allocation_lock = asyncio.Lock()
        self._slot_reservations: Dict[int, int] = {}
        self._slot_condition = asyncio.Condition(self._slot_allocation_lock)
        self._slot_recovery: Dict[int, _SlotRecovery] = {}
        self._requests: Dict[str, _BrowserRequest] = {}
        self._cleanup_tasks: set[asyncio.Task] = set()
        self._closed = False
        self._queue_timeouts = 0
        self._queue_wait_total_seconds = 0.0
        self._admitted_requests = 0

        self._browser_count = 1
        self._round_robin_index = 0
        self._stats = {"req_total": 0, "gen_ok": 0, "gen_fail": 0, "api_403": 0}
        self._idle_reaper_task: Optional[asyncio.Task] = None

    async def _ensure_idle_reaper(self):
        if self._closed:
            return
        if self._idle_reaper_task is None or self._idle_reaper_task.done():
            self._idle_reaper_task = asyncio.create_task(self._idle_reaper_loop())

    async def _idle_reaper_loop(self):
        while True:
            try:
                await asyncio.sleep(self.MAINTENANCE_INTERVAL_SECONDS)
                async with self._slot_condition:
                    if self._closed:
                        return
                    for slot_id, state in self._slot_recovery.items():
                        if (
                            state.retiring
                            and state.needs_recycle
                            and state.phase == "cooldown"
                        ):
                            self._resume_recovery_locked(slot_id)
                idle_ttl = int(getattr(config, "browser_idle_ttl_seconds", 600) or 600)
                async with self._browsers_lock:
                    browsers = list(self._browsers.items())
                for slot_id, browser in browsers:
                    reserved = False
                    try:
                        if not browser.has_shared_browser():
                            continue
                        if browser.idle_seconds() < idle_ttl:
                            continue
                        async with self._slot_condition:
                            if self._is_slot_busy_for_allocation(slot_id) or any(
                                r.slot_id == slot_id and r.hold
                                for r in self._requests.values()
                            ):
                                continue
                            self._reserve_slot_locked(slot_id)
                            reserved = True
                        await browser.recycle_browser(
                            reason=f"idle_ttl_{idle_ttl}s", rotate_profile=False
                        )
                    except Exception as e:
                        debug_logger.log_warning(
                            f"[BrowserCaptcha] idle reaper failed: {e}"
                        )
                    finally:
                        if reserved:
                            await self._release_slot_reservation(slot_id)
            except asyncio.CancelledError:
                return
            except Exception as e:
                debug_logger.log_warning(
                    f"[BrowserCaptcha] idle reaper loop error: {e}"
                )

    @classmethod
    async def get_instance(cls, db=None) -> "BrowserCaptchaService":
        async with cls._lock:
            instance = cls._instance
            if instance is not None:
                if instance._closed:
                    raise RuntimeError("Browser captcha service is closing")
                return instance

            instance = cls(db)
            try:
                await instance._load_browser_count()
                if instance._closed:
                    raise RuntimeError("Browser captcha service is closing")
                await instance._ensure_idle_reaper()
                if instance._closed:
                    raise RuntimeError("Browser captcha service is closing")
            except BaseException:
                instance._closed = True
                if instance._idle_reaper_task:
                    instance._idle_reaper_task.cancel()
                    await asyncio.gather(
                        instance._idle_reaper_task, return_exceptions=True
                    )
                raise
            cls._instance = instance
            return instance

    async def _load_browser_count(self):
        if self.db:
            try:
                captcha_config = await self.db.get_captcha_config()
                self._browser_count = max(1, captcha_config.browser_count)
                debug_logger.log_info(
                    f"[BrowserCaptcha] 浏览器数量配置: {self._browser_count}"
                )
            except Exception as e:
                debug_logger.log_warning(
                    f"[BrowserCaptcha] 加载 browser_count 配置失败: {e}，使用默认值 1"
                )
                self._browser_count = 1

    async def reload_browser_count(self):
        old_count = self._browser_count
        await self._load_browser_count()
        await self._ensure_idle_reaper()
        async with self._slot_condition:
            if self._browser_count < old_count:
                for slot_id in range(self._browser_count, old_count):
                    state = self._slot_recovery.setdefault(slot_id, _SlotRecovery())
                    state.phase = "draining"
                    state.needs_recycle = True
                    state.retiring = True
                    state.reason = "slot_removed"
                    state.cooldown_until = 0.0
                    self._resume_recovery_locked(slot_id)
            self._slot_condition.notify_all()
        if self._browser_count > old_count:
            await self.warmup_browser_slots()

    async def _warmup_browser_slot(
        self,
        browser_id: int,
        *,
        token_id: Optional[int] = None,
        token_proxy_url: Optional[str] = None,
    ):
        async with self._slot_condition:
            if self._closed or self._is_slot_busy_for_allocation(browser_id):
                return
            self._reserve_slot_locked(browser_id)
        try:
            browser = await self._get_or_create_browser(browser_id)
            await browser._get_or_create_shared_browser(
                token_proxy_url=token_proxy_url,
                token_id=token_id,
            )
            debug_logger.log_info(
                f"[BrowserCaptcha] warmed browser slot {browser_id} "
                f"(token_id={token_id or '-'})"
            )
        except Exception as e:
            debug_logger.log_warning(
                f"[BrowserCaptcha] warmup for slot {browser_id} failed: {e}"
            )
        finally:
            await self._release_slot_reservation(browser_id)

    async def _get_warmup_token_specs(self) -> List[Tuple[int, Optional[str]]]:
        if not self.db or not hasattr(self.db, "get_active_tokens"):
            return []

        try:
            tokens = await self.db.get_active_tokens()
        except Exception as e:
            debug_logger.log_warning(
                f"[BrowserCaptcha] 读取预热 token 列表失败: {e}"
            )
            return []

        specs: List[Tuple[int, Optional[str]]] = []
        for token in tokens:
            if getattr(token, "ban_reason", None):
                continue
            token_key = getattr(token, "id", None)
            if token_key is None:
                continue
            google_cookies = str(getattr(token, "google_cookies", "") or "").strip()
            session_token = str(getattr(token, "st", "") or "").strip()
            if not (google_cookies or session_token):
                continue
            proxy_url = str(getattr(token, "captcha_proxy_url", "") or "").strip()
            specs.append((int(token_key), proxy_url or None))
            if len(specs) >= self._browser_count:
                break
        return specs

    async def warmup_browser_slots(self):
        token_specs = await self._get_warmup_token_specs()
        token_spec_by_id = {
            token_id: (token_id, token_proxy_url)
            for token_id, token_proxy_url in token_specs
        }
        warmup_targets: List[Tuple[int, Optional[Tuple[int, Optional[str]]]]] = []

        async with self._browsers_lock:
            if token_spec_by_id:
                pending_specs = [
                    spec
                    for token_id, spec in token_spec_by_id.items()
                    if token_id
                    not in {
                        self._slot_bound_token_id(browser_id)
                        for browser_id in range(self._browser_count)
                    }
                ]
                for browser_id in range(self._browser_count):
                    browser = self._browsers.get(browser_id)
                    bound_token_id = self._slot_bound_token_id(browser_id)
                    if browser is not None and bound_token_id in token_spec_by_id:
                        continue
                    if not pending_specs:
                        continue
                    warmup_targets.append((browser_id, pending_specs.pop(0)))
            elif not any(
                self._has_warmed_browser_for_allocation(browser_id)
                for browser_id in range(self._browser_count)
            ):
                warmup_targets = [(0, None)]

        tasks = [
            self._warmup_browser_slot(
                browser_id,
                token_id=spec[0] if spec is not None else None,
                token_proxy_url=spec[1] if spec is not None else None,
            )
            for browser_id, spec in warmup_targets
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _is_slot_busy_for_allocation(self, slot_id: int) -> bool:
        state = self._slot_recovery.get(slot_id)
        if state and state.phase not in {"ready", "probe"}:
            return True
        if any(
            request.slot_id == slot_id and request.in_browser_op
            for request in self._requests.values()
        ):
            return True
        if self._slot_reservations.get(slot_id, 0) > 0:
            return True
        browser = self._browsers.get(slot_id)
        return bool(browser and getattr(browser, "is_busy", lambda: False)())
    def _has_warmed_browser_for_allocation(self, slot_id: int) -> bool:
        browser = self._browsers.get(slot_id)
        return bool(browser and getattr(browser, "has_shared_browser", lambda: False)())

    def _slot_bound_token_id(self, slot_id: int) -> Optional[int]:
        browser = self._browsers.get(slot_id)
        bound_token_id = getattr(browser, "_shared_bound_token_id", None)
        try:
            return int(bound_token_id) if bound_token_id is not None else None
        except (TypeError, ValueError):
            return None

    def _reserve_slot_locked(self, slot_id: int):
        self._slot_reservations[slot_id] = self._slot_reservations.get(slot_id, 0) + 1

    async def _release_slot_reservation(self, slot_id: Optional[int]):
        if slot_id is None:
            return
        async with self._slot_condition:
            current = self._slot_reservations.get(slot_id, 0)
            if current <= 1:
                self._slot_reservations.pop(slot_id, None)
            else:
                self._slot_reservations[slot_id] = current - 1
            self._resume_recovery_locked(slot_id)
            self._slot_condition.notify_all()

    def _resume_recovery_locked(self, slot_id: int):
        state = self._slot_recovery.get(slot_id)
        if not state or not state.needs_recycle or self._closed:
            return
        if state.task and not state.task.done():
            return
        if state.cooldown_until > time.monotonic():
            return
        state.phase = "draining"
        state.task = asyncio.create_task(self._recover_slot(slot_id))

    async def _recover_slot(self, slot_id: int):
        state = self._slot_recovery[slot_id]
        deadline = time.monotonic() + self.DRAIN_WAIT_SECONDS
        try:
            async with self._slot_condition:
                while (
                    self._slot_reservations.get(slot_id, 0)
                    or any(r.slot_id == slot_id for r in self._requests.values())
                    or (
                        self._browsers.get(slot_id)
                        and self._browsers[slot_id].is_busy()
                    )
                ):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        state.reason = "drain_timeout"
                        break
                    try:
                        await asyncio.wait_for(self._slot_condition.wait(), remaining)
                    except asyncio.TimeoutError:
                        continue
                for ref, req in list(self._requests.items()):
                    if req.slot_id == slot_id:
                        self._requests.pop(ref, None)
                        if req.owner and req.owner_callback:
                            try:
                                req.owner.remove_done_callback(req.owner_callback)
                            except Exception:
                                pass
                self._slot_reservations.pop(slot_id, None)
                state.phase = "recovering"
                browser = self._browsers.get(slot_id)

            if browser:
                await asyncio.wait_for(
                    browser.recycle_browser(
                        reason=state.reason,
                        rotate_profile=not state.retiring or state.failure_streak > 0,
                    ),
                    timeout=self.RECOVERY_TIMEOUT_SECONDS,
                )
            async with self._slot_condition:
                state.needs_recycle = False
                if state.retiring:
                    state.retiring = False
                    if slot_id >= self._browser_count:
                        self._browsers.pop(slot_id, None)
                        self._slot_recovery.pop(slot_id, None)
                        return
                    if not state.failure_streak:
                        state.phase = "ready"
                        return
                state.recovery_count += 1
                state.phase = "cooldown"
                state.cooldown_until = (
                    time.monotonic()
                    + self.RECOVERY_BACKOFF_SECONDS[
                        min(
                            max(state.failure_streak - 1, 0),
                            len(self.RECOVERY_BACKOFF_SECONDS) - 1,
                        )
                    ]
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            async with self._slot_condition:
                state.phase = "cooldown"
                state.reason = "recovery_failed"
                state.cooldown_until = (
                    time.monotonic() + self.RECOVERY_BACKOFF_SECONDS[-1]
                )
        finally:
            async with self._slot_condition:
                if state.task is asyncio.current_task():
                    state.task = None
                self._slot_condition.notify_all()

    async def _finish_request(self, browser_ref, *, success: Optional[bool] = None):
        async with self._slot_condition:
            request = self._requests.get(browser_ref)
            if not request:
                return
            if (
                request.borrower is not None
                and request.borrower is not asyncio.current_task()
                and self._slot_reservations.get(request.slot_id, 0)
            ):
                if success is False:
                    request.failed = True
                return
            self._requests.pop(browser_ref)
            request.owner.remove_done_callback(request.owner_callback)
            state = self._slot_recovery.get(request.slot_id)
            if (
                state
                and state.phase == "probe"
                and not request.failed
                and success is True
                and request.generation
                == getattr(request.browser, "_runtime_generation", 0)
            ):
                state.phase = "ready"
                state.failure_streak = 0
            self._resume_recovery_locked(request.slot_id)
            self._slot_condition.notify_all()

    def _on_request_owner_done(self, request: _BrowserRequest):
        if self._closed or request.ref not in self._requests:
            return
        task = asyncio.create_task(self._finish_request(request.ref, success=False))
        self._cleanup_tasks.add(task)
        task.add_done_callback(self._cleanup_tasks.discard)

    @asynccontextmanager
    async def _borrow_browser(
        self,
        project_id=None,
        token_id=None,
        *,
        browser_ref=None,
        wait_timeout=None,
    ):
        request = None
        async with self._slot_condition:
            candidate = self._requests.get(browser_ref)
            if (
                candidate
                and candidate.token_id == token_id
                and not candidate.owner.done()
            ):
                state = self._slot_recovery.get(candidate.slot_id)
                if (
                    candidate.generation
                    == getattr(candidate.browser, "_runtime_generation", 0)
                    and not candidate.failed
                    and (not state or state.phase in {"ready", "probe"})
                    and not self._slot_reservations.get(candidate.slot_id, 0)
                ):
                    request = candidate
                    request.hold = False
                    request.in_browser_op = True
                    request.borrower = asyncio.current_task()
                    self._reserve_slot_locked(request.slot_id)
        if not request:
            if candidate and candidate.token_id == token_id:
                if self._slot_reservations.get(candidate.slot_id, 0):
                    raise RuntimeError("Browser captcha request is already submitting")
                await self._finish_request(candidate.ref, success=False)
            slot_id = await self._select_browser_id(
                project_id,
                token_id=token_id,
                timeout_seconds=wait_timeout,
            )
            try:
                browser = await self._get_or_create_browser(slot_id)
                request = _BrowserRequest(
                    ref=f"{slot_id}:lease-{uuid.uuid4().hex}/",
                    slot_id=slot_id,
                    browser=browser,
                    token_id=token_id,
                    generation=getattr(browser, "_runtime_generation", 0),
                    owner=asyncio.current_task(),
                    borrower=asyncio.current_task(),
                    in_browser_op=True,
                )
                request.owner_callback = lambda owner: self._on_request_owner_done(
                    request
                )
                async with self._slot_condition:
                    if self._closed:
                        raise RuntimeError("Browser captcha service is closed")
                    self._requests[request.ref] = request
                    request.owner.add_done_callback(request.owner_callback)
            except BaseException:
                await self._release_slot_reservation(slot_id)
                raise
        try:
            yield request
        except BaseException as exc:
            request.failed = True
            request.generation = getattr(request.browser, "_runtime_generation", 0)
            if isinstance(exc, Exception):
                setattr(exc, "browser_ref", request.ref)
                await self.report_error(request.ref, error_reason=str(exc))
            raise
        finally:
            request.borrower = None
            if not request.hold:
                await self._finish_request(request.ref, success=not request.failed)
            else:
                async with self._slot_condition:
                    request.in_browser_op = False
                    self._slot_condition.notify_all()
            await self._release_slot_reservation(request.slot_id)
    async def _select_browser_id(
        self,
        project_id: Optional[str],
        token_id: Optional[int] = None,
        timeout_seconds: Optional[float] = None,
    ) -> int:
        _ = project_id
        expected_token_id = CamoufoxWorker._normalize_token_key(token_id)
        wait_seconds = (
            self.QUEUE_WAIT_SECONDS
            if timeout_seconds is None
            else max(0, float(timeout_seconds))
        )
        queued_at = time.monotonic()
        deadline = queued_at + wait_seconds
        async with self._slot_condition:
            while True:
                if self._closed:
                    raise RuntimeError("Browser captcha service is closed")
                now = time.monotonic()
                for slot_id, state in self._slot_recovery.items():
                    if state.phase == "cooldown" and state.cooldown_until <= now:
                        if state.needs_recycle:
                            self._resume_recovery_locked(slot_id)
                        else:
                            state.phase = "probe"
                matching_idle_slot: Optional[int] = None
                unbound_warm_idle_slot: Optional[int] = None
                cold_idle_slot: Optional[int] = None
                unleased_idle_slot: Optional[int] = None
                leased_idle_slot: Optional[int] = None

                for offset in range(self._browser_count):
                    slot_id = (self._round_robin_index + offset) % self._browser_count
                    if self._is_slot_busy_for_allocation(slot_id):
                        continue

                    bound_token_id = self._slot_bound_token_id(slot_id)
                    if (
                        expected_token_id is not None
                        and bound_token_id == expected_token_id
                    ):
                        matching_idle_slot = slot_id
                        break
                    if expected_token_id is not None and any(
                        r.slot_id == slot_id
                        and r.hold
                        and CamoufoxWorker._normalize_token_key(r.token_id)
                        == expected_token_id
                        for r in self._requests.values()
                    ):
                        matching_idle_slot = slot_id
                        break

                    has_other_hold_lease = any(
                        r.slot_id == slot_id
                        and r.hold
                        and (
                            expected_token_id is None
                            or CamoufoxWorker._normalize_token_key(r.token_id)
                            != expected_token_id
                        )
                        for r in self._requests.values()
                    )
                    if has_other_hold_lease:
                        if leased_idle_slot is None:
                            leased_idle_slot = slot_id
                        continue

                    if unleased_idle_slot is None:
                        unleased_idle_slot = slot_id

                    browser = self._browsers.get(slot_id)
                    if browser is None:
                        if cold_idle_slot is None:
                            cold_idle_slot = slot_id
                        continue

                    if (
                        bound_token_id is None
                        and self._has_warmed_browser_for_allocation(slot_id)
                    ):
                        if unbound_warm_idle_slot is None:
                            unbound_warm_idle_slot = slot_id

                if matching_idle_slot is not None:
                    selected_slot = matching_idle_slot
                elif unbound_warm_idle_slot is not None:
                    selected_slot = unbound_warm_idle_slot
                elif cold_idle_slot is not None:
                    selected_slot = cold_idle_slot
                elif unleased_idle_slot is not None:
                    selected_slot = unleased_idle_slot
                else:
                    selected_slot = leased_idle_slot
                if selected_slot is not None:
                    self._round_robin_index = (selected_slot + 1) % self._browser_count
                    self._reserve_slot_locked(selected_slot)
                    self._admitted_requests += 1
                    self._queue_wait_total_seconds += time.monotonic() - queued_at
                    return selected_slot

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._queue_timeouts += 1
                    raise BrowserCaptchaQueueTimeout(
                        "Browser captcha queue wait timeout"
                    )
                wake_delays = [
                    state.cooldown_until - time.monotonic()
                    for state in self._slot_recovery.values()
                    if state.phase == "cooldown"
                    and state.cooldown_until > time.monotonic()
                ]
                delay = min([remaining, *wake_delays])
                try:
                    await asyncio.wait_for(self._slot_condition.wait(), delay)
                except asyncio.TimeoutError:
                    pass

    async def _get_or_create_browser(self, browser_id: int) -> CamoufoxWorker:
        async with self._browsers_lock:
            if self._closed:
                raise RuntimeError("Browser captcha service is closed")
            if browser_id not in self._browsers:
                user_data_dir = os.path.join(
                    self.base_user_data_dir, f"browser_{browser_id}"
                )
                browser = CamoufoxWorker(browser_id, user_data_dir, db=self.db)
                debug_logger.log_info(
                    f"[BrowserCaptcha] 创建 Camoufox+BiDi 浏览器实例 {browser_id}"
                )
                self._browsers[browser_id] = browser
            return self._browsers[browser_id]

    @staticmethod
    def _parse_browser_ref(
        browser_ref: Optional[Union[int, str]],
    ) -> tuple[Optional[int], Optional[str]]:
        if browser_ref is None:
            return None, None
        if isinstance(browser_ref, int):
            return browser_ref, None
        if isinstance(browser_ref, str):
            raw = browser_ref.strip()
            if raw.isdigit():
                return int(raw), None
            browser_id_part, sep, request_ref = raw.partition(":")
            if sep and browser_id_part.isdigit() and request_ref:
                return int(browser_id_part), request_ref
        return None, None

    async def _resolve_token_proxy_url(self, token_id: Optional[int]) -> Optional[str]:
        if not token_id or not self.db:
            return None
        try:
            token = await self.db.get_token(token_id)
            if token and token.captcha_proxy_url and token.captcha_proxy_url.strip():
                return token.captcha_proxy_url.strip()
        except Exception as e:
            debug_logger.log_warning(
                f"[BrowserCaptcha] 读取 token({token_id}) 打码代理失败: {e}"
            )
        return None

    async def get_token(
        self,
        project_id: str,
        action: str = "IMAGE_GENERATION",
        token_id: int = None,
    ) -> tuple[Optional[str], Union[int, str], Optional[Dict[str, Any]]]:
        self._stats["req_total"] += 1
        token_proxy_url = await self._resolve_token_proxy_url(token_id)
        async with self._borrow_browser(project_id, token_id) as request:
            try:
                token, session_id = await asyncio.wait_for(
                    request.browser.get_token(
                        project_id,
                        self.website_key,
                        action,
                        token_proxy_url=token_proxy_url,
                        token_id=token_id,
                    ),
                    timeout=max(1, float(config.flow_timeout)),
                )
            except Exception:
                self._stats["gen_fail"] += 1
                raise
            request.generation = getattr(request.browser, "_runtime_generation", 0)
            request.fingerprint = (
                deepcopy(request.browser.get_last_fingerprint()) if token else None
            )
            if token:
                async with self._slot_condition:
                    self._requests.pop(request.ref)
                    request.ref += str(session_id or "")
                    self._requests[request.ref] = request
                    request.hold = True
                self._stats["gen_ok"] += 1
            else:
                self._stats["gen_fail"] += 1
        return token, request.ref, deepcopy(request.fingerprint)

    async def refresh_google_cookies(
        self, project_id: Optional[str], *, token_id: int
    ) -> Dict[str, Any]:
        if not token_id:
            raise ValueError("Cookie 刷新必须指定 Token ID")
        token_proxy_url = await self._resolve_token_proxy_url(token_id)
        async with self._borrow_browser(project_id, token_id) as request:
            return await request.browser.refresh_google_cookies(
                token_id=token_id, token_proxy_url=token_proxy_url
            )

    async def get_custom_token(
        self,
        website_url: str,
        website_key: str,
        action: str = "homepage",
        enterprise: bool = False,
    ) -> tuple[Optional[str], int]:
        async with self._borrow_browser() as request:
            token = await asyncio.wait_for(
                request.browser.get_custom_token(
                    website_url=website_url,
                    website_key=website_key,
                    action=action,
                    enterprise=enterprise,
                ),
                timeout=max(1, float(config.flow_timeout)),
            )
            request.generation = getattr(request.browser, "_runtime_generation", 0)
            request.failed = True
            return token, request.slot_id

    async def get_custom_score(
        self,
        website_url: str,
        website_key: str,
        verify_url: str,
        action: str = "homepage",
        enterprise: bool = False,
    ) -> tuple[Dict[str, Any], int]:
        async with self._borrow_browser() as request:
            payload = await asyncio.wait_for(
                request.browser.get_custom_score(
                    website_url=website_url,
                    website_key=website_key,
                    verify_url=verify_url,
                    action=action,
                    enterprise=enterprise,
                ),
                timeout=max(1, float(config.flow_timeout)),
            )
            request.generation = getattr(request.browser, "_runtime_generation", 0)
            request.failed = True
            return payload, request.slot_id

    async def get_fingerprint(
        self, browser_ref: Optional[Union[int, str]]
    ) -> Optional[Dict[str, Any]]:
        browser_id, _ = self._parse_browser_ref(browser_ref)
        if browser_id is None:
            return None
        if isinstance(browser_ref, str) and ":lease-" in browser_ref:
            request = self._requests.get(browser_ref)
            return deepcopy(request.fingerprint) if request else None
        async with self._browsers_lock:
            browser = self._browsers.get(browser_id)
            return browser.get_last_fingerprint() if browser else None

    @staticmethod
    def _is_recaptcha_rejection(error_text: str) -> bool:
        normalized = str(error_text or "").lower()
        return "recaptcha" in normalized and any(
            marker in normalized
            for marker in (
                "evaluation failed",
                "verification failed",
                "验证失败",
                "extension_hijack_detected",
            )
        )

    async def submit_flow_request(
        self,
        project_id: str,
        action: str,
        token_id: Optional[int],
        url: str,
        at_token: str,
        json_data: Dict[str, Any],
        timeout: int,
        browser_ref: Optional[Union[int, str]] = None,
    ) -> tuple[Dict[str, Any], Union[int, str], Optional[Dict[str, Any]]]:
        deadline = time.monotonic() + max(1, float(timeout))
        token_proxy_url = await self._resolve_token_proxy_url(token_id)
        async with self._borrow_browser(
            project_id,
            token_id,
            browser_ref=browser_ref,
            wait_timeout=min(
                self.QUEUE_WAIT_SECONDS, max(0, deadline - time.monotonic())
            ),
        ) as request:
            remaining = max(0.001, deadline - time.monotonic())
            try:
                response = await asyncio.wait_for(
                    request.browser.submit_flow_request(
                        project_id=project_id,
                        website_key=self.website_key,
                        action=action,
                        url=url,
                        at_token=at_token,
                        json_data=json_data,
                        timeout=remaining,
                        token_proxy_url=token_proxy_url,
                        token_id=token_id,
                    ),
                    timeout=remaining,
                )
            except asyncio.TimeoutError as exc:
                error = RuntimeError(
                    "Flow browser submission timed out; submission status is unknown"
                )
                error.submission_uncertain = True
                raise error from exc
            request.generation = getattr(request.browser, "_runtime_generation", 0)
            request.fingerprint = deepcopy(request.browser.get_last_fingerprint())
            if int(response.get("status") or 0) >= 400:
                request.failed = True
                await self.report_error(
                    request.ref, error_reason=response.get("text") or ""
                )
            return response, request.ref, deepcopy(request.fingerprint)

    async def execute_flow_frontend_rpc(
        self,
        *,
        rpc_id: str,
        argument: List[Any],
        project_id: Optional[str] = None,
        source_path: Optional[str] = None,
        timeout: int = 90,
        token_id: Optional[int] = None,
        browser_ref: Optional[Union[int, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        if not getattr(config, "browser_in_page_rpc", True):
            return None
        deadline = time.monotonic() + max(5.0, float(timeout or 90))
        token_proxy_url = await self._resolve_token_proxy_url(token_id)
        async with self._borrow_browser(
            project_id,
            token_id,
            browser_ref=browser_ref,
            wait_timeout=min(
                self.QUEUE_WAIT_SECONDS, max(0.0, deadline - time.monotonic())
            ),
        ) as request:
            executor = getattr(request.browser, "execute_flow_frontend_rpc", None)
            if not callable(executor):
                return None
            remaining = max(5.0, deadline - time.monotonic())
            try:
                response = await asyncio.wait_for(
                    executor(
                        rpc_id=rpc_id,
                        argument=argument,
                        project_id=project_id,
                        source_path=source_path,
                        timeout=int(remaining),
                        token_id=token_id,
                        token_proxy_url=token_proxy_url,
                    ),
                    timeout=remaining + 10.0,
                )
            except asyncio.TimeoutError as exc:
                error = RuntimeError(
                    f"Flow frontend RPC ({rpc_id}) timed out after {int(remaining)}s"
                )
                if rpc_id in {
                    "ogiZ0b",
                    "SPrCad",
                    "YhhmEf",
                    "eb1hJf",
                    "nprQif",
                    "MZZa6b",
                    "fZytfe",
                    "p0UkFb",
                }:
                    error.submission_uncertain = True
                raise error from exc
            request.generation = getattr(request.browser, "_runtime_generation", 0)
            request.fingerprint = deepcopy(request.browser.get_last_fingerprint())
            if isinstance(response, dict) and int(response.get("status") or 0) >= 400:
                request.failed = True
                await self.report_error(
                    request.ref, error_reason=response.get("text") or ""
                )
            return response

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
        browser_ref: Optional[Union[int, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        deadline = time.monotonic() + max(30.0, float(timeout or 120))
        token_proxy_url = await self._resolve_token_proxy_url(token_id)
        async with self._borrow_browser(
            project_id,
            token_id,
            browser_ref=browser_ref,
            wait_timeout=min(
                self.QUEUE_WAIT_SECONDS, max(0.0, deadline - time.monotonic())
            ),
        ) as request:
            executor = getattr(request.browser, "generate_native_image", None)
            if not callable(executor):
                return None
            remaining = max(30.0, deadline - time.monotonic())
            response = await asyncio.wait_for(
                executor(
                    project_id=project_id,
                    prompt=prompt,
                    aspect_ratio=aspect_ratio,
                    model_name=model_name,
                    image_inputs=image_inputs,
                    timeout=int(remaining),
                    token_id=token_id,
                    token_proxy_url=token_proxy_url,
                ),
                timeout=remaining + 15.0,
            )
            request.generation = getattr(request.browser, "_runtime_generation", 0)
            request.fingerprint = deepcopy(request.browser.get_last_fingerprint())
            return response

    async def generate_native_video(
        self,
        *,
        project_id: str,
        prompt: str,
        aspect_ratio: str = "VIDEO_ASPECT_RATIO_LANDSCAPE",
        model_key: str = "",
        timeout: int = 180,
        token_id: Optional[int] = None,
        browser_ref: Optional[Union[int, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        deadline = time.monotonic() + max(45.0, float(timeout or 180))
        token_proxy_url = await self._resolve_token_proxy_url(token_id)
        async with self._borrow_browser(
            project_id,
            token_id,
            browser_ref=browser_ref,
            wait_timeout=min(
                self.QUEUE_WAIT_SECONDS, max(0.0, deadline - time.monotonic())
            ),
        ) as request:
            executor = getattr(request.browser, "generate_native_video", None)
            if not callable(executor):
                return None
            remaining = max(45.0, deadline - time.monotonic())
            response = await asyncio.wait_for(
                executor(
                    project_id=project_id,
                    prompt=prompt,
                    aspect_ratio=aspect_ratio,
                    model_key=model_key,
                    timeout=int(remaining),
                    token_id=token_id,
                    token_proxy_url=token_proxy_url,
                ),
                timeout=remaining + 15.0,
            )
            request.generation = getattr(request.browser, "_runtime_generation", 0)
            request.fingerprint = deepcopy(request.browser.get_last_fingerprint())
            return response

    async def report_error(self, browser_ref=None, error_reason: Optional[str] = None):
        slot_id, handle = self._parse_browser_ref(browser_ref)
        if slot_id is None:
            return
        async with self._slot_condition:
            request = self._requests.get(browser_ref)
            if handle and handle.startswith("lease-") and not request:
                return
            browser = self._browsers.get(slot_id)
            if not browser or (
                request
                and (
                    request.browser is not browser
                    or request.generation
                    != getattr(browser, "_runtime_generation", 0)
                    or (
                        request.token_id is not None
                        and request.token_id
                        != getattr(browser, "_shared_bound_token_id", None)
                    )
                )
            ):
                return
            if request:
                request.failed = True
            if not self._is_recaptcha_rejection(error_reason):
                return
            if request and request.risk_reported:
                return
            if request:
                request.risk_reported = True
            state = self._slot_recovery.setdefault(slot_id, _SlotRecovery())
            if not state.needs_recycle or state.retiring:
                state.failure_streak += 1
                self._stats["api_403"] += 1
            if hasattr(browser, "note_recaptcha_rejected"):
                try:
                    browser.note_recaptcha_rejected()
                except Exception:
                    pass
            if state.failure_streak < 3 and hasattr(browser, "note_recaptcha_rejected"):
                return
            state.needs_recycle = True
            state.reason = "recaptcha_rejected"
            state.phase = "draining"
            state.cooldown_until = 0.0
            self._resume_recovery_locked(slot_id)
            self._slot_condition.notify_all()

    async def report_request_finished(
        self, browser_ref=None, *, success: Optional[bool] = None
    ):
        await self._finish_request(browser_ref, success=success)

    async def close(self):
        async with self._slot_condition:
            self._closed = True
            owners = {
                owner
                for request in self._requests.values()
                for owner in (request.owner, request.borrower)
                if owner is not None
                and owner is not asyncio.current_task()
                and not owner.done()
            }
            tasks = list(self._cleanup_tasks) + [
                state.task for state in self._slot_recovery.values() if state.task
            ]
            for request in self._requests.values():
                request.owner.remove_done_callback(request.owner_callback)
            self._requests.clear()
            self._slot_reservations.clear()
            self._slot_condition.notify_all()
        for task in tasks:
            task.cancel()
        for owner in owners:
            owner.cancel()
        if tasks or owners:
            await asyncio.gather(*tasks, *owners, return_exceptions=True)
        async with self._browsers_lock:
            browsers = list(self._browsers.values())
            self._browsers.clear()

        if self._idle_reaper_task and not self._idle_reaper_task.done():
            self._idle_reaper_task.cancel()
            try:
                await self._idle_reaper_task
            except asyncio.CancelledError:
                pass

        for browser in browsers:
            try:
                await browser.recycle_browser(
                    reason="service_shutdown", rotate_profile=False
                )
            except Exception:
                pass
        async with type(self)._lock:
            if type(self)._instance is self:
                type(self)._instance = None

    async def release_token_browsers(
        self, token_id: Optional[int], reason: str = "token_relogin"
    ) -> None:
        normalized_token = (
            CamoufoxWorker._normalize_token_key(token_id)
            if token_id is not None
            else None
        )
        async with self._browsers_lock:
            candidates = list(self._browsers.items())

        for slot_id, browser in candidates:
            reserved = False
            should_recycle = False
            wait_deadline = time.monotonic() + 15.0
            try:
                async with self._slot_condition:
                    while not self._closed and self._is_slot_busy_for_allocation(slot_id):
                        remaining = wait_deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        try:
                            await asyncio.wait_for(
                                self._slot_condition.wait(), remaining
                            )
                        except asyncio.TimeoutError:
                            break
                    bound_token = self._slot_bound_token_id(slot_id)
                    has_matching_req = any(
                        r.slot_id == slot_id
                        and (
                            normalized_token is None
                            or CamoufoxWorker._normalize_token_key(r.token_id)
                            == normalized_token
                        )
                        for r in self._requests.values()
                    )
                    if (
                        normalized_token is None
                        or bound_token == normalized_token
                        or has_matching_req
                    ):
                        self._reserve_slot_locked(slot_id)
                        reserved = True
                        should_recycle = True
                        for ref, req in list(self._requests.items()):
                            if req.slot_id == slot_id and (
                                normalized_token is None
                                or CamoufoxWorker._normalize_token_key(req.token_id)
                                == normalized_token
                            ):
                                self._requests.pop(ref, None)
                                if req.owner and req.owner_callback:
                                    try:
                                        req.owner.remove_done_callback(
                                            req.owner_callback
                                        )
                                    except Exception:
                                        pass
                if should_recycle:
                    try:
                        await browser.recycle_browser(
                            reason=reason, rotate_profile=False
                        )
                    except Exception:
                        pass
            finally:
                if reserved:
                    await self._release_slot_reservation(slot_id)
    def get_stats(self):
        now = time.monotonic()
        states = []
        slot_ids = sorted(set(range(self._browser_count)) | set(self._browsers))
        for slot_id in slot_ids:
            state = self._slot_recovery.get(slot_id, _SlotRecovery())
            active_count = sum(r.slot_id == slot_id for r in self._requests.values())
            busy = (
                any(
                    r.slot_id == slot_id and r.in_browser_op
                    for r in self._requests.values()
                )
                or self._slot_reservations.get(slot_id, 0)
                or bool(
                    self._browsers.get(slot_id) and self._browsers[slot_id].is_busy()
                )
            )
            phase = (
                "busy" if busy and state.phase in {"ready", "probe"} else state.phase
            )
            states.append(
                {
                    "browser_id": slot_id,
                    "state": phase,
                    "active_request_count": active_count,
                    "cooldown_remaining_seconds": round(
                        max(0, state.cooldown_until - now), 2
                    ),
                    "recovery_count": state.recovery_count,
                    "failure_streak": state.failure_streak,
                    "last_reason": state.reason,
                }
            )
        recovering = [
            item["browser_id"]
            for item in states
            if item["state"] in {"draining", "recovering"}
        ]
        return {
            "total_solve_count": self._stats["gen_ok"],
            "total_error_count": self._stats["gen_fail"],
            "risk_403_count": self._stats["api_403"],
            "browser_count": len(self._browsers),
            "configured_browser_count": self._browser_count,
            "busy_browser_count": sum(item["state"] == "busy" for item in states),
            "idle_browser_count": sum(
                item["state"] in {"ready", "probe"} for item in states
            ),
            "shared_browser_count": sum(
                browser.has_shared_browser() for browser in self._browsers.values()
            ),
            "recovering_browser_count": len(recovering),
            "recovering_slots": recovering,
            "queue_timeout_count": self._queue_timeouts,
            "average_queue_wait_ms": round(
                self._queue_wait_total_seconds * 1000 / max(1, self._admitted_requests),
                2,
            ),
            "project_affinity_count": 0,
            "browsers": states,
        }
