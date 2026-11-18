"""Current flow.google.com RPC implementation.

The public Flow web application exposes protobuf messages through Google
batchexecute.  This mixin overrides every runtime path that previously called
the retired Labs tRPC and AI Sandbox REST endpoints.
"""

import asyncio
import base64
import contextlib
import copy
import hashlib
import json
import re
import struct
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Tuple
from ..core.logger import debug_logger

from .browser_cookie_utils import (
    serialize_cookie_header_for_url,
    validate_flow_cookie_storage,
)
from .flow_cookies import FlowCookieJar

def _safe_updated_flow_cookies(owner, original) -> str:
    """读取 Cookie 最新快照，缺失实现时安全降级。

    旧版/自定义 flow_frontend 覆盖可能没有 get_updated_flow_cookies，
    直接调用会抛 AttributeError（历史报错：
    'FlowClient' object has no attribute 'get_updated_flow_cookies'）。
    """
    getter = getattr(owner, "get_updated_flow_cookies", None)
    if getter is None:
        return str(original or "").strip()
    try:
        return getter(original)
    except Exception:
        return str(original or "").strip()


class CurrentFlowClientMixin:
    FRONTEND_ACCESS_TOKEN = "flow-frontend-cookie"
    CURRENT_IMAGE_MODELS = frozenset({"BELUGA", "HARBOR_SEAL", "GEM_PIX_2"})
    DEFAULT_TTS_MODEL_KEY = "gemini_v4s_tts_flow"
    FLOW_PRESET_VOICES: Dict[str, str] = {
        "achernar": "Achernar",
        "achird": "Achird",
        "algenib": "Algenib",
        "algieba": "Algieba",
        "alnilam": "Alnilam",
        "aoede": "Aoede",
        "autonoe": "Autonoe",
        "callirrhoe": "Callirrhoe",
        "charon": "Charon",
        "despina": "Despina",
        "enceladus": "Enceladus",
        "erinome": "Erinome",
        "fenrir": "Fenrir",
        "gacrux": "Gacrux",
        "iapetus": "Iapetus",
        "kore": "Kore",
        "laomedeia": "Laomedeia",
        "leda": "Leda",
        "orus": "Orus",
        "puck": "Puck",
        "pulcherrima": "Pulcherrima",
        "rasalgethi": "Rasalgethi",
        "sadachbia": "Sadachbia",
        "sadaltager": "Sadaltager",
        "schedar": "Schedar",
        "sulafat": "Sulafat",
        "umbriel": "Umbriel",
        "vindemiatrix": "Vindemiatrix",
        "zephyr": "Zephyr",
        "zubenelgenubi": "Zubenelgenubi",
    }
    PROJECT_CACHE_TTL_SECONDS = 20.0
    HTTP_FIRST_CONTROL_PLANE_RPCS = frozenset(
        {
            "Zzl0ze",  # GetProject (voices, characters, settings, workflows)
            "jwpduf",  # CheckVideoStatus polling
            "as29s",   # GetMedia / GetMediaUrlRedirect
            "nzlxg",   # GetCredits
            "wDg3C",   # GetEntities
            "mrlkwd",  # ListAgentSessions
            "GN0Bre",  # GetAgentSession
            "TunYMc",  # RenameAgentSession
            "Dcn2Le",  # DeleteAgentSession
            "Yizz8d",  # GetUserPreferences
            "DA4VGb",  # UpdateUserPreferences
            "Kcr7Ub",  # UpdateProjectSettings
            "cz8Z4b",  # DeleteMedia / DeleteWorkflow
            "pGCYOe",  # ArchiveWorkflow
            "lt8g5",   # UpdateMediaVisibility
            "mYWVGd",  # RenameWorkflow
            "C4BZMd",  # CreateEntity (character)
            "rzMKMb",  # UpdateEntity (character)
            "Sc7aEb",  # CopyMediaToEntitySlot
            "LXpojc",  # ClearEntityReferenceSlot
        }
    )

    def _get_runtime_config(self):
        raise NotImplementedError

    async def _frontend_cookie(
        self,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> str:
        return await self._resolve_flow_frontend_cookie_header(
            google_cookies=google_cookies,
            token_id=token_id,
        )

    @staticmethod
    def _normalize_media_name(media_name: str) -> str:
        normalized = str(media_name or "").strip().rstrip("/")
        if "/" in normalized:
            normalized = normalized.rsplit("/", 1)[-1]
        return normalized

    @classmethod
    def _find_uuid_value(
        cls,
        value: Any,
        excluded: Optional[set[str]] = None,
    ) -> str:
        excluded = excluded or set()
        if isinstance(value, str):
            for candidate in re.findall(
                r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                value,
                re.IGNORECASE,
            ):
                if candidate.lower() not in {item.lower() for item in excluded}:
                    return candidate
            return ""
        if isinstance(value, list):
            for item in value:
                candidate = cls._find_uuid_value(item, excluded)
                if candidate:
                    return candidate
        elif isinstance(value, dict):
            for item in value.values():
                candidate = cls._find_uuid_value(item, excluded)
                if candidate:
                    return candidate
        return ""

    @classmethod
    def _find_media_url(cls, payload: Any) -> str:
        return cls._find_flow_content_url(
            payload, "video"
        ) or cls._find_flow_content_url(payload, "image")

    @classmethod
    def _find_encoded_media(cls, payload: Any) -> str:
        if isinstance(payload, str):
            if payload.startswith("https://"):
                return ""
            if len(payload) >= 512 and re.fullmatch(r"[A-Za-z0-9+/=_-]+", payload):
                return payload
            return ""
        if isinstance(payload, list):
            for item in payload:
                encoded = cls._find_encoded_media(item)
                if encoded:
                    return encoded
        elif isinstance(payload, dict):
            for item in payload.values():
                encoded = cls._find_encoded_media(item)
                if encoded:
                    return encoded
        return ""

    def _can_use_camoufox_in_page_rpc(
        self, token_id: Optional[int], runtime_cfg: Any
    ) -> bool:
        from .browser_captcha import BrowserCaptchaService

        return (
            token_id is not None
            and BrowserCaptchaService._instance is not None
            and str(getattr(runtime_cfg, "captcha_method", "") or "").strip().lower()
            in {"browser", "camoufox"}
            and bool(getattr(runtime_cfg, "browser_in_page_rpc", True))
        )

    async def _execute_in_page_camoufox_rpc(
        self,
        *,
        rpc_id: str,
        argument: List[Any],
        project_id: Optional[str],
        source_path: Optional[str],
        timeout: int,
        token_id: Optional[int],
        browser_ref: Optional[str],
    ) -> Tuple[bool, Any]:
        from .browser_captcha import BrowserCaptchaService

        service = await BrowserCaptchaService.get_instance(self.db)
        in_page_executor = getattr(service, "execute_flow_frontend_rpc", None)
        if not callable(in_page_executor):
            return False, None
        in_page_result = await in_page_executor(
            rpc_id=rpc_id,
            argument=argument,
            project_id=project_id,
            source_path=source_path,
            timeout=timeout,
            token_id=token_id,
            browser_ref=browser_ref,
        )
        if isinstance(in_page_result, dict) and in_page_result.get("text"):
            response_text = str(in_page_result.get("text") or "")
            frames = self._parse_batchexecute_frames(response_text)
            self._raise_batchexecute_error(frames, rpc_id)
            payload = self._extract_batchexecute_payload(frames, rpc_id)
            if payload is None:
                raise RuntimeError(
                    f"Flow frontend RPC returned no payload: rpc={rpc_id}"
                )
            return True, payload
        return False, None

    def _should_fallback_http_rpc_to_camoufox(
        self, err: Exception, *, token_id: Optional[int]
    ) -> bool:
        msg = str(err or "")
        lower = msg.lower()
        if any(
            marker in lower
            for marker in (
                "public_error_unusual_activity",
                "http error 403",
                "code=403",
                "bootstrap metadata is unavailable",
                "requires google session cookies",
            )
        ):
            return True
        if token_id is not None and ("401" in lower or "认证失败" in msg):
            try:
                from .browser_captcha import BrowserCaptchaService

                svc = BrowserCaptchaService._instance
                get_warm = getattr(svc, "get_warm_bound_token_ids", None) if svc else None
                if callable(get_warm) and int(token_id) in (get_warm(idle_only=False) or set()):
                    return True
            except Exception:
                pass
        return False

    async def _call_http_control_plane_rpc(
        self,
        *,
        rpc_id: str,
        argument: List[Any],
        google_cookies: Optional[str],
        token_id: Optional[int],
        project_id: Optional[str],
        source_path: Optional[str],
        timeout: int,
    ) -> Any:
        cookie_storage = await self._resolve_flow_frontend_cookie_storage(
            google_cookies=google_cookies,
            token_id=token_id,
        )
        rpc_kwargs: Dict[str, Any] = {
            "rpc_id": rpc_id,
            "argument": argument,
            "cookie_header": "",
            "timeout": timeout,
            "project_id": project_id,
            "source_path": source_path,
            "cookie_storage": cookie_storage,
        }
        if token_id is not None:
            rpc_kwargs["token_id"] = token_id
        return await self._call_flow_frontend_rpc(**rpc_kwargs)

    async def _current_rpc(
        self,
        *,
        rpc_id: str,
        argument: List[Any],
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
        project_id: Optional[str] = None,
        source_path: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> Any:
        effective_timeout = timeout or self._get_control_plane_timeout()
        browser_ref_fn = getattr(self, "_current_browser_ref", None)
        browser_ref = browser_ref_fn(token_id) if callable(browser_ref_fn) else None
        runtime_cfg = self._get_runtime_config()
        can_use_camoufox = self._can_use_camoufox_in_page_rpc(token_id, runtime_cfg)

        # Low-risk / read-only control-plane RPCs without an active browser lease
        # execute over fast HTTP first, avoiding browser slot contention & cold restarts.
        prefer_http_first = (
            not browser_ref and rpc_id in self.HTTP_FIRST_CONTROL_PLANE_RPCS
        )
        if prefer_http_first:
            try:
                return await self._call_http_control_plane_rpc(
                    rpc_id=rpc_id,
                    argument=argument,
                    google_cookies=google_cookies,
                    token_id=token_id,
                    project_id=project_id,
                    source_path=source_path,
                    timeout=effective_timeout,
                )
            except Exception as http_err:
                if not (
                    can_use_camoufox
                    and self._should_fallback_http_rpc_to_camoufox(
                        http_err, token_id=token_id
                    )
                ):
                    raise
                debug_logger.log_warning(
                    f"[Flow RPC] HTTP 控制面 RPC ({rpc_id}) 遇到风控或会话差异，回退至 Camoufox 页内执行: {http_err}"
                )

        if bool(browser_ref) or can_use_camoufox:
            try:
                handled, payload = await self._execute_in_page_camoufox_rpc(
                    rpc_id=rpc_id,
                    argument=argument,
                    project_id=project_id,
                    source_path=source_path,
                    timeout=effective_timeout,
                    token_id=token_id,
                    browser_ref=browser_ref,
                )
                if handled:
                    return payload
            except Exception as in_page_err:
                err_desc = (
                    f"{type(in_page_err).__name__}: {in_page_err}"
                    if str(in_page_err)
                    else type(in_page_err).__name__
                )
                debug_logger.log_warning(
                    f"[Camoufox RPC] 页面内 RPC ({rpc_id}) 失败: {err_desc}"
                )
                raise

        return await self._call_http_control_plane_rpc(
            rpc_id=rpc_id,
            argument=argument,
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=project_id,
            source_path=source_path,
            timeout=effective_timeout,
        )
    async def st_to_at(
        self,
        st: str = "",
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> dict:
        if str(google_cookies or "").strip():
            validate_flow_cookie_storage(google_cookies)
        google_cookies = await self._resolve_flow_frontend_cookie_storage(
            google_cookies=google_cookies, token_id=token_id
        )
        credits = await self.get_credits(
            self.FRONTEND_ACCESS_TOKEN,
            google_cookies=google_cookies,
            token_id=token_id,
        )
        expires = datetime.now(timezone.utc) + timedelta(minutes=55)
        return {
            "access_token": self.FRONTEND_ACCESS_TOKEN,
            "expires": expires.isoformat().replace("+00:00", "Z"),
            "user": {},
            "credits": credits.get("credits", 0),
            "userPaygateTier": credits.get("userPaygateTier"),
            "user_paygate_tier": credits.get("userPaygateTier"),
            "_credits_result": dict(credits)
            if isinstance(credits, dict)
            else {"credits": 0},
            "google_cookies": _safe_updated_flow_cookies(self, google_cookies),
        }

    async def create_project(
        self,
        st: str,
        title: str,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> str:
        payload = await self._current_rpc(
            rpc_id="jHPbke",
            argument=["projects/*", [None, [title]], [None, 22]],
            google_cookies=google_cookies,
            token_id=token_id,
            timeout=max(self._get_control_plane_timeout(), 20),
        )
        project_id = self._extract_flow_project_id(payload)
        if not project_id:
            raise RuntimeError("Flow frontend createProject response missing projectId")
        return project_id

    async def delete_project(
        self,
        st: str,
        project_id: str,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> None:
        await self._current_rpc(
            rpc_id="QI2zvc",
            argument=[project_id],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=project_id,
        )
        self._invalidate_project_snapshot_cache(project_id, token_id=token_id)

    async def get_credits(
        self,
        at: str = "",
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> dict:
        payload = await self._current_rpc(
            rpc_id="nzlxg",
            argument=[],
            google_cookies=google_cookies,
            token_id=token_id,
        )
        values = payload if isinstance(payload, list) else []
        credits = next(
            (
                int(value)
                for value in values
                if isinstance(value, (int, float)) and not isinstance(value, bool)
            ),
            0,
        )
        return {
            "credits": credits,
            "userPaygateTier": (
                "PAYGATE_TIER_ONE" if credits > 0 else "PAYGATE_TIER_NOT_PAID"
            ),
            "frontendRpc": "nzlxg",
        }

    async def get_media(
        self,
        at: str,
        media_name: str,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
        project_id: Optional[str] = None,
        operation_id: Optional[str] = None,
    ) -> dict:
        normalized_name = self._normalize_media_name(media_name)
        normalized_operation = self._normalize_media_name(operation_id or "")
        payload = await self._current_rpc(
            rpc_id="as29s",
            argument=[normalized_operation or normalized_name],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=project_id,
            source_path=(
                f"/project/{project_id}/edit/{normalized_name}" if project_id else None
            ),
        )
        media_url = self._find_media_url(payload)
        audio_url = self._find_flow_content_url(payload, "audio")
        resolved_url = media_url or audio_url
        resolved_project_id = (
            str(payload[1] or "")
            if isinstance(payload, list) and len(payload) > 1
            else ""
        )
        workflow_id = (
            str(payload[2] or "")
            if isinstance(payload, list) and len(payload) > 2
            else ""
        )
        if audio_url or "/audio/" in resolved_url:
            media_type = "audio"
        elif "/video/" in resolved_url:
            media_type = "video"
        elif "/image/" in resolved_url or resolved_url:
            media_type = "image"
        else:
            media_type = "unknown"
        return {
            "name": normalized_name,
            "projectId": resolved_project_id,
            "workflowId": workflow_id,
            "mediaType": media_type,
            "url": resolved_url,
            "video": {
                "fifeUrl": media_url,
                "generatedVideo": {"fifeUrl": media_url},
            },
            "image": {"generatedImage": {"fifeUrl": media_url}},
            "audio": {"fifeUrl": audio_url},
            "frontendRpc": "as29s",
        }

    async def get_media_url_redirect(
        self,
        st: str,
        media_name: str,
        media_url_type: str = "MEDIA_URL_TYPE_FULL_MEDIA",
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
        project_id: Optional[str] = None,
    ) -> Optional[str]:
        media = await self.get_media(
            self.FRONTEND_ACCESS_TOKEN,
            media_name,
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=project_id,
        )
        return (
            str(
                (media.get("video") or {}).get("fifeUrl")
                or media.get("url")
                or ""
            ).strip()
            or None
        )
    async def delete_media(
        self,
        st: str,
        media_names: List[str],
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
        project_id: Optional[str] = None,
    ) -> None:
        names = [self._normalize_media_name(name) for name in media_names if name]
        if not names:
            return
        await self._current_rpc(
            rpc_id="cz8Z4b",
            argument=[names, None, project_id],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=project_id,
        )
        if project_id:
            self._invalidate_project_snapshot_cache(project_id, token_id=token_id)

    async def upload_image(
        self,
        at: str,
        image_bytes: bytes,
        aspect_ratio: str = "IMAGE_ASPECT_RATIO_LANDSCAPE",
        project_id: Optional[str] = None,
        token_id: Optional[int] = None,
        google_cookies: Optional[str] = None,
    ) -> str:
        normalized_project_id = str(project_id or "").strip()
        if not normalized_project_id:
            raise ValueError("Flow frontend image upload requires project_id")
        mime_type = self._detect_image_mime_type(image_bytes)
        extension = "png" if "png" in mime_type else "jpg"
        filename = f"flow2api_{uuid.uuid4().hex[:12]}.{extension}"
        recaptcha_token, browser_id = await self._get_recaptcha_token(
            normalized_project_id,
            action="UPLOAD_IMAGE",
            token_id=token_id,
            website_url=self._build_flow_frontend_project_page_url(
                normalized_project_id
            ),
        )
        if not recaptcha_token:
            raise RuntimeError("Failed to obtain reCAPTCHA token for image upload")
        try:
            context = self._frontend_project_context(
                normalized_project_id,
                recaptcha_token,
            )
            payload = await self._current_rpc(
                rpc_id="maseQ",
                argument=[
                    context,
                    base64.b64encode(image_bytes).decode("ascii"),
                    mime_type,
                    1,
                    None,
                    None,
                    None,
                    None,
                    filename,
                    None,
                    str(uuid.uuid4()).upper(),
                    str(uuid.uuid4()).upper(),
                ],
                google_cookies=google_cookies,
                token_id=token_id,
                project_id=normalized_project_id,
                timeout=max(self._get_control_plane_timeout(), 90),
            )
            media_url = self._find_flow_content_url(payload, "image")
            media_id = ""
            if media_url:
                match = re.search(r"/image/([0-9a-f-]{36})", media_url, re.I)
                media_id = match.group(1) if match else ""
            media_id = media_id or self._find_uuid_value(
                payload,
                excluded={normalized_project_id},
            )
            if not media_id:
                raise RuntimeError("Flow frontend upload response missing mediaId")
            self._invalidate_project_snapshot_cache(
                normalized_project_id, token_id=token_id
            )
            return media_id
        finally:
            await self._notify_browser_captcha_request_finished(browser_id)

    async def generate_image(
        self,
        at: str,
        project_id: str,
        prompt: str,
        model_name: str,
        aspect_ratio: str,
        image_inputs: Optional[List[Dict]] = None,
        token_id: Optional[int] = None,
        token_image_concurrency: Optional[int] = None,
        progress_callback: Optional[Callable[[str, int], Awaitable[None]]] = None,
        google_cookies: Optional[str] = None,
    ) -> tuple[dict, str, Dict[str, Any]]:
        # Older internal callers may still use the retired Nano Banana 2 key.
        # Update it before submission; never retry an unavailable current model
        # with a retired upstream field.
        if model_name == "NARWHAL":
            model_name = "BELUGA"
        if model_name not in self.CURRENT_IMAGE_MODELS:
            raise ValueError(f"Unsupported Flow image model: {model_name}")
        await self._frontend_cookie(google_cookies, token_id)
        max_retries = max(1, int(self._get_runtime_config().flow_max_retries or 1))
        trace: Dict[str, Any] = {"max_retries": max_retries, "generation_attempts": []}
        last_error: Optional[Exception] = None
        cookie_storage = await self._resolve_flow_frontend_cookie_storage(
            google_cookies=google_cookies,
            token_id=token_id,
        )
        if not str(cookie_storage or "").strip():
            raise RuntimeError("Flow frontend generation requires Google session cookies")
        for retry_attempt in range(max_retries):
            started_at = time.time()
            browser_id = None
            submission_accepted = False
            attempt = {
                "attempt": retry_attempt + 1, "recaptcha_ok": False,
                "image_model_name": model_name,
            }
            try:
                if progress_callback:
                    await progress_callback("solving_image_captcha", 38)
                # client always submits explicit model and generation parameters.
                token, browser_id = await self._get_recaptcha_token(
                    project_id,
                    action="IMAGE_GENERATION",
                    token_id=token_id,
                    website_url=self._build_flow_frontend_project_page_url(project_id),
                )
                attempt["recaptcha_ok"] = bool(token)
                if not token:
                    raise RuntimeError("Failed to obtain reCAPTCHA token")
                if progress_callback:
                    await progress_callback("submitting_image", 48)
                session_id = str(uuid.uuid4()).upper()
                payload = await self._current_rpc(
                    rpc_id="ogiZ0b",
                    argument=self._build_frontend_image_generation_argument(
                        project_id=project_id,
                        prompt=prompt,
                        model_name=model_name,
                        aspect_ratio=aspect_ratio,
                        recaptcha_token=token,
                        session_id=session_id,
                        image_inputs=image_inputs,
                    ),
                    google_cookies=google_cookies,
                    token_id=token_id,
                    project_id=project_id,
                    timeout=max(
                        self._get_runtime_config().flow_image_request_timeout, 90
                    ),
                )
                submission_accepted = True
                result = self._normalize_frontend_image_generation_response(payload)
                attempt["success"] = True
                attempt["duration_ms"] = int((time.time() - started_at) * 1000)
                trace["generation_attempts"].append(attempt)
                trace["final_success_attempt"] = retry_attempt + 1
                return result, session_id, trace
            except Exception as error:
                last_error = error
                attempt["success"] = False
                attempt["error"] = str(error)[:240]
                attempt["duration_ms"] = int((time.time() - started_at) * 1000)
                trace["generation_attempts"].append(attempt)
                if (
                    submission_accepted
                    or bool(getattr(error, "outcome_unknown", False))
                    or bool(getattr(error, "submission_uncertain", False))
                ):
                    raise
                retry_handler = getattr(
                    self, "_handle_retryable_generation_error", None
                )
                if retry_handler is not None:
                    should_retry = await retry_handler(
                        error,
                        retry_attempt,
                        max_retries,
                        browser_id,
                        project_id,
                        "Personal image generation",
                    )
                    if not should_retry:
                        raise
                if retry_attempt >= max_retries - 1:
                    raise
            finally:
                if isinstance(browser_id, str) and ":lease-" in browser_id:
                    await self._notify_browser_captcha_request_finished(
                        browser_id, success=submission_accepted,
                    )
                else:
                    await self._notify_browser_captcha_request_finished(browser_id)
        raise last_error or RuntimeError("Flow frontend image generation failed")

    async def upsample_image(
        self,
        at: str,
        project_id: str,
        media_id: str,
        target_resolution: str = "UPSAMPLE_IMAGE_RESOLUTION_4K",
        user_paygate_tier: str = "PAYGATE_TIER_NOT_PAID",
        session_id: Optional[str] = None,
        token_id: Optional[int] = None,
        google_cookies: Optional[str] = None,
    ) -> str:
        # Each protected RPC uses the selected provider and its own action token.
        token, browser_id = await self._get_recaptcha_token(
            project_id,
            action="IMAGE_GENERATION",
            token_id=token_id,
            website_url=self._build_flow_frontend_project_page_url(project_id),
        )
        if not token:
            raise RuntimeError("Failed to obtain reCAPTCHA token for image upsample")
        try:
            # The page sends enum values (verified by capturing the real UI request):
            # 1 = 2K, 2 = 4K.
            resolution = 2 if "4K" in str(target_resolution).upper() else 1
            payload = await self._current_rpc(
                rpc_id="SPrCad",
                argument=[
                    self._normalize_media_name(media_id),
                    resolution,
                    self._frontend_project_context(project_id, token),
                ],
                google_cookies=google_cookies,
                token_id=token_id,
                project_id=project_id,
                timeout=max(
                    int(self._get_runtime_config().upsample_timeout or 300), 90
                ),
            )
            return self._find_media_url(payload) or self._find_encoded_media(payload)
        finally:
            await self._notify_browser_captcha_request_finished(browser_id)

    async def _current_generate_video(
        self,
        *,
        project_id: str,
        prompt: str,
        model_key: str,
        aspect_ratio: str,
        mode: str,
        token_id: Optional[int],
        token_video_concurrency: Optional[int],
        google_cookies: Optional[str],
        reference_media_ids: Optional[List[str]] = None,
        reference_entity_ids: Optional[List[Any]] = None,
        reference_audio_ids: Optional[List[Any]] = None,
        reference_likeness_ids: Optional[List[Any]] = None,
        start_media_id: Optional[str] = None,
        end_media_id: Optional[str] = None,
        video_media_id: Optional[str] = None,
        resolution: Optional[str] = None,
    ) -> dict:
        await self._frontend_cookie(google_cookies, token_id)
        max_retries = max(1, int(self._get_runtime_config().flow_max_retries or 1))
        model_keys = [str(model_key or "").strip()]
        # Flow currently exposes both relaxed and standard short-duration keys.
        # Some Pro accounts return code=[5] for the relaxed key; retry the exact
        # same duration/mode without the suffix before repeating the same request.
        if "_relaxed" in model_keys[0]:
            model_keys.append(model_keys[0].replace("_relaxed", ""))
        attempt_budget = max(max_retries, len(model_keys), 2)
        fallback_model_key: Optional[str] = None
        last_error: Optional[Exception] = None
        risk_retries = 0
        for retry_attempt in range(attempt_budget):
            browser_id = None
            request_model_key = fallback_model_key or model_keys[0]
            submission_accepted = False
            try:
                token, browser_id = await self._get_recaptcha_token(
                    project_id,
                    action="VIDEO_GENERATION",
                    token_id=token_id,
                    website_url=self._build_flow_frontend_project_page_url(project_id),
                )
                if not token:
                    raise RuntimeError("Failed to obtain reCAPTCHA token")
                rpc_id, argument = self._build_frontend_video_generation_argument(
                    project_id=project_id,
                    prompt=prompt,
                    model_key=request_model_key,
                    aspect_ratio=aspect_ratio,
                    recaptcha_token=token,
                    # Protocol requests use an independent session id instead of
                    # a native StreamChat session.
                    session_id=str(uuid.uuid4()).upper(),
                    mode=mode,
                    reference_media_ids=reference_media_ids,
                    reference_entity_ids=reference_entity_ids,
                    reference_audio_ids=reference_audio_ids,
                    reference_likeness_ids=reference_likeness_ids,
                    start_media_id=start_media_id,
                    end_media_id=end_media_id,
                    video_media_id=video_media_id,
                    resolution=resolution,
                )
                payload = await self._current_rpc(
                    rpc_id=rpc_id,
                    argument=argument,
                    google_cookies=google_cookies,
                    token_id=token_id,
                    project_id=project_id,
                    timeout=max(self._get_video_submit_timeout(), 90),
                )
                submission_accepted = True
                return self._normalize_frontend_video_submission(
                    payload,
                    project_id=project_id,
                    aspect_ratio=aspect_ratio,
                    model_key=request_model_key,
                    rpc_id=rpc_id,
                )
            except Exception as error:
                last_error = error
                if (
                    submission_accepted
                    or bool(getattr(error, "outcome_unknown", False))
                    or bool(getattr(error, "submission_uncertain", False))
                ):
                    raise
                error_text = str(error).lower()
                is_risk = "public_error_unusual_activity" in error_text
                if is_risk:
                    notify_err = getattr(self, "_notify_browser_captcha_error", None)
                    if callable(notify_err):
                        try:
                            await notify_err(
                                browser_id=getattr(error, "browser_ref", None) or browser_id,
                                project_id=project_id,
                                error_reason="reCAPTCHA verification failed: PUBLIC_ERROR_UNUSUAL_ACTIVITY",
                                error_message=str(error),
                            )
                        except Exception:
                            pass
                    if risk_retries < 1 and retry_attempt < attempt_budget - 1:
                        risk_retries += 1
                        debug_logger.log_warning(
                            f"[VIDEO] RPC ({rpc_id}) 遇到 PUBLIC_ERROR_UNUSUAL_ACTIVITY，重置独立打码 Tab 后重试一次"
                        )
                        await asyncio.sleep(1.5)
                        continue
                    raise
                # 账号权限/内容安全/登录失效等错误不重复提交同一账号
                if any(
                    marker in error_text
                    for marker in (
                        "model_access_denied",
                        "public_error_unsafe_generation",
                        "public_error_prominent_people_filter_failed",
                        "too much traffic",
                        "登录态已失效",
                        "登录已失效",
                    )
                ):
                    raise
                is_missing_model = bool(
                    re.search(r"code\s*=\s*\[?5\]?", error_text)
                    or "not_found" in error_text
                    or "not found" in error_text
                )
                if is_missing_model and request_model_key != model_keys[-1]:
                    fallback_model_key = model_keys[-1]
                    debug_logger.log_warning(
                        "[VIDEO] Flow 未找到 relaxed 模型 key，回退同一时长标准 key: "
                        f"{request_model_key} -> {model_keys[-1]}"
                    )
                    continue
                if retry_attempt >= max_retries - 1:
                    raise
            finally:
                if isinstance(browser_id, str) and ":lease-" in browser_id:
                    await self._notify_browser_captcha_request_finished(
                        browser_id, success=submission_accepted,
                    )
                else:
                    await self._notify_browser_captcha_request_finished(browser_id)
        raise last_error or RuntimeError("Flow frontend video generation failed")

    async def generate_video_text(
        self,
        at: str,
        project_id: str,
        prompt: str,
        model_key: str,
        aspect_ratio: str,
        use_v2_model_config: bool = False,
        user_paygate_tier: str = "PAYGATE_TIER_ONE",
        token_id: Optional[int] = None,
        token_video_concurrency: Optional[int] = None,
        google_cookies: Optional[str] = None,
        resolution: Optional[str] = None,
    ) -> dict:
        return await self._current_generate_video(
            project_id=project_id,
            prompt=prompt,
            model_key=model_key,
            aspect_ratio=aspect_ratio,
            mode="text",
            resolution=resolution,
            token_id=token_id,
            token_video_concurrency=token_video_concurrency,
            google_cookies=google_cookies,
        )

    async def generate_video_reference_images(
        self,
        at: str,
        project_id: str,
        prompt: str,
        model_key: str,
        aspect_ratio: str,
        reference_images: List[Dict],
        user_paygate_tier: str = "PAYGATE_TIER_ONE",
        token_id: Optional[int] = None,
        token_video_concurrency: Optional[int] = None,
        google_cookies: Optional[str] = None,
        resolution: Optional[str] = None,
        reference_entities: Optional[List[Any]] = None,
        reference_audios: Optional[List[Any]] = None,
    ) -> dict:
        media_ids = [
            str(item.get("mediaId") or item.get("name") or "").strip()
            for item in reference_images or []
            if isinstance(item, dict)
        ]
        return await self._current_generate_video(
            project_id=project_id,
            prompt=prompt,
            model_key=model_key,
            aspect_ratio=aspect_ratio,
            mode="references",
            reference_media_ids=media_ids,
            reference_entity_ids=reference_entities,
            reference_audio_ids=reference_audios,
            resolution=resolution,
            token_id=token_id,
            token_video_concurrency=token_video_concurrency,
            google_cookies=google_cookies,
        )

    async def generate_video_start_end(
        self,
        at: str,
        project_id: str,
        prompt: str,
        model_key: str,
        aspect_ratio: str,
        start_media_id: str,
        end_media_id: str,
        use_v2_model_config: bool = False,
        user_paygate_tier: str = "PAYGATE_TIER_ONE",
        token_id: Optional[int] = None,
        token_video_concurrency: Optional[int] = None,
        google_cookies: Optional[str] = None,
        resolution: Optional[str] = None,
    ) -> dict:
        return await self._current_generate_video(
            project_id=project_id,
            prompt=prompt,
            model_key=model_key,
            aspect_ratio=aspect_ratio,
            mode="start_end",
            start_media_id=start_media_id,
            end_media_id=end_media_id,
            resolution=resolution,
            token_id=token_id,
            token_video_concurrency=token_video_concurrency,
            google_cookies=google_cookies,
        )

    async def generate_video_start_image(
        self,
        at: str,
        project_id: str,
        prompt: str,
        model_key: str,
        aspect_ratio: str,
        start_media_id: str,
        use_v2_model_config: bool = False,
        user_paygate_tier: str = "PAYGATE_TIER_ONE",
        token_id: Optional[int] = None,
        token_video_concurrency: Optional[int] = None,
        google_cookies: Optional[str] = None,
        resolution: Optional[str] = None,
    ) -> dict:
        return await self._current_generate_video(
            project_id=project_id,
            prompt=prompt,
            model_key=model_key,
            aspect_ratio=aspect_ratio,
            mode="start",
            start_media_id=start_media_id,
            resolution=resolution,
            token_id=token_id,
            token_video_concurrency=token_video_concurrency,
            google_cookies=google_cookies,
        )

    async def generate_video_extend(
        self,
        at: str,
        project_id: str,
        prompt: str,
        model_key: str,
        aspect_ratio: str,
        video_media_id: str,
        user_paygate_tier: str = "PAYGATE_TIER_ONE",
        token_id: Optional[int] = None,
        token_video_concurrency: Optional[int] = None,
        google_cookies: Optional[str] = None,
        resolution: Optional[str] = None,
    ) -> dict:
        return await self._current_generate_video(
            project_id=project_id,
            prompt=prompt,
            model_key=model_key,
            aspect_ratio=aspect_ratio,
            mode="extend",
            video_media_id=video_media_id,
            resolution=resolution,
            token_id=token_id,
            token_video_concurrency=token_video_concurrency,
            google_cookies=google_cookies,
        )

    async def generate_omni_reference_video(
        self,
        at: str,
        st: str,
        project_id: str,
        prompt: str,
        aspect_ratio: str,
        reference_media_ids: List[str],
        model_usage_key: str = "macaron_cookie_r2v_8s",
        model_display_name: str = "Omni 1.2 Flash",
        duration: int = 8,
        user_paygate_tier: str = "PAYGATE_TIER_ONE",
        token_id: Optional[int] = None,
        token_video_concurrency: Optional[int] = None,
        google_cookies: Optional[str] = None,
        resolution: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._current_generate_video(
            project_id=project_id,
            prompt=prompt,
            model_key=model_usage_key,
            aspect_ratio=aspect_ratio,
            mode="references",
            reference_media_ids=reference_media_ids,
            resolution=resolution,
            token_id=token_id,
            token_video_concurrency=token_video_concurrency,
            google_cookies=google_cookies,
        )
    async def upsample_video(
        self,
        at: str,
        project_id: str,
        video_media_id: str,
        aspect_ratio: str,
        resolution: str,
        model_key: str,
        user_paygate_tier: str = "PAYGATE_TIER_ONE",
        token_id: Optional[int] = None,
        token_video_concurrency: Optional[int] = None,
        google_cookies: Optional[str] = None,
    ) -> dict:
        return await self._current_generate_video(
            project_id=project_id,
            prompt="",
            model_key=model_key,
            aspect_ratio=aspect_ratio,
            mode="upsample",
            video_media_id=video_media_id,
            resolution=resolution,
            token_id=token_id,
            token_video_concurrency=token_video_concurrency,
            google_cookies=google_cookies,
        )

    async def check_video_status(
        self,
        at: str,
        operations: List[Dict],
        token_id: Optional[int] = None,
        google_cookies: Optional[str] = None,
    ) -> dict:
        operation_ids: List[str] = []
        for operation in operations or []:
            operation_id = str(
                (operation or {}).get("name")
                or ((operation or {}).get("operation") or {}).get("name")
                or ""
            ).strip()
            if operation_id and operation_id not in operation_ids:
                operation_ids.append(operation_id)
        if not operation_ids:
            raise ValueError("视频状态查询缺少 Flow frontend operation ID")
        first_operation = operations[0] if operations else {}
        project_id = str((first_operation or {}).get("projectId") or "").strip()
        is_stream_chat_video = any(
            str((op or {}).get("frontendRpc") or "") == "StreamChat"
            for op in (operations or [])
        )
        normalized: Dict[str, Any] = {"operations": [dict(op) for op in (operations or [])]}
        if not is_stream_chat_video:
            try:
                payload = await self._current_rpc(
                    rpc_id="jwpduf",
                    argument=[None, None, [[operation_id] for operation_id in operation_ids]],
                    google_cookies=google_cookies,
                    token_id=token_id,
                    project_id=project_id or None,
                    timeout=max(self._get_video_poll_timeout(), 30),
                )
                normalized = self._normalize_frontend_video_status(payload, operations)
            except Exception as jwp_err:
                if "401" in str(jwp_err) or "403" in str(jwp_err):
                    raise
                debug_logger.log_warning(
                    f"[VIDEO POLL] jwpduf 查询失败，回退 as29s 媒体查询: {jwp_err}"
                )
        checked_operations = normalized.get("operations") or []
        for operation in checked_operations:
            if "SUCCESSFUL" in str(operation.get("status") or ""):
                continue
            operation_body = operation.get("operation") or {}
            metadata = operation_body.get("metadata") or {}
            video_info = (
                metadata.get("video") if isinstance(metadata.get("video"), dict) else {}
            )
            media_name = str(
                operation.get("mediaName")
                or video_info.get("mediaName")
                or operation.get("name")
                or operation_body.get("name")
                or ""
            ).strip()
            if not media_name:
                continue
            try:
                operation_id = str(
                    operation.get("name") or operation_body.get("name") or ""
                ).strip()
                media = await self.get_media(
                    at,
                    media_name,
                    google_cookies=google_cookies,
                    token_id=token_id,
                    project_id=project_id or None,
                    operation_id=operation_id or None,
                )
            except Exception as media_error:
                debug_logger.log_warning(
                    f"[VIDEO POLL] media fallback failed for {media_name}: {media_error}"
                )
                continue
            video_url = str((media.get("video") or {}).get("fifeUrl") or "").strip()
            if "/video/" not in video_url:
                continue
            video_info = dict(video_info or {})
            video_info["fifeUrl"] = video_url
            video_info.setdefault("mediaName", media_name)
            metadata = dict(metadata)
            metadata["video"] = video_info
            operation_body = dict(operation_body)
            operation_body["metadata"] = metadata
            operation["operation"] = operation_body
            operation["status"] = "MEDIA_GENERATION_STATUS_SUCCESSFUL"
            operation["progress"] = 100
        return normalized

    async def run_concatenation(
        self,
        at: str,
        original_media_id: str,
        extend_media_id: str,
        **_: Any,
    ) -> dict:
        raise RuntimeError(
            "Current Flow frontend does not expose a media concatenation RPC"
        )

    async def poll_concatenation_status(
        self,
        at: str,
        operation_name: str,
        timeout: int = 300,
        poll_interval: int = 3,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> dict:
        raise RuntimeError(
            "Current Flow frontend does not expose a media concatenation RPC"
        )

    async def get_flow_creation_agent_session(
        self,
        at: str,
        project_id: str,
        *,
        account_id: Optional[str] = None,
        allow_global_fallback: bool = True,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Optional[str]:
        payload = await self._current_rpc(
            rpc_id="mrlkwd",
            argument=[project_id],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=project_id,
        )
        return self._find_uuid_value(payload, excluded={project_id}) or None

    async def get_flow_creation_agent_session_detail(
        self,
        at: str,
        agent_session_id: str,
        *,
        account_id: Optional[str] = None,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        payload = await self._current_rpc(
            rpc_id="GN0Bre",
            argument=[agent_session_id],
            google_cookies=google_cookies,
            token_id=token_id,
        )
        return {"session": payload, "frontendRpc": "GN0Bre"}

    @staticmethod
    def pcm_to_wav(
        pcm_bytes: bytes,
        *,
        sample_rate: int = 24000,
        channels: int = 1,
        bits_per_sample: int = 16,
    ) -> bytes:
        """Wrap raw linear PCM audio with a 44-byte RIFF/WAVE header (matching Flow's VLb)."""
        if bits_per_sample <= 0 or bits_per_sample % 8 != 0:
            raise ValueError(f"Unsupported bits_per_sample: {bits_per_sample}")
        block_align = (bits_per_sample // 8) * channels
        trimmed_len = len(pcm_bytes) - (len(pcm_bytes) % block_align) if block_align else len(pcm_bytes)
        pcm_payload = pcm_bytes[:trimmed_len]
        byte_rate = sample_rate * block_align
        header = struct.pack(
            "<4sI4s4sIHHIIHH4sI",
            b"RIFF",
            36 + len(pcm_payload),
            b"WAVE",
            b"fmt ",
            16,
            1,  # PCM format
            channels,
            sample_rate,
            byte_rate,
            block_align,
            bits_per_sample,
            b"data",
            len(pcm_payload),
        )
        return header + pcm_payload

    @staticmethod
    def _parse_flow_timestamp(raw: Any) -> Optional[str]:
        if not isinstance(raw, (list, tuple)) or not raw:
            return None
        sec = raw[0]
        if not isinstance(sec, (int, float)) or isinstance(sec, bool):
            return None
        nsec = raw[1] if len(raw) > 1 and isinstance(raw[1], (int, float)) else 0
        try:
            dt = datetime.fromtimestamp(float(sec) + float(nsec) / 1e9, tz=timezone.utc)
            return dt.isoformat().replace("+00:00", "Z")
        except Exception:
            return None

    @classmethod
    def _normalize_voice_title(cls, voice: Optional[str]) -> str:
        raw = str(voice or "").strip()
        if not raw:
            return "Achernar"
        if raw.lower().startswith("voices/"):
            raw = raw.split("/", 1)[1].strip()
        return cls.FLOW_PRESET_VOICES.get(raw.lower(), raw)

    @classmethod
    def _normalize_preset_voice(cls, raw: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, list) or len(raw) < 3:
            return None
        voice_id = str(raw[0] or "").strip()
        if not voice_id:
            return None
        display_name = str(raw[2] or cls.FLOW_PRESET_VOICES.get(voice_id.lower(), voice_id)).strip()
        description = ""
        sample_url = ""
        enabled = True
        meta_block = raw[3] if len(raw) > 3 and isinstance(raw[3], list) else None
        if isinstance(meta_block, list) and len(meta_block) > 10 and isinstance(meta_block[10], list):
            for entry in meta_block[10]:
                if isinstance(entry, list) and entry:
                    if len(entry) > 0 and entry[0]:
                        display_name = str(entry[0]).strip()
                    if len(entry) > 1 and entry[1]:
                        description = str(entry[1]).strip()
                    if len(entry) > 2 and entry[2] is not None:
                        enabled = bool(entry[2])
                    if len(entry) > 3 and isinstance(entry[3], str):
                        sample_url = entry[3].strip()
                    break
        return {
            "id": voice_id,
            "name": display_name,
            "type": "preset",
            "description": description,
            "enabled": enabled,
            "sampleUrl": sample_url,
        }

    @classmethod
    def _normalize_custom_voice(
        cls,
        media_item: Any,
        workflow_names: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(media_item, list) or len(media_item) < 11:
            return None
        media_id = str(media_item[0] or "").strip()
        if not cls._is_uuid(media_id):
            return None
        project_id = str(media_item[1] or "").strip()
        workflow_id = str(media_item[2] or "").strip()
        meta = media_item[5] if isinstance(media_item[5], list) else []
        created_at = cls._parse_flow_timestamp(meta[0]) if meta else None
        audio_spec = media_item[10] if isinstance(media_item[10], list) else None
        audio_url = cls._find_flow_content_url(media_item, "audio")
        model_name = ""
        transcript = ""
        style = ""
        mime_type = ""
        speakers: List[Dict[str, str]] = []
        if isinstance(audio_spec, list) and audio_spec:
            inner = audio_spec[0] if isinstance(audio_spec[0], list) else audio_spec
            if isinstance(inner, list):
                if len(inner) > 6 and isinstance(inner[6], str):
                    transcript = inner[6]
                if len(inner) > 7 and isinstance(inner[7], str):
                    style = inner[7]
                if len(inner) > 9 and isinstance(inner[9], str):
                    model_name = inner[9]
                if len(inner) > 10 and isinstance(inner[10], str):
                    mime_type = inner[10]
                if len(inner) > 11 and isinstance(inner[11], list):
                    for sp in inner[11]:
                        if isinstance(sp, list) and sp:
                            speakers.append(
                                {
                                    "voice": str(sp[0] or ""),
                                    "label": str(sp[1] if len(sp) > 1 and sp[1] else sp[0] or ""),
                                }
                            )
        is_audio = bool(
            audio_url
            or "tts" in model_name.lower()
            or mime_type.startswith("audio/")
        )
        if not is_audio:
            return None
        display_name = (
            (workflow_names or {}).get(workflow_id)
            or (speakers[0]["label"] if speakers else "")
            or media_id
        )
        return {
            "id": media_id,
            "mediaId": media_id,
            "workflowId": workflow_id,
            "projectId": project_id,
            "name": display_name,
            "type": "custom",
            "model": model_name or cls.DEFAULT_TTS_MODEL_KEY,
            "mimeType": mime_type or "audio/wav",
            "transcript": transcript,
            "style": style,
            "speakers": speakers,
            "audioUrl": audio_url,
            "createdAt": created_at,
        }

    @classmethod
    def _normalize_character_entity(cls, raw: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, list) or len(raw) < 4:
            return None
        project_id = str(raw[0] or "").strip()
        entity_id = str(raw[1] or "").strip()
        if not cls._is_uuid(entity_id):
            return None
        collection_id = str(raw[2] or "").strip() or None
        info = raw[3] if isinstance(raw[3], list) else []
        display_name = str(info[1] or "").strip() if len(info) > 1 and info[1] is not None else ""
        favorited = bool(info[3]) if len(info) > 3 and info[3] is not None else False
        archived = bool(info[4]) if len(info) > 4 and info[4] is not None else False
        char_info = info[2] if len(info) > 2 and isinstance(info[2], list) else []
        image_refs: List[str] = []
        if len(char_info) > 0 and isinstance(char_info[0], list):
            for item in char_info[0]:
                if isinstance(item, list):
                    for sub in item:
                        if isinstance(sub, str) and sub.strip():
                            image_refs.append(sub.strip())
                elif isinstance(item, str) and item.strip():
                    image_refs.append(item.strip())
        voice_ids: List[str] = []
        if len(char_info) > 1 and isinstance(char_info[1], list):
            for audio_ref in char_info[1]:
                if isinstance(audio_ref, list):
                    ref_val = next(
                        (str(v).strip() for v in audio_ref if isinstance(v, str) and v.strip()),
                        "",
                    )
                    if ref_val:
                        voice_ids.append(ref_val)
                elif isinstance(audio_ref, str) and audio_ref.strip():
                    voice_ids.append(audio_ref.strip())
        personality_notes = (
            str(char_info[2]).strip()
            if len(char_info) > 2 and isinstance(char_info[2], str)
            else ""
        )
        created_at = cls._parse_flow_timestamp(raw[6]) if len(raw) > 6 else None
        updated_at = cls._parse_flow_timestamp(raw[7]) if len(raw) > 7 else None
        return {
            "id": entity_id,
            "projectId": project_id,
            "collectionId": collection_id,
            "displayName": display_name,
            "voiceId": voice_ids[0] if voice_ids else None,
            "voiceIds": voice_ids,
            "imageRefs": image_refs,
            "personalityNotes": personality_notes,
            "favorited": favorited,
            "archived": archived,
            "createdAt": created_at,
            "updatedAt": updated_at,
        }

    def _ensure_project_snapshot_caches(
        self,
    ) -> Tuple[Dict[Tuple[str, str], Tuple[float, Dict[str, Any]]], Dict[Tuple[str, str], asyncio.Task]]:
        cache = getattr(self, "_project_snapshot_cache", None)
        if cache is None:
            cache = {}
            self._project_snapshot_cache = cache
        inflight = getattr(self, "_project_snapshot_inflight", None)
        if inflight is None:
            inflight = {}
            self._project_snapshot_inflight = inflight
        return cache, inflight

    def _build_project_snapshot_cache_key(
        self,
        project_id: str,
        *,
        token_id: Optional[int] = None,
        google_cookies: Optional[str] = None,
    ) -> Tuple[str, str]:
        if token_id is not None:
            scope = f"tok:{int(token_id)}"
        else:
            digest = hashlib.sha256(
                str(google_cookies or "").encode("utf-8")
            ).hexdigest()[:16]
            scope = f"ck:{digest}"
        return (scope, str(project_id or "").strip())

    def _invalidate_project_snapshot_cache(
        self, project_id: Optional[str], *, token_id: Optional[int] = None
    ) -> None:
        norm_pid = str(project_id or "").strip()
        if not norm_pid:
            return
        cache = getattr(self, "_project_snapshot_cache", None)
        if not cache:
            return
        scope_prefix = f"tok:{int(token_id)}" if token_id is not None else None
        stale_keys = [
            key
            for key in list(cache.keys())
            if key[1] == norm_pid and (scope_prefix is None or key[0] == scope_prefix or key[0].startswith("ck:"))
        ]
        for key in stale_keys:
            cache.pop(key, None)

    async def get_project(
        self,
        project_id: str,
        *,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        normalized_project_id = str(project_id or "").strip()
        if not normalized_project_id:
            raise ValueError("project_id is required")

        cache, inflight = self._ensure_project_snapshot_caches()
        cache_key = self._build_project_snapshot_cache_key(
            normalized_project_id,
            token_id=token_id,
            google_cookies=google_cookies,
        )
        ttl = float(getattr(self, "PROJECT_CACHE_TTL_SECONDS", 20.0) or 0.0)
        now = time.monotonic()
        if not force_refresh and ttl > 0:
            cached_entry = cache.get(cache_key)
            if cached_entry is not None:
                expires_at, cached_value = cached_entry
                if expires_at > now:
                    return copy.deepcopy(cached_value)
                cache.pop(cache_key, None)

            existing_task = inflight.get(cache_key)
            if existing_task is not None and not existing_task.done():
                shared_result = await asyncio.shield(existing_task)
                return copy.deepcopy(shared_result)

        async def _load_once() -> Dict[str, Any]:
            payload = await self._current_rpc(
                rpc_id="Zzl0ze",
                argument=[f"projects/{normalized_project_id}", None, None, None, [1]],
                google_cookies=google_cookies,
                token_id=token_id,
                project_id=normalized_project_id,
                source_path=f"/project/{normalized_project_id}",
            )
            return self._parse_get_project_payload(normalized_project_id, payload)

        if not force_refresh and ttl > 0:
            task = asyncio.create_task(_load_once())
            inflight[cache_key] = task
            try:
                result = await asyncio.shield(task)
            finally:
                if inflight.get(cache_key) is task:
                    inflight.pop(cache_key, None)
            if len(cache) > 128:
                expired_keys = [k for k, (exp, _) in list(cache.items()) if exp <= time.monotonic()]
                for k in expired_keys:
                    cache.pop(k, None)
            cache[cache_key] = (time.monotonic() + ttl, result)
            return copy.deepcopy(result)

        return await _load_once()

    def _parse_get_project_payload(
        self, normalized_project_id: str, payload: Any
    ) -> Dict[str, Any]:
        if not isinstance(payload, list):
            raise RuntimeError("Flow GetProject (Zzl0ze) returned unexpected payload")
        scenes = payload[0] if len(payload) > 0 and isinstance(payload[0], list) else []
        workflows = payload[1] if len(payload) > 1 and isinstance(payload[1], list) else []
        media = payload[2] if len(payload) > 2 and isinstance(payload[2], list) else []
        raw_voices = payload[3] if len(payload) > 3 and isinstance(payload[3], list) else []
        raw_entities = payload[5] if len(payload) > 5 and isinstance(payload[5], list) else []

        workflow_names: Dict[str, str] = {}
        normalized_workflows: List[Dict[str, Any]] = []
        for wf in workflows:
            if not isinstance(wf, list) or not wf:
                continue
            wf_id = str(wf[0] or "").strip()
            wf_meta = wf[3] if len(wf) > 3 and isinstance(wf[3], list) else []
            wf_name = str(wf_meta[0] or "").strip() if wf_meta and wf_meta[0] is not None else ""
            if wf_id and wf_name:
                workflow_names[wf_id] = wf_name
            normalized_workflows.append(
                {
                    "workflowId": wf_id,
                    "displayName": wf_name,
                    "primaryMediaId": str(wf_meta[4] or "").strip() if len(wf_meta) > 4 else "",
                    "createdAt": self._parse_flow_timestamp(wf_meta[1]) if len(wf_meta) > 1 else None,
                }
            )

        preset_voices = [
            item
            for item in (self._normalize_preset_voice(v) for v in raw_voices)
            if item is not None
        ]
        custom_voices = [
            item
            for item in (self._normalize_custom_voice(m, workflow_names) for m in media)
            if item is not None
        ]
        characters = [
            item
            for item in (self._normalize_character_entity(e) for e in raw_entities)
            if item is not None and not item.get("archived")
        ]
        raw_settings = payload[7] if len(payload) > 7 and isinstance(payload[7], list) else None
        return {
            "projectId": normalized_project_id,
            "scenesCount": len(scenes),
            "workflows": normalized_workflows,
            "mediaCount": len(media),
            "presetVoices": preset_voices,
            "customVoices": custom_voices,
            "characters": characters,
            "settings": self._normalize_project_settings(raw_settings),
            "frontendRpc": "Zzl0ze",
        }

    async def list_voices(
        self,
        project_id: str,
        *,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        project_data = await self.get_project(
            project_id,
            google_cookies=google_cookies,
            token_id=token_id,
        )
        return {
            "projectId": project_data["projectId"],
            "presetVoices": project_data["presetVoices"],
            "customVoices": project_data["customVoices"],
            "frontendRpc": "Zzl0ze",
        }

    async def get_voice(
        self,
        project_id: str,
        voice_ref: str,
        *,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        ref = str(voice_ref or "").strip()
        if not ref:
            return None
        voices = await self.list_voices(
            project_id,
            google_cookies=google_cookies,
            token_id=token_id,
        )
        ref_lower = ref.lower()
        for preset in voices["presetVoices"]:
            if preset["id"].lower() == ref_lower or preset["name"].lower() == ref_lower:
                return preset
        for custom in voices["customVoices"]:
            if (
                custom["id"] == ref
                or custom.get("workflowId") == ref
                or custom["name"].lower() == ref_lower
            ):
                try:
                    detail = await self.get_media(
                        self.FRONTEND_ACCESS_TOKEN,
                        custom["mediaId"],
                        google_cookies=google_cookies,
                        token_id=token_id,
                        project_id=project_id,
                    )
                    refreshed_url = str((detail.get("audio") or {}).get("fifeUrl") or detail.get("url") or "").strip()
                    if refreshed_url:
                        custom = {**custom, "audioUrl": refreshed_url}
                except Exception:
                    pass
                return custom
        if self._is_uuid(ref):
            detail = await self.get_media(
                self.FRONTEND_ACCESS_TOKEN,
                ref,
                google_cookies=google_cookies,
                token_id=token_id,
                project_id=project_id,
            )
            audio_url = str((detail.get("audio") or {}).get("fifeUrl") or detail.get("url") or "").strip()
            if audio_url:
                return {
                    "id": ref,
                    "mediaId": ref,
                    "workflowId": detail.get("workflowId") or "",
                    "projectId": detail.get("projectId") or project_id,
                    "name": ref,
                    "type": "custom",
                    "audioUrl": audio_url,
                }
        return None

    async def generate_voice_audio(
        self,
        *,
        project_id: str,
        text: str,
        voice: str = "Achernar",
        style: str = "",
        name: Optional[str] = None,
        model_key: str = DEFAULT_TTS_MODEL_KEY,
        save: bool = False,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_project_id = str(project_id or "").strip()
        if not normalized_project_id:
            raise ValueError("project_id is required")
        transcript = str(text or "").strip()
        if not transcript:
            raise ValueError("text is required for TTS audio generation")
        voice_title = self._normalize_voice_title(voice)
        custom_label = str(name or "").strip() or voice_title
        style_notes = str(style or "").strip()
        effective_model = str(model_key or self.DEFAULT_TTS_MODEL_KEY).strip()

        await self._frontend_cookie(google_cookies, token_id)
        payload = None
        max_attempts = 2
        for attempt_idx in range(max_attempts):
            recaptcha_token, browser_id = await self._get_recaptcha_token(
                normalized_project_id,
                action="AUDIO_GENERATION",
                token_id=token_id,
                website_url=self._build_flow_frontend_project_page_url(normalized_project_id),
            )
            if not recaptcha_token:
                raise RuntimeError("Failed to obtain reCAPTCHA token for audio generation")
            submission_accepted = False
            try:
                payload = await self._current_rpc(
                    rpc_id="no0P6",
                    argument=[
                        [
                            [
                                transcript,
                                [[voice_title, custom_label]],
                                effective_model,
                                style_notes,
                                2,
                            ]
                        ],
                        self._frontend_project_context(normalized_project_id, recaptcha_token),
                    ],
                    google_cookies=google_cookies,
                    token_id=token_id,
                    project_id=normalized_project_id,
                    timeout=max(self._get_control_plane_timeout(), 90),
                )
                submission_accepted = True
                break
            except Exception as exc:
                error_text = str(exc or "")
                is_risk = "public_error_unusual_activity" in error_text.lower()
                notify_err = getattr(self, "_notify_browser_captcha_error", None)
                if callable(notify_err):
                    try:
                        await notify_err(
                            browser_id=getattr(exc, "browser_ref", None) or browser_id,
                            project_id=normalized_project_id,
                            error_reason=(
                                "reCAPTCHA verification failed: PUBLIC_ERROR_UNUSUAL_ACTIVITY"
                                if is_risk
                                else error_text[:120]
                            ),
                            error_message=error_text,
                        )
                    except Exception:
                        pass
                if not is_risk or attempt_idx >= max_attempts - 1:
                    raise
                await asyncio.sleep(1.5)
            finally:
                if isinstance(browser_id, str) and ":lease-" in browser_id:
                    await self._notify_browser_captcha_request_finished(
                        browser_id, success=submission_accepted
                    )
                else:
                    await self._notify_browser_captcha_request_finished(browser_id)
        media_id = ""
        workflow_id = ""
        if isinstance(payload, list):
            media_list = payload[0] if len(payload) > 0 and isinstance(payload[0], list) else []
            if media_list and isinstance(media_list[0], list):
                media_id = str(media_list[0][0] or "").strip()
                if len(media_list[0]) > 2 and media_list[0][2]:
                    workflow_id = str(media_list[0][2]).strip()
            if not workflow_id and len(payload) > 1 and isinstance(payload[1], list) and payload[1]:
                if isinstance(payload[1][0], list) and payload[1][0]:
                    workflow_id = str(payload[1][0][0] or "").strip()
        if not media_id:
            raise RuntimeError("Flow BatchGenerateAudio (no0P6) response missing mediaId")

        media_detail = await self.get_media(
            self.FRONTEND_ACCESS_TOKEN,
            media_id,
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=normalized_project_id,
        )
        audio_url = str(
            (media_detail.get("audio") or {}).get("fifeUrl")
            or media_detail.get("url")
            or ""
        ).strip()

        saved = False
        if save:
            await self._current_rpc(
                rpc_id="lt8g5",
                argument=[
                    [media_id, None, None, None, None, [None, None, None, None, None, None, None, None, None, 1]],
                    [["media.media_metadata.visibility"]],
                ],
                google_cookies=google_cookies,
                token_id=token_id,
                project_id=normalized_project_id,
            )
            if workflow_id:
                await self._current_rpc(
                    rpc_id="mYWVGd",
                    argument=[
                        [workflow_id, None, None, [custom_label], normalized_project_id],
                        [["metadata.display_name"]],
                    ],
                    google_cookies=google_cookies,
                    token_id=token_id,
                    project_id=normalized_project_id,
                )
            saved = True

        self._invalidate_project_snapshot_cache(normalized_project_id, token_id=token_id)
        return {
            "mediaId": media_id,
            "workflowId": workflow_id,
            "projectId": normalized_project_id,
            "voice": voice_title,
            "name": custom_label,
            "style": style_notes,
            "model": effective_model,
            "saved": saved,
            "audioUrl": audio_url,
            "format": "pcm_24khz_16bit_mono",
            "frontendRpc": "no0P6",
        }

    async def delete_voice(
        self,
        *,
        project_id: str,
        workflow_id: Optional[str] = None,
        media_id: Optional[str] = None,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_project_id = str(project_id or "").strip()
        if not normalized_project_id:
            raise ValueError("project_id is required")
        target_workflow_id = str(workflow_id or "").strip()
        if not target_workflow_id and media_id:
            detail = await self.get_media(
                self.FRONTEND_ACCESS_TOKEN,
                media_id,
                google_cookies=google_cookies,
                token_id=token_id,
                project_id=normalized_project_id,
            )
            target_workflow_id = str(detail.get("workflowId") or "").strip()
            if not target_workflow_id:
                project_data = await self.get_project(
                    normalized_project_id,
                    google_cookies=google_cookies,
                    token_id=token_id,
                )
                for custom in project_data["customVoices"]:
                    if custom.get("mediaId") == media_id or custom.get("id") == media_id:
                        target_workflow_id = str(custom.get("workflowId") or "").strip()
                        break
        if not target_workflow_id:
            raise ValueError("workflow_id (or resolvable media_id) is required to delete voice")
        await self._current_rpc(
            rpc_id="pGCYOe",
            argument=[
                [[target_workflow_id, None, None, [None, None, True], normalized_project_id]],
                [["metadata.archived"]],
            ],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=normalized_project_id,
        )
        await self._current_rpc(
            rpc_id="cz8Z4b",
            argument=[None, [target_workflow_id], normalized_project_id],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=normalized_project_id,
        )
        self._invalidate_project_snapshot_cache(normalized_project_id, token_id=token_id)
        return {
            "deleted": True,
            "projectId": normalized_project_id,
            "workflowId": target_workflow_id,
            "mediaId": media_id or "",
        }

    async def list_characters(
        self,
        project_id: str,
        *,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        project_data = await self.get_project(
            project_id,
            google_cookies=google_cookies,
            token_id=token_id,
        )
        return {
            "projectId": project_data["projectId"],
            "characters": project_data["characters"],
            "frontendRpc": "Zzl0ze",
        }

    async def get_characters(
        self,
        entity_ids: List[str],
        *,
        project_id: Optional[str] = None,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        ids = [str(eid or "").strip() for eid in entity_ids or [] if str(eid or "").strip()]
        if not ids:
            return []
        payload = await self._current_rpc(
            rpc_id="wDg3C",
            argument=[None, ids],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=project_id,
        )
        raw_entries = payload[0] if isinstance(payload, list) and payload and isinstance(payload[0], list) else []
        results: List[Dict[str, Any]] = []
        for entry in raw_entries:
            raw_entity = (
                entry[0]
                if isinstance(entry, list) and entry and isinstance(entry[0], list)
                else entry
            )
            item = self._normalize_character_entity(raw_entity)
            if item is not None:
                results.append(item)
        return results

    async def create_character(
        self,
        *,
        project_id: str,
        display_name: str,
        voice_id: Optional[str] = None,
        personality_notes: Optional[str] = None,
        image_refs: Optional[List[str]] = None,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_project_id = str(project_id or "").strip()
        if not normalized_project_id:
            raise ValueError("project_id is required")
        name = str(display_name or "").strip() or "Unnamed Character"
        payload = await self._current_rpc(
            rpc_id="C4BZMd",
            argument=[[normalized_project_id, None, None, [1, name, [[], []]]], None],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=normalized_project_id,
        )
        raw_entity = payload[0] if isinstance(payload, list) and payload and isinstance(payload[0], list) else payload
        created = self._normalize_character_entity(raw_entity)
        if not created:
            raise RuntimeError("Flow CreateEntity (C4BZMd) returned invalid character entity")
        self._invalidate_project_snapshot_cache(normalized_project_id, token_id=token_id)
        if voice_id is not None or personality_notes is not None or image_refs is not None:
            return await self.update_character(
                project_id=normalized_project_id,
                entity_id=created["id"],
                voice_id=voice_id,
                personality_notes=personality_notes,
                image_refs=image_refs,
                google_cookies=google_cookies,
                token_id=token_id,
            )
        return created

    async def update_character(
        self,
        *,
        project_id: str,
        entity_id: str,
        display_name: Optional[str] = None,
        voice_id: Optional[str] = None,
        personality_notes: Optional[str] = None,
        image_refs: Optional[List[str]] = None,
        favorited: Optional[bool] = None,
        archived: Optional[bool] = None,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_project_id = str(project_id or "").strip()
        normalized_entity_id = str(entity_id or "").strip()
        if not normalized_project_id or not normalized_entity_id:
            raise ValueError("project_id and entity_id are required")

        field_masks: List[str] = []
        char_info: List[Any] = [None, None, None]
        has_char_info = False

        if voice_id is not None:
            raw_voice = str(voice_id).strip()
            if raw_voice.lower().startswith("voices/"):
                raw_voice = raw_voice.split("/", 1)[1].strip()
            if not raw_voice:
                char_info[1] = []
            elif self._is_uuid(raw_voice):
                char_info[1] = [[raw_voice]]
            else:
                char_info[1] = [[None, raw_voice.lower()]]
            field_masks.append("entity_info.character_info.audio_references")
            has_char_info = True
        if personality_notes is not None:
            char_info[2] = str(personality_notes)
            field_masks.append("entity_info.character_info.personality_notes")
            has_char_info = True

        entity_info: List[Any] = [
            1,
            display_name if display_name is not None else None,
            char_info if has_char_info else None,
            favorited,
            archived,
        ]
        if display_name is not None:
            field_masks.append("entity_info.display_name")
        if favorited is not None:
            field_masks.append("entity_info.is_favorited")
        if archived is not None:
            field_masks.append("entity_info.archived")

        clean_image_refs: Optional[List[str]] = None
        if image_refs is not None:
            clean_image_refs = [
                self._normalize_media_name(mid)
                for mid in image_refs
                if str(mid or "").strip()
            ]

        if not field_masks and clean_image_refs is None:
            raise ValueError("At least one character field must be provided for update")

        updated: Optional[Dict[str, Any]] = None
        if field_masks:
            payload = await self._current_rpc(
                rpc_id="rzMKMb",
                argument=[
                    [normalized_project_id, normalized_entity_id, None, entity_info],
                    [field_masks],
                ],
                google_cookies=google_cookies,
                token_id=token_id,
                project_id=normalized_project_id,
            )
            raw_entity = (
                payload[0]
                if isinstance(payload, list) and payload and isinstance(payload[0], list)
                else payload
            )
            updated = self._normalize_character_entity(raw_entity)
            if not updated:
                raise RuntimeError(
                    "Flow UpdateEntity (rzMKMb) returned invalid character entity"
                )

        if clean_image_refs is not None:
            existing_slots = 0
            if updated is not None:
                existing_slots = len(updated.get("imageRefs") or [])
            else:
                existing_list = await self.get_characters(
                    [normalized_entity_id],
                    project_id=normalized_project_id,
                    google_cookies=google_cookies,
                    token_id=token_id,
                )
                if existing_list:
                    existing_slots = len(existing_list[0].get("imageRefs") or [])
            for slot_index, mid in enumerate(clean_image_refs):
                await self.copy_project_media_to_character_slot(
                    project_id=normalized_project_id,
                    entity_id=normalized_entity_id,
                    media_id=mid,
                    slot_index=slot_index,
                    google_cookies=google_cookies,
                    token_id=token_id,
                )
            for stale_slot in range(len(clean_image_refs), existing_slots):
                await self.clear_character_reference_slot(
                    project_id=normalized_project_id,
                    entity_id=normalized_entity_id,
                    slot_index=stale_slot,
                    google_cookies=google_cookies,
                    token_id=token_id,
                )
            refreshed_list = await self.get_characters(
                [normalized_entity_id],
                project_id=normalized_project_id,
                google_cookies=google_cookies,
                token_id=token_id,
            )
            if refreshed_list:
                updated = refreshed_list[0]
            elif updated is None:
                raise RuntimeError(
                    "Flow GetEntities (wDg3C) returned empty after image slot update"
                )

        self._invalidate_project_snapshot_cache(normalized_project_id, token_id=token_id)
        return updated
    async def delete_character(
        self,
        *,
        project_id: str,
        entity_id: str,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_project_id = str(project_id or "").strip()
        normalized_entity_id = str(entity_id or "").strip()
        if not normalized_project_id or not normalized_entity_id:
            raise ValueError("project_id and entity_id are required")
        await self.update_character(
            project_id=normalized_project_id,
            entity_id=normalized_entity_id,
            archived=True,
            google_cookies=google_cookies,
            token_id=token_id,
        )
        await self._current_rpc(
            rpc_id="cz8Z4b",
            argument=[None, None, normalized_project_id, [normalized_entity_id]],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=normalized_project_id,
        )
        self._invalidate_project_snapshot_cache(normalized_project_id, token_id=token_id)
        return {
            "deleted": True,
            "projectId": normalized_project_id,
            "characterId": normalized_entity_id,
        }

    async def create_flow_entity(self, *args: Any, **kwargs: Any) -> str:
        project_id = kwargs.get("project_id") or (args[0] if len(args) > 0 else "")
        display_name = kwargs.get("display_name") or (args[1] if len(args) > 1 else "")
        if project_id and display_name:
            created = await self.create_character(
                project_id=str(project_id),
                display_name=str(display_name),
                voice_id=kwargs.get("voice_id"),
                personality_notes=kwargs.get("personality_notes"),
                image_refs=kwargs.get("image_refs"),
                google_cookies=kwargs.get("google_cookies"),
                token_id=kwargs.get("token_id"),
            )
            return str(created["id"])
        raise RuntimeError(
            "Flow entities require project_id and display_name"
        )

    async def copy_project_media_to_character_slot(
        self,
        *,
        project_id: str,
        entity_id: str,
        media_id: str,
        slot_index: int = 0,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_project_id = str(project_id or "").strip()
        normalized_entity_id = str(entity_id or "").strip()
        normalized_media_id = self._normalize_media_name(media_id)
        if not normalized_project_id or not normalized_entity_id or not normalized_media_id:
            raise ValueError("project_id, entity_id, and media_id are required")
        payload = await self._current_rpc(
            rpc_id="Sc7aEb",
            argument=[
                normalized_media_id,
                None,
                None,
                normalized_project_id,
                None,
                None,
                None,
                [None, None, [normalized_entity_id, [int(slot_index)]]],
            ],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=normalized_project_id,
        )
        self._invalidate_project_snapshot_cache(normalized_project_id, token_id=token_id)
        copied_media_id = ""
        workflow_id = ""
        if isinstance(payload, list) and payload and isinstance(payload[0], list):
            copied_media_id = str(payload[0][0] or "").strip()
            if len(payload[0]) > 2 and payload[0][2]:
                workflow_id = str(payload[0][2]).strip()
        return {
            "projectId": normalized_project_id,
            "characterId": normalized_entity_id,
            "slotIndex": int(slot_index),
            "sourceMediaId": normalized_media_id,
            "mediaId": copied_media_id,
            "workflowId": workflow_id,
            "frontendRpc": "Sc7aEb",
        }

    async def clear_character_reference_slot(
        self,
        *,
        project_id: str,
        entity_id: str,
        slot_index: int = 0,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_project_id = str(project_id or "").strip()
        normalized_entity_id = str(entity_id or "").strip()
        if not normalized_project_id or not normalized_entity_id:
            raise ValueError("project_id and entity_id are required")
        await self._current_rpc(
            rpc_id="LXpojc",
            argument=[None, normalized_project_id, normalized_entity_id, int(slot_index)],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=normalized_project_id,
        )
        self._invalidate_project_snapshot_cache(normalized_project_id, token_id=token_id)
        return {
            "cleared": True,
            "projectId": normalized_project_id,
            "characterId": normalized_entity_id,
            "slotIndex": int(slot_index),
            "frontendRpc": "LXpojc",
        }

    # ── Phase 2: Creation Agent & Settings ────────────────────────────────────

    PERMISSION_ACTION_CODES: Dict[Any, int] = {
        1: 1,
        "1": 1,
        "approve": 1,
        "approved": 1,
        "allow": 1,
        "yes": 1,
        2: 2,
        "2": 2,
        "approve_sticky": 2,
        "approved_sticky": 2,
        "always_approve": 2,
        "always_allow": 2,
        "sticky": 2,
        3: 3,
        "3": 3,
        "deny": 3,
        "denied": 3,
        "reject": 3,
        "no": 3,
    }

    PERMISSION_ACTION_DEFAULT_LABELS: Dict[int, str] = {
        1: "Approve",
        2: "Always approve",
        3: "Deny",
    }

    CREDIT_SPEND_APPROVAL_POLICY_NAMES: Dict[int, str] = {
        0: "unspecified",
        1: "always_ask",
        2: "auto_approve",
        3: "ask_above_threshold",
    }

    @classmethod
    def _normalize_permission_action(cls, action: Any) -> int:
        key = action if isinstance(action, int) else str(action or "").strip().lower()
        if key in cls.PERMISSION_ACTION_CODES:
            return cls.PERMISSION_ACTION_CODES[key]
        raise ValueError(
            f"Unsupported permission action: {action!r} (expected approve/approved_sticky/deny or 1/2/3)"
        )

    @staticmethod
    def _merge_stream_text(accumulated: str, incoming: str) -> tuple[str, str]:
        """Deduplicate cumulative prefix vs incremental delta text chunks (BOQ `o$b` semantics)."""
        if not incoming:
            return accumulated, ""
        if not accumulated:
            return incoming, incoming
        if incoming.startswith(accumulated):
            delta = incoming[len(accumulated) :]
            return incoming, delta
        if accumulated.endswith(incoming):
            return accumulated, ""
        return accumulated + incoming, incoming

    @classmethod
    def _decode_proto_value(cls, val: Any) -> Any:
        if not isinstance(val, list):
            return val
        # protobuf google.protobuf.Value jspb representation:
        # [null] -> null, [null, num] -> number, [null, null, str] -> string,
        # [null, null, null, bool] -> bool, [null, null, null, null, struct] -> dict,
        # [null, null, null, null, null, [items]] -> list
        if len(val) >= 6 and isinstance(val[5], list) and all(x is None for x in val[:5]):
            items = val[5][0] if val[5] and isinstance(val[5][0], list) else val[5]
            return [cls._decode_proto_value(item) for item in items]
        if len(val) >= 5 and val[4] is not None and all(x is None for x in val[:4]):
            return cls._decode_proto_struct(val[4])
        if len(val) >= 4 and val[3] is not None and all(x is None for x in val[:3]):
            return bool(val[3])
        if len(val) >= 3 and val[2] is not None and val[0] is None and val[1] is None:
            return val[2]
        if len(val) >= 2 and val[1] is not None and val[0] is None:
            return val[1]
        if len(val) == 1 and val[0] is None:
            return None
        if val and all(
            isinstance(item, list) and len(item) >= 2 and isinstance(item[0], str)
            for item in val
        ):
            return {item[0]: cls._decode_proto_value(item[1]) for item in val}
        return [cls._decode_proto_value(item) for item in val]

    @classmethod
    def _decode_proto_struct(cls, raw_struct: Any) -> Dict[str, Any]:
        if isinstance(raw_struct, dict):
            return raw_struct
        if not isinstance(raw_struct, list) or not raw_struct:
            return {}
        entries = raw_struct[0] if isinstance(raw_struct[0], list) else raw_struct
        out: Dict[str, Any] = {}
        if not isinstance(entries, list):
            return out
        for entry in entries:
            if isinstance(entry, list) and len(entry) >= 2 and isinstance(entry[0], str):
                out[entry[0]] = cls._decode_proto_value(entry[1])
        return out

    @staticmethod
    def _extract_a2ui_literal(node: Any) -> Optional[str]:
        if isinstance(node, dict):
            if isinstance(node.get("literalString"), str):
                return node["literalString"]
            if isinstance(node.get("literal"), str):
                return node["literal"]
        if isinstance(node, str):
            return node
        return None

    @classmethod
    def _parse_a2ui_payload(cls, raw_val: Any) -> Optional[Dict[str, Any]]:
        obj = raw_val
        if isinstance(raw_val, str):
            stripped = raw_val.strip()
            if not stripped or not (stripped.startswith("{") or stripped.startswith("[")):
                return None
            try:
                obj = json.loads(stripped)
            except (TypeError, ValueError):
                return None
        items = obj if isinstance(obj, list) else ([obj] if isinstance(obj, dict) else [])
        a2ui_keys = {"beginRendering", "surfaceUpdate", "dataModelUpdate", "deleteSurface"}
        matched = [item for item in items if isinstance(item, dict) and any(k in item for k in a2ui_keys)]
        if not matched:
            return None

        text_blocks: List[str] = []
        options: List[Dict[str, Any]] = []
        for item in matched:
            su = item.get("surfaceUpdate")
            if not isinstance(su, dict):
                continue
            for comp_wrap in su.get("components") or []:
                if not isinstance(comp_wrap, dict):
                    continue
                comp = comp_wrap.get("component")
                if not isinstance(comp, dict):
                    continue
                if isinstance(comp.get("Text"), dict):
                    txt = cls._extract_a2ui_literal(comp["Text"].get("text"))
                    if txt is not None and txt.strip():
                        hint = str(comp["Text"].get("usageHint") or "").lower()
                        prefix = {
                            "h1": "# ",
                            "h2": "## ",
                            "h3": "### ",
                            "h4": "#### ",
                            "h5": "##### ",
                        }.get(hint, "")
                        formatted = f"*{txt}*" if hint == "caption" else f"{prefix}{txt}"
                        text_blocks.append(formatted)
                if isinstance(comp.get("MultipleChoice"), dict):
                    for opt in comp["MultipleChoice"].get("options") or []:
                        if not isinstance(opt, dict):
                            continue
                        label = cls._extract_a2ui_literal(opt.get("label"))
                        value = opt.get("value")
                        if label is not None:
                            options.append({"label": label, "value": value})
        return {
            "text": "\n\n".join(text_blocks),
            "options": options,
            "messages": matched,
        }

    @classmethod
    def _parse_stream_chat_chunk_item(cls, j_chunk: Any) -> List[Dict[str, Any]]:
        """Parse one BOQ `_.j_` message `[kN_meta, [SY_events...]]` into normalized events."""
        if not isinstance(j_chunk, list):
            return []
        events: List[Dict[str, Any]] = []
        meta = cls._decode_proto_struct(j_chunk[0] if len(j_chunk) > 0 else None)
        if meta:
            thought_val = meta.get("thought") or meta.get("thinking")
            if isinstance(thought_val, str) and thought_val:
                events.append({"type": "thinking", "text": thought_val})
            text_val = meta.get("text")
            if isinstance(text_val, str) and text_val:
                a2ui = cls._parse_a2ui_payload(text_val)
                if a2ui:
                    events.append({"type": "a2ui", **a2ui})
                else:
                    events.append({"type": "text", "text": text_val})

        sy_list = j_chunk[1] if len(j_chunk) > 1 and isinstance(j_chunk[1], list) else []
        for sy in sy_list:
            if not isinstance(sy, list):
                continue
            event_id = str(sy[0] or "").strip() if len(sy) > 0 and sy[0] is not None else ""
            # Field 3 (index 2): message text or A2UI
            if len(sy) > 2 and sy[2] is not None:
                raw_msg = sy[2]
                txt = ""
                if isinstance(raw_msg, str):
                    txt = raw_msg
                elif isinstance(raw_msg, list) and raw_msg:
                    txt = str(raw_msg[0] or "") if isinstance(raw_msg[0], str) else ""
                elif isinstance(raw_msg, dict):
                    txt = str(raw_msg.get("text") or "")
                if txt:
                    a2ui = cls._parse_a2ui_payload(txt)
                    if a2ui:
                        events.append({"type": "a2ui", "eventId": event_id, **a2ui})
                    else:
                        events.append({"type": "text", "eventId": event_id, "text": txt})

            # Field 4 (index 3): _.TY tool invocation [invocation_id, tool_name, kN_args, step_id]
            if len(sy) > 3 and isinstance(sy[3], list):
                ty = sy[3]
                tool_name = str(ty[1] or "").strip() if len(ty) > 1 and ty[1] is not None else ""
                args = cls._decode_proto_struct(ty[2] if len(ty) > 2 else None)
                inv_id = str(ty[0] or "").strip() if len(ty) > 0 and ty[0] is not None else ""
                if tool_name == "ask_for_permission":
                    events.append(
                        {
                            "type": "permission",
                            "eventId": event_id,
                            "invocationId": inv_id,
                            "source": "tool",
                            "totalCost": args.get("total_cost", 0),
                            "numImages": args.get("num_images", 0),
                            "numVideos": args.get("num_videos", 0),
                            "numTotal": args.get("num_total", 0),
                            "args": args,
                        }
                    )
                elif tool_name:
                    events.append(
                        {
                            "type": "tool_call",
                            "eventId": event_id,
                            "invocationId": inv_id,
                            "toolName": tool_name,
                            "args": args,
                        }
                    )

            # Field 5 (index 4): _.aZ tool result [invocation_id, tool_name, kN_result, ..., [kN_list]]
            if len(sy) > 4 and isinstance(sy[4], list):
                az = sy[4]
                tool_name = str(az[1] or "").strip() if len(az) > 1 and az[1] is not None else ""
                inv_id = str(az[0] or "").strip() if len(az) > 0 and az[0] is not None else ""
                res_items: List[Dict[str, Any]] = []
                if len(az) > 4 and isinstance(az[4], list) and az[4]:
                    for item in az[4]:
                        decoded = cls._decode_proto_struct(item)
                        if decoded:
                            res_items.append(decoded)
                elif len(az) > 2 and az[2] is not None:
                    decoded = cls._decode_proto_struct(az[2])
                    if decoded:
                        res_items.append(decoded)
                events.append(
                    {
                        "type": "tool_result",
                        "eventId": event_id,
                        "invocationId": inv_id,
                        "toolName": tool_name,
                        "results": res_items,
                    }
                )
                for item in res_items:
                    mid = str(item.get("media_id") or item.get("mediaId") or "").strip()
                    if cls._is_uuid(mid):
                        events.append(
                            {
                                "type": "media",
                                "eventId": event_id,
                                "mediaId": mid,
                                "url": str(item.get("url") or item.get("fife_url") or "").strip(),
                                "mediaType": str(item.get("media_type") or "").strip() or None,
                            }
                        )

            # Field 6 (index 5): direct media block
            if len(sy) > 5 and isinstance(sy[5], list):
                mid = cls._extract_flow_project_id(sy[5])
                if mid:
                    events.append({"type": "media", "eventId": event_id, "mediaId": mid})

            # Field 7 (index 6): _.ZY error [error_code, ...]
            if len(sy) > 6 and isinstance(sy[6], list):
                zy = sy[6]
                err_code = zy[0] if zy else None
                events.append(
                    {
                        "type": "error",
                        "eventId": event_id,
                        "code": err_code,
                        "message": str(zy[1] if len(zy) > 1 and zy[1] is not None else f"Stream error code={err_code}"),
                    }
                )

            # Field 8 (index 7): _.Zfc title update [title]
            if len(sy) > 7 and isinstance(sy[7], list) and sy[7]:
                title = str(sy[7][0] or "").strip()
                if title:
                    events.append({"type": "title", "eventId": event_id, "title": title})

        # Defensive sweep for raw media_id / flow-content URLs inside chunk
        seen_media = {e.get("mediaId") for e in events if e.get("type") == "media"}
        for mid in cls._extract_stream_chat_media_ids([j_chunk]):
            if mid not in seen_media:
                seen_media.add(mid)
                events.append({"type": "media", "mediaId": mid})
        return events

    @classmethod
    def _parse_webchannel_dec_event(cls, dec: Any) -> List[Dict[str, Any]]:
        """Parse one BOQ `_.Dec` event (returned by `GN0Bre` turns or WebChannel)."""
        if not isinstance(dec, list):
            return []
        event_id = str(dec[0] or "").strip() if len(dec) > 0 and dec[0] is not None else ""
        out: List[Dict[str, Any]] = []
        # Field 10 (index 9): L$b [text, is_final, type(1=text, 2=thinking)]
        if len(dec) > 9 and isinstance(dec[9], list):
            l_evt = dec[9]
            txt = str(l_evt[0] or "") if len(l_evt) > 0 and l_evt[0] is not None else ""
            kind = l_evt[2] if len(l_evt) > 2 else 1
            if txt.strip():
                out.append(
                    {
                        "type": "thinking" if kind == 2 else "text",
                        "eventId": event_id,
                        "text": txt,
                    }
                )
        # Field 13 (index 12): ZY error
        if len(dec) > 12 and isinstance(dec[12], list):
            zy = dec[12]
            out.append(
                {
                    "type": "error",
                    "eventId": event_id,
                    "code": zy[0] if zy else None,
                    "message": str(zy[1] if len(zy) > 1 and zy[1] is not None else f"Stream error code={zy[0] if zy else ''}"),
                }
            )
        # Field 14 (index 13): tec title
        if len(dec) > 13 and isinstance(dec[13], list) and dec[13]:
            title = str(dec[13][0] or "").strip()
            if title:
                out.append({"type": "title", "eventId": event_id, "title": title})
        # Field 15 (index 14): D$b permission elicitation
        if len(dec) > 14 and isinstance(dec[14], list):
            d_evt = dec[14]
            bp_id = str(d_evt[0] or "").strip() if len(d_evt) > 0 and d_evt[0] is not None else ""
            tools = d_evt[3] if len(d_evt) > 3 and isinstance(d_evt[3], list) else []
            num_img = int(d_evt[4] or 0) if len(d_evt) > 4 and d_evt[4] is not None else (1 if "IMAGE_GENERATION" in tools else 0)
            num_vid = int(d_evt[5] or 0) if len(d_evt) > 5 and d_evt[5] is not None else (1 if "VIDEO_GENERATION" in tools else 0)
            out.append(
                {
                    "type": "permission",
                    "eventId": event_id,
                    "permissionId": bp_id,
                    "source": "elicitation",
                    "totalCost": int(d_evt[2] or 0) if len(d_evt) > 2 and d_evt[2] is not None else 0,
                    "numImages": num_img,
                    "numVideos": num_vid,
                    "numTotal": num_img + num_vid,
                    "tools": tools,
                }
            )
        # Field 19 (index 18): O$b A2UI messages [[kN_1, ...]]
        if len(dec) > 18 and isinstance(dec[18], list):
            o_evt = dec[18]
            kns = o_evt[0] if o_evt and isinstance(o_evt[0], list) else o_evt
            raw_msgs = [cls._decode_proto_struct(item) for item in kns if isinstance(item, (list, dict))]
            a2ui = cls._parse_a2ui_payload(raw_msgs)
            if a2ui:
                out.append({"type": "a2ui", "eventId": event_id, **a2ui})
        # Field 20 (index 19): H$b media status/output
        if len(dec) > 19 and isinstance(dec[19], list):
            h_evt = dec[19]
            mid = str(h_evt[0] or "").strip() if h_evt and h_evt[0] is not None else ""
            if mid:
                out.append(
                    {
                        "type": "media",
                        "eventId": event_id,
                        "mediaId": mid,
                        "workflowId": str(h_evt[8] or "").strip() if len(h_evt) > 8 and h_evt[8] is not None else "",
                    }
                )
        return out

    @classmethod
    def _parse_stream_chat_frames(cls, frames: List[Any]) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []

        def _handle_entry(entry: Any) -> None:
            if (
                isinstance(entry, list)
                and len(entry) >= 3
                and entry[0] == "wrb.fr"
                and isinstance(entry[2], str)
            ):
                try:
                    inner = json.loads(entry[2])
                except (TypeError, ValueError):
                    return
                if isinstance(inner, list) and len(inner) == 1 and isinstance(inner[0], list):
                    j_chunk = inner[0]
                else:
                    j_chunk = inner
                events.extend(cls._parse_stream_chat_chunk_item(j_chunk))
                return
            if isinstance(entry, list) and entry and all(isinstance(sub, list) for sub in entry):
                if any(sub and sub[0] in {"wrb.fr", "er", "di", "af.httprm"} for sub in entry):
                    for sub in entry:
                        _handle_entry(sub)
                    return
            if isinstance(entry, list):
                events.extend(cls._parse_stream_chat_chunk_item(entry))

        for frame in frames:
            _handle_entry(frame)
        return events

    @classmethod
    def _build_agent_user_message(
        cls,
        prompt: str = "",
        *,
        image_refs: Optional[List[Any]] = None,
        character_refs: Optional[List[Any]] = None,
        workflow_refs: Optional[List[Any]] = None,
    ) -> List[Any]:
        """Construct BOQ `_.g_` message array `[Au, hh_media_refs?, Vo_audio_refs?, Nw_character_refs?, tx_likeness_refs?]`."""
        parts: List[Any] = []
        text = str(prompt or "").strip()
        if text:
            parts.append([text])

        normalized_media: List[List[Any]] = []
        for ref in image_refs or []:
            if isinstance(ref, dict):
                mid = cls._normalize_media_name(ref.get("mediaId") or ref.get("id") or "")
                title = str(ref.get("title") or "").strip()
            else:
                mid = cls._normalize_media_name(str(ref or ""))
                title = ""
            if mid:
                item = [mid, title] if title else [mid]
                normalized_media.append(item)
                parts.append([None, [item]])

        normalized_chars: List[List[Any]] = []
        for cref in character_refs or []:
            if isinstance(cref, dict):
                cid = str(cref.get("id") or cref.get("characterId") or "").strip()
                cname = str(cref.get("displayName") or cref.get("name") or "").strip()
            else:
                cid = str(cref or "").strip()
                cname = ""
            if cid:
                citem = [cid, cname] if cname else [cid]
                normalized_chars.append(citem)
                parts.append([None, [None, None, citem]])

        normalized_wfs: List[List[Any]] = []
        for wref in workflow_refs or []:
            if isinstance(wref, dict):
                wid = str(wref.get("mediaId") or wref.get("workflowId") or wref.get("id") or "").strip()
                wtitle = str(wref.get("title") or wref.get("name") or "").strip()
            else:
                wid = str(wref or "").strip()
                wtitle = ""
            if wid:
                normalized_wfs.append([wid])
                parts.append([None, [None, [wid, wtitle] if wtitle else [wid]]])

        if not parts:
            parts.append([""])

        g_msg: List[Any] = [[parts]]
        if normalized_media or normalized_wfs or normalized_chars:
            g_msg.append(normalized_media or None)
        if normalized_wfs or normalized_chars:
            g_msg.append(normalized_wfs or None)
        if normalized_chars:
            g_msg.append(normalized_chars)
        return g_msg

    def _build_agent_webchannel_request(
        self,
        *,
        project_id: str,
        prompt: str = "",
        recaptcha_token: str,
        session_id: str,
        image_refs: Optional[List[Any]] = None,
        character_refs: Optional[List[Any]] = None,
        workflow_refs: Optional[List[Any]] = None,
        permission_action: Optional[int] = None,
        permission_id: Optional[str] = None,
        turn_index: int = 1,
    ) -> str:
        """Construct BOQ `_.rhc` WebChannel payload JSON array."""
        sgc = [
            f"projects/{project_id}",
            None,
            [recaptcha_token, 1],
            None,
            None,
            int(turn_index),
        ]
        if permission_id:
            perm_code = int(permission_action or 1)
            approved = perm_code in {1, 2}
            gpc: List[Any] = [str(permission_id), approved]
            if perm_code == 2:
                gpc.append(2)
            rhc: List[Any] = [None, None, gpc, None, None, sgc]
        else:
            g_msg = self._build_agent_user_message(
                prompt,
                image_refs=image_refs,
                character_refs=character_refs,
                workflow_refs=workflow_refs,
            )
            rhc = [[str(session_id), g_msg], None, None, None, None, sgc]
        return json.dumps(rhc, ensure_ascii=False, separators=(",", ":"))

    @classmethod
    def _consume_webchannel_buffer(cls, buf: str) -> tuple[List[Any], str]:
        """Extract all complete Google WebChannel JSON envelopes from streaming chunk buffer."""
        decoder = json.JSONDecoder()
        envelopes: List[Any] = []
        pos = 0
        length = len(buf)
        while pos < length:
            while pos < length and buf[pos] in " \t\r\n":
                pos += 1
            if pos >= length:
                break
            # Skip length prefix line like "199\n" before JSON array "["
            if buf[pos].isdigit():
                nl = buf.find("\n", pos)
                if nl < 0:
                    break
                if buf[pos:nl].strip().isdigit():
                    pos = nl + 1
                    while pos < length and buf[pos] in " \t\r\n":
                        pos += 1
            if pos >= length:
                break
            try:
                obj, end_pos = decoder.raw_decode(buf, pos)
            except ValueError:
                break
            envelopes.append(obj)
            pos = end_pos
        return envelopes, buf[pos:]

    @classmethod
    def _parse_webchannel_envelopes(cls, envelopes: List[Any]) -> tuple[List[Dict[str, Any]], bool]:
        events: List[Dict[str, Any]] = []
        stream_done = False
        for env in envelopes:
            if not isinstance(env, list):
                continue
            for entry in env:
                if not (isinstance(entry, list) and len(entry) >= 2):
                    continue
                payload = entry[1]
                if payload == ["noop"]:
                    continue
                if payload == ["close"]:
                    stream_done = True
                    continue
                if isinstance(payload, dict) and isinstance(payload.get("__sm__"), dict):
                    sm = payload["__sm__"]
                    status_rows = sm.get("status")
                    err_code = None
                    err_msg = "WebChannel stream error"
                    if (
                        isinstance(status_rows, list)
                        and status_rows
                        and isinstance(status_rows[0], list)
                        and status_rows[0]
                        and isinstance(status_rows[0][0], list)
                    ):
                        st_entry = status_rows[0][0]
                        err_code = st_entry[0] if len(st_entry) > 0 else None
                        base_msg = str(st_entry[1] or "").strip() if len(st_entry) > 1 else ""
                        details = st_entry[2] if len(st_entry) > 2 else None
                        detail_text = json.dumps(details, ensure_ascii=False) if details else ""
                        if base_msg and detail_text:
                            err_msg = f"{base_msg} ({detail_text})"
                        elif base_msg or detail_text:
                            err_msg = base_msg or detail_text
                    events.append({"type": "error", "code": err_code, "message": err_msg})
                    stream_done = True
                    continue
                if not isinstance(payload, list):
                    continue
                for g_item in payload:
                    if not isinstance(g_item, list):
                        continue
                    if len(g_item) > 1 and g_item[1]:
                        stream_done = True
                    dec_list = (
                        g_item[2]
                        if len(g_item) > 2 and isinstance(g_item[2], list)
                        else []
                    )
                    for dec in dec_list:
                        events.extend(cls._parse_webchannel_dec_event(dec))
        return events, stream_done

    def _build_agent_stream_chat_request(
        self,
        *,
        project_id: str,
        prompt: str,
        recaptcha_token: str,
        session_id: str,
        image_refs: Optional[List[Any]] = None,
        character_refs: Optional[List[Any]] = None,
        workflow_refs: Optional[List[Any]] = None,
        permission_action: Optional[int] = None,
        client_context: Optional[List[Any]] = None,
    ) -> str:
        g_msg = self._build_agent_user_message(
            prompt,
            image_refs=image_refs,
            character_refs=character_refs,
            workflow_refs=workflow_refs,
        )
        sgc = [f"projects/{project_id}", None, [recaptcha_token, 1], None, None, 1]
        inner: List[Any] = [str(session_id), g_msg, sgc]
        if permission_action is not None or client_context is not None:
            inner.append(None)
            inner.append(int(permission_action) if permission_action is not None else None)
        if client_context is not None:
            inner.append(client_context)
        inner_json = json.dumps(inner, ensure_ascii=False, separators=(",", ":"))
        return json.dumps([None, inner_json], ensure_ascii=False, separators=(",", ":"))

    async def stream_agent_chat(
        self,
        *,
        project_id: str,
        prompt: str = "",
        session_id: Optional[str] = None,
        image_refs: Optional[List[Any]] = None,
        character_refs: Optional[List[Any]] = None,
        workflow_refs: Optional[List[Any]] = None,
        permission_action: Optional[Any] = None,
        permission_id: Optional[str] = None,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
        timeout: int = 180,
        resolve_media_urls: bool = True,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Stream a Flow Creation Agent turn (`streamCreationAgentChat/channel` WebChannel or HTTP fallback)."""
        normalized_project_id = str(project_id or "").strip()
        if not normalized_project_id:
            raise ValueError("project_id is required")
        resolved_session_id = str(session_id or "").strip() or str(uuid.uuid4())
        perm_code = (
            self._normalize_permission_action(permission_action)
            if permission_action is not None
            else None
        )
        effective_prompt = str(prompt or "").strip()
        if not effective_prompt and perm_code is not None:
            effective_prompt = self.PERMISSION_ACTION_DEFAULT_LABELS[perm_code]
        if not effective_prompt and not (
            image_refs or character_refs or workflow_refs or permission_id
        ):
            raise ValueError("prompt or attachments/permission_action is required")

        def build_webchannel_req(rec_tok: str) -> str:
            return self._build_agent_webchannel_request(
                project_id=normalized_project_id,
                prompt=effective_prompt,
                recaptcha_token=rec_tok,
                session_id=resolved_session_id,
                image_refs=image_refs,
                character_refs=character_refs,
                workflow_refs=workflow_refs,
                permission_action=perm_code,
                permission_id=permission_id,
            )

        def build_req(rec_tok: str) -> str:
            return self._build_agent_stream_chat_request(
                project_id=normalized_project_id,
                prompt=effective_prompt,
                recaptcha_token=rec_tok,
                session_id=resolved_session_id,
                image_refs=image_refs,
                character_refs=character_refs,
                workflow_refs=workflow_refs,
                permission_action=perm_code,
            )

        yield {
            "event": "session",
            "projectId": normalized_project_id,
            "sessionId": resolved_session_id,
        }

        runtime_cfg = self._get_runtime_config()
        from .browser_captcha import BrowserCaptchaService

        use_camoufox_stream = (
            token_id is not None
            and BrowserCaptchaService._instance is not None
            and str(getattr(runtime_cfg, "captcha_method", "") or "").strip().lower()
            in {"browser", "camoufox"}
            and bool(getattr(runtime_cfg, "browser_in_page_rpc", True))
        )

        acc_text = ""
        acc_thinking = ""
        title = ""
        a2ui_messages: List[Dict[str, Any]] = []
        tool_calls: List[Dict[str, Any]] = []
        tool_results: List[Dict[str, Any]] = []
        media_map: Dict[str, Dict[str, Any]] = {}
        pending_permission: Optional[Dict[str, Any]] = None
        error_events: List[Dict[str, Any]] = []
        collected_events: List[Dict[str, Any]] = []

        async def _consume_events(evts: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
            nonlocal acc_text, acc_thinking, title, pending_permission
            for ev in evts:
                etype = ev.get("type")
                if etype == "text":
                    acc_text, delta = self._merge_stream_text(acc_text, str(ev.get("text") or ""))
                    if delta:
                        out = {"event": "text", "delta": delta, "text": acc_text}
                        collected_events.append(out)
                        yield out
                elif etype == "thinking":
                    acc_thinking, delta = self._merge_stream_text(acc_thinking, str(ev.get("text") or ""))
                    if delta:
                        out = {"event": "thinking", "delta": delta, "thinking": acc_thinking}
                        collected_events.append(out)
                        yield out
                elif etype == "a2ui":
                    a2ui_messages.extend(ev.get("messages") or [])
                    a2ui_txt = str(ev.get("text") or "")
                    if a2ui_txt:
                        acc_text, delta = self._merge_stream_text(acc_text, a2ui_txt)
                    out = {
                        "event": "a2ui",
                        "text": a2ui_txt,
                        "options": ev.get("options") or [],
                        "messages": ev.get("messages") or [],
                    }
                    collected_events.append(out)
                    yield out
                elif etype == "tool_call":
                    tool_calls.append(ev)
                    out = {"event": "tool_call", **{k: v for k, v in ev.items() if k != "type"}}
                    collected_events.append(out)
                    yield out
                elif etype == "tool_result":
                    tool_results.append(ev)
                    out = {"event": "tool_result", **{k: v for k, v in ev.items() if k != "type"}}
                    collected_events.append(out)
                    yield out
                elif etype == "media":
                    mid = str(ev.get("mediaId") or "").strip()
                    if mid:
                        existing = media_map.get(mid) or {"mediaId": mid}
                        for k, v in ev.items():
                            if k not in {"type", "eventId"} and v:
                                existing[k] = v
                        media_map[mid] = existing
                        out = {"event": "media", **existing}
                        collected_events.append(out)
                        yield out
                elif etype == "permission":
                    pending_permission = {k: v for k, v in ev.items() if k != "type"}
                    out = {"event": "permission", **pending_permission}
                    collected_events.append(out)
                    yield out
                elif etype == "title":
                    title = str(ev.get("title") or "").strip()
                    out = {"event": "title", "title": title}
                    collected_events.append(out)
                    yield out
                elif etype == "error":
                    error_events.append(ev)
                    out = {"event": "error", **{k: v for k, v in ev.items() if k != "type"}}
                    collected_events.append(out)
                    yield out

        if use_camoufox_stream:
            service = await BrowserCaptchaService.get_instance(self.db)
            wc_buf = ""
            line_buf = ""
            async with contextlib.aclosing(
                service.stream_flow_agent_chat(
                    project_id=normalized_project_id,
                    request_builder=build_webchannel_req,
                    timeout=timeout,
                    token_id=token_id,
                )
            ) as agent_stream:
                async for chunk in agent_stream:
                    delta_raw = str(chunk.get("delta") or "")
                    transport = str(chunk.get("transport") or "webchannel")
                    if transport == "webchannel":
                        if delta_raw:
                            wc_buf += delta_raw
                        envelopes, wc_buf = self._consume_webchannel_buffer(wc_buf)
                        if envelopes:
                            parsed, wc_done = self._parse_webchannel_envelopes(envelopes)
                            async for ev_out in _consume_events(parsed):
                                yield ev_out
                            if wc_done:
                                break
                    else:
                        if delta_raw:
                            line_buf += delta_raw
                        if chunk.get("done"):
                            lines = line_buf.splitlines()
                            line_buf = ""
                        elif "\n" in line_buf:
                            parts = line_buf.split("\n")
                            lines = parts[:-1]
                            line_buf = parts[-1]
                        else:
                            lines = []
                        if lines:
                            chunk_text = "\n".join(lines)
                            err_match = re.search(r"PUBLIC_ERROR_[A-Z_]+", chunk_text)
                            if err_match:
                                raise RuntimeError(
                                    f"Flow Agent StreamChat rejected: error={err_match.group(0)}"
                                )
                            frames = self._parse_batchexecute_frames(chunk_text)
                            self._raise_batchexecute_error(frames, "StreamChat")
                            parsed = self._parse_stream_chat_frames(frames)
                            async for ev_out in _consume_events(parsed):
                                yield ev_out
        else:
            cookie_storage = await self._resolve_flow_frontend_cookie_storage(
                google_cookies, token_id
            )
            cookie_header = self._parse_flow_cookie_storage(cookie_storage)
            await self._frontend_cookie(cookie_storage, token_id)
            recaptcha_token, browser_id = await self._get_recaptcha_token(
                normalized_project_id,
                action="CHAT_GENERATION",
                token_id=token_id,
                website_url=self._build_flow_frontend_project_page_url(normalized_project_id),
            )
            if not recaptcha_token:
                raise RuntimeError("Failed to obtain reCAPTCHA token for agent chat")
            try:
                stream_res = await self._call_flow_stream_chat_payload(
                    project_id=normalized_project_id,
                    f_request=build_req(recaptcha_token),
                    cookie_header=cookie_header,
                    timeout=timeout,
                    cookie_storage=cookie_storage,
                )
            finally:
                await self._notify_browser_captcha_request_finished(browser_id)
            parsed = self._parse_stream_chat_frames(stream_res.get("frames") or [])
            async for ev_out in _consume_events(parsed):
                yield ev_out

        if (
            error_events
            and not acc_text
            and not acc_thinking
            and not a2ui_messages
            and not media_map
            and not pending_permission
        ):
            raise RuntimeError(
                str(error_events[-1].get("message") or "Flow Agent WebChannel failed")
            )

        # Best-effort media URL hydration for generated artifacts
        if resolve_media_urls and media_map:
            for mid, m_info in list(media_map.items()):
                if not m_info.get("url") and self._is_uuid(mid):
                    try:
                        detail = await self.get_media(
                            self.FRONTEND_ACCESS_TOKEN,
                            mid,
                            google_cookies=google_cookies,
                            token_id=token_id,
                            project_id=normalized_project_id,
                        )
                        if detail.get("url"):
                            m_info["url"] = detail["url"]
                        if detail.get("mediaType"):
                            m_info["mediaType"] = detail["mediaType"]
                    except Exception:
                        pass

        yield {
            "event": "done",
            "projectId": normalized_project_id,
            "sessionId": resolved_session_id,
            "title": title,
            "text": acc_text,
            "thinking": acc_thinking,
            "a2ui": a2ui_messages,
            "toolCalls": tool_calls,
            "toolResults": tool_results,
            "media": list(media_map.values()),
            "pendingPermission": pending_permission,
            "errors": error_events,
            "endpoint": (
                "flowCreationAgent/streamCreationAgentChat"
                if use_camoufox_stream
                else "FlowCreationAgentService/StreamChat"
            ),
        }
    async def _call_flow_stream_chat_payload(
        self,
        *,
        project_id: str,
        f_request: str,
        cookie_header: str,
        timeout: int,
        cookie_storage: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute raw `FlowCreationAgentService/StreamChat` payload via HTTP fallback."""
        page_url = f"https://flow.google.com/project/{project_id}"
        bootstrap_cookie_header = (
            serialize_cookie_header_for_url(cookie_storage, page_url)
            if str(cookie_storage or "").strip()
            else cookie_header
        )
        if not bootstrap_cookie_header:
            raise RuntimeError("Flow StreamChat requires Google session cookies")
        original_cookies = cookie_storage if str(cookie_storage or "").strip() else cookie_header
        cookie_jar = FlowCookieJar(self.get_updated_flow_cookies(original_cookies))
        bootstrap_text = await self._load_flow_frontend_bootstrap(
            page_url, bootstrap_cookie_header, timeout, cookie_jar=cookie_jar
        )
        self._remember_flow_cookies(original_cookies, cookie_jar)
        bl_m = re.search(r"boq_labs-ai-sandbox-frontend_[A-Za-z0-9_.-]+", bootstrap_text)
        sid_m = re.search(r'"FdrFJe":"(-?[0-9]+)"', bootstrap_text)
        at_tok = self._extract_stream_chat_at_token(bootstrap_text)
        if not bl_m or not sid_m or not at_tok:
            raise RuntimeError("Flow frontend bootstrap metadata or XSRF token is unavailable")
        from urllib.parse import quote
        import random

        url = (
            f"https://flow.google.com{self.STREAM_CHAT_RPC_PATH}"
            f"?bl={quote(bl_m.group(0), safe='')}"
            f"&f.sid={sid_m.group(1)}&hl=en"
            f"&_reqid={random.randint(100000, 999999)}&rt=c"
        )
        rpc_cookie = serialize_cookie_header_for_url(cookie_jar.export_storage(), url)
        headers = {
            "Accept": "*/*",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "Cookie": rpc_cookie,
            "Origin": "https://flow.google.com",
            "Referer": "https://flow.google.com/",
            "x-client-deadline-ms": "600000",
            "x-same-domain": "1",
        }
        body = "f.req=" + quote(f_request, safe="") + "&at=" + quote(at_tok, safe="")
        text = await self._make_text_request(
            method="POST",
            url=url,
            headers=headers,
            raw_body=body,
            timeout=timeout,
            apply_default_client_headers=False,
            return_error_response=True,
            redact_sensitive_logs=True,
            cookie_jar=cookie_jar,
        )
        err_match = re.search(r"PUBLIC_ERROR_[A-Z_]+", text)
        if err_match:
            raise RuntimeError(f"Flow StreamChat rejected: error={err_match.group(0)}")
        frames = self._parse_batchexecute_frames(text)
        self._remember_flow_cookies(original_cookies, cookie_jar)
        return {"frames": frames, "responseText": text}

    async def agent_chat(
        self,
        *,
        project_id: str,
        prompt: str = "",
        session_id: Optional[str] = None,
        image_refs: Optional[List[Any]] = None,
        character_refs: Optional[List[Any]] = None,
        workflow_refs: Optional[List[Any]] = None,
        permission_action: Optional[Any] = None,
        permission_id: Optional[str] = None,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
        timeout: int = 180,
    ) -> Dict[str, Any]:
        """Execute a complete Flow Creation Agent turn and return structured summary."""
        events: List[Dict[str, Any]] = []
        done_payload: Dict[str, Any] = {}
        async for item in self.stream_agent_chat(
            project_id=project_id,
            prompt=prompt,
            session_id=session_id,
            image_refs=image_refs,
            character_refs=character_refs,
            workflow_refs=workflow_refs,
            permission_action=permission_action,
            permission_id=permission_id,
            google_cookies=google_cookies,
            token_id=token_id,
            timeout=timeout,
            resolve_media_urls=True,
        ):
            if item.get("event") == "done":
                done_payload = dict(item)
                done_payload.pop("event", None)
            else:
                events.append(item)
        done_payload["events"] = events
        return done_payload

    async def approve_agent_action(
        self,
        *,
        project_id: str,
        session_id: str,
        action: Any = "approved",
        label: Optional[str] = None,
        permission_id: Optional[str] = None,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
        timeout: int = 180,
    ) -> Dict[str, Any]:
        """Respond to a pending Flow Creation Agent permission prompt and resume the turn."""
        perm_code = self._normalize_permission_action(action)
        effective_label = str(label or "").strip() or self.PERMISSION_ACTION_DEFAULT_LABELS[perm_code]
        resolved_permission_id = str(permission_id or "").strip() or None
        if not resolved_permission_id:
            runtime_cfg = self._get_runtime_config()
            from .browser_captcha import BrowserCaptchaService
            if (
                token_id is not None
                and BrowserCaptchaService._instance is not None
                and str(getattr(runtime_cfg, "captcha_method", "") or "").strip().lower()
                in {"browser", "camoufox"}
                and bool(getattr(runtime_cfg, "browser_in_page_rpc", True))
            ):
                try:
                    sess = await self.get_agent_session(
                        session_id,
                        project_id=project_id,
                        google_cookies=google_cookies,
                        token_id=token_id,
                    )
                    for turn in reversed(sess.get("turns") or []):
                        perm = turn.get("permission")
                        if isinstance(perm, dict) and perm.get("permissionId"):
                            resolved_permission_id = str(perm["permissionId"]).strip()
                            break
                except Exception:
                    pass
        return await self.agent_chat(
            project_id=project_id,
            prompt=effective_label,
            session_id=session_id,
            permission_action=perm_code,
            permission_id=resolved_permission_id,
            google_cookies=google_cookies,
            token_id=token_id,
            timeout=timeout,
        )

    # ── Agent Session Management RPCs ─────────────────────────────────────────

    @classmethod
    def _normalize_agent_session_item(cls, raw: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, list) or not raw:
            return None
        sid = str(raw[0] or "").strip()
        if not sid:
            return None
        meta = raw[1] if len(raw) > 1 and isinstance(raw[1], list) else []
        title = str(meta[0] or "").strip() if meta and meta[0] is not None else ""
        updated_at = None
        for candidate in (meta[2] if len(meta) > 2 else None, meta[1] if len(meta) > 1 else None):
            parsed = cls._parse_flow_timestamp(candidate)
            if parsed:
                updated_at = parsed
                break
        return {
            "sessionId": sid,
            "title": title,
            "updatedAt": updated_at,
        }

    async def list_agent_sessions(
        self,
        project_id: str,
        *,
        page_size: Optional[int] = None,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_project_id = str(project_id or "").strip()
        if not normalized_project_id:
            raise ValueError("project_id is required")
        arg: List[Any] = [normalized_project_id]
        if page_size is not None and int(page_size) > 0:
            arg.append(int(page_size))
        payload = await self._current_rpc(
            rpc_id="mrlkwd",
            argument=arg,
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=normalized_project_id,
            source_path=f"/project/{normalized_project_id}",
        )
        raw_items = (
            payload[0]
            if isinstance(payload, list) and payload and isinstance(payload[0], list)
            else (payload if isinstance(payload, list) else [])
        )
        sessions = [
            item
            for item in (self._normalize_agent_session_item(row) for row in raw_items)
            if item is not None
        ]
        return {
            "projectId": normalized_project_id,
            "sessions": sessions,
            "frontendRpc": "mrlkwd",
        }

    @classmethod
    def _extract_user_message_from_g(cls, raw_g: Any) -> Dict[str, Any]:
        if not isinstance(raw_g, list):
            return {"text": "", "imageRefs": [], "characterRefs": []}
        texts: List[str] = []
        media_refs: List[str] = []
        au = raw_g[0] if len(raw_g) > 0 and isinstance(raw_g[0], list) else []
        parts = au[0] if len(au) > 0 and isinstance(au[0], list) else []
        for part in parts:
            if not isinstance(part, list):
                continue
            if len(part) > 0 and isinstance(part[0], str) and part[0].strip():
                texts.append(part[0])
            if len(part) > 1 and isinstance(part[1], list):
                rz_items = part[1][0] if part[1] and isinstance(part[1][0], list) else part[1]
                for qz in rz_items:
                    if isinstance(qz, list) and qz and isinstance(qz[0], str):
                        media_refs.append(qz[0])
        for extra in (raw_g[1] if len(raw_g) > 1 and isinstance(raw_g[1], list) else []):
            if isinstance(extra, list) and extra and isinstance(extra[0], str) and extra[0] not in media_refs:
                media_refs.append(extra[0])
        char_refs = []
        for c_item in (raw_g[2] if len(raw_g) > 2 and isinstance(raw_g[2], list) else []):
            if isinstance(c_item, list) and c_item and isinstance(c_item[0], str):
                char_refs.append(c_item[0])
        return {
            "text": "\n".join(texts),
            "imageRefs": media_refs,
            "characterRefs": char_refs,
        }

    @classmethod
    def _normalize_agent_turn(cls, raw_turn: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(raw_turn, list):
            return None
        user_msg = cls._extract_user_message_from_g(raw_turn[0] if len(raw_turn) > 0 else None)
        chunks = raw_turn[1] if len(raw_turn) > 1 and isinstance(raw_turn[1], list) else []
        dec_events = raw_turn[2] if len(raw_turn) > 2 and isinstance(raw_turn[2], list) else []
        approvals = raw_turn[3] if len(raw_turn) > 3 and isinstance(raw_turn[3], list) else []
        parsed_events: List[Dict[str, Any]] = []
        for dec in dec_events:
            parsed_events.extend(cls._parse_webchannel_dec_event(dec))
        if not dec_events:
            for ch in chunks:
                parsed_events.extend(cls._parse_stream_chat_chunk_item(ch))
        acc_text = ""
        acc_thinking = ""
        tool_calls: List[Dict[str, Any]] = []
        tool_results: List[Dict[str, Any]] = []
        media: List[Dict[str, Any]] = []
        a2ui: List[Dict[str, Any]] = []
        permission: Optional[Dict[str, Any]] = None
        for ev in parsed_events:
            et = ev.get("type")
            if et == "text":
                acc_text, _ = cls._merge_stream_text(acc_text, str(ev.get("text") or ""))
            elif et == "thinking":
                acc_thinking, _ = cls._merge_stream_text(acc_thinking, str(ev.get("text") or ""))
            elif et == "a2ui":
                a2ui.extend(ev.get("messages") or [])
                if ev.get("text"):
                    acc_text, _ = cls._merge_stream_text(acc_text, str(ev.get("text")))
            elif et == "tool_call":
                tool_calls.append(ev)
            elif et == "tool_result":
                tool_results.append(ev)
            elif et == "media":
                media.append(ev)
            elif et == "permission":
                permission = ev
        resolved_approvals = []
        for rkc in approvals:
            if isinstance(rkc, list) and rkc:
                resolved_approvals.append(
                    {
                        "permissionId": str(rkc[0] or ""),
                        "approved": bool(rkc[1]) if len(rkc) > 1 else None,
                        "actionCode": rkc[2] if len(rkc) > 2 else None,
                    }
                )
        return {
            "user": user_msg,
            "text": acc_text,
            "thinking": acc_thinking,
            "a2ui": a2ui,
            "toolCalls": tool_calls,
            "toolResults": tool_results,
            "media": media,
            "permission": permission,
            "approvals": resolved_approvals,
            "state": raw_turn[4] if len(raw_turn) > 4 else None,
        }

    async def get_agent_session(
        self,
        session_id: str,
        *,
        project_id: Optional[str] = None,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            raise ValueError("session_id is required")
        normalized_project_id = str(project_id or "").strip() or None
        payload = await self._current_rpc(
            rpc_id="GN0Bre",
            argument=[normalized_session_id],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=normalized_project_id,
        )
        meta_raw = payload[0] if isinstance(payload, list) and len(payload) > 0 else None
        turns_raw = payload[1] if isinstance(payload, list) and len(payload) > 1 and isinstance(payload[1], list) else []
        meta = self._normalize_agent_session_item(meta_raw) or {
            "sessionId": normalized_session_id,
            "title": "",
            "updatedAt": None,
        }
        turns = [
            t
            for t in (self._normalize_agent_turn(item) for item in turns_raw)
            if t is not None
        ]
        return {
            **meta,
            "projectId": normalized_project_id,
            "turns": turns,
            "frontendRpc": "GN0Bre",
        }

    async def rename_agent_session(
        self,
        session_id: str,
        title: str,
        *,
        project_id: Optional[str] = None,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_session_id = str(session_id or "").strip()
        normalized_title = str(title or "").strip()
        if not normalized_session_id or not normalized_title:
            raise ValueError("session_id and title are required")
        normalized_project_id = str(project_id or "").strip()
        await self._current_rpc(
            rpc_id="TunYMc",
            argument=[normalized_session_id, normalized_title, normalized_project_id],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=normalized_project_id or None,
        )
        return {
            "sessionId": normalized_session_id,
            "title": normalized_title,
            "projectId": normalized_project_id or None,
            "frontendRpc": "TunYMc",
        }

    async def delete_agent_session(
        self,
        session_id: str,
        *,
        project_id: Optional[str] = None,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            raise ValueError("session_id is required")
        await self._current_rpc(
            rpc_id="Dcn2Le",
            argument=[normalized_session_id],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=str(project_id or "").strip() or None,
        )
        return {
            "deleted": True,
            "sessionId": normalized_session_id,
            "frontendRpc": "Dcn2Le",
        }

    # ── User Preferences & Project Settings RPCs ──────────────────────────────

    @staticmethod
    def _normalize_user_preferences(raw: Any) -> Dict[str, Any]:
        arr = raw if isinstance(raw, list) else []

        def _get(idx: int) -> Any:
            return arr[idx] if 0 <= idx < len(arr) else None

        return {
            "lastAcknowledgedChangeLogId": _get(1),
            "dismissedAnnouncementIds": _get(2) if isinstance(_get(2), list) else [],
            "shouldClearPromptBoxOnSubmit": bool(_get(5)) if _get(5) is not None else None,
            "hasSoundOnTileHover": bool(_get(6)) if _get(6) is not None else None,
            "isTileDetailsAlwaysVisible": bool(_get(7)) if _get(7) is not None else None,
            "shouldReturnSilentVideos": bool(_get(9)) if _get(9) is not None else None,
            "isChatPanelOpen": bool(_get(12)) if _get(12) is not None else None,
            "isWatermarkEnabledByUser": bool(_get(13)) if _get(13) is not None else None,
        }

    async def get_user_preferences(
        self,
        *,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        payload = await self._current_rpc(
            rpc_id="Yizz8d",
            argument=[],
            google_cookies=google_cookies,
            token_id=token_id,
            source_path="/projects",
        )
        return {
            **self._normalize_user_preferences(payload),
            "frontendRpc": "Yizz8d",
        }

    async def update_user_preferences(
        self,
        updates: Dict[str, Any],
        *,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        field_specs = {
            "last_acknowledged_change_log_id": (1, "last_acknowledged_change_log_id", str),
            "lastAcknowledgedChangeLogId": (1, "last_acknowledged_change_log_id", str),
            "should_clear_prompt_box_on_submit": (5, "should_clear_prompt_box_on_submit", bool),
            "shouldClearPromptBoxOnSubmit": (5, "should_clear_prompt_box_on_submit", bool),
            "has_sound_on_tile_hover": (6, "has_sound_on_tile_hover", bool),
            "hasSoundOnTileHover": (6, "has_sound_on_tile_hover", bool),
            "is_tile_details_always_visible": (7, "is_tile_details_always_visible", bool),
            "isTileDetailsAlwaysVisible": (7, "is_tile_details_always_visible", bool),
            "should_return_silent_videos": (9, "should_return_silent_videos", bool),
            "shouldReturnSilentVideos": (9, "should_return_silent_videos", bool),
            "is_chat_panel_open": (12, "is_chat_panel_open", bool),
            "isChatPanelOpen": (12, "is_chat_panel_open", bool),
            "is_watermark_enabled_by_user": (13, "is_watermark_enabled_by_user", bool),
            "isWatermarkEnabledByUser": (13, "is_watermark_enabled_by_user", bool),
        }
        arr: List[Any] = [None] * 14
        paths: List[str] = []
        for key, val in (updates or {}).items():
            if val is None or key not in field_specs:
                continue
            idx, mask_path, caster = field_specs[key]
            arr[idx] = caster(val)
            if mask_path not in paths:
                paths.append(mask_path)
        if not paths:
            raise ValueError("At least one valid user preference field is required")
        while arr and arr[-1] is None:
            arr.pop()
        payload = await self._current_rpc(
            rpc_id="DA4VGb",
            argument=[arr, [paths]],
            google_cookies=google_cookies,
            token_id=token_id,
            source_path="/projects",
        )
        return {
            **self._normalize_user_preferences(payload),
            "updatedFields": paths,
            "frontendRpc": "DA4VGb",
        }

    @classmethod
    def _normalize_project_settings(cls, raw: Any) -> Dict[str, Any]:
        arr = raw if isinstance(raw, list) else []
        pinned = arr[0] if len(arr) > 0 and isinstance(arr[0], list) else []
        brief_raw = arr[1] if len(arr) > 1 and isinstance(arr[1], list) else []
        gen_defaults_raw = arr[2] if len(arr) > 2 and isinstance(arr[2], list) else []
        policy_raw = arr[3] if len(arr) > 3 else None
        chat_open_raw = arr[5] if len(arr) > 5 else None

        image_defaults = None
        video_defaults = None
        if gen_defaults_raw:
            if len(gen_defaults_raw) > 0 and isinstance(gen_defaults_raw[0], list):
                img = gen_defaults_raw[0]
                image_defaults = {
                    "aspectRatio": img[1] if len(img) > 1 else None,
                    "count": img[2] if len(img) > 2 else None,
                    "model": img[3] if len(img) > 3 else None,
                }
            if len(gen_defaults_raw) > 1 and isinstance(gen_defaults_raw[1], list):
                vid = gen_defaults_raw[1]
                video_defaults = {
                    "model": vid[0] if len(vid) > 0 else None,
                    "aspectRatio": vid[1] if len(vid) > 1 else None,
                    "count": vid[2] if len(vid) > 2 else None,
                }

        brief_notes = []
        if brief_raw:
            notes_list = brief_raw[1] if len(brief_raw) > 1 and isinstance(brief_raw[1], list) else []
            for n in notes_list:
                if isinstance(n, list) and n:
                    brief_notes.append(
                        {
                            "id": n[0] if len(n) > 0 else None,
                            "title": n[1] if len(n) > 1 else None,
                            "content": n[2] if len(n) > 2 else None,
                        }
                    )

        policy_int = int(policy_raw) if isinstance(policy_raw, (int, float)) else None
        return {
            "pinnedApplets": pinned,
            "projectBrief": brief_notes,
            "defaultGenerationSettings": {
                "imageDefaults": image_defaults,
                "videoDefaults": video_defaults,
            },
            "creditSpendApprovalPolicy": policy_int,
            "creditSpendApprovalPolicyName": (
                cls.CREDIT_SPEND_APPROVAL_POLICY_NAMES.get(policy_int, "unknown")
                if policy_int is not None
                else None
            ),
            "chatPanelOpen": bool(chat_open_raw) if chat_open_raw is not None else None,
        }

    async def get_project_settings(
        self,
        project_id: str,
        *,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        proj = await self.get_project(
            project_id, google_cookies=google_cookies, token_id=token_id
        )
        return {
            "projectId": proj["projectId"],
            **(proj.get("settings") or {}),
            "frontendRpc": "Zzl0ze",
        }

    async def update_project_settings(
        self,
        project_id: str,
        updates: Dict[str, Any],
        *,
        google_cookies: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_project_id = str(project_id or "").strip()
        if not normalized_project_id:
            raise ValueError("project_id is required")
        mz: List[Any] = [None] * 6
        paths: List[str] = []

        chat_open = updates.get("chat_panel_open")
        if chat_open is None:
            chat_open = updates.get("chatPanelOpen")
        if chat_open is not None:
            mz[5] = bool(chat_open)
            paths.append("chat_panel_open")

        policy = updates.get("credit_spend_approval_policy")
        if policy is None:
            policy = updates.get("creditSpendApprovalPolicy")
        if policy is not None:
            if isinstance(policy, str):
                rev = {v: k for k, v in self.CREDIT_SPEND_APPROVAL_POLICY_NAMES.items()}
                p_key = policy.strip().lower()
                policy_val = rev[p_key] if p_key in rev else int(policy)
            else:
                policy_val = int(policy)
            mz[3] = policy_val
            paths.append("credit_spend_approval_policy")

        pinned = updates.get("pinned_applets")
        if pinned is None:
            pinned = updates.get("pinnedApplets")
        if pinned is not None:
            mz[0] = [str(x) for x in pinned]
            paths.append("pinned_applets")

        image_defaults = updates.get("image_defaults") or updates.get("imageDefaults")
        video_defaults = updates.get("video_defaults") or updates.get("videoDefaults")
        if isinstance(image_defaults, dict) or isinstance(video_defaults, dict):
            lz: List[Any] = [None, None]
            if isinstance(image_defaults, dict):
                jz: List[Any] = [None, None, None, None]
                if image_defaults.get("aspectRatio") is not None:
                    jz[1] = image_defaults.get("aspectRatio")
                if image_defaults.get("count") is not None:
                    jz[2] = int(image_defaults["count"])
                if image_defaults.get("model") is not None:
                    jz[3] = str(image_defaults["model"])
                lz[0] = jz
                paths.append("default_generation_settings.image_defaults")
            if isinstance(video_defaults, dict):
                kz: List[Any] = [None, None, None]
                if video_defaults.get("model") is not None:
                    kz[0] = str(video_defaults["model"])
                if video_defaults.get("aspectRatio") is not None:
                    kz[1] = video_defaults.get("aspectRatio")
                if video_defaults.get("count") is not None:
                    kz[2] = int(video_defaults["count"])
                lz[1] = kz
                paths.append("default_generation_settings.video_defaults")
            mz[2] = lz

        if not paths:
            raise ValueError("At least one valid project setting field is required")
        while mz and mz[-1] is None:
            mz.pop()

        payload = await self._current_rpc(
            rpc_id="Kcr7Ub",
            argument=[f"projects/{normalized_project_id}", mz, [paths]],
            google_cookies=google_cookies,
            token_id=token_id,
            project_id=normalized_project_id,
            source_path=f"/project/{normalized_project_id}",
        )
        self._invalidate_project_snapshot_cache(normalized_project_id, token_id=token_id)
        raw_mz = payload[0] if isinstance(payload, list) and payload and isinstance(payload[0], list) else (payload if isinstance(payload, list) else mz)
        return {
            "projectId": normalized_project_id,
            **self._normalize_project_settings(raw_mz),
            "updatedFields": paths,
            "frontendRpc": "Kcr7Ub",
        }
    async def _warmup_flow_video_frontend_context(
        self, *args: Any, **kwargs: Any
    ) -> None:
        return None

    async def _labs_trpc_get_with_st(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        raise RuntimeError("The retired Labs tRPC transport is disabled")

    async def _labs_trpc_post_with_st(
        self, *args: Any, **kwargs: Any
    ) -> Dict[str, Any]:
        raise RuntimeError("The retired Labs tRPC transport is disabled")

    async def _aisandbox_request(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        raise RuntimeError("The retired AI Sandbox REST transport is disabled")
