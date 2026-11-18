"""Service layer for Flow Voices, Characters, Assets, and Agent APIs."""

import asyncio
import base64
import contextvars
import hashlib
import inspect
import json
import re
from collections import OrderedDict
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

from curl_cffi.requests import AsyncSession
from fastapi import HTTPException

from pathlib import Path
import tempfile
import uuid
from ..core.account_tiers import (
    normalize_user_paygate_tier,
    supports_model_for_tier,
)
from ..core.config import config
from ..core.database import Database
from ..core.flow_errors import FlowErrorCategory, classify_flow_error
from ..core.logger import debug_logger
from ..core.models import RequestLog, Token
from ..core.prompt_tools import (
    parse_prompt_mentions,
    resolve_prompt_mentions,
    strip_sd_banned_keywords,
)
from ..core.redaction import redact_sensitive_text
from .video_chain import check_ffmpeg_available, concat_videos, extract_tail_frame
DATA_URL_RE = re.compile(r"^data:(?P<mime>[^;]+);base64,(?P<data>.+)$", re.DOTALL)
_resolved_flow_token_id: contextvars.ContextVar[Optional[int]] = contextvars.ContextVar(
    "_resolved_flow_token_id", default=None
)


def get_resolved_flow_token_id() -> Optional[int]:
    return _resolved_flow_token_id.get()


def set_resolved_flow_token_id(token_id: Optional[int]) -> contextvars.Token:
    return _resolved_flow_token_id.set(token_id)


def reset_resolved_flow_token_id(ctx_token: contextvars.Token) -> None:
    _resolved_flow_token_id.reset(ctx_token)


class FlowFeatureService:
    """Orchestrates account/project resolution and Flow Voices/Characters/Assets/Agent calls."""

    ASSET_OWNER_CACHE_LIMIT = 2048

    def __init__(
        self,
        flow_client,
        token_manager,
        load_balancer,
        db,
        file_cache=None,
        proxy_manager=None,
    ):
        self.flow_client = flow_client
        self.token_manager = token_manager
        self.load_balancer = load_balancer
        self.db = db
        self.file_cache = file_cache
        self.proxy_manager = proxy_manager
        self._asset_owner_cache: "OrderedDict[str, Tuple[int, Optional[str]]]" = OrderedDict()

    def remember_asset_owner(
        self,
        media_id: Optional[str],
        token_id: Optional[int],
        project_id: Optional[str] = None,
    ) -> None:
        norm_mid = str(media_id or "").strip()
        if not norm_mid or token_id is None:
            return
        norm_pid = str(project_id or "").strip() or None
        self._asset_owner_cache.pop(norm_mid, None)
        self._asset_owner_cache[norm_mid] = (int(token_id), norm_pid)
        while len(self._asset_owner_cache) > self.ASSET_OWNER_CACHE_LIMIT:
            self._asset_owner_cache.popitem(last=False)

    async def _lookup_asset_owner(
        self, media_id: str
    ) -> Optional[Tuple[int, Optional[str]]]:
        norm_mid = str(media_id or "").strip()
        if not norm_mid:
            return None
        cached = self._asset_owner_cache.get(norm_mid)
        if cached is not None:
            self._asset_owner_cache.move_to_end(norm_mid)
            return cached
        if isinstance(self.db, Database):
            try:
                async with self.db._connect() as conn:
                    cursor = await conn.execute(
                        """
                        SELECT token_id FROM request_logs
                        WHERE status_code = 200
                          AND token_id IS NOT NULL
                          AND id > COALESCE((SELECT MAX(id) - 500 FROM request_logs), 0)
                          AND response_body LIKE ?
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (f"%{norm_mid}%",),
                    )
                    row = await cursor.fetchone()
                    if row and row[0] is not None:
                        owner_tid = int(row[0])
                        self.remember_asset_owner(norm_mid, owner_tid, None)
                        return (owner_tid, None)
            except Exception:
                pass
        return None

    def _record_account_fault(self, token_id: Optional[int], err: Exception) -> bool:
        """Record risk/auth cooldown on load_balancer and return True if failover-eligible."""
        if isinstance(err, HTTPException):
            sc = int(err.status_code)
            detail = redact_sensitive_text(str(err.detail or ""))
            if sc in {401, 403, 429, 502, 503}:
                if token_id is not None and self.load_balancer is not None:
                    mark_cd = getattr(self.load_balancer, "mark_token_cooldown", None)
                    if callable(mark_cd):
                        ttl = 120.0 if sc == 401 else 180.0
                        mark_cd(token_id, seconds=ttl, reason=f"HTTP {sc}: {detail[:100]}")
                if sc == 401 and token_id is not None and self.token_manager is not None:
                    clear_cache = getattr(self.token_manager, "_clear_at_validation_cache", None)
                    if callable(clear_cache):
                        clear_cache(token_id)
                return True
            return False

        classified = classify_flow_error(err)
        if classified.category in (
            FlowErrorCategory.CONTENT_POLICY,
            FlowErrorCategory.INVALID_INPUT,
            FlowErrorCategory.NOT_FOUND,
        ):
            return False

        if token_id is not None and classified.cooldown_seconds > 0:
            if self.load_balancer is not None:
                mark_cd = getattr(self.load_balancer, "mark_token_cooldown", None)
                if callable(mark_cd):
                    mark_cd(
                        token_id,
                        seconds=classified.cooldown_seconds,
                        reason=classified.sanitized_detail[:120],
                    )
            if classified.category == FlowErrorCategory.AUTH_EXPIRED and self.token_manager is not None:
                clear_cache = getattr(self.token_manager, "_clear_at_validation_cache", None)
                if callable(clear_cache):
                    clear_cache(token_id)
        return classified.failover_other_token

    async def _run_with_account_failover(
        self,
        op_fn: Callable[[Token, str], Awaitable[Dict[str, Any]]],
        *,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
        project_id: Optional[str] = None,
        require_project: bool = True,
        prefer_warm_browser: bool = False,
        max_attempts: int = 2,
        preferred_owner: Optional[Tuple[int, Optional[str]]] = None,
        failover_on_404: bool = False,
    ) -> Dict[str, Any]:
        pinned = token_id is not None or bool(str(email or "").strip())
        if pinned:
            tok, resolved_pid = await self.resolve_account_context(
                email=email,
                token_id=token_id,
                project_id=project_id,
                require_project=require_project,
                prefer_warm_browser=prefer_warm_browser,
            )
            try:
                return await op_fn(tok, resolved_pid)
            except Exception as exc:
                self._record_account_fault(tok.id, exc)
                raise

        tried_ids: Set[int] = set()
        last_exc: Optional[Exception] = None
        total_attempts = max(1, int(max_attempts))

        for attempt in range(total_attempts):
            candidate_tid: Optional[int] = None
            candidate_pid = project_id
            if (
                attempt == 0
                and preferred_owner is not None
                and preferred_owner[0] not in tried_ids
            ):
                candidate_tid = preferred_owner[0]
                if not candidate_pid and preferred_owner[1]:
                    candidate_pid = preferred_owner[1]

            try:
                if candidate_tid is not None:
                    try:
                        tok, resolved_pid = await self.resolve_account_context(
                            token_id=candidate_tid,
                            project_id=candidate_pid,
                            require_project=require_project,
                            prefer_warm_browser=prefer_warm_browser,
                        )
                    except HTTPException:
                        tried_ids.add(candidate_tid)
                        tok, resolved_pid = await self.resolve_account_context(
                            project_id=project_id,
                            require_project=require_project,
                            excluded_token_ids=tried_ids,
                            prefer_warm_browser=prefer_warm_browser,
                        )
                else:
                    tok, resolved_pid = await self.resolve_account_context(
                        project_id=project_id,
                        require_project=require_project,
                        excluded_token_ids=tried_ids if tried_ids else None,
                        prefer_warm_browser=prefer_warm_browser,
                    )
            except HTTPException as ctx_err:
                if ctx_err.status_code == 401 and attempt + 1 < total_attempts:
                    resolved_tid = get_resolved_flow_token_id()
                    if resolved_tid is not None:
                        tried_ids.add(int(resolved_tid))
                    last_exc = ctx_err
                    continue
                if last_exc is not None and ctx_err.status_code == 503:
                    raise last_exc
                raise

            if tok.id is not None:
                tried_ids.add(int(tok.id))
            try:
                return await op_fn(tok, resolved_pid)
            except Exception as exc:
                last_exc = exc
                eligible = self._record_account_fault(tok.id, exc)
                if (
                    failover_on_404
                    and isinstance(exc, HTTPException)
                    and exc.status_code == 404
                ):
                    eligible = True
                if not eligible or attempt + 1 >= total_attempts:
                    raise
                debug_logger.log_warning(
                    f"[FlowFeatures] Token {tok.id} 请求失败 ({exc})，自动切换备用账号重试 ({attempt + 1}/{total_attempts})"
                )
        if last_exc is not None:
            raise last_exc
        raise HTTPException(status_code=503, detail="No active Flow account available")

    async def resolve_account_context(
        self,
        *,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
        project_id: Optional[str] = None,
        require_project: bool = True,
        excluded_token_ids: Optional[Set[int]] = None,
        prefer_warm_browser: bool = False,
    ) -> Tuple[Token, str]:
        """Resolve an active, session-validated Token and target project UUID."""
        token: Optional[Token] = None
        if token_id is not None:
            token = await self.db.get_token(int(token_id))
            if not token:
                raise HTTPException(
                    status_code=404, detail=f"Token not found: {token_id}"
                )
            if not token.is_active:
                raise HTTPException(
                    status_code=400, detail=f"Token {token_id} is inactive"
                )
        elif str(email or "").strip():
            clean_email = str(email).strip()
            by_email_fn = getattr(self.db, "get_token_by_email", None)
            if inspect.iscoroutinefunction(by_email_fn):
                candidate = await by_email_fn(clean_email)
                if isinstance(candidate, Token):
                    token = candidate
            if token is None and hasattr(self.db, "get_all_tokens"):
                target_email = clean_email.casefold()
                all_tokens = await self.db.get_all_tokens()
                for item in all_tokens or []:
                    if (item.email or "").strip().casefold() == target_email:
                        token = item
                        break
            if not token:
                raise HTTPException(
                    status_code=404, detail=f"Account not found: {email}"
                )
            if not token.is_active:
                raise HTTPException(
                    status_code=400, detail=f"Account {email} is inactive"
                )
        else:
            if self.load_balancer is not None:
                if excluded_token_ids or prefer_warm_browser:
                    try:
                        token = await self.load_balancer.select_token(
                            for_image_generation=False,
                            for_video_generation=False,
                            excluded_token_ids=excluded_token_ids,
                            prefer_warm_browser=prefer_warm_browser,
                        )
                    except TypeError:
                        token = await self.load_balancer.select_token(
                            for_image_generation=False,
                            for_video_generation=False,
                        )
                else:
                    token = await self.load_balancer.select_token(
                        for_image_generation=False,
                        for_video_generation=False,
                    )
                if (
                    token is not None
                    and excluded_token_ids
                    and token.id in excluded_token_ids
                ):
                    token = None
            if token is None and hasattr(self.db, "get_active_tokens"):
                active = await self.db.get_active_tokens()
                if excluded_token_ids:
                    active = [t for t in (active or []) if t.id not in excluded_token_ids]
                token = active[0] if active else None
            if token is None:
                raise HTTPException(
                    status_code=503, detail="No active Flow account available"
                )

        if token and token.id is not None:
            _resolved_flow_token_id.set(token.id)
        valid_token = await self.token_manager.ensure_valid_token(token)
        if not valid_token:
            raise HTTPException(
                status_code=401,
                detail=f"Flow session is invalid or expired for token {token.id}",
            )
        if valid_token and valid_token.id is not None:
            _resolved_flow_token_id.set(valid_token.id)

        resolved_project_id = str(project_id or "").strip()
        if require_project and not resolved_project_id:
            resolved_project_id = str(valid_token.current_project_id or "").strip()
            if not resolved_project_id:
                resolved_project_id = await self.token_manager.ensure_project_exists(
                    valid_token.id
                )

        return valid_token, resolved_project_id

    @staticmethod
    def _serialize_log_payload(payload: Any) -> str:
        if payload is None:
            return "{}"
        try:
            sanitized = debug_logger._truncate_large_fields(payload, max_length=256)
            return json.dumps(sanitized, ensure_ascii=False, default=str)
        except Exception:
            return json.dumps({"raw": str(payload)[:512]}, ensure_ascii=False)

    async def log_api_request(
        self,
        *,
        token_id: Optional[int],
        operation: str,
        request_data: Any,
        response_data: Any,
        status_code: int,
        duration: float,
        log_id: Optional[int] = None,
        status_text: Optional[str] = None,
        progress: Optional[int] = None,
    ) -> Optional[int]:
        """Persist a Flow feature API request/response record to request_logs when DB supports it."""
        if self.db is None:
            return None
        try:
            effective_status_text = status_text or (
                "completed"
                if status_code == 200
                else "failed"
                if status_code >= 400
                else "processing"
            )
            effective_progress = progress
            if effective_progress is None:
                effective_progress = 100 if status_code == 200 else 0
            effective_progress = max(0, min(100, int(effective_progress)))
            request_body = self._serialize_log_payload(request_data)
            response_body = self._serialize_log_payload(response_data)

            if log_id:
                update_fn = getattr(self.db, "update_request_log", None)
                if callable(update_fn):
                    res = update_fn(
                        log_id,
                        token_id=token_id,
                        operation=operation,
                        request_body=request_body,
                        response_body=response_body,
                        status_code=status_code,
                        duration=round(max(0.0, float(duration)), 3),
                        status_text=effective_status_text,
                        progress=effective_progress,
                    )
                    if inspect.isawaitable(res):
                        await res
                return log_id

            add_fn = getattr(self.db, "add_request_log", None)
            if not callable(add_fn):
                return None
            log = RequestLog(
                token_id=token_id,
                operation=operation,
                request_body=request_body,
                response_body=response_body,
                status_code=status_code,
                duration=round(max(0.0, float(duration)), 3),
                status_text=effective_status_text,
                progress=effective_progress,
            )
            res = add_fn(log)
            if inspect.isawaitable(res):
                created_id = await res
                return created_id if isinstance(created_id, int) else None
            return res if isinstance(res, int) else None
        except Exception as exc:
            debug_logger.log_warning(f"[FlowFeatures] Failed to log request ({operation}): {exc}")
            return None

    async def get_project(
        self,
        *,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        async def _op(token: Token, resolved_project_id: str) -> Dict[str, Any]:
            data = await self.flow_client.get_project(
                resolved_project_id,
                google_cookies=token.google_cookies,
                token_id=token.id,
            )
            for cv in data.get("customVoices") or []:
                if isinstance(cv, dict) and cv.get("mediaId"):
                    self.remember_asset_owner(cv.get("mediaId"), token.id, resolved_project_id)
            data["tokenId"] = token.id
            data["email"] = token.email
            return data

        return await self._run_with_account_failover(
            _op,
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
            prefer_warm_browser=False,
        )

    async def get_asset(
        self,
        media_id: str,
        *,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        owner = (
            await self._lookup_asset_owner(media_id)
            if (token_id is None and not str(email or "").strip())
            else None
        )

        async def _op(token: Token, resolved_project_id: str) -> Dict[str, Any]:
            detail = await self.flow_client.get_media(
                self.flow_client.FRONTEND_ACCESS_TOKEN,
                media_id,
                google_cookies=token.google_cookies,
                token_id=token.id,
                project_id=resolved_project_id or None,
            )
            if not detail.get("url"):
                raise HTTPException(
                    status_code=404, detail=f"Asset not found or expired: {media_id}"
                )
            final_pid = detail.get("projectId") or resolved_project_id
            self.remember_asset_owner(detail["name"], token.id, final_pid)
            return {
                "mediaId": detail["name"],
                "mediaType": detail.get("mediaType", "unknown"),
                "projectId": final_pid,
                "workflowId": detail.get("workflowId") or "",
                "url": detail["url"],
                "tokenId": token.id,
                "email": token.email,
                "frontendRpc": detail.get("frontendRpc", "as29s"),
            }

        return await self._run_with_account_failover(
            _op,
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=False,
            prefer_warm_browser=False,
            max_attempts=3,
            preferred_owner=owner,
            failover_on_404=True,
        )

    async def _download_binary(
        self, url: str, *, media_type: str = "audio"
    ) -> bytes:
        proxy_url = None
        if self.proxy_manager is not None:
            try:
                if hasattr(self.proxy_manager, "get_media_proxy_url"):
                    proxy_url = await self.proxy_manager.get_media_proxy_url()
                elif hasattr(self.proxy_manager, "get_request_proxy_url"):
                    proxy_url = await self.proxy_manager.get_request_proxy_url()
            except Exception as exc:
                debug_logger.log_warning(f"[FlowFeatures] Resolve download proxy failed: {exc}")

        get_pooled = (
            getattr(self.file_cache, "get_http_session", None)
            if self.file_cache is not None
            else None
        )
        if callable(get_pooled):
            pool_key, pooled_session = await get_pooled(
                impersonate="chrome",
                proxy_url=proxy_url,
                verify=True,
            )
            try:
                resp = await pooled_session.get(
                    url,
                    timeout=90,
                    proxy=proxy_url,
                    impersonate="chrome",
                    headers={"Referer": "https://flow.google.com/"},
                )
                if resp.status_code >= 400:
                    raise RuntimeError(
                        f"Failed to download Flow media ({resp.status_code})"
                    )
                return bytes(resp.content or b"")
            except Exception:
                evict_fn = getattr(self.file_cache, "evict_http_session", None)
                if callable(evict_fn):
                    await evict_fn(pool_key, pooled_session)
                raise

        session_kwargs: Dict[str, Any] = {
            "timeout": 90,
            "impersonate": "chrome",
        }
        if proxy_url:
            session_kwargs["proxy"] = proxy_url
        async with AsyncSession(**session_kwargs) as session:
            resp = await session.get(
                url,
                headers={"Referer": "https://flow.google.com/"},
            )
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"Failed to download Flow media ({resp.status_code})"
                )
            return bytes(resp.content or b"")
    @staticmethod
    def _decode_image_input(
        *,
        image_base64: Optional[str] = None,
    ) -> bytes:
        raw = str(image_base64 or "").strip()
        if not raw:
            raise ValueError("image_base64 or image_url is required")
        match = DATA_URL_RE.match(raw)
        if match:
            raw = match.group("data").strip()
        try:
            decoded = base64.b64decode(raw, validate=True)
        except Exception as exc:
            raise ValueError(f"Invalid base64 image payload: {exc}") from exc
        if not decoded:
            raise ValueError("Decoded image payload is empty")
        return decoded

    async def upload_asset(
        self,
        *,
        image_bytes: Optional[bytes] = None,
        image_base64: Optional[str] = None,
        image_url: Optional[str] = None,
        aspect_ratio: str = "IMAGE_ASPECT_RATIO_LANDSCAPE",
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        if image_bytes is None:
            if str(image_base64 or "").strip():
                image_bytes = self._decode_image_input(image_base64=image_base64)
            elif str(image_url or "").strip():
                from ..api.routes import _validate_safe_remote_url

                await _validate_safe_remote_url(image_url)
                image_bytes = await self._download_binary(
                    str(image_url).strip(), media_type="image"
                )
            else:
                raise ValueError("Provide file, image_base64, or image_url")

        async def _op(token: Token, resolved_project_id: str) -> Dict[str, Any]:
            media_id = await self.flow_client.upload_image(
                token.at or self.flow_client.FRONTEND_ACCESS_TOKEN,
                image_bytes,
                aspect_ratio=aspect_ratio,
                project_id=resolved_project_id,
                token_id=token.id,
                google_cookies=token.google_cookies,
            )
            self.remember_asset_owner(media_id, token.id, resolved_project_id)
            return {
                "mediaId": media_id,
                "projectId": resolved_project_id,
                "tokenId": token.id,
                "email": token.email,
                "frontendRpc": "maseQ",
            }

        return await self._run_with_account_failover(
            _op,
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
            prefer_warm_browser=True,
        )

    # ── Voices / TTS ──────────────────────────────────────────────────────────

    async def list_voices(
        self,
        *,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        async def _op(token: Token, resolved_project_id: str) -> Dict[str, Any]:
            result = await self.flow_client.list_voices(
                resolved_project_id,
                google_cookies=token.google_cookies,
                token_id=token.id,
            )
            for cv in result.get("customVoices") or []:
                if isinstance(cv, dict) and cv.get("mediaId"):
                    self.remember_asset_owner(cv.get("mediaId"), token.id, resolved_project_id)
            result["tokenId"] = token.id
            result["email"] = token.email
            return result

        return await self._run_with_account_failover(
            _op,
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
            prefer_warm_browser=False,
        )

    async def get_voice(
        self,
        voice_ref: str,
        *,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
        voice = await self.flow_client.get_voice(
            resolved_project_id,
            voice_ref,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        if not voice:
            raise HTTPException(
                status_code=404, detail=f"Voice not found: {voice_ref}"
            )
        return {
            **voice,
            "projectId": voice.get("projectId") or resolved_project_id,
            "tokenId": token.id,
            "email": token.email,
        }

    def _cache_audio_bytes(
        self,
        audio_bytes: bytes,
        ext: str,
        base_url: Optional[str] = None,
    ) -> Optional[str]:
        if self.file_cache is None or not audio_bytes:
            return None
        digest = hashlib.md5(f"{time.time()}-{len(audio_bytes)}".encode()).hexdigest()
        filename = f"{digest}.{ext}"
        file_path = self.file_cache.get_cache_path(filename)
        self.file_cache._write_cached_content(file_path, audio_bytes)
        if base_url:
            return f"{base_url.rstrip('/')}/tmp/{filename}"
        if getattr(config, "cache_base_url", None):
            return f"{str(config.cache_base_url).rstrip('/')}/tmp/{filename}"
        return f"/tmp/{filename}"

    async def create_voice(
        self,
        *,
        text: str,
        voice: str = "Achernar",
        style: str = "",
        name: Optional[str] = None,
        model_key: Optional[str] = None,
        save: bool = False,
        response_format: str = "json",
        cache_audio: bool = False,
        base_url: Optional[str] = None,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        fmt = str(response_format or "json").strip().lower()
        if fmt not in {"json", "wav", "pcm"}:
            raise ValueError(f"Unsupported response_format: {response_format}")

        async def _op(token: Token, resolved_project_id: str) -> Dict[str, Any]:
            result = await self.flow_client.generate_voice_audio(
                project_id=resolved_project_id,
                text=text,
                voice=voice,
                style=style,
                name=name,
                model_key=model_key or self.flow_client.DEFAULT_TTS_MODEL_KEY,
                save=save,
                google_cookies=token.google_cookies,
                token_id=token.id,
            )
            self.remember_asset_owner(
                result.get("mediaId"), token.id, resolved_project_id
            )
            result["tokenId"] = token.id
            result["email"] = token.email

            if fmt in {"wav", "pcm"} or cache_audio:
                audio_url = str(result.get("audioUrl") or "").strip()
                if not audio_url:
                    raise RuntimeError("Generated voice audio has no downloadable URL")
                pcm_bytes = await self._download_binary(audio_url, media_type="audio")
                payload_bytes = (
                    self.flow_client.pcm_to_wav(pcm_bytes) if fmt != "pcm" else pcm_bytes
                )
                if fmt in {"wav", "pcm"}:
                    return {
                        **result,
                        "audioBytes": payload_bytes,
                        "contentType": "audio/wav" if fmt == "wav" else "audio/pcm",
                    }
                if cache_audio:
                    wav_bytes = self.flow_client.pcm_to_wav(pcm_bytes)
                    cached_url = await asyncio.to_thread(
                        self._cache_audio_bytes, wav_bytes, "wav", base_url
                    )
                    if cached_url:
                        result["wavUrl"] = cached_url

            return result

        return await self._run_with_account_failover(
            _op,
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
            prefer_warm_browser=True,
        )

    async def delete_voice(
        self,
        voice_ref: str,
        *,
        workflow_id: Optional[str] = None,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
        ref = str(voice_ref or "").strip()
        target_wf = str(workflow_id or "").strip()
        target_media = None
        if not target_wf:
            voices = await self.flow_client.list_voices(
                resolved_project_id,
                google_cookies=token.google_cookies,
                token_id=token.id,
            )
            for custom in voices.get("customVoices") or []:
                if custom.get("workflowId") == ref:
                    target_wf = ref
                    target_media = custom.get("mediaId")
                    break
                if custom.get("mediaId") == ref or custom.get("id") == ref:
                    target_wf = str(custom.get("workflowId") or "").strip()
                    target_media = ref
                    break
            if not target_wf:
                target_media = ref
        res = await self.flow_client.delete_voice(
            project_id=resolved_project_id,
            workflow_id=target_wf or None,
            media_id=target_media,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        res["tokenId"] = token.id
        res["email"] = token.email
        return res

    # ── Characters ────────────────────────────────────────────────────────────

    async def list_characters(
        self,
        *,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        async def _op(token: Token, resolved_project_id: str) -> Dict[str, Any]:
            result = await self.flow_client.list_characters(
                resolved_project_id,
                google_cookies=token.google_cookies,
                token_id=token.id,
            )
            result["tokenId"] = token.id
            result["email"] = token.email
            return result

        return await self._run_with_account_failover(
            _op,
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
            prefer_warm_browser=False,
        )

    async def get_character(
        self,
        character_id: str,
        *,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=False,
        )
        items = await self.flow_client.get_characters(
            [character_id],
            project_id=resolved_project_id or None,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        if not items:
            raise HTTPException(
                status_code=404, detail=f"Character not found: {character_id}"
            )
        return {
            **items[0],
            "tokenId": token.id,
            "email": token.email,
        }

    async def create_character(
        self,
        *,
        display_name: str,
        voice_id: Optional[str] = None,
        personality_notes: Optional[str] = None,
        image_refs: Optional[List[str]] = None,
        face_prompt: Optional[str] = None,
        body_prompt: Optional[str] = None,
        aspect_ratio: str = "IMAGE_ASPECT_RATIO_PORTRAIT",
        model_name: str = "GEM_PIX_2",
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
        has_face_prompt = bool(str(face_prompt or "").strip())
        has_body_prompt = bool(str(body_prompt or "").strip())
        if not has_face_prompt and not has_body_prompt:
            created = await self.flow_client.create_character(
                project_id=resolved_project_id,
                display_name=display_name,
                voice_id=voice_id,
                personality_notes=personality_notes,
                image_refs=image_refs,
                google_cookies=token.google_cookies,
                token_id=token.id,
            )
            return {
                **created,
                "tokenId": token.id,
                "email": token.email,
            }

        # Two-step Saga: anchor entity first so generated media IDs stay traceable on partial failure.
        created = await self.flow_client.create_character(
            project_id=resolved_project_id,
            display_name=display_name,
            voice_id=voice_id,
            personality_notes=personality_notes,
            image_refs=image_refs,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        character_id = str(created["id"])
        generated_media_ids: List[str] = []
        face_media_id: Optional[str] = None
        body_media_id: Optional[str] = None
        try:
            if has_face_prompt:
                face_res, _, _ = await self.flow_client.generate_image(
                    at=token.at or getattr(self.flow_client, "FRONTEND_ACCESS_TOKEN", ""),
                    project_id=resolved_project_id,
                    prompt=str(face_prompt).strip(),
                    model_name=model_name,
                    aspect_ratio=aspect_ratio,
                    token_id=token.id,
                    google_cookies=token.google_cookies,
                )
                face_media = (face_res or {}).get("media") or []
                if not face_media or not face_media[0].get("name"):
                    raise RuntimeError("Face portrait generation returned no mediaId")
                face_media_id = str(face_media[0]["name"]).strip()
                generated_media_ids.append(face_media_id)
                self.remember_asset_owner(face_media_id, token.id, resolved_project_id)

            if has_body_prompt:
                anchor_refs = (
                    [{"imageUsageType": "IMAGE_USAGE_TYPE_ASSET", "mediaId": face_media_id}]
                    if face_media_id
                    else None
                )
                body_res, _, _ = await self.flow_client.generate_image(
                    at=token.at or getattr(self.flow_client, "FRONTEND_ACCESS_TOKEN", ""),
                    project_id=resolved_project_id,
                    prompt=str(body_prompt).strip(),
                    model_name=model_name,
                    aspect_ratio=aspect_ratio,
                    image_inputs=anchor_refs,
                    token_id=token.id,
                    google_cookies=token.google_cookies,
                )
                body_media = (body_res or {}).get("media") or []
                if not body_media or not body_media[0].get("name"):
                    raise RuntimeError("Full-body reference generation returned no mediaId")
                body_media_id = str(body_media[0]["name"]).strip()
                generated_media_ids.append(body_media_id)
                self.remember_asset_owner(body_media_id, token.id, resolved_project_id)

            combined_refs: List[str] = []
            for mid in [*(image_refs or []), *generated_media_ids]:
                clean_mid = str(mid or "").strip()
                if clean_mid and clean_mid not in combined_refs:
                    combined_refs.append(clean_mid)

            updated = await self.flow_client.update_character(
                project_id=resolved_project_id,
                entity_id=character_id,
                image_refs=combined_refs,
                google_cookies=token.google_cookies,
                token_id=token.id,
            )
            return {
                **updated,
                "generatedMediaIds": generated_media_ids,
                "faceMediaId": face_media_id,
                "bodyMediaId": body_media_id,
                "tokenId": token.id,
                "email": token.email,
            }
        except Exception as exc:
            raise RuntimeError(
                f"Character creation saga failed (characterId={character_id}, generatedMediaIds={generated_media_ids}): {exc}"
            ) from exc
    async def update_character(
        self,
        character_id: str,
        *,
        display_name: Optional[str] = None,
        voice_id: Optional[str] = None,
        personality_notes: Optional[str] = None,
        image_refs: Optional[List[str]] = None,
        favorited: Optional[bool] = None,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
        updated = await self.flow_client.update_character(
            project_id=resolved_project_id,
            entity_id=character_id,
            display_name=display_name,
            voice_id=voice_id,
            personality_notes=personality_notes,
            image_refs=image_refs,
            favorited=favorited,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        return {
            **updated,
            "tokenId": token.id,
            "email": token.email,
        }

    async def delete_character(
        self,
        character_id: str,
        *,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
        res = await self.flow_client.delete_character(
            project_id=resolved_project_id,
            entity_id=character_id,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        res["tokenId"] = token.id
        res["email"] = token.email
        return res

    # ── Phase 2: Creation Agent & Settings ────────────────────────────────────

    async def stream_agent_chat(
        self,
        *,
        prompt: str = "",
        session_id: Optional[str] = None,
        project_id: Optional[str] = None,
        image_refs: Optional[List[Any]] = None,
        character_refs: Optional[List[Any]] = None,
        workflow_refs: Optional[List[Any]] = None,
        permission_action: Optional[Any] = None,
        permission_id: Optional[str] = None,
        timeout: int = 180,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
        async for event in self.flow_client.stream_agent_chat(
            project_id=resolved_project_id,
            prompt=prompt,
            session_id=session_id,
            image_refs=image_refs,
            character_refs=character_refs,
            workflow_refs=workflow_refs,
            permission_action=permission_action,
            permission_id=permission_id,
            google_cookies=token.google_cookies,
            token_id=token.id,
            timeout=timeout,
        ):
            if event.get("event") == "done":
                if self.token_manager is not None:
                    await self.token_manager.record_success(token.id)
                yield {
                    **event,
                    "tokenId": token.id,
                    "email": token.email,
                }
            else:
                yield event

    async def agent_chat(
        self,
        *,
        prompt: str = "",
        session_id: Optional[str] = None,
        project_id: Optional[str] = None,
        image_refs: Optional[List[Any]] = None,
        character_refs: Optional[List[Any]] = None,
        workflow_refs: Optional[List[Any]] = None,
        permission_action: Optional[Any] = None,
        timeout: int = 180,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
        result = await self.flow_client.agent_chat(
            project_id=resolved_project_id,
            prompt=prompt,
            session_id=session_id,
            image_refs=image_refs,
            character_refs=character_refs,
            workflow_refs=workflow_refs,
            permission_action=permission_action,
            google_cookies=token.google_cookies,
            token_id=token.id,
            timeout=timeout,
        )
        if self.token_manager is not None:
            await self.token_manager.record_success(token.id)
        result["tokenId"] = token.id
        result["email"] = token.email
        return result

    async def approve_agent_action(
        self,
        session_id: str,
        *,
        action: Any = "approved",
        label: Optional[str] = None,
        permission_id: Optional[str] = None,
        project_id: Optional[str] = None,
        timeout: int = 180,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
        result = await self.flow_client.approve_agent_action(
            project_id=resolved_project_id,
            session_id=session_id,
            action=action,
            label=label,
            permission_id=permission_id,
            google_cookies=token.google_cookies,
            token_id=token.id,
            timeout=timeout,
        )
        if self.token_manager is not None:
            await self.token_manager.record_success(token.id)
        result["tokenId"] = token.id
        result["email"] = token.email
        return result

    async def stream_approve_agent_action(
        self,
        session_id: str,
        *,
        action: Any = "approved",
        label: Optional[str] = None,
        permission_id: Optional[str] = None,
        project_id: Optional[str] = None,
        timeout: int = 180,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        perm_code = self.flow_client._normalize_permission_action(action)
        effective_label = (
            str(label or "").strip()
            or self.flow_client.PERMISSION_ACTION_DEFAULT_LABELS[perm_code]
        )
        resolved_permission_id = str(permission_id or "").strip() or None
        resolved_project_id = project_id
        resolved_token_id = token_id
        runtime_cfg = self.flow_client._get_runtime_config()
        from .browser_captcha import BrowserCaptchaService
        if (
            not resolved_permission_id
            and BrowserCaptchaService._instance is not None
            and str(getattr(runtime_cfg, "captcha_method", "") or "").strip().lower()
            in {"browser", "camoufox"}
            and bool(getattr(runtime_cfg, "browser_in_page_rpc", True))
        ):
            tok, resolved_pid = await self.resolve_account_context(
                email=email,
                token_id=token_id,
                project_id=project_id,
                require_project=True,
            )
            resolved_project_id = resolved_pid
            resolved_token_id = tok.id
            if tok.id is not None:
                try:
                    sess = await self.flow_client.get_agent_session(
                        session_id,
                        project_id=resolved_pid,
                        google_cookies=tok.google_cookies,
                        token_id=tok.id,
                    )
                    for turn in reversed(sess.get("turns") or []):
                        perm = turn.get("permission")
                        if isinstance(perm, dict) and perm.get("permissionId"):
                            resolved_permission_id = str(perm["permissionId"]).strip()
                            break
                except Exception:
                    pass
        async for event in self.stream_agent_chat(
            prompt=effective_label,
            session_id=session_id,
            project_id=resolved_project_id,
            permission_action=perm_code,
            permission_id=resolved_permission_id,
            timeout=timeout,
            email=email,
            token_id=resolved_token_id,
        ):
            yield event

    async def list_agent_sessions(
        self,
        *,
        project_id: Optional[str] = None,
        page_size: Optional[int] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
        result = await self.flow_client.list_agent_sessions(
            resolved_project_id,
            page_size=page_size,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        result["tokenId"] = token.id
        result["email"] = token.email
        return result

    async def get_agent_session(
        self,
        session_id: str,
        *,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=False,
        )
        result = await self.flow_client.get_agent_session(
            session_id,
            project_id=resolved_project_id or None,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        result["tokenId"] = token.id
        result["email"] = token.email
        return result

    async def rename_agent_session(
        self,
        session_id: str,
        title: str,
        *,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=False,
        )
        result = await self.flow_client.rename_agent_session(
            session_id,
            title,
            project_id=resolved_project_id or None,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        result["tokenId"] = token.id
        result["email"] = token.email
        return result

    async def delete_agent_session(
        self,
        session_id: str,
        *,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=False,
        )
        result = await self.flow_client.delete_agent_session(
            session_id,
            project_id=resolved_project_id or None,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        result["tokenId"] = token.id
        result["email"] = token.email
        return result

    async def get_user_preferences(
        self,
        *,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, _ = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=None,
            require_project=False,
        )
        result = await self.flow_client.get_user_preferences(
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        result["tokenId"] = token.id
        result["email"] = token.email
        return result

    async def update_user_preferences(
        self,
        updates: Dict[str, Any],
        *,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, _ = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=None,
            require_project=False,
        )
        result = await self.flow_client.update_user_preferences(
            updates,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        result["tokenId"] = token.id
        result["email"] = token.email
        return result

    async def get_project_settings(
        self,
        *,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
        result = await self.flow_client.get_project_settings(
            resolved_project_id,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        result["tokenId"] = token.id
        result["email"] = token.email
        return result

    async def update_project_settings(
        self,
        updates: Dict[str, Any],
        *,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
        result = await self.flow_client.update_project_settings(
            resolved_project_id,
            updates,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        result["tokenId"] = token.id
        result["email"] = token.email
        return result

    async def list_models(
        self,
        *,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        from .generation_handler import MODEL_CONFIG

        token, _ = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=None,
            require_project=False,
        )
        tier = normalize_user_paygate_tier(getattr(token, "user_paygate_tier", None))
        image_models: List[Dict[str, Any]] = []
        video_models: List[Dict[str, Any]] = []
        for alias, cfg in MODEL_CONFIG.items():
            supported = supports_model_for_tier(alias, tier)
            item = {
                "id": alias,
                "type": cfg.get("type"),
                "aspectRatio": cfg.get("aspect_ratio"),
                "supported": supported,
            }
            if cfg.get("type") == "image":
                item["upstreamModel"] = cfg.get("model_name")
                if cfg.get("upsample"):
                    item["upsample"] = cfg.get("upsample")
                image_models.append(item)
            else:
                item["upstreamModel"] = cfg.get("model_key")
                item["videoType"] = cfg.get("video_type")
                if cfg.get("video_resolution"):
                    item["resolution"] = cfg.get("video_resolution")
                video_models.append(item)

        return {
            "tokenId": token.id,
            "email": token.email,
            "accountTier": tier,
            "credits": getattr(token, "credits", 0),
            "imageModels": image_models,
            "videoModels": video_models,
            "nativeVideoModels": [
                "macaron_cookie_r2v_8s_relaxed",
                "macaron_cookie_r2v_8s",
                "veo_3_1_r2v_fast_landscape_ultra",
                "veo_3_1_t2v_fast_landscape",
                "veo_3_1_i2v_s_fast_fl",
                "veo_3_1_extension_lite",
            ],
            "supportedResolutions": {
                "image": ["standard", "2K", "4K"],
                "video": ["720p", "1080p", "4k"],
            },
        }

    async def generate_reference_video(
        self,
        *,
        prompt: str,
        model_key: str = "macaron_cookie_r2v_8s_relaxed",
        aspect_ratio: str = "VIDEO_ASPECT_RATIO_LANDSCAPE",
        character_refs: Optional[List[Any]] = None,
        voice_refs: Optional[List[Any]] = None,
        image_refs: Optional[List[Any]] = None,
        resolution: Optional[str] = None,
        clean_prompt: bool = False,
        resolve_mentions: bool = True,
        wait: bool = True,
        poll_interval: float = 5.0,
        timeout: int = 360,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        effective_prompt = str(prompt or "")
        removed_terms: List[str] = []
        if clean_prompt:
            effective_prompt, removed_terms = strip_sd_banned_keywords(effective_prompt)

        media_ids: List[str] = []
        for iref in image_refs or []:
            if isinstance(iref, dict):
                mid = str(iref.get("mediaId") or iref.get("id") or iref.get("name") or "").strip()
            else:
                mid = str(iref or "").strip()
            if mid and mid not in media_ids:
                media_ids.append(mid)

        resolved_char_refs: List[Any] = list(character_refs or [])
        has_mentions = bool(resolve_mentions and parse_prompt_mentions(effective_prompt))

        async def _op(token: Token, resolved_project_id: str) -> Dict[str, Any]:
            op_media_ids = list(media_ids)
            op_char_refs = list(resolved_char_refs)
            resolved_mentions: List[Dict[str, str]] = []

            if has_mentions:
                project_snapshot = await self.flow_client.get_project(
                    resolved_project_id,
                    google_cookies=token.google_cookies,
                    token_id=token.id,
                )
                mention_res = resolve_prompt_mentions(
                    effective_prompt,
                    characters=project_snapshot.get("characters"),
                    workflows=project_snapshot.get("workflows"),
                )
                resolved_mentions = mention_res.resolved_mentions
                existing_cids = {
                    str(c.get("id") or c.get("characterId") or c.get("entityId") if isinstance(c, dict) else c).strip()
                    for c in op_char_refs
                }
                for cid in mention_res.character_ids:
                    if cid not in existing_cids:
                        op_char_refs.append(cid)
                        existing_cids.add(cid)
                for mid in mention_res.image_media_ids:
                    if mid not in op_media_ids:
                        op_media_ids.append(mid)

            has_refs = bool(op_media_ids or op_char_refs or voice_refs)
            is_start_i2v = "_i2v_" in str(model_key or "") and bool(op_media_ids)
            mode = "start" if is_start_i2v else ("references" if has_refs else "text")

            submission = await self.flow_client._current_generate_video(
                project_id=resolved_project_id,
                prompt=effective_prompt,
                model_key=model_key,
                aspect_ratio=aspect_ratio,
                mode=mode,
                reference_media_ids=None if is_start_i2v else (op_media_ids or None),
                start_media_id=op_media_ids[0] if is_start_i2v else None,
                reference_entity_ids=op_char_refs or None,
                reference_audio_ids=voice_refs or None,
                resolution=resolution,
                token_id=token.id,
                token_video_concurrency=None,
                google_cookies=token.google_cookies,
            )
            ops = submission.get("operations") or []
            extra_meta: Dict[str, Any] = {}
            if removed_terms:
                extra_meta["removedTerms"] = removed_terms
                extra_meta["cleanedPrompt"] = effective_prompt
            if resolved_mentions:
                extra_meta["resolvedMentions"] = resolved_mentions
            if not wait or not ops:
                submission["projectId"] = resolved_project_id
                submission["tokenId"] = token.id
                submission["email"] = token.email
                submission.update(extra_meta)
                return submission
            deadline = asyncio.get_event_loop().time() + max(timeout, 30)
            last_status = submission
            while asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(poll_interval)
                last_status = await self.flow_client.check_video_status(
                    at=token.at or "",
                    operations=ops,
                    token_id=token.id,
                    google_cookies=token.google_cookies,
                )
                checked_ops = last_status.get("operations") or []
                if checked_ops:
                    first_op = checked_ops[0]
                    status = str(first_op.get("status") or "")
                    meta_video = ((first_op.get("operation") or {}).get("metadata") or {}).get("video") or {}
                    fife_url = meta_video.get("fifeUrl")
                    if status == "MEDIA_GENERATION_STATUS_SUCCESSFUL" and fife_url:
                        self.remember_asset_owner(
                            (first_op.get("operation") or {}).get("name")
                            or meta_video.get("mediaGenerationId"),
                            token.id,
                            resolved_project_id,
                        )
                        return {
                            "status": status,
                            "videoUrl": fife_url,
                            "mediaId": (first_op.get("operation") or {}).get("name") or meta_video.get("mediaGenerationId"),
                            "observedModel": meta_video.get("observedModel") or meta_video.get("model"),
                            "projectId": resolved_project_id,
                            "tokenId": token.id,
                            "email": token.email,
                            "operations": checked_ops,
                            **extra_meta,
                        }
                    if "FAILED" in status:
                        raise RuntimeError(f"Flow video generation failed: {first_op}")
            return {
                "status": "TIMEOUT",
                "projectId": resolved_project_id,
                "tokenId": token.id,
                "email": token.email,
                "lastStatus": last_status,
                **extra_meta,
            }

        has_explicit_refs = bool(media_ids or resolved_char_refs or voice_refs or has_mentions)
        return await self._run_with_account_failover(
            _op,
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
            prefer_warm_browser=True,
            max_attempts=1 if (has_explicit_refs and bool(str(project_id or "").strip())) else 2,
        )

    async def chain_videos(
        self,
        *,
        segments: List[Dict[str, Any]],
        frame_offset_ms: int = 200,
        concat: bool = True,
        clean_prompt: bool = False,
        resolve_mentions: bool = True,
        poll_interval: float = 5.0,
        timeout: int = 360,
        base_url: Optional[str] = None,
        project_id: Optional[str] = None,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        if not segments:
            raise ValueError("segments must contain at least 1 video segment")

        ffmpeg_bin, ffprobe_bin = check_ffmpeg_available()
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
            prefer_warm_browser=True,
        )

        segment_results: List[Dict[str, Any]] = []
        clip_media_ids: List[str] = []
        next_frame_media_id: Optional[str] = None
        aspect_to_image_ratio = {
            "VIDEO_ASPECT_RATIO_PORTRAIT": "IMAGE_ASPECT_RATIO_PORTRAIT",
            "VIDEO_ASPECT_RATIO_LANDSCAPE": "IMAGE_ASPECT_RATIO_LANDSCAPE",
        }

        with tempfile.TemporaryDirectory(prefix="flow2api-chain-") as tmp_dir_str:
            tmp_dir = Path(tmp_dir_str)
            clip_paths: List[Path] = []

            for idx, seg in enumerate(segments):
                seg_prompt = str(seg.get("prompt") or "").strip()
                if not seg_prompt:
                    raise ValueError(f"Segment {idx} prompt is required")
                seg_aspect = str(seg.get("aspect_ratio") or "VIDEO_ASPECT_RATIO_LANDSCAPE")
                seg_image_refs = list(seg.get("image_refs") or [])
                if next_frame_media_id:
                    seg_image_refs = [next_frame_media_id, *seg_image_refs]
                default_model = (
                    "veo_3_1_i2v_s_fast_fl"
                    if seg_image_refs and not seg.get("character_refs")
                    else "macaron_cookie_r2v_8s_relaxed"
                )
                seg_model = str(seg.get("model_key") or default_model)

                res = await self.generate_reference_video(
                    prompt=seg_prompt,
                    model_key=seg_model,
                    aspect_ratio=seg_aspect,
                    character_refs=seg.get("character_refs"),
                    voice_refs=seg.get("voice_refs"),
                    image_refs=seg_image_refs or None,
                    resolution=seg.get("resolution"),
                    clean_prompt=clean_prompt,
                    resolve_mentions=resolve_mentions,
                    wait=True,
                    poll_interval=poll_interval,
                    timeout=timeout,
                    project_id=resolved_project_id,
                    token_id=token.id,
                )
                if res.get("status") != "MEDIA_GENERATION_STATUS_SUCCESSFUL" or not res.get("videoUrl"):
                    raise RuntimeError(
                        f"Video chain segment {idx} did not complete successfully (status={res.get('status')})"
                    )
                clip_media_id = str(res.get("mediaId") or "").strip()
                if clip_media_id:
                    clip_media_ids.append(clip_media_id)

                need_local_clip = concat or (idx < len(segments) - 1)
                if need_local_clip:
                    clip_bytes = await self._download_binary(
                        str(res["videoUrl"]), media_type="video"
                    )
                    clip_path = tmp_dir / f"seg_{idx}.mp4"
                    clip_path.write_bytes(clip_bytes)
                    clip_paths.append(clip_path)

                    if idx < len(segments) - 1:
                        frame_path = tmp_dir / f"tail_{idx}.jpg"
                        frame_bytes = await extract_tail_frame(
                            ffmpeg_bin,
                            ffprobe_bin,
                            clip_path,
                            frame_path,
                            offset_ms=frame_offset_ms,
                        )
                        uploaded_frame_id = await self.flow_client.upload_image(
                            token.at or getattr(self.flow_client, "FRONTEND_ACCESS_TOKEN", ""),
                            frame_bytes,
                            aspect_ratio=aspect_to_image_ratio.get(
                                seg_aspect, "IMAGE_ASPECT_RATIO_LANDSCAPE"
                            ),
                            project_id=resolved_project_id,
                            token_id=token.id,
                            google_cookies=token.google_cookies,
                        )
                        self.remember_asset_owner(
                            uploaded_frame_id, token.id, resolved_project_id
                        )
                        next_frame_media_id = uploaded_frame_id

                segment_results.append(
                    {
                        "index": idx,
                        "mediaId": clip_media_id,
                        "videoUrl": res.get("videoUrl"),
                        "modelKey": seg_model,
                        "bridgedFromMediaId": seg_image_refs[0] if idx > 0 and seg_image_refs else None,
                    }
                )

            final_video_url = segment_results[-1]["videoUrl"]
            if concat and clip_paths:
                merged_name = f"chain_{uuid.uuid4().hex[:16]}.mp4"
                if self.file_cache is not None:
                    merged_path = self.file_cache.get_cache_path(merged_name)
                    await concat_videos(ffmpeg_bin, clip_paths, merged_path)
                    if base_url:
                        final_video_url = f"{base_url.rstrip('/')}/tmp/{merged_name}"
                    elif getattr(config, "cache_base_url", None):
                        final_video_url = f"{str(config.cache_base_url).rstrip('/')}/tmp/{merged_name}"
                    else:
                        final_video_url = f"/tmp/{merged_name}"

        return {
            "status": "MEDIA_GENERATION_STATUS_SUCCESSFUL",
            "projectId": resolved_project_id,
            "tokenId": token.id,
            "email": token.email,
            "segmentCount": len(segment_results),
            "clipMediaIds": clip_media_ids,
            "segments": segment_results,
            "finalVideoUrl": final_video_url,
        }
