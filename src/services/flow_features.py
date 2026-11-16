"""Service layer for Flow Voices, Characters, Assets, and Agent APIs."""

import asyncio
import base64
import contextvars
import hashlib
import inspect
import json
import re
import time
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from curl_cffi.requests import AsyncSession
from fastapi import HTTPException

from ..core.config import config
from ..core.logger import debug_logger
from ..core.models import RequestLog, Token


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

    async def resolve_account_context(
        self,
        *,
        email: Optional[str] = None,
        token_id: Optional[int] = None,
        project_id: Optional[str] = None,
        require_project: bool = True,
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
            target_email = str(email).strip().casefold()
            all_tokens = await self.db.get_all_tokens()
            for item in all_tokens:
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
                token = await self.load_balancer.select_token(
                    for_image_generation=False,
                    for_video_generation=False,
                )
            if token is None and hasattr(self.db, "get_active_tokens"):
                active = await self.db.get_active_tokens()
                token = active[0] if active else None
            if token is None:
                raise HTTPException(
                    status_code=503, detail="No active Flow account available"
                )

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
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
        data = await self.flow_client.get_project(
            resolved_project_id,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        data["tokenId"] = token.id
        data["email"] = token.email
        return data

    async def get_asset(
        self,
        media_id: str,
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
        return {
            "mediaId": detail["name"],
            "mediaType": detail.get("mediaType", "unknown"),
            "projectId": detail.get("projectId") or resolved_project_id,
            "workflowId": detail.get("workflowId") or "",
            "url": detail["url"],
            "tokenId": token.id,
            "email": token.email,
            "frontendRpc": detail.get("frontendRpc", "as29s"),
        }

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

        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
        media_id = await self.flow_client.upload_image(
            token.at or self.flow_client.FRONTEND_ACCESS_TOKEN,
            image_bytes,
            aspect_ratio=aspect_ratio,
            project_id=resolved_project_id,
            token_id=token.id,
            google_cookies=token.google_cookies,
        )
        return {
            "mediaId": media_id,
            "projectId": resolved_project_id,
            "tokenId": token.id,
            "email": token.email,
            "frontendRpc": "maseQ",
        }

    # ── Voices / TTS ──────────────────────────────────────────────────────────

    async def list_voices(
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
        result = await self.flow_client.list_voices(
            resolved_project_id,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        result["tokenId"] = token.id
        result["email"] = token.email
        return result

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

        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
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
        token, resolved_project_id = await self.resolve_account_context(
            email=email,
            token_id=token_id,
            project_id=project_id,
            require_project=True,
        )
        result = await self.flow_client.list_characters(
            resolved_project_id,
            google_cookies=token.google_cookies,
            token_id=token.id,
        )
        result["tokenId"] = token.id
        result["email"] = token.email
        return result

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
        wait: bool = True,
        poll_interval: float = 5.0,
        timeout: int = 360,
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
        media_ids: List[str] = []
        for iref in image_refs or []:
            if isinstance(iref, dict):
                mid = str(iref.get("mediaId") or iref.get("id") or iref.get("name") or "").strip()
            else:
                mid = str(iref or "").strip()
            if mid:
                media_ids.append(mid)
        has_refs = bool(media_ids or character_refs or voice_refs)
        mode = "references" if has_refs else "text"
        submission = await self.flow_client._current_generate_video(
            project_id=resolved_project_id,
            prompt=prompt,
            model_key=model_key,
            aspect_ratio=aspect_ratio,
            mode=mode,
            reference_media_ids=media_ids or None,
            reference_entity_ids=character_refs or None,
            reference_audio_ids=voice_refs or None,
            resolution=resolution,
            token_id=token.id,
            token_video_concurrency=None,
            google_cookies=token.google_cookies,
        )
        ops = submission.get("operations") or []
        if not wait or not ops:
            submission["projectId"] = resolved_project_id
            submission["tokenId"] = token.id
            submission["email"] = token.email
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
                    return {
                        "status": status,
                        "videoUrl": fife_url,
                        "mediaId": (first_op.get("operation") or {}).get("name") or meta_video.get("mediaGenerationId"),
                        "projectId": resolved_project_id,
                        "tokenId": token.id,
                        "email": token.email,
                        "operations": checked_ops,
                    }
                if "FAILED" in status:
                    raise RuntimeError(f"Flow video generation failed: {first_op}")
        return {
            "status": "TIMEOUT",
            "projectId": resolved_project_id,
            "tokenId": token.id,
            "email": token.email,
            "lastStatus": last_status,
        }

