"""REST API routes for Google Flow Voices, Characters, Assets, and Agent."""

import json
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Literal, Optional, Union
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, ValidationError

from ..core.auth import verify_api_key_flexible
from ..services.flow_features import (
    FlowFeatureService,
    get_resolved_flow_token_id,
    reset_resolved_flow_token_id,
    set_resolved_flow_token_id,
)

router = APIRouter(prefix="/v1/flow", tags=["flow"])

flow_feature_service: Optional[FlowFeatureService] = None


def set_flow_feature_service(service: FlowFeatureService) -> None:
    global flow_feature_service
    flow_feature_service = service


def _ensure_service() -> FlowFeatureService:
    if flow_feature_service is None:
        raise HTTPException(
            status_code=500, detail="Flow feature service not initialized"
        )
    return flow_feature_service


def _translate_service_error(exc: Exception) -> HTTPException:
    if isinstance(exc, HTTPException):
        return exc
    if isinstance(exc, ValueError):
        return HTTPException(status_code=400, detail=str(exc))
    msg = str(exc or "Flow request failed")
    msg_lower = msg.lower()
    if "code=401" in msg_lower or "登录认证失败" in msg or "unauthenticated" in msg_lower:
        return HTTPException(status_code=401, detail=msg)
    if "not_found" in msg_lower or "code=[5]" in msg_lower or "code=5" in msg_lower:
        return HTTPException(status_code=404, detail=msg)
    if "public_error_" in msg_lower or "code=[3]" in msg_lower or "code=3" in msg_lower:
        return HTTPException(status_code=400, detail=msg)
    return HTTPException(status_code=502, detail=msg)


def _sanitize_flow_response_for_log(result: Any) -> Any:
    if isinstance(result, dict) and "audioBytes" in result:
        sanitized = {k: v for k, v in result.items() if k != "audioBytes"}
        audio_bytes = result.get("audioBytes")
        sanitized["audioBytesLength"] = (
            len(audio_bytes) if isinstance(audio_bytes, (bytes, bytearray)) else 0
        )
        return sanitized
    return result


async def _safe_log_flow_call(
    svc: FlowFeatureService,
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
    log_fn = getattr(svc, "log_api_request", None)
    if not callable(log_fn):
        return None
    return await log_fn(
        token_id=token_id,
        operation=operation,
        request_data=request_data,
        response_data=response_data,
        status_code=status_code,
        duration=duration,
        log_id=log_id,
        status_text=status_text,
        progress=progress,
    )


async def _execute_logged_flow_call(
    svc: FlowFeatureService,
    *,
    operation: str,
    request_data: Dict[str, Any],
    fn: Callable[[], Awaitable[Any]],
    explicit_token_id: Optional[int] = None,
) -> Any:
    started = time.perf_counter()
    ctx_tok = set_resolved_flow_token_id(None)
    try:
        result = await fn()
        duration = time.perf_counter() - started
        resolved_token_id = (
            result.get("tokenId")
            if isinstance(result, dict) and result.get("tokenId") is not None
            else get_resolved_flow_token_id() or explicit_token_id
        )
        await _safe_log_flow_call(
            svc,
            token_id=resolved_token_id,
            operation=operation,
            request_data=request_data,
            response_data=_sanitize_flow_response_for_log(result),
            status_code=200,
            duration=duration,
        )
        return result
    except Exception as exc:
        duration = time.perf_counter() - started
        http_exc = _translate_service_error(exc)
        resolved_token_id = get_resolved_flow_token_id() or explicit_token_id
        await _safe_log_flow_call(
            svc,
            token_id=resolved_token_id,
            operation=operation,
            request_data=request_data,
            response_data={"detail": http_exc.detail},
            status_code=http_exc.status_code,
            duration=duration,
        )
        raise http_exc from exc
    finally:
        reset_resolved_flow_token_id(ctx_tok)
class UploadAssetRequest(BaseModel):
    image_base64: Optional[str] = None
    image_url: Optional[str] = None
    aspect_ratio: str = "IMAGE_ASPECT_RATIO_LANDSCAPE"
    project_id: Optional[str] = None
    email: Optional[str] = None
    token_id: Optional[int] = None


class CreateVoiceRequest(BaseModel):
    text: str = Field(..., min_length=1)
    voice: str = "Achernar"
    style: str = ""
    name: Optional[str] = None
    model_key: Optional[str] = None
    save: bool = False
    response_format: Literal["json", "wav", "pcm"] = "json"
    cache_audio: bool = False
    project_id: Optional[str] = None
    email: Optional[str] = None
    token_id: Optional[int] = None


class CreateCharacterRequest(BaseModel):
    display_name: str = Field(..., min_length=1)
    voice_id: Optional[str] = None
    personality_notes: Optional[str] = None
    image_refs: Optional[List[str]] = None
    project_id: Optional[str] = None
    email: Optional[str] = None
    token_id: Optional[int] = None


class UpdateCharacterRequest(BaseModel):
    display_name: Optional[str] = None
    voice_id: Optional[str] = None
    personality_notes: Optional[str] = None
    image_refs: Optional[List[str]] = None
    favorited: Optional[bool] = None
    project_id: Optional[str] = None
    email: Optional[str] = None
    token_id: Optional[int] = None


@router.get("/projects/{project_id}")
async def get_flow_project(
    project_id: str,
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_get_project",
        request_data={"project_id": project_id, "email": email, "token_id": token_id},
        explicit_token_id=token_id,
        fn=lambda: svc.get_project(
            project_id=project_id, email=email, token_id=token_id
        ),
    )


@router.get("/assets/{media_id}")
async def get_flow_asset(
    media_id: str,
    project_id: Optional[str] = Query(None),
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_get_asset",
        request_data={
            "media_id": media_id,
            "project_id": project_id,
            "email": email,
            "token_id": token_id,
        },
        explicit_token_id=token_id,
        fn=lambda: svc.get_asset(
            media_id, project_id=project_id, email=email, token_id=token_id
        ),
    )


_UPLOAD_ASSET_OPENAPI_EXTRA = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "image_base64": {"type": "string", "nullable": True},
                        "image_url": {"type": "string", "nullable": True},
                        "aspect_ratio": {
                            "type": "string",
                            "default": "IMAGE_ASPECT_RATIO_LANDSCAPE",
                        },
                        "project_id": {"type": "string", "nullable": True},
                        "email": {"type": "string", "nullable": True},
                        "token_id": {"type": "integer", "nullable": True},
                    },
                }
            },
            "multipart/form-data": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "file": {"type": "string", "format": "binary"},
                        "image_base64": {"type": "string", "nullable": True},
                        "image_url": {"type": "string", "nullable": True},
                        "aspect_ratio": {
                            "type": "string",
                            "default": "IMAGE_ASPECT_RATIO_LANDSCAPE",
                        },
                        "project_id": {"type": "string", "nullable": True},
                        "email": {"type": "string", "nullable": True},
                        "token_id": {"type": "integer", "nullable": True},
                    },
                }
            },
        },
    }
}


@router.post("/assets", openapi_extra=_UPLOAD_ASSET_OPENAPI_EXTRA)
async def upload_flow_asset(
    raw_request: Request,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    content_type = (raw_request.headers.get("content-type") or "").lower()
    image_bytes: Optional[bytes] = None
    image_base64: Optional[str] = None
    image_url: Optional[str] = None
    aspect_ratio = "IMAGE_ASPECT_RATIO_LANDSCAPE"
    project_id: Optional[str] = None
    email: Optional[str] = None
    token_id: Optional[int] = None
    log_request_data: Dict[str, Any]

    if "multipart/form-data" in content_type:
        form = await raw_request.form()
        upload_file = form.get("file")
        filename = None
        try:
            if upload_file is not None and callable(getattr(upload_file, "read", None)):
                filename = getattr(upload_file, "filename", None) or "upload"
                image_bytes = await upload_file.read()
                if not image_bytes:
                    raise HTTPException(status_code=400, detail="Uploaded file is empty")
        finally:
            close_fn = getattr(upload_file, "close", None)
            if callable(close_fn):
                await close_fn()
        raw_b64 = form.get("image_base64")
        raw_url = form.get("image_url")
        raw_ar = form.get("aspect_ratio")
        raw_pid = form.get("project_id")
        raw_email = form.get("email")
        raw_tid = form.get("token_id")
        image_base64 = str(raw_b64).strip() if isinstance(raw_b64, str) and raw_b64.strip() else None
        image_url = str(raw_url).strip() if isinstance(raw_url, str) and raw_url.strip() else None
        if isinstance(raw_ar, str) and raw_ar.strip():
            aspect_ratio = raw_ar.strip()
        project_id = str(raw_pid).strip() if isinstance(raw_pid, str) and raw_pid.strip() else None
        email = str(raw_email).strip() if isinstance(raw_email, str) and raw_email.strip() else None
        if isinstance(raw_tid, str) and raw_tid.strip():
            try:
                token_id = int(raw_tid.strip())
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="Invalid token_id") from exc
        log_request_data = {
            "upload_mode": "multipart",
            "filename": filename,
            "file_bytes": len(image_bytes) if image_bytes is not None else 0,
            "has_image_base64": bool(image_base64),
            "image_url": image_url,
            "aspect_ratio": aspect_ratio,
            "project_id": project_id,
            "email": email,
            "token_id": token_id,
        }
    else:
        try:
            raw_payload = await raw_request.json()
        except Exception as exc:
            raise HTTPException(status_code=422, detail="Invalid JSON request body") from exc
        try:
            parsed = UploadAssetRequest.model_validate(raw_payload)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=exc.errors()) from exc
        image_base64 = parsed.image_base64
        image_url = parsed.image_url
        aspect_ratio = parsed.aspect_ratio
        project_id = parsed.project_id
        email = parsed.email
        token_id = parsed.token_id
        log_request_data = parsed.model_dump(exclude_none=True)

    return await _execute_logged_flow_call(
        svc,
        operation="flow_upload_asset",
        request_data=log_request_data,
        explicit_token_id=token_id,
        fn=lambda: svc.upload_asset(
            image_bytes=image_bytes,
            image_base64=image_base64,
            image_url=image_url,
            aspect_ratio=aspect_ratio,
            project_id=project_id,
            email=email,
            token_id=token_id,
        ),
    )


@router.get("/voices")
async def list_flow_voices(
    project_id: Optional[str] = Query(None),
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_list_voices",
        request_data={"project_id": project_id, "email": email, "token_id": token_id},
        explicit_token_id=token_id,
        fn=lambda: svc.list_voices(
            project_id=project_id, email=email, token_id=token_id
        ),
    )


@router.get("/voices/{voice_ref}")
async def get_flow_voice(
    voice_ref: str,
    project_id: Optional[str] = Query(None),
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_get_voice",
        request_data={
            "voice_ref": voice_ref,
            "project_id": project_id,
            "email": email,
            "token_id": token_id,
        },
        explicit_token_id=token_id,
        fn=lambda: svc.get_voice(
            voice_ref, project_id=project_id, email=email, token_id=token_id
        ),
    )


@router.post("/voices")
async def create_flow_voice(
    request: CreateVoiceRequest,
    raw_request: Request,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    base_url = f"{raw_request.url.scheme}://{raw_request.headers.get('host', raw_request.url.netloc)}"
    result = await _execute_logged_flow_call(
        svc,
        operation="flow_create_voice",
        request_data=request.model_dump(exclude_none=True),
        explicit_token_id=request.token_id,
        fn=lambda: svc.create_voice(
            text=request.text,
            voice=request.voice,
            style=request.style,
            name=request.name,
            model_key=request.model_key,
            save=request.save,
            response_format=request.response_format,
            cache_audio=request.cache_audio,
            base_url=base_url,
            project_id=request.project_id,
            email=request.email,
            token_id=request.token_id,
        ),
    )
    if request.response_format in {"wav", "pcm"} and "audioBytes" in result:
        headers = {
            "X-Flow-Media-Id": str(result.get("mediaId") or ""),
            "X-Flow-Workflow-Id": str(result.get("workflowId") or ""),
            "X-Flow-Project-Id": str(result.get("projectId") or ""),
        }
        return Response(
            content=result["audioBytes"],
            media_type=result.get("contentType", "audio/wav"),
            headers=headers,
        )
    return result


@router.delete("/voices/{voice_ref}")
async def delete_flow_voice(
    voice_ref: str,
    workflow_id: Optional[str] = Query(None),
    project_id: Optional[str] = Query(None),
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_delete_voice",
        request_data={
            "voice_ref": voice_ref,
            "workflow_id": workflow_id,
            "project_id": project_id,
            "email": email,
            "token_id": token_id,
        },
        explicit_token_id=token_id,
        fn=lambda: svc.delete_voice(
            voice_ref,
            workflow_id=workflow_id,
            project_id=project_id,
            email=email,
            token_id=token_id,
        ),
    )


@router.get("/characters")
async def list_flow_characters(
    project_id: Optional[str] = Query(None),
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_list_characters",
        request_data={"project_id": project_id, "email": email, "token_id": token_id},
        explicit_token_id=token_id,
        fn=lambda: svc.list_characters(
            project_id=project_id, email=email, token_id=token_id
        ),
    )


@router.get("/characters/{character_id}")
async def get_flow_character(
    character_id: str,
    project_id: Optional[str] = Query(None),
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_get_character",
        request_data={
            "character_id": character_id,
            "project_id": project_id,
            "email": email,
            "token_id": token_id,
        },
        explicit_token_id=token_id,
        fn=lambda: svc.get_character(
            character_id, project_id=project_id, email=email, token_id=token_id
        ),
    )


@router.post("/characters")
async def create_flow_character(
    request: CreateCharacterRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_create_character",
        request_data=request.model_dump(exclude_none=True),
        explicit_token_id=request.token_id,
        fn=lambda: svc.create_character(
            display_name=request.display_name,
            voice_id=request.voice_id,
            personality_notes=request.personality_notes,
            image_refs=request.image_refs,
            project_id=request.project_id,
            email=request.email,
            token_id=request.token_id,
        ),
    )


@router.patch("/characters/{character_id}")
async def update_flow_character(
    character_id: str,
    request: UpdateCharacterRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_update_character",
        request_data={
            "character_id": character_id,
            **request.model_dump(exclude_none=True),
        },
        explicit_token_id=request.token_id,
        fn=lambda: svc.update_character(
            character_id,
            display_name=request.display_name,
            voice_id=request.voice_id,
            personality_notes=request.personality_notes,
            image_refs=request.image_refs,
            favorited=request.favorited,
            project_id=request.project_id,
            email=request.email,
            token_id=request.token_id,
        ),
    )


@router.delete("/characters/{character_id}")
async def delete_flow_character(
    character_id: str,
    project_id: Optional[str] = Query(None),
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_delete_character",
        request_data={
            "character_id": character_id,
            "project_id": project_id,
            "email": email,
            "token_id": token_id,
        },
        explicit_token_id=token_id,
        fn=lambda: svc.delete_character(
            character_id, project_id=project_id, email=email, token_id=token_id
        ),
    )


# ── Phase 2: Creation Agent & Settings ────────────────────────────────────────


class AgentChatRequest(BaseModel):
    prompt: str = ""
    session_id: Optional[str] = None
    project_id: Optional[str] = None
    image_refs: Optional[List[Any]] = None
    character_refs: Optional[List[Any]] = None
    workflow_refs: Optional[List[Any]] = None
    stream: bool = False
    timeout: int = Field(default=180, ge=10, le=600)
    email: Optional[str] = None
    token_id: Optional[int] = None


class AgentApproveRequest(BaseModel):
    action: Union[str, int] = "approved"
    label: Optional[str] = None
    permission_id: Optional[str] = None
    project_id: Optional[str] = None
    stream: bool = False
    timeout: int = Field(default=180, ge=10, le=600)
    email: Optional[str] = None
    token_id: Optional[int] = None

class RenameAgentSessionRequest(BaseModel):
    title: str = Field(..., min_length=1)
    project_id: Optional[str] = None
    email: Optional[str] = None
    token_id: Optional[int] = None


class UpdateUserPreferencesRequest(BaseModel):
    should_clear_prompt_box_on_submit: Optional[bool] = None
    has_sound_on_tile_hover: Optional[bool] = None
    is_tile_details_always_visible: Optional[bool] = None
    should_return_silent_videos: Optional[bool] = None
    is_chat_panel_open: Optional[bool] = None
    is_watermark_enabled_by_user: Optional[bool] = None
    last_acknowledged_change_log_id: Optional[str] = None
    email: Optional[str] = None
    token_id: Optional[int] = None


class UpdateProjectSettingsRequest(BaseModel):
    chat_panel_open: Optional[bool] = None
    credit_spend_approval_policy: Optional[Union[int, str]] = None
    pinned_applets: Optional[List[str]] = None
    image_defaults: Optional[Dict[str, Any]] = None
    video_defaults: Optional[Dict[str, Any]] = None
    email: Optional[str] = None
    token_id: Optional[int] = None


async def _format_agent_sse(
    events: AsyncIterator[Dict[str, Any]],
    *,
    svc: Optional[FlowFeatureService] = None,
    operation: str = "flow_agent_chat",
    request_data: Optional[Dict[str, Any]] = None,
    explicit_token_id: Optional[int] = None,
    log_id: Optional[int] = None,
) -> AsyncIterator[str]:
    started = time.perf_counter()
    ctx_tok = set_resolved_flow_token_id(None)
    done_payload: Optional[Dict[str, Any]] = None
    stream_error: Optional[HTTPException] = None
    resolved_token_id = explicit_token_id
    try:
        async for item in events:
            evt_name = str(item.get("event") or "message")
            payload = {k: v for k, v in item.items() if k != "event"}
            if item.get("tokenId") is not None:
                resolved_token_id = item.get("tokenId")
            if evt_name == "done":
                done_payload = payload
            elif evt_name == "error" and stream_error is None:
                err_detail = payload.get("detail") or payload.get("message") or payload
                err_status = int(payload.get("status") or 502)
                stream_error = HTTPException(status_code=err_status, detail=str(err_detail))
            yield f"event: {evt_name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
    except Exception as exc:
        err = _translate_service_error(exc)
        stream_error = err
        yield f"event: error\ndata: {json.dumps({'status': err.status_code, 'detail': err.detail}, ensure_ascii=False)}\n\n"
    finally:
        duration = time.perf_counter() - started
        resolved_token_id = get_resolved_flow_token_id() or resolved_token_id
        reset_resolved_flow_token_id(ctx_tok)
        if svc is not None:
            try:
                if stream_error is not None:
                    await _safe_log_flow_call(
                        svc,
                        token_id=resolved_token_id,
                        operation=operation,
                        request_data=request_data or {},
                        response_data={"detail": stream_error.detail},
                        status_code=stream_error.status_code,
                        duration=duration,
                        log_id=log_id,
                        status_text="failed",
                        progress=0,
                    )
                elif done_payload is not None:
                    await _safe_log_flow_call(
                        svc,
                        token_id=resolved_token_id,
                        operation=operation,
                        request_data=request_data or {},
                        response_data=done_payload,
                        status_code=200,
                        duration=duration,
                        log_id=log_id,
                        status_text="completed",
                        progress=100,
                    )
                else:
                    await _safe_log_flow_call(
                        svc,
                        token_id=resolved_token_id,
                        operation=operation,
                        request_data=request_data or {},
                        response_data={"detail": "Stream interrupted before completion"},
                        status_code=499,
                        duration=duration,
                        log_id=log_id,
                        status_text="failed",
                        progress=0,
                    )
            except Exception:
                pass


@router.post("/agent/chat")
async def flow_agent_chat(
    request: AgentChatRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    req_data = request.model_dump(exclude_none=True)
    if request.stream:
        log_id = await _safe_log_flow_call(
            svc,
            token_id=request.token_id,
            operation="flow_agent_chat",
            request_data=req_data,
            response_data={"streaming": True},
            status_code=102,
            duration=0.0,
            status_text="processing",
            progress=10,
        )
        events = svc.stream_agent_chat(
            prompt=request.prompt,
            session_id=request.session_id,
            project_id=request.project_id,
            image_refs=request.image_refs,
            character_refs=request.character_refs,
            workflow_refs=request.workflow_refs,
            timeout=request.timeout,
            email=request.email,
            token_id=request.token_id,
        )
        return StreamingResponse(
            _format_agent_sse(
                events,
                svc=svc,
                operation="flow_agent_chat",
                request_data=req_data,
                explicit_token_id=request.token_id,
                log_id=log_id,
            ),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )
    return await _execute_logged_flow_call(
        svc,
        operation="flow_agent_chat",
        request_data=req_data,
        explicit_token_id=request.token_id,
        fn=lambda: svc.agent_chat(
            prompt=request.prompt,
            session_id=request.session_id,
            project_id=request.project_id,
            image_refs=request.image_refs,
            character_refs=request.character_refs,
            workflow_refs=request.workflow_refs,
            timeout=request.timeout,
            email=request.email,
            token_id=request.token_id,
        ),
    )


@router.post("/agent/sessions/{session_id}/approve")
async def approve_flow_agent_action(
    session_id: str,
    request: AgentApproveRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    req_data = {"session_id": session_id, **request.model_dump(exclude_none=True)}
    if request.stream:
        log_id = await _safe_log_flow_call(
            svc,
            token_id=request.token_id,
            operation="flow_agent_approve",
            request_data=req_data,
            response_data={"streaming": True},
            status_code=102,
            duration=0.0,
            status_text="processing",
            progress=10,
        )
        events = svc.stream_approve_agent_action(
            session_id,
            action=request.action,
            label=request.label,
            permission_id=request.permission_id,
            project_id=request.project_id,
            timeout=request.timeout,
            email=request.email,
            token_id=request.token_id,
        )
        return StreamingResponse(
            _format_agent_sse(
                events,
                svc=svc,
                operation="flow_agent_approve",
                request_data=req_data,
                explicit_token_id=request.token_id,
                log_id=log_id,
            ),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )
    return await _execute_logged_flow_call(
        svc,
        operation="flow_agent_approve",
        request_data=req_data,
        explicit_token_id=request.token_id,
        fn=lambda: svc.approve_agent_action(
            session_id,
            action=request.action,
            label=request.label,
            permission_id=request.permission_id,
            project_id=request.project_id,
            timeout=request.timeout,
            email=request.email,
            token_id=request.token_id,
        ),
    )


@router.get("/agent/sessions")
async def list_flow_agent_sessions(
    project_id: Optional[str] = Query(None),
    page_size: Optional[int] = Query(None, ge=1, le=200),
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_list_agent_sessions",
        request_data={
            "project_id": project_id,
            "page_size": page_size,
            "email": email,
            "token_id": token_id,
        },
        explicit_token_id=token_id,
        fn=lambda: svc.list_agent_sessions(
            project_id=project_id, page_size=page_size, email=email, token_id=token_id
        ),
    )


@router.get("/agent/sessions/{session_id}")
async def get_flow_agent_session(
    session_id: str,
    project_id: Optional[str] = Query(None),
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_get_agent_session",
        request_data={
            "session_id": session_id,
            "project_id": project_id,
            "email": email,
            "token_id": token_id,
        },
        explicit_token_id=token_id,
        fn=lambda: svc.get_agent_session(
            session_id, project_id=project_id, email=email, token_id=token_id
        ),
    )


@router.patch("/agent/sessions/{session_id}")
async def rename_flow_agent_session(
    session_id: str,
    request: RenameAgentSessionRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_rename_agent_session",
        request_data={
            "session_id": session_id,
            **request.model_dump(exclude_none=True),
        },
        explicit_token_id=request.token_id,
        fn=lambda: svc.rename_agent_session(
            session_id,
            request.title,
            project_id=request.project_id,
            email=request.email,
            token_id=request.token_id,
        ),
    )


@router.delete("/agent/sessions/{session_id}")
async def delete_flow_agent_session(
    session_id: str,
    project_id: Optional[str] = Query(None),
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_delete_agent_session",
        request_data={
            "session_id": session_id,
            "project_id": project_id,
            "email": email,
            "token_id": token_id,
        },
        explicit_token_id=token_id,
        fn=lambda: svc.delete_agent_session(
            session_id, project_id=project_id, email=email, token_id=token_id
        ),
    )


@router.get("/user/preferences")
async def get_flow_user_preferences(
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_get_user_preferences",
        request_data={"email": email, "token_id": token_id},
        explicit_token_id=token_id,
        fn=lambda: svc.get_user_preferences(email=email, token_id=token_id),
    )


@router.patch("/user/preferences")
async def update_flow_user_preferences(
    request: UpdateUserPreferencesRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    updates = request.model_dump(exclude={"email", "token_id"}, exclude_none=True)
    return await _execute_logged_flow_call(
        svc,
        operation="flow_update_user_preferences",
        request_data=request.model_dump(exclude_none=True),
        explicit_token_id=request.token_id,
        fn=lambda: svc.update_user_preferences(
            updates, email=request.email, token_id=request.token_id
        ),
    )


@router.get("/projects/{project_id}/settings")
async def get_flow_project_settings(
    project_id: str,
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_get_project_settings",
        request_data={"project_id": project_id, "email": email, "token_id": token_id},
        explicit_token_id=token_id,
        fn=lambda: svc.get_project_settings(
            project_id=project_id, email=email, token_id=token_id
        ),
    )


@router.patch("/projects/{project_id}/settings")
async def update_flow_project_settings(
    project_id: str,
    request: UpdateProjectSettingsRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    updates = request.model_dump(exclude={"email", "token_id"}, exclude_none=True)
    return await _execute_logged_flow_call(
        svc,
        operation="flow_update_project_settings",
        request_data={"project_id": project_id, **request.model_dump(exclude_none=True)},
        explicit_token_id=request.token_id,
        fn=lambda: svc.update_project_settings(
            updates, project_id=project_id, email=request.email, token_id=request.token_id
        ),
    )


class GenerateReferenceVideoRequest(BaseModel):
    prompt: str = Field(..., min_length=1)
    model_key: str = Field(default="macaron_cookie_r2v_8s_relaxed")
    aspect_ratio: str = Field(default="VIDEO_ASPECT_RATIO_LANDSCAPE")
    character_refs: Optional[List[Any]] = None
    voice_refs: Optional[List[Any]] = None
    image_refs: Optional[List[Any]] = None
    resolution: Optional[str] = None
    wait: bool = Field(default=True)
    poll_interval: float = Field(default=5.0, ge=1.0, le=30.0)
    timeout: int = Field(default=360, ge=30, le=900)
    project_id: Optional[str] = None
    email: Optional[str] = None
    token_id: Optional[int] = None


@router.post("/videos")
async def generate_flow_reference_video(
    request: GenerateReferenceVideoRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    return await _execute_logged_flow_call(
        svc,
        operation="flow_generate_video",
        request_data=request.model_dump(exclude_none=True),
        explicit_token_id=request.token_id,
        fn=lambda: svc.generate_reference_video(
            prompt=request.prompt,
            model_key=request.model_key,
            aspect_ratio=request.aspect_ratio,
            character_refs=request.character_refs,
            voice_refs=request.voice_refs,
            image_refs=request.image_refs,
            resolution=request.resolution,
            wait=request.wait,
            poll_interval=request.poll_interval,
            timeout=request.timeout,
            project_id=request.project_id,
            email=request.email,
            token_id=request.token_id,
        ),
    )
