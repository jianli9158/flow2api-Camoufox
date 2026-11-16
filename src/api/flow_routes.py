"""REST API routes for Google Flow Voices, Characters, Assets, and Agent."""

import json
from typing import Any, AsyncIterator, Dict, List, Literal, Optional, Union
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..core.auth import verify_api_key_flexible
from ..services.flow_features import FlowFeatureService

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
    try:
        return await svc.get_project(
            project_id=project_id, email=email, token_id=token_id
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


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
    try:
        return await svc.get_asset(
            media_id, project_id=project_id, email=email, token_id=token_id
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


@router.post("/assets")
async def upload_flow_asset(
    request: UploadAssetRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    try:
        return await svc.upload_asset(
            image_base64=request.image_base64,
            image_url=request.image_url,
            aspect_ratio=request.aspect_ratio,
            project_id=request.project_id,
            email=request.email,
            token_id=request.token_id,
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


@router.get("/voices")
async def list_flow_voices(
    project_id: Optional[str] = Query(None),
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    try:
        return await svc.list_voices(
            project_id=project_id, email=email, token_id=token_id
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


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
    try:
        return await svc.get_voice(
            voice_ref, project_id=project_id, email=email, token_id=token_id
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


@router.post("/voices")
async def create_flow_voice(
    request: CreateVoiceRequest,
    raw_request: Request,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    base_url = f"{raw_request.url.scheme}://{raw_request.headers.get('host', raw_request.url.netloc)}"
    try:
        result = await svc.create_voice(
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
    except Exception as exc:
        raise _translate_service_error(exc) from exc


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
    try:
        return await svc.delete_voice(
            voice_ref,
            workflow_id=workflow_id,
            project_id=project_id,
            email=email,
            token_id=token_id,
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


@router.get("/characters")
async def list_flow_characters(
    project_id: Optional[str] = Query(None),
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    try:
        return await svc.list_characters(
            project_id=project_id, email=email, token_id=token_id
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


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
    try:
        return await svc.get_character(
            character_id, project_id=project_id, email=email, token_id=token_id
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


@router.post("/characters")
async def create_flow_character(
    request: CreateCharacterRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    try:
        return await svc.create_character(
            display_name=request.display_name,
            voice_id=request.voice_id,
            personality_notes=request.personality_notes,
            image_refs=request.image_refs,
            project_id=request.project_id,
            email=request.email,
            token_id=request.token_id,
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


@router.patch("/characters/{character_id}")
async def update_flow_character(
    character_id: str,
    request: UpdateCharacterRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    try:
        return await svc.update_character(
            character_id,
            display_name=request.display_name,
            voice_id=request.voice_id,
            personality_notes=request.personality_notes,
            image_refs=request.image_refs,
            favorited=request.favorited,
            project_id=request.project_id,
            email=request.email,
            token_id=request.token_id,
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


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
    try:
        return await svc.delete_character(
            character_id, project_id=project_id, email=email, token_id=token_id
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


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


async def _format_agent_sse(events: AsyncIterator[Dict[str, Any]]) -> AsyncIterator[str]:
    try:
        async for item in events:
            evt_name = str(item.get("event") or "message")
            payload = {k: v for k, v in item.items() if k != "event"}
            yield f"event: {evt_name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
    except Exception as exc:
        err = _translate_service_error(exc)
        yield f"event: error\ndata: {json.dumps({'status': err.status_code, 'detail': err.detail}, ensure_ascii=False)}\n\n"


@router.post("/agent/chat")
async def flow_agent_chat(
    request: AgentChatRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    try:
        if request.stream:
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
                _format_agent_sse(events),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        return await svc.agent_chat(
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
    except Exception as exc:
        raise _translate_service_error(exc) from exc


@router.post("/agent/sessions/{session_id}/approve")
async def approve_flow_agent_action(
    session_id: str,
    request: AgentApproveRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    try:
        if request.stream:
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
                _format_agent_sse(events),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )
        return await svc.approve_agent_action(
            session_id,
            action=request.action,
            label=request.label,
            permission_id=request.permission_id,
            project_id=request.project_id,
            timeout=request.timeout,
            email=request.email,
            token_id=request.token_id,
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


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
    try:
        return await svc.list_agent_sessions(
            project_id=project_id, page_size=page_size, email=email, token_id=token_id
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


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
    try:
        return await svc.get_agent_session(
            session_id, project_id=project_id, email=email, token_id=token_id
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


@router.patch("/agent/sessions/{session_id}")
async def rename_flow_agent_session(
    session_id: str,
    request: RenameAgentSessionRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    try:
        return await svc.rename_agent_session(
            session_id,
            request.title,
            project_id=request.project_id,
            email=request.email,
            token_id=request.token_id,
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


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
    try:
        return await svc.delete_agent_session(
            session_id, project_id=project_id, email=email, token_id=token_id
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


@router.get("/user/preferences")
async def get_flow_user_preferences(
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    try:
        return await svc.get_user_preferences(email=email, token_id=token_id)
    except Exception as exc:
        raise _translate_service_error(exc) from exc


@router.patch("/user/preferences")
async def update_flow_user_preferences(
    request: UpdateUserPreferencesRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    updates = request.model_dump(exclude={"email", "token_id"}, exclude_none=True)
    try:
        return await svc.update_user_preferences(
            updates, email=request.email, token_id=request.token_id
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


@router.get("/projects/{project_id}/settings")
async def get_flow_project_settings(
    project_id: str,
    email: Optional[str] = Query(None),
    token_id: Optional[int] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    try:
        return await svc.get_project_settings(
            project_id=project_id, email=email, token_id=token_id
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc


@router.patch("/projects/{project_id}/settings")
async def update_flow_project_settings(
    project_id: str,
    request: UpdateProjectSettingsRequest,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    svc = _ensure_service()
    updates = request.model_dump(exclude={"email", "token_id"}, exclude_none=True)
    try:
        return await svc.update_project_settings(
            updates, project_id=project_id, email=request.email, token_id=request.token_id
        )
    except Exception as exc:
        raise _translate_service_error(exc) from exc
