"""Built-in MCP Streamable HTTP endpoint (/mcp) with token-bucket spend protection."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from ..core.auth import (
    AuthManager,
    assert_api_key_model_allowed,
    verify_api_key_flexible,
)
from ..core.model_resolver import get_base_model_aliases
from ..core.config import config
from ..core.flow_errors import classify_flow_error
from ..core.redaction import redact_sensitive_text
from ..services.video_chain import check_ffmpeg_available

router = APIRouter(tags=["mcp"])

_flow_feature_service: Any = None
_generation_handler: Any = None


def set_mcp_dependencies(flow_feature_service: Any, generation_handler: Any = None) -> None:
    global _flow_feature_service, _generation_handler
    _flow_feature_service = flow_feature_service
    _generation_handler = generation_handler


class TokenBucketSpendLimiter:
    """Thread/async-safe token bucket preventing runaway agent spend loops."""

    def __init__(self, *, capacity: float = 8.0, refill_per_second: float = 1.0 / 20.0):
        self.capacity = max(1.0, float(capacity))
        self.refill_per_second = max(0.001, float(refill_per_second))
        self._buckets: Dict[str, tuple[float, float]] = {}
        self._lock = asyncio.Lock()

    async def consume(self, key: str, cost: float = 1.0) -> tuple[bool, float]:
        bucket_key = str(key or "default")
        now = time.monotonic()
        async with self._lock:
            tokens, last_ts = self._buckets.get(bucket_key, (self.capacity, now))
            elapsed = max(0.0, now - last_ts)
            tokens = min(self.capacity, tokens + elapsed * self.refill_per_second)
            if tokens >= cost:
                tokens -= cost
                self._buckets[bucket_key] = (tokens, now)
                return True, 0.0
            self._buckets[bucket_key] = (tokens, now)
            deficit = cost - tokens
            retry_after = max(1.0, deficit / self.refill_per_second)
            return False, retry_after

    def reset(self) -> None:
        self._buckets.clear()


spend_limiter = TokenBucketSpendLimiter(capacity=8.0, refill_per_second=1.0 / 20.0)

_SPEND_PROTECTED_TOOLS = frozenset(
    {
        "flow_generate_image",
        "flow_generate_video",
        "flow_create_character",
    }
)
_PAID_CREDIT_TOOLS = frozenset({"flow_generate_video"})


def build_flow_capabilities(*, no_spend: bool = False) -> Dict[str, Any]:
    """Zero-upstream capability declaration for REST (`GET /v1/flow/capabilities`) and MCP (`capabilities://self`)."""
    ffmpeg_ok, _ffmpeg_msg = check_ffmpeg_available()
    return {
        "schema": 1,
        "adapter": "flow2api-camoufox-bidi",
        "noSpend": bool(no_spend),
        "capabilities": {
            "generate.image": "yes",
            "generate.video": "no" if no_spend else "yes",
            "video.chain": "no" if (no_spend or not ffmpeg_ok) else "yes",
            "submit.no_wait": "yes",
            "status": "yes",
            "download": "yes",
            "upload": "yes",
            "abort.local": "yes",
            "cancel.server": "no",
            "idempotent_submit": "no",
            "credits.balance": "yes",
            "characters.saga": "yes",
            "mentions": "yes",
        },
        "notes": {
            "generate.video": (
                "Disabled because mcp.no_spend / X-Flow-No-Spend is enabled."
                if no_spend
                else "Enabled; consumes Google Flow Veo credits."
            ),
            "video.chain": (
                "Requires local ffmpeg/ffprobe and spend mode enabled."
                if (no_spend or not ffmpeg_ok)
                else "Available via local ffmpeg/ffprobe tail-frame extraction and concat."
            ),
            "cancel.server": "Google Flow does not cancel accepted generations server-side.",
            "idempotent_submit": "Paid video submits are non-idempotent; post-submit timeouts return charged=maybe and next_action=reconcile.",
        },
    }


def _is_no_spend_active(request: Optional[Request] = None) -> bool:
    if config.mcp_no_spend:
        return True
    if request is not None:
        hdr = str(request.headers.get("x-flow-no-spend") or "").strip().lower()
        if hdr in {"1", "true", "yes", "on"}:
            return True
    return False


def _visible_mcp_tools(*, no_spend: bool) -> List[Dict[str, Any]]:
    if not no_spend:
        return list(MCP_TOOL_DEFINITIONS)
    return [t for t in MCP_TOOL_DEFINITIONS if t["name"] not in _PAID_CREDIT_TOOLS]
MCP_TOOL_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "name": "flow_list_models",
        "description": "List available Flow image/video models, tier support, and account credits.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "email": {"type": "string"},
                "token_id": {"type": "integer"},
            },
        },
    },
    {
        "name": "flow_get_project",
        "description": "Fetch Flow project snapshot including characters, workflows, and scenes.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "string"},
                "email": {"type": "string"},
                "token_id": {"type": "integer"},
            },
            "required": ["project_id"],
        },
    },
    {
        "name": "flow_list_voices",
        "description": "List preset and custom voices in a Flow project.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "string"},
                "email": {"type": "string"},
                "token_id": {"type": "integer"},
            },
        },
    },
    {
        "name": "flow_list_characters",
        "description": "List saved characters in a Flow project.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_id": {"type": "string"},
                "email": {"type": "string"},
                "token_id": {"type": "integer"},
            },
        },
    },
    {
        "name": "flow_create_character",
        "description": "Create a Flow character, optionally running the face_prompt -> body_prompt Saga.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "display_name": {"type": "string"},
                "voice_id": {"type": "string"},
                "personality_notes": {"type": "string"},
                "image_refs": {"type": "array", "items": {"type": "string"}},
                "face_prompt": {"type": "string"},
                "body_prompt": {"type": "string"},
                "aspect_ratio": {"type": "string"},
                "project_id": {"type": "string"},
                "email": {"type": "string"},
                "token_id": {"type": "integer"},
            },
            "required": ["display_name"],
        },
    },
    {
        "name": "flow_generate_image",
        "description": "Generate an image with Flow (e.g. gemini-3.0-pro-image-landscape or gemini-3.1-flash-image-landscape).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "prompt": {"type": "string"},
                "model": {"type": "string", "default": "gemini-3.0-pro-image-landscape"},
            },
            "required": ["prompt"],
        },
    },
    {
        "name": "flow_generate_video",
        "description": "Generate a native Flow reference/I2V/T2V video (supports @CharacterName mentions).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "prompt": {"type": "string"},
                "model_key": {"type": "string", "default": "macaron_cookie_r2v_8s_relaxed"},
                "aspect_ratio": {"type": "string", "default": "VIDEO_ASPECT_RATIO_LANDSCAPE"},
                "character_refs": {"type": "array", "items": {"type": "string"}},
                "voice_refs": {"type": "array", "items": {"type": "string"}},
                "image_refs": {"type": "array", "items": {"type": "string"}},
                "clean_prompt": {"type": "boolean", "default": False},
                "resolve_mentions": {"type": "boolean", "default": True},
                "wait": {"type": "boolean", "default": True},
                "project_id": {"type": "string"},
                "email": {"type": "string"},
                "token_id": {"type": "integer"},
            },
            "required": ["prompt"],
        },
    },
    {
        "name": "flow_get_asset",
        "description": "Resolve a fresh signed URL for a Flow media_id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "media_id": {"type": "string"},
                "project_id": {"type": "string"},
                "email": {"type": "string"},
                "token_id": {"type": "integer"},
            },
            "required": ["media_id"],
        },
    },
    {
        "name": "flow_tts",
        "description": "Synthesize speech audio with Flow Voices.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string"},
                "voice": {"type": "string", "default": "Achernar"},
                "style": {"type": "string", "default": ""},
                "project_id": {"type": "string"},
                "email": {"type": "string"},
                "token_id": {"type": "integer"},
            },
            "required": ["text"],
        },
    },
]


def _jsonrpc_result(req_id: Any, result: Any) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _jsonrpc_error(
    req_id: Any, code: int, message: str, data: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    err: Dict[str, Any] = {"code": code, "message": redact_sensitive_text(message)}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": err}


async def _dispatch_mcp_tool(
    tool_name: str,
    args: Dict[str, Any],
    *,
    caller_key: str,
    no_spend: bool = False,
) -> Dict[str, Any]:
    if _flow_feature_service is None:
        raise RuntimeError("Flow feature service is not initialized")

    if no_spend and tool_name in _PAID_CREDIT_TOOLS:
        raise HTTPException(
            status_code=403,
            detail=f"MCP no-spend mode blocks paid credit tool '{tool_name}'",
        )

    if tool_name == "flow_generate_image":
        target_img_model = str(args.get("model") or "gemini-3.0-pro-image-landscape")
        assert_api_key_model_allowed(
            caller_key,
            target_img_model,
            raw_model=target_img_model,
            base_aliases=get_base_model_aliases(),
        )
    elif tool_name == "flow_generate_video":
        from .flow_routes import _assert_flow_video_model_allowed

        target_vid_model = str(args.get("model_key") or "macaron_cookie_r2v_8s_relaxed")
        _assert_flow_video_model_allowed(caller_key, target_vid_model)

    needs_spend_token = tool_name in _SPEND_PROTECTED_TOOLS
    if tool_name == "flow_create_character" and not (
        args.get("face_prompt") or args.get("body_prompt")
    ):
        needs_spend_token = False

    if needs_spend_token:
        allowed, retry_after = await spend_limiter.consume(caller_key, cost=1.0)
        if not allowed:
            raise HTTPException(
                status_code=429,
                detail=f"MCP spend-protection bucket exhausted; retry in {int(retry_after)}s",
            )
    svc = _flow_feature_service
    if tool_name == "flow_list_models":
        from .flow_routes import _filter_flow_models_payload

        raw_models = await svc.list_models(
            email=args.get("email"),
            token_id=args.get("token_id"),
        )
        return _filter_flow_models_payload(raw_models, caller_key)
    if tool_name == "flow_get_project":
        return await svc.get_project(
            project_id=str(args["project_id"]),
            email=args.get("email"),
            token_id=args.get("token_id"),
        )
    if tool_name == "flow_list_voices":
        return await svc.list_voices(
            project_id=args.get("project_id"),
            email=args.get("email"),
            token_id=args.get("token_id"),
        )
    if tool_name == "flow_list_characters":
        return await svc.list_characters(
            project_id=args.get("project_id"),
            email=args.get("email"),
            token_id=args.get("token_id"),
        )
    if tool_name == "flow_create_character":
        return await svc.create_character(
            display_name=str(args["display_name"]),
            voice_id=args.get("voice_id"),
            personality_notes=args.get("personality_notes"),
            image_refs=args.get("image_refs"),
            face_prompt=args.get("face_prompt"),
            body_prompt=args.get("body_prompt"),
            aspect_ratio=str(args.get("aspect_ratio") or "IMAGE_ASPECT_RATIO_PORTRAIT"),
            project_id=args.get("project_id"),
            email=args.get("email"),
            token_id=args.get("token_id"),
        )
    if tool_name == "flow_generate_video":
        return await svc.generate_reference_video(
            prompt=str(args["prompt"]),
            model_key=str(args.get("model_key") or "macaron_cookie_r2v_8s_relaxed"),
            aspect_ratio=str(args.get("aspect_ratio") or "VIDEO_ASPECT_RATIO_LANDSCAPE"),
            character_refs=args.get("character_refs"),
            voice_refs=args.get("voice_refs"),
            image_refs=args.get("image_refs"),
            clean_prompt=bool(args.get("clean_prompt", False)),
            resolve_mentions=bool(args.get("resolve_mentions", True)),
            wait=bool(args.get("wait", True)),
            project_id=args.get("project_id"),
            email=args.get("email"),
            token_id=args.get("token_id"),
        )
    if tool_name == "flow_get_asset":
        return await svc.get_asset(
            str(args["media_id"]),
            project_id=args.get("project_id"),
            email=args.get("email"),
            token_id=args.get("token_id"),
        )
    if tool_name == "flow_tts":
        return await svc.create_voice(
            text=str(args["text"]),
            voice=str(args.get("voice") or "Achernar"),
            style=str(args.get("style") or ""),
            project_id=args.get("project_id"),
            email=args.get("email"),
            token_id=args.get("token_id"),
        )
    if tool_name == "flow_generate_image":
        if _generation_handler is None:
            raise RuntimeError("GenerationHandler is not initialized")
        chunks: List[str] = []
        async for raw_chunk in _generation_handler.handle_generation(
            model=str(args.get("model") or "gemini-3.0-pro-image-landscape"),
            prompt=str(args["prompt"]),
            stream=False,
        ):
            chunks.append(raw_chunk)
        if not chunks:
            raise RuntimeError("Image generation returned no result")
        parsed = json.loads(chunks[-1])
        if isinstance(parsed, dict) and parsed.get("error"):
            err_obj = parsed["error"]
            raise HTTPException(
                status_code=int(err_obj.get("status_code") or 500),
                detail=str(err_obj.get("message") or "Image generation failed"),
            )
        return parsed

    raise ValueError(f"Unknown MCP tool: {tool_name}")


@router.get("/mcp")
async def mcp_server_info(
    request: Request,
    api_key: str = Depends(verify_api_key_flexible),
):
    _ = api_key
    no_spend = _is_no_spend_active(request)
    return {
        "name": "flow2api-mcp",
        "version": "1.0.0",
        "protocolVersion": "2025-03-26",
        "transport": "streamable-http",
        "noSpend": no_spend,
        "tools": [t["name"] for t in _visible_mcp_tools(no_spend=no_spend)],
        "resources": ["capabilities://self"],
    }


@router.post("/mcp")
async def mcp_rpc_endpoint(
    request: Request,
    api_key: str = Depends(verify_api_key_flexible),
):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(
            status_code=400,
            content=_jsonrpc_error(None, -32700, "Parse error: invalid JSON"),
        )

    if not isinstance(body, dict) or body.get("jsonrpc") != "2.0" or "method" not in body:
        return JSONResponse(
            status_code=400,
            content=_jsonrpc_error(
                body.get("id") if isinstance(body, dict) else None,
                -32600,
                "Invalid JSON-RPC 2.0 request",
            ),
        )

    req_id = body.get("id")
    method = str(body.get("method") or "").strip()
    params = body.get("params") if isinstance(body.get("params"), dict) else {}
    no_spend = _is_no_spend_active(request)

    if method == "notifications/initialized":
        return JSONResponse(status_code=202, content={"ok": True})
    if method == "ping":
        return JSONResponse(content=_jsonrpc_result(req_id, {}))
    if method == "initialize":
        return JSONResponse(
            content=_jsonrpc_result(
                req_id,
                {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {
                        "tools": {"listChanged": False},
                        "resources": {"listChanged": False},
                    },
                    "serverInfo": {"name": "flow2api-mcp", "version": "1.0.0"},
                },
            )
        )
    if method == "resources/list":
        return JSONResponse(
            content=_jsonrpc_result(
                req_id,
                {
                    "resources": [
                        {
                            "uri": "capabilities://self",
                            "name": "Flow2API Capabilities",
                            "description": "Zero-upstream capability declaration including noSpend and video.chain availability.",
                            "mimeType": "application/json",
                        }
                    ]
                },
            )
        )
    if method == "resources/read":
        uri = str(params.get("uri") or "").strip()
        if uri != "capabilities://self":
            return JSONResponse(
                status_code=404,
                content=_jsonrpc_error(req_id, -32002, f"Resource not found: {uri}"),
            )
        caps = build_flow_capabilities(no_spend=no_spend)
        return JSONResponse(
            content=_jsonrpc_result(
                req_id,
                {
                    "contents": [
                        {
                            "uri": "capabilities://self",
                            "mimeType": "application/json",
                            "text": json.dumps(caps, ensure_ascii=False),
                        }
                    ]
                },
            )
        )
    if method == "tools/list":
        return JSONResponse(
            content=_jsonrpc_result(
                req_id, {"tools": _visible_mcp_tools(no_spend=no_spend)}
            )
        )
    if method == "tools/call":
        tool_name = str(params.get("name") or "").strip()
        tool_args = params.get("arguments")
        if not isinstance(tool_args, dict):
            tool_args = {}
        if not tool_name:
            return JSONResponse(
                status_code=400,
                content=_jsonrpc_error(req_id, -32602, "Missing tool name"),
            )
        is_billed_tool = tool_name in _PAID_CREDIT_TOOLS
        try:
            tool_result = await _dispatch_mcp_tool(
                tool_name, tool_args, caller_key=api_key, no_spend=no_spend
            )
            return JSONResponse(
                content=_jsonrpc_result(
                    req_id,
                    {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(tool_result, ensure_ascii=False),
                            }
                        ],
                        "isError": False,
                    },
                )
            )
        except HTTPException as http_exc:
            detail_text = redact_sensitive_text(str(http_exc.detail or ""))
            if http_exc.status_code == 403 and "no-spend" in detail_text.lower():
                payload = {
                    "error": detail_text,
                    "status_code": 403,
                    "category": "no_spend_blocked",
                    "retryable": False,
                    "charged": "no",
                    "next_action": "fix_input",
                    "remediation_hint": "MCP no-spend 模式已禁用付费视频工具，如需使用请关闭 FLOW2API_MCP_NO_SPEND 或移除 X-Flow-No-Spend 请求头。",
                }
                return JSONResponse(
                    status_code=403,
                    content=_jsonrpc_error(req_id, -32030, detail_text, data=payload),
                )
            if http_exc.status_code == 403 and "未公开此模型" in detail_text:
                payload = {
                    "error": detail_text,
                    "status_code": 403,
                    "category": "model_not_allowed",
                    "retryable": False,
                    "charged": "no",
                    "next_action": "fix_input",
                    "remediation_hint": "当前 API Key 未开放该模型，请在管理后台 API Key 设置中勾选对应模型。",
                }
                return JSONResponse(
                    status_code=403,
                    content=_jsonrpc_error(req_id, -32031, detail_text, data=payload),
                )
            classified = classify_flow_error(
                detail_text, billed_operation=is_billed_tool
            )
            payload = {
                "error": detail_text,
                "status_code": http_exc.status_code,
                "category": classified.category.value,
                "retryable": classified.retryable_same_token,
                "charged": classified.charged,
                "next_action": classified.next_action,
                "remediation_hint": classified.remediation_hint,
            }
            if http_exc.status_code == 429:
                return JSONResponse(
                    status_code=429,
                    content=_jsonrpc_error(req_id, -32029, detail_text, data=payload),
                )
            return JSONResponse(
                content=_jsonrpc_result(
                    req_id,
                    {
                        "content": [{"type": "text", "text": json.dumps(payload, ensure_ascii=False)}],
                        "isError": True,
                    },
                )
            )
        except Exception as exc:
            post_submit = bool(
                getattr(exc, "submission_uncertain", False)
                or getattr(exc, "outcome_unknown", False)
            )
            classified = classify_flow_error(
                exc, post_submit=post_submit, billed_operation=is_billed_tool
            )
            payload = {
                "error": redact_sensitive_text(str(exc)),
                "status_code": classified.http_status,
                "category": classified.category.value,
                "retryable": classified.retryable_same_token,
                "charged": classified.charged,
                "next_action": classified.next_action,
                "remediation_hint": classified.remediation_hint,
            }
            return JSONResponse(
                content=_jsonrpc_result(
                    req_id,
                    {
                        "content": [{"type": "text", "text": json.dumps(payload, ensure_ascii=False)}],
                        "isError": True,
                    },
                )
            )

    return JSONResponse(
        status_code=404,
        content=_jsonrpc_error(req_id, -32601, f"Method not found: {method}"),
    )
