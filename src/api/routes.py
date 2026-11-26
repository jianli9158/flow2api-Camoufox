"""API routes for OpenAI-compatible and Gemini generateContent endpoints."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import asyncio
import base64
import binascii
import inspect
import ipaddress
import json
import mimetypes
import re
import socket
import time
from types import SimpleNamespace
from urllib.parse import unquote, urljoin, urlparse

from curl_cffi.requests import AsyncSession
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import JSONResponse, StreamingResponse

from ..core.auth import (
    AuthManager,
    assert_api_key_model_allowed,
    verify_api_key_flexible,
)
from ..core.config import config
from ..core.logger import debug_logger
from ..core.model_resolver import (
    get_base_model_aliases,
    get_canonical_model_name,
    resolve_model_name,
)
from ..core.models import (
    ChatCompletionRequest,
    ChatMessage,
    GeminiContent,
    GeminiGenerateContentRequest,
    GeminiPredictRequest,
    OpenAIImageGenerationRequest,
)
from ..core.prompt_tools import validate_base64_media_payload
from ..services.generation_handler import MODEL_CONFIG, GenerationHandler
router = APIRouter()

MARKDOWN_IMAGE_RE = re.compile(r"!\[.*?\]\((.*?)\)")
HTML_VIDEO_RE = re.compile(r"<video[^>]+src=['\"](.*?)['\"]", re.IGNORECASE)
DATA_URL_RE = re.compile(r"^data:(?P<mime>[^;]+);base64,(?P<data>.+)$", re.DOTALL)
MAX_INBOUND_IMAGE_BYTES = 50 * 1024 * 1024
MEDIA_PROMPT_TOOL_BLOCK_RE = re.compile(
    r"<tools>.*?</tools>", re.IGNORECASE | re.DOTALL
)
MEDIA_SYSTEM_INSTRUCTION_MARKERS = (
    "<tools>",
    "</tools>",
    "function calling ai model",
    "function signatures",
    '"$schema"',
    '"additionalproperties"',
)
MEDIA_PROMPT_PREAMBLE_PATTERNS = (
    re.compile(r"^you are a function calling ai model\.?$", re.IGNORECASE),
    re.compile(
        r"^you are provided with function signatures within .* xml tags\.?$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^you may call one or more functions to assist with the user query\.?$",
        re.IGNORECASE,
    ),
    re.compile(
        r"^don't make assumptions about what values to plug into functions\.?$",
        re.IGNORECASE,
    ),
    re.compile(r"^here are the available tools:.*$", re.IGNORECASE),
)
GEMINI_STATUS_MAP = {
    400: "INVALID_ARGUMENT",
    401: "UNAUTHENTICATED",
    403: "PERMISSION_DENIED",
    404: "NOT_FOUND",
    409: "ABORTED",
    429: "RESOURCE_EXHAUSTED",
    500: "INTERNAL",
    502: "UNAVAILABLE",
    503: "UNAVAILABLE",
    504: "DEADLINE_EXCEEDED",
    499: "CANCELLED",
}

# Dependency injection will be set up in main.py
generation_handler: GenerationHandler = None


@dataclass
class NormalizedGenerationRequest:
    """Internal request shape shared by OpenAI and Gemini entrypoints."""

    model: str
    prompt: str
    images: List[bytes]
    messages: Optional[List[ChatMessage]] = None
    video_media_id: Optional[str] = None


def set_generation_handler(handler: GenerationHandler):
    """Set generation handler instance."""
    global generation_handler
    generation_handler = handler


def _ensure_generation_handler() -> GenerationHandler:
    if generation_handler is None:
        raise HTTPException(
            status_code=500, detail="Generation handler not initialized"
        )
    return generation_handler


def _build_model_description(model_config: Dict[str, Any]) -> str:
    """Build a human-readable description for model listing endpoints."""
    display_name = str(model_config.get("display_name") or "").strip()
    if display_name:
        return display_name
    description = f"{model_config['type'].capitalize()} generation"
    if model_config["type"] == "image":
        description += f" - {model_config['model_name']}"
    else:
        description += f" - {model_config['model_key']}"
    return description


def _build_model_parameters_metadata(model_config: Dict[str, Any]) -> Dict[str, Any]:
    """Build standard AI Studio / OpenAI parameter metadata for a model entry."""
    mtype = str(model_config.get("type") or "image")
    if mtype == "image":
        return {
            "type": "image",
            "default_parameters": {
                "aspectRatio": model_config.get("default_aspect_ratio", "16:9"),
                "imageSize": model_config.get("default_image_size", "2K"),
            },
            "supported_parameters": {
                "aspectRatio": list(
                    model_config.get(
                        "supported_aspect_ratios",
                        ["16:9", "9:16", "1:1", "4:3", "3:4"],
                    )
                ),
                "imageSize": list(
                    model_config.get("supported_image_sizes", ["1K", "2K", "4K"])
                ),
            },
        }
    return {
        "type": "video",
        "default_parameters": {
            "aspectRatio": model_config.get("default_aspect_ratio", "16:9"),
            "durationSeconds": model_config.get("default_duration", 8),
            "resolution": model_config.get("default_resolution", "720p"),
        },
        "supported_parameters": {
            "aspectRatio": list(
                model_config.get("supported_aspect_ratios", ["16:9", "9:16"])
            ),
            "durationSeconds": list(model_config.get("supported_durations", [8])),
            "resolution": list(
                model_config.get("supported_resolutions", ["720p", "1080p", "4K"])
            ),
        },
    }


def _get_openai_model_catalog() -> List[Dict[str, Any]]:
    """Collect OpenAI-compatible canonical model list entries."""
    return [
        {
            "id": model_id,
            "description": _build_model_description(model_config),
            **_build_model_parameters_metadata(model_config),
        }
        for model_id, model_config in MODEL_CONFIG.items()
        if model_config.get("listed", True)
    ]


def _get_gemini_model_catalog() -> Dict[str, str]:
    """Collect Gemini-compatible canonical model metadata for /models endpoints."""
    catalog: Dict[str, str] = {}

    for model_id, model_config in MODEL_CONFIG.items():
        if not model_config.get("listed", True):
            continue
        catalog.setdefault(model_id, _build_model_description(model_config))

    return catalog


def _build_gemini_model_resource(model_id: str, description: str) -> Dict[str, Any]:
    """Build a Gemini-compatible model resource payload."""
    cfg = MODEL_CONFIG.get(model_id) or {}
    mtype = str(cfg.get("type") or "image")
    methods = ["generateContent", "streamGenerateContent"]
    if mtype == "image":
        methods.append("predict")
    resource: Dict[str, Any] = {
        "name": f"models/{model_id}",
        "displayName": model_id,
        "description": description,
        "version": "flow2api",
        "inputTokenLimit": 0,
        "outputTokenLimit": 0,
        "supportedGenerationMethods": methods,
    }
    if cfg:
        meta = _build_model_parameters_metadata(cfg)
        resource["defaultParameters"] = meta["default_parameters"]
        resource["supportedParameters"] = meta["supported_parameters"]
    return resource
def _decode_data_url(data_url: str) -> tuple[str, bytes]:
    match = DATA_URL_RE.match(data_url)
    if not match:
        raise HTTPException(status_code=400, detail="Invalid data URL")
    raw_b64 = match.group("data")
    try:
        return match.group("mime"), base64.b64decode(raw_b64, validate=True)
    except (ValueError, binascii.Error):
        compact = "".join(raw_b64.split())
        try:
            return match.group("mime"), base64.b64decode(compact, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise HTTPException(status_code=400, detail="Invalid data URL") from exc


def _detect_image_mime_type(image_bytes: bytes, fallback: str = "image/png") -> str:
    from ..core.prompt_tools import detect_image_mime_type

    return detect_image_mime_type(image_bytes, fallback=fallback)


def _guess_mime_type(uri: str, fallback: str) -> str:
    guessed, _ = mimetypes.guess_type(urlparse(uri).path)
    return guessed or fallback


_BLOCKED_IP_NETWORKS = (
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("240.0.0.0/4"),
    ipaddress.ip_network("::/128"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("ff00::/8"),
)


def _is_disallowed_ip(ip_obj: ipaddress._BaseAddress) -> bool:
    candidates = [ip_obj]
    if isinstance(ip_obj, ipaddress.IPv6Address) and ip_obj.ipv4_mapped is not None:
        candidates.append(ip_obj.ipv4_mapped)
    for addr in candidates:
        if (
            addr.is_loopback
            or addr.is_link_local
            or addr.is_private
            or addr.is_unspecified
            or addr.is_multicast
            or addr.is_reserved
            or any(addr in network for network in _BLOCKED_IP_NETWORKS)
        ):
            return True
    return False


async def _validate_safe_remote_url(url: str) -> None:
    """Validate that a remote image URL uses HTTP(S) and resolves only to public IPs."""
    parsed = urlparse(str(url or "").strip())
    scheme = (parsed.scheme or "").lower()
    if scheme not in ("http", "https"):
        raise ValueError(f"Unsupported URL scheme: {scheme or 'empty'}")

    host = (parsed.hostname or "").strip()
    if not host:
        raise ValueError("URL hostname is required")
    if host.lower() in ("localhost", "localhost.localdomain"):
        raise ValueError(f"Disallowed hostname: {host}")

    raw_host_ip = host.split("%", 1)[0]
    try:
        literal_ip = ipaddress.ip_address(raw_host_ip)
    except ValueError:
        literal_ip = None

    if literal_ip is not None:
        if _is_disallowed_ip(literal_ip):
            raise ValueError(f"Disallowed IP address: {literal_ip}")
        return

    try:
        addr_infos = await asyncio.to_thread(socket.getaddrinfo, host, None)
    except Exception as exc:
        raise ValueError(f"Failed to resolve hostname {host}: {exc}") from exc

    if not addr_infos:
        raise ValueError(f"Hostname {host} resolved to no addresses")

    for info in addr_infos:
        sockaddr = info[4]
        if not sockaddr:
            continue
        ip_text = str(sockaddr[0]).split("%", 1)[0]
        ip_obj = ipaddress.ip_address(ip_text)
        if _is_disallowed_ip(ip_obj):
            raise ValueError(f"Hostname {host} resolved to disallowed IP {ip_obj}")


async def retrieve_image_data(url: str) -> Optional[bytes]:
    """Read image bytes from local /tmp cache or remote URL."""
    file_cache = getattr(generation_handler, "file_cache", None)
    try:
        if "/tmp/" in url and file_cache:
            raw_path = unquote(urlparse(url).path)
            raw_name = raw_path.split("/tmp/")[-1]
            if (
                not raw_name
                or "/" in raw_name
                or "\\" in raw_name
                or ".." in raw_name
                or "\x00" in raw_name
            ):
                debug_logger.log_warning(
                    f"[CONTEXT] 拒绝不安全的本地缓存路径: {raw_name!r}"
                )
                return None

            cache_root = file_cache.cache_dir.resolve()
            candidate = (cache_root / raw_name).resolve()
            if not candidate.is_relative_to(cache_root):
                debug_logger.log_warning(
                    f"[CONTEXT] 拒绝越界的本地缓存路径: {raw_name!r}"
                )
                return None

            if candidate.exists() and candidate.is_file():
                data = await asyncio.to_thread(candidate.read_bytes)
                if data:
                    return data
    except Exception as exc:
        debug_logger.log_warning(f"[CONTEXT] 本地缓存读取失败: {str(exc)}")
        return None

    proxy_url = None
    try:
        if file_cache and hasattr(file_cache, "_resolve_download_proxy"):
            proxy_url = await file_cache._resolve_download_proxy("image")
    except Exception as exc:
        debug_logger.log_warning(f"[CONTEXT] 图片下载代理解析失败: {str(exc)}")

    async def _close_response(resp: Any) -> None:
        quit_now = getattr(resp, "quit_now", None)
        if quit_now is not None and callable(getattr(quit_now, "set", None)):
            try:
                quit_now.set()
            except Exception:
                pass
        aclose_fn = getattr(resp, "aclose", None)
        if callable(aclose_fn):
            try:
                res = aclose_fn()
                if inspect.isawaitable(res):
                    await asyncio.wait_for(res, timeout=2.0)
                return
            except Exception:
                pass
        close_fn = getattr(resp, "close", None)
        if callable(close_fn):
            try:
                res = close_fn()
                if inspect.isawaitable(res):
                    await res
            except Exception:
                pass

    def _extract_content_length(resp: Any) -> Optional[int]:
        headers_obj = getattr(resp, "headers", None)
        if not headers_obj:
            return None
        raw_val = None
        get_fn = getattr(headers_obj, "get", None)
        if callable(get_fn):
            for hdr_key in ("content-length", "Content-Length"):
                candidate = get_fn(hdr_key)
                if isinstance(candidate, (str, bytes, int)):
                    raw_val = candidate
                    break
        if raw_val is None:
            items_fn = getattr(headers_obj, "items", None)
            if callable(items_fn):
                for k, v in items_fn():
                    if str(k).lower() == "content-length" and isinstance(
                        v, (str, bytes, int)
                    ):
                        raw_val = v
                        break
        if raw_val is None:
            return None
        try:
            return int(
                raw_val.decode("ascii") if isinstance(raw_val, bytes) else str(raw_val).strip()
            )
        except (TypeError, ValueError, UnicodeDecodeError):
            return None

    try:
        current_url = url

        async def _download_with_session(session: Any) -> Optional[bytes]:
            nonlocal current_url
            for _ in range(5):
                await _validate_safe_remote_url(current_url)
                response = await session.get(
                    current_url,
                    timeout=60,
                    proxies={"http": proxy_url, "https": proxy_url} if proxy_url else None,
                    headers={
                        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                        "Accept-Encoding": "gzip, deflate, br",
                        "Connection": "keep-alive",
                        "Referer": "https://flow.google.com/",
                    },
                    impersonate="chrome120",
                    verify=True,
                    allow_redirects=False,
                    stream=True,
                )
                try:
                    if response.status_code in (301, 302, 303, 307, 308):
                        headers_obj = getattr(response, "headers", None)
                        location = None
                        if headers_obj and callable(getattr(headers_obj, "get", None)):
                            location = headers_obj.get("location") or headers_obj.get(
                                "Location"
                            )
                        if not location:
                            break
                        current_url = urljoin(current_url, location)
                        continue
                    if response.status_code != 200:
                        debug_logger.log_warning(
                            f"[CONTEXT] 图片下载失败，状态码: {response.status_code}"
                        )
                        break

                    content_length = _extract_content_length(response)
                    if (
                        content_length is not None
                        and content_length > MAX_INBOUND_IMAGE_BYTES
                    ):
                        debug_logger.log_warning(
                            f"[CONTEXT] 图片 Content-Length 超过上限 ({content_length} > {MAX_INBOUND_IMAGE_BYTES}): {current_url}"
                        )
                        return None

                    is_unstreamed_curl_response = (
                        getattr(response, "curl", None) is not None
                        and getattr(response, "queue", None) is None
                    )
                    aiter_fn = (
                        None
                        if is_unstreamed_curl_response
                        else getattr(response, "aiter_content", None)
                    )
                    if callable(aiter_fn):
                        buf = bytearray()
                        saw_stream_chunk = False
                        async for chunk in aiter_fn():
                            if not chunk:
                                continue
                            saw_stream_chunk = True
                            if len(buf) + len(chunk) > MAX_INBOUND_IMAGE_BYTES:
                                debug_logger.log_warning(
                                    f"[CONTEXT] 图片流式下载超过大小上限 ({MAX_INBOUND_IMAGE_BYTES} bytes): {current_url}"
                                )
                                return None
                            buf.extend(chunk)
                        if saw_stream_chunk:
                            return bytes(buf)

                    raw_content = getattr(response, "content", None)
                    if isinstance(raw_content, (bytes, bytearray)) and raw_content:
                        if len(raw_content) > MAX_INBOUND_IMAGE_BYTES:
                            debug_logger.log_warning(
                                f"[CONTEXT] 图片响应体超过大小上限 ({len(raw_content)} > {MAX_INBOUND_IMAGE_BYTES}): {current_url}"
                            )
                            return None
                        return bytes(raw_content)
                    return None
                finally:
                    await _close_response(response)
            return None
        get_pooled = getattr(file_cache, "get_http_session", None) if file_cache else None
        if callable(get_pooled):
            pool_key, pooled_session = await get_pooled(
                impersonate="chrome120", proxy_url=proxy_url, verify=True
            )
            try:
                return await _download_with_session(pooled_session)
            except Exception:
                evict_fn = getattr(file_cache, "evict_http_session", None)
                if callable(evict_fn):
                    await evict_fn(pool_key, pooled_session)
                raise

        async with AsyncSession() as session:
            return await _download_with_session(session)
    except Exception as exc:
        debug_logger.log_error(f"[CONTEXT] 图片下载异常: {str(exc)}")
    return None

async def _load_image_bytes_from_uri(uri: str) -> bytes:
    clean_uri = str(uri or "").strip()
    if not clean_uri:
        raise HTTPException(status_code=400, detail="Image URI cannot be empty")

    if clean_uri.startswith("data:image"):
        _, image_bytes = _decode_data_url(clean_uri)
        return image_bytes

    if (
        clean_uri.startswith("http://")
        or clean_uri.startswith("https://")
        or "/tmp/" in clean_uri
    ):
        image_bytes = await retrieve_image_data(clean_uri)
        if image_bytes:
            return image_bytes
        raise HTTPException(
            status_code=400, detail=f"Failed to load image from {clean_uri}"
        )

    try:
        return validate_base64_media_payload(clean_uri, expected_kind="image")
    except (ValueError, binascii.Error) as exc:
        preview = clean_uri if len(clean_uri) <= 256 else f"{clean_uri[:256]}..."
        raise HTTPException(
            status_code=400, detail=f"Unsupported image URI: {preview}"
        ) from exc

def _coerce_gemini_contents(raw_contents: Optional[List[Any]]) -> List[GeminiContent]:
    contents: List[GeminiContent] = []
    for item in raw_contents or []:
        if isinstance(item, GeminiContent):
            contents.append(item)
        else:
            contents.append(GeminiContent.model_validate(item))
    return contents


def _extract_text_from_gemini_content(content: Optional[GeminiContent]) -> str:
    if content is None:
        return ""
    text_parts = [part.text.strip() for part in content.parts if part.text]
    return "\n".join(part for part in text_parts if part).strip()


def _should_ignore_media_system_instruction(system_instruction: str) -> bool:
    """Drop agent/tool scaffolding before sending media prompts upstream."""
    if not system_instruction:
        return False

    normalized = system_instruction.lower()
    if len(system_instruction) > 1200:
        return True

    return any(marker in normalized for marker in MEDIA_SYSTEM_INSTRUCTION_MARKERS)


def _sanitize_media_prompt(prompt: str) -> str:
    """Strip agent/tool scaffolding that image/video models cannot use."""
    if not prompt:
        return ""

    sanitized = MEDIA_PROMPT_TOOL_BLOCK_RE.sub(" ", prompt.strip())
    cleaned_lines: List[str] = []
    for raw_line in sanitized.splitlines():
        line = raw_line.strip()
        if not line:
            if cleaned_lines and cleaned_lines[-1] != "":
                cleaned_lines.append("")
            continue
        if any(pattern.fullmatch(line) for pattern in MEDIA_PROMPT_PREAMBLE_PATTERNS):
            continue
        cleaned_lines.append(line)

    sanitized = "\n".join(cleaned_lines).strip()
    sanitized = re.sub(r"\n{3,}", "\n\n", sanitized)
    return sanitized.strip()


async def _extract_prompt_and_images_from_openai_messages(
    messages: List[ChatMessage],
) -> tuple[str, List[bytes], Optional[str]]:
    """Extract prompt, images, and optional video_media_id from messages.

    Returns:
        (prompt, images, video_media_id)
        video_media_id is set when an image_url starts with "extend://"
    """
    last_message = messages[-1]
    content = last_message.content
    prompt_parts: List[str] = []
    images: List[bytes] = []
    video_media_id: Optional[str] = None

    if isinstance(content, str):
        prompt_parts.append(content)
    elif isinstance(content, list):
        for item in content:
            item_type = item.get("type")
            if item_type == "text":
                text = item.get("text", "").strip()
                if text:
                    prompt_parts.append(text)
            elif item_type == "image_url":
                image_url = item.get("image_url", {}).get("url", "")
                # extend://MEDIA_ID 用于视频续写
                if image_url.startswith("extend://"):
                    video_media_id = image_url[len("extend://"):]
                else:
                    images.append(await _load_image_bytes_from_uri(image_url))

    prompt = "\n".join(part for part in prompt_parts if part).strip()
    return prompt, images, video_media_id


async def _append_openai_reference_images(
    model: str,
    messages: List[ChatMessage],
    images: List[bytes],
) -> List[bytes]:
    model_config = MODEL_CONFIG.get(model)
    if not model_config or model_config["type"] != "image" or len(messages) <= 1:
        return images

    debug_logger.log_info(f"[CONTEXT] 开始查找历史参考图，消息数量: {len(messages)}")

    for msg in reversed(messages[:-1]):
        if msg.role == "assistant" and isinstance(msg.content, str):
            matches = MARKDOWN_IMAGE_RE.findall(msg.content)
            if not matches:
                continue

            for image_url in reversed(matches):
                if not image_url.startswith("http") and "/tmp/" not in image_url:
                    continue
                try:
                    downloaded_bytes = await retrieve_image_data(image_url)
                    if downloaded_bytes:
                        images.insert(0, downloaded_bytes)
                        debug_logger.log_info(
                            f"[CONTEXT] ✅ 添加历史参考图: {image_url}"
                        )
                        return images
                    debug_logger.log_warning(
                        f"[CONTEXT] 图片下载失败或为空，尝试下一个: {image_url}"
                    )
                except Exception as exc:
                    debug_logger.log_error(f"[CONTEXT] 处理参考图时出错: {str(exc)}")
    return images


async def _extract_prompt_and_images_from_gemini_contents(
    contents: List[GeminiContent],
) -> tuple[str, List[bytes]]:
    if not contents:
        raise HTTPException(status_code=400, detail="contents cannot be empty")

    target_content = next(
        (
            content
            for content in reversed(contents)
            if (content.role or "user") == "user"
        ),
        contents[-1],
    )

    prompt_parts: List[str] = []
    images: List[bytes] = []

    for part in target_content.parts:
        if part.text:
            text = part.text.strip()
            if text:
                prompt_parts.append(text)
        elif part.inlineData is not None:
            mime_type = part.inlineData.mimeType.lower()
            if not mime_type.startswith("image/"):
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported inlineData mime type: {part.inlineData.mimeType}",
                )
            try:
                images.append(
                    validate_base64_media_payload(
                        part.inlineData.data, expected_kind="image"
                    )
                )
            except (ValueError, binascii.Error) as exc:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid base64 inlineData payload",
                ) from exc
        elif part.fileData is not None:
            mime_type = (part.fileData.mimeType or "").lower()
            if mime_type and not mime_type.startswith("image/"):
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported fileData mime type: {part.fileData.mimeType}",
                )
            images.append(await _load_image_bytes_from_uri(part.fileData.fileUri))

    prompt = "\n".join(part for part in prompt_parts if part).strip()
    return prompt, images


def _resolve_request_model(
    model: str, request: Any, images: Optional[List[bytes]] = None
) -> str:
    resolved_model = resolve_model_name(
        model=model, request=request, model_config=MODEL_CONFIG, images=images
    )
    if resolved_model != model:
        debug_logger.log_info(f"[ROUTE] 模型名已转换: {model} → {resolved_model}")
    return resolved_model


def _get_request_base_url(request: Optional[Request]) -> Optional[str]:
    """根据实际请求头推导对外可访问的基础地址（含反代头与 server_path_prefix）。"""
    if request is None:
        return None
    headers = getattr(request, "headers", None) or {}
    forwarded_proto = (
        (headers.get("x-forwarded-proto") or "").split(",")[0].strip()
    )
    forwarded_host = (
        (headers.get("x-forwarded-host") or "").split(",")[0].strip()
    )
    url_obj = getattr(request, "url", None)
    fallback_host = (getattr(url_obj, "netloc", None) or "").strip()
    host = (forwarded_host or headers.get("host") or fallback_host).strip()

    if not host:
        return None

    proto = forwarded_proto or getattr(url_obj, "scheme", None) or "http"
    raw_prefix = str(getattr(config, "server_path_prefix", "") or "").strip()
    prefix = f"/{raw_prefix.strip('/')}" if raw_prefix and raw_prefix != "/" else ""
    return f"{proto}://{host}{prefix}"

async def _normalize_openai_request(
    request: ChatCompletionRequest,
) -> NormalizedGenerationRequest:
    if request.messages:
        (
            prompt,
            images,
            video_media_id,
        ) = await _extract_prompt_and_images_from_openai_messages(request.messages)
        if request.image and not images:
            images.append(await _load_image_bytes_from_uri(request.image))
        model = _resolve_request_model(request.model, request, images=images)
        initial_image_count = len(images)
        images = await _append_openai_reference_images(model, request.messages, images)
        if len(images) != initial_image_count:
            model = _resolve_request_model(request.model, request, images=images)
        return NormalizedGenerationRequest(
            model=model,
            prompt=prompt,
            images=images,
            messages=request.messages,
            video_media_id=video_media_id,
        )

    if request.contents:
        gemini_request = GeminiGenerateContentRequest(
            contents=_coerce_gemini_contents(request.contents),
            generationConfig=request.generationConfig,
        )
        normalized = await _normalize_gemini_request(request.model, gemini_request)
        normalized.messages = request.messages
        return normalized

    raise HTTPException(status_code=400, detail="Messages or contents cannot be empty")


async def _normalize_gemini_request(
    model: str,
    request: GeminiGenerateContentRequest,
) -> NormalizedGenerationRequest:
    prompt, images = await _extract_prompt_and_images_from_gemini_contents(
        request.contents
    )
    resolved_model = _resolve_request_model(model, request, images=images)
    system_instruction = _extract_text_from_gemini_content(request.systemInstruction)
    model_config = MODEL_CONFIG.get(resolved_model)
    media_model = bool(model_config and model_config.get("type") in {"image", "video"})

    if media_model:
        prompt = _sanitize_media_prompt(prompt)

    if system_instruction:
        if media_model and _should_ignore_media_system_instruction(system_instruction):
            debug_logger.log_warning(
                f"[GEMINI] 忽略媒体模型的 systemInstruction: model={resolved_model}, len={len(system_instruction)}"
            )
        else:
            if media_model:
                system_instruction = _sanitize_media_prompt(system_instruction)
            prompt = f"{system_instruction}\n\n{prompt}".strip()

    return NormalizedGenerationRequest(
        model=resolved_model,
        prompt=prompt,
        images=images,
    )


async def _collect_non_stream_result(
    model: str,
    prompt: str,
    images: List[bytes],
    base_url_override: Optional[str] = None,
    video_media_id: Optional[str] = None,
    raw_request: Optional[Request] = None,
) -> str:
    handler = _ensure_generation_handler()

    async def _consume() -> Optional[str]:
        last_chunk: Optional[str] = None
        async for chunk in handler.handle_generation(
            model=model,
            prompt=prompt,
            images=images if images else None,
            stream=False,
            base_url_override=base_url_override,
            video_media_id=video_media_id,
        ):
            last_chunk = chunk
        return last_chunk

    if raw_request is None:
        result = await _consume()
    else:
        gen_task = asyncio.create_task(_consume())
        client_disconnected = False

        async def _watch_disconnect() -> None:
            nonlocal client_disconnected
            try:
                while not gen_task.done():
                    if await raw_request.is_disconnected():
                        client_disconnected = True
                        if not gen_task.done():
                            gen_task.cancel()
                        return
                    await asyncio.sleep(0.4)
            except Exception:
                return

        watch_task = asyncio.create_task(_watch_disconnect())
        try:
            result = await gen_task
        except asyncio.CancelledError:
            if client_disconnected:
                raise HTTPException(status_code=499, detail="Client disconnected")
            raise
        finally:
            if not watch_task.done():
                watch_task.cancel()
                await asyncio.gather(watch_task, return_exceptions=True)

    if result is None:
        raise HTTPException(status_code=500, detail="Generation failed: No response")

    return result

def _parse_handler_result(result: str) -> Dict[str, Any]:
    try:
        return json.loads(result)
    except json.JSONDecodeError:
        return {"result": result}


def _get_error_status_code(payload: Dict[str, Any]) -> int:
    error = payload.get("error")
    if isinstance(error, dict):
        status_code = error.get("status_code")
        if isinstance(status_code, int):
            return status_code
        if isinstance(status_code, str) and status_code.isdigit():
            return int(status_code)
        return 400
    return 200


def _build_openai_json_response(payload: Dict[str, Any]) -> JSONResponse:
    return JSONResponse(content=payload, status_code=_get_error_status_code(payload))


def _build_gemini_error_payload(status_code: int, message: str) -> Dict[str, Any]:
    return {
        "error": {
            "code": status_code,
            "message": message,
            "status": GEMINI_STATUS_MAP.get(status_code, "UNKNOWN"),
        }
    }


def _build_gemini_error_response_from_handler(payload: Dict[str, Any]) -> JSONResponse:
    error = payload.get("error", {})
    status_code = _get_error_status_code(payload)
    message = error.get("message", "Generation failed")
    return JSONResponse(
        status_code=status_code,
        content=_build_gemini_error_payload(status_code, message),
    )


def _extract_openai_message_content(payload: Dict[str, Any]) -> str:
    choices = payload.get("choices", [])
    if not choices:
        return payload.get("result", "")

    message = choices[0].get("message", {})
    content = message.get("content", "")
    return content if isinstance(content, str) else ""


def _extract_url_from_openai_payload(payload: Dict[str, Any]) -> Optional[str]:
    direct_url = payload.get("url")
    if isinstance(direct_url, str) and direct_url.strip():
        return direct_url.strip()

    content = _extract_openai_message_content(payload).strip()
    if not content:
        return None

    image_match = MARKDOWN_IMAGE_RE.search(content)
    if image_match:
        return image_match.group(1).strip()

    video_match = HTML_VIDEO_RE.search(content)
    if video_match:
        return video_match.group(1).strip()

    return None


def _enrich_payload_with_direct_url(payload: Dict[str, Any]) -> Dict[str, Any]:
    extracted_url = _extract_url_from_openai_payload(payload)
    if extracted_url and not payload.get("url"):
        payload["url"] = extracted_url
    return payload


async def _build_image_parts_from_uri(uri: str) -> List[Dict[str, Any]]:
    if uri.startswith("data:image"):
        match = DATA_URL_RE.match(uri)
        if not match:
            raise HTTPException(status_code=400, detail="Invalid data URL")
        return [
            {"inlineData": {"mimeType": match.group("mime"), "data": match.group("data")}}
        ]
    image_bytes = await retrieve_image_data(uri)
    if image_bytes:
        mime_type = _detect_image_mime_type(
            image_bytes,
            fallback=_guess_mime_type(uri, "image/png"),
        )
        return [
            {
                "inlineData": {
                    "mimeType": mime_type,
                    "data": base64.b64encode(image_bytes).decode("ascii"),
                }
            }
        ]

    return [
        {
            "fileData": {
                "mimeType": _guess_mime_type(uri, "image/png"),
                "fileUri": uri,
            }
        },
        {"text": uri},
    ]


def _build_video_parts_from_uri(uri: str) -> List[Dict[str, Any]]:
    return [
        {
            "fileData": {
                "mimeType": _guess_mime_type(uri, "video/mp4"),
                "fileUri": uri,
            }
        }
    ]


async def _build_gemini_parts_from_output(output: str) -> List[Dict[str, Any]]:
    if not output:
        return []

    image_matches = MARKDOWN_IMAGE_RE.findall(output)
    if image_matches:
        parts: List[Dict[str, Any]] = []
        for uri in image_matches:
            parts.extend(await _build_image_parts_from_uri(uri))
        return parts

    video_matches = HTML_VIDEO_RE.findall(output)
    if video_matches:
        parts: List[Dict[str, Any]] = []
        for uri in video_matches:
            parts.extend(_build_video_parts_from_uri(uri))
        return parts

    return [{"text": output}]


async def _build_gemini_success_payload(
    payload: Dict[str, Any],
    response_model: str,
) -> Dict[str, Any]:
    output = _extract_openai_message_content(payload)
    return {
        "candidates": [
            {
                "content": {
                    "role": "model",
                    "parts": await _build_gemini_parts_from_output(output),
                },
                "finishReason": "STOP",
                "index": 0,
            }
        ],
        "modelVersion": response_model,
    }


def _normalize_finish_reason(reason: Optional[str]) -> Optional[str]:
    if reason is None:
        return None
    mapping = {
        "stop": "STOP",
        "length": "MAX_TOKENS",
        "content_filter": "SAFETY",
    }
    return mapping.get(reason, "STOP")


async def _convert_openai_stream_chunk_to_gemini_event(
    payload: Dict[str, Any],
    response_model: str,
) -> Optional[str]:
    choices = payload.get("choices", [])
    if not choices:
        return None

    choice = choices[0]
    delta = choice.get("delta", {})
    text = delta.get("reasoning_content") or delta.get("content") or ""
    finish_reason = _normalize_finish_reason(choice.get("finish_reason"))

    candidate: Dict[str, Any] = {"index": choice.get("index", 0)}
    if text:
        candidate["content"] = {
            "role": "model",
            "parts": await _build_gemini_parts_from_output(text),
        }
    if finish_reason:
        candidate["finishReason"] = finish_reason

    if len(candidate) == 1:
        return None

    chunk = {
        "candidates": [candidate],
        "modelVersion": response_model,
    }
    return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"


async def _iter_generation_chunks_with_disconnect(
    stream_iter: Any,
    raw_request: Optional[Request],
    *,
    disconnect_state: Optional[Dict[str, bool]] = None,
    poll_interval: float = 0.25,
):
    """Iterate an async generator while racing pending anext() against client disconnect."""
    if raw_request is None:
        try:
            async for chunk in stream_iter:
                yield chunk
        finally:
            aclose_fn = getattr(stream_iter, "aclose", None)
            if callable(aclose_fn):
                try:
                    await aclose_fn()
                except Exception:
                    pass
        return

    client_disconnected = False
    pending_next_task: Optional[asyncio.Task] = None
    watcher_stopped = False

    async def _next_chunk() -> Any:
        return await anext(stream_iter)

    async def _watch_disconnect() -> None:
        nonlocal client_disconnected
        try:
            while not watcher_stopped:
                if await raw_request.is_disconnected():
                    client_disconnected = True
                    if disconnect_state is not None:
                        disconnect_state["disconnected"] = True
                    task = pending_next_task
                    if task is not None and not task.done():
                        task.cancel()
                    return
                await asyncio.sleep(poll_interval)
        except asyncio.CancelledError:
            return
        except Exception:
            return

    watch_task = asyncio.create_task(_watch_disconnect())
    try:
        while True:
            if client_disconnected or await raw_request.is_disconnected():
                client_disconnected = True
                if disconnect_state is not None:
                    disconnect_state["disconnected"] = True
                break
            pending_next_task = asyncio.create_task(_next_chunk())
            try:
                chunk = await pending_next_task
            except StopAsyncIteration:
                break
            except asyncio.CancelledError:
                if client_disconnected:
                    break
                raise
            finally:
                pending_next_task = None
            yield chunk
    finally:
        watcher_stopped = True
        if pending_next_task is not None and not pending_next_task.done():
            pending_next_task.cancel()
            await asyncio.gather(pending_next_task, return_exceptions=True)
        if not watch_task.done():
            watch_task.cancel()
        await asyncio.gather(watch_task, return_exceptions=True)
        aclose_fn = getattr(stream_iter, "aclose", None)
        if callable(aclose_fn):
            try:
                await aclose_fn()
            except Exception:
                pass


async def _iterate_openai_stream(
    normalized: NormalizedGenerationRequest,
    base_url_override: Optional[str] = None,
    raw_request: Optional[Request] = None,
):
    handler = _ensure_generation_handler()
    disconnect_state = {"disconnected": False}
    stream_iter = handler.handle_generation(
        model=normalized.model,
        prompt=normalized.prompt,
        images=normalized.images if normalized.images else None,
        stream=True,
        base_url_override=base_url_override,
        video_media_id=normalized.video_media_id,
    )
    async for chunk in _iter_generation_chunks_with_disconnect(
        stream_iter, raw_request, disconnect_state=disconnect_state
    ):
        if chunk.startswith("data: "):
            yield chunk
            continue

        payload = _parse_handler_result(chunk)
        yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

    if not disconnect_state["disconnected"] and (
        raw_request is None or not await raw_request.is_disconnected()
    ):
        yield "data: [DONE]\n\n"


async def _iterate_gemini_stream(
    normalized: NormalizedGenerationRequest,
    response_model: str,
    base_url_override: Optional[str] = None,
    raw_request: Optional[Request] = None,
):
    handler = _ensure_generation_handler()
    stream_iter = handler.handle_generation(
        model=normalized.model,
        prompt=normalized.prompt,
        images=normalized.images if normalized.images else None,
        stream=True,
        base_url_override=base_url_override,
        video_media_id=normalized.video_media_id,
    )
    async for chunk in _iter_generation_chunks_with_disconnect(stream_iter, raw_request):
        if chunk.startswith("data: "):
            payload_text = chunk[6:].strip()
            if payload_text == "[DONE]":
                continue
            payload = _parse_handler_result(payload_text)
        else:
            payload = _parse_handler_result(chunk)
        if "error" in payload:
            yield (
                f"data: {json.dumps(_build_gemini_error_payload(_get_error_status_code(payload), payload['error'].get('message', 'Generation failed')), ensure_ascii=False)}\n\n"
            )
            return

        event = await _convert_openai_stream_chunk_to_gemini_event(
            payload,
            response_model,
        )
        if event:
            yield event

@router.get("/v1/models")
async def list_models(api_key: str = Depends(verify_api_key_flexible)):
    """List available canonical models."""
    policy = AuthManager.get_policy_for_verified_key(api_key)
    models = [
        {
            "id": model["id"],
            "object": "model",
            "owned_by": "flow2api",
            "description": model["description"],
            "type": model["type"],
            "default_parameters": model["default_parameters"],
            "supported_parameters": model["supported_parameters"],
        }
        for model in _get_openai_model_catalog()
        if policy.allows_listed_model(model["id"])
    ]

    return {"object": "list", "data": models}


@router.get("/v1/models/aliases")
async def list_model_aliases(api_key: str = Depends(verify_api_key_flexible)):
    """List simplified model aliases for generationConfig-based resolution."""
    policy = AuthManager.get_policy_for_verified_key(api_key)
    alias_models = [
        {
            "id": alias_id,
            "object": "model",
            "owned_by": "flow2api",
            "description": description,
            "is_alias": True,
        }
        for alias_id, description in get_base_model_aliases().items()
        if policy.allows_listed_model(alias_id)
    ]
    return {"object": "list", "data": alias_models}


@router.get("/v1beta/models")
@router.get("/models")
async def list_gemini_models(api_key: str = Depends(verify_api_key_flexible)):
    """List available canonical models using Gemini-compatible response shape."""
    policy = AuthManager.get_policy_for_verified_key(api_key)
    catalog = _get_gemini_model_catalog()
    return {
        "models": [
            _build_gemini_model_resource(model_id, description)
            for model_id, description in catalog.items()
            if policy.allows_listed_model(model_id)
        ]
    }


@router.get("/v1beta/models/{model}")
@router.get("/models/{model}")
async def get_gemini_model(model: str, api_key: str = Depends(verify_api_key_flexible)):
    """Return a single model using Gemini-compatible response shape."""
    policy = AuthManager.get_policy_for_verified_key(api_key)
    catalog = _get_gemini_model_catalog()
    canonical = get_canonical_model_name(model)
    target_id = model if model in catalog else (canonical if canonical in catalog else None)
    description = catalog.get(target_id) if target_id else None
    if not description or not target_id:
        cfg = MODEL_CONFIG.get(model)
        if cfg:
            target_id = model
            description = _build_model_description(cfg)
    if not description or not target_id or not policy.allows_execution(
        target_id,
        raw_model=model,
        canonical_model=canonical,
        base_aliases=get_base_model_aliases(),
    ):
        return JSONResponse(
            status_code=404,
            content=_build_gemini_error_payload(404, f"Model not found: {model}"),
        )

    return _build_gemini_model_resource(target_id, description)


async def _materialize_openai_image_item(
    payload: Dict[str, Any],
    *,
    prompt: str,
    response_format: str,
) -> Dict[str, Any]:
    """Build one OpenAI `data[]` entry from a GenerationHandler completion payload."""
    url = _extract_url_from_openai_payload(payload)
    if not url:
        raise HTTPException(
            status_code=502,
            detail="Image generation returned no image URL",
        )
    fmt = str(response_format or "url").strip().lower()
    if fmt == "b64_json":
        if url.startswith("data:image"):
            match = DATA_URL_RE.match(url)
            if match:
                return {"b64_json": match.group("data"), "revised_prompt": prompt}
        img_bytes = await retrieve_image_data(url)
        if img_bytes:
            return {
                "b64_json": base64.b64encode(img_bytes).decode("ascii"),
                "revised_prompt": prompt,
            }
        raise HTTPException(
            status_code=502,
            detail="Failed to encode generated image as b64_json",
        )
    return {"url": url, "revised_prompt": prompt}


async def _decode_any_image_input(value: Any) -> bytes:
    """Decode an uploaded UploadFile, data URL, HTTP(S) URL, or raw base64 string into bytes."""
    if hasattr(value, "read") and callable(value.read):
        data = await value.read()
        if not data:
            raise HTTPException(status_code=400, detail="Uploaded image file is empty")
        return data
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    text = str(value or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Image input cannot be empty")
    if text.startswith(("data:image", "http://", "https://")) or "/tmp/" in text:
        return await _load_image_bytes_from_uri(text)
    try:
        return base64.b64decode(text)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Invalid base64 image input: {exc}")


async def _gather_tasks_fail_fast(coros: List[Any]) -> List[Any]:
    if len(coros) == 1:
        return [await coros[0]]
    tasks = [asyncio.create_task(coro) for coro in coros]
    try:
        return list(await asyncio.gather(*tasks))
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


async def _run_openai_images_request(
    *,
    raw_model: str,
    prompt: str,
    n: int,
    response_format: str,
    param_carrier: Any,
    images: List[bytes],
    raw_request: Request,
    api_key: str,
) -> JSONResponse:
    prompt_clean = str(prompt or "").strip()
    if not prompt_clean:
        raise HTTPException(status_code=400, detail="Prompt cannot be empty")
    count = int(n or 1)
    if count < 1 or count > 4:
        raise HTTPException(status_code=400, detail="n must be between 1 and 4")

    resolved_model = _resolve_request_model(raw_model, param_carrier, images=images)
    canonical_model = get_canonical_model_name(resolved_model)
    assert_api_key_model_allowed(
        api_key,
        resolved_model,
        raw_model=raw_model,
        canonical_model=canonical_model,
        base_aliases=get_base_model_aliases(),
    )
    model_cfg = MODEL_CONFIG.get(resolved_model)
    if not model_cfg or model_cfg.get("type") != "image":
        raise HTTPException(
            status_code=400,
            detail=f"Model '{raw_model}' is not an image generation model",
        )

    request_base_url = _get_request_base_url(raw_request)

    async def _generate_one() -> Dict[str, Any]:
        return _enrich_payload_with_direct_url(
            _parse_handler_result(
                await _collect_non_stream_result(
                    resolved_model,
                    prompt_clean,
                    images,
                    base_url_override=request_base_url,
                    raw_request=raw_request,
                )
            )
        )

    payloads = await _gather_tasks_fail_fast([_generate_one() for _ in range(count)])
    for payload in payloads:
        if "error" in payload:
            return _build_openai_json_response(payload)

    items = await _gather_tasks_fail_fast(
        [
            _materialize_openai_image_item(
                payload,
                prompt=prompt_clean,
                response_format=response_format,
            )
            for payload in payloads
        ]
    )

    return JSONResponse(
        content={
            "created": int(time.time()),
            "model": canonical_model,
            "data": items,
        }
    )


@router.post("/v1/images/generations")
async def create_image_generation(
    request: OpenAIImageGenerationRequest,
    raw_request: Request,
    api_key: str = Depends(verify_api_key_flexible),
):
    """Standard OpenAI image generation endpoint (defaults to 16:9 + 2K)."""
    try:
        return await _run_openai_images_request(
            raw_model=request.model or "gemini-3.1-flash-image",
            prompt=request.prompt,
            n=request.n,
            response_format=request.response_format or "url",
            param_carrier=request,
            images=[],
            raw_request=raw_request,
            api_key=api_key,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/v1/images/edits")
async def create_image_edit(
    raw_request: Request,
    api_key: str = Depends(verify_api_key_flexible),
):
    """Standard OpenAI image edit endpoint (supports multipart/form-data and JSON)."""
    try:
        content_type = (raw_request.headers.get("content-type") or "").lower()
        images: List[bytes] = []
        if "multipart/form-data" in content_type:
            form = await raw_request.form()
            upload_entries: List[Any] = []
            try:
                raw_model = str(form.get("model") or "gemini-3.1-flash-image").strip()
                prompt = str(form.get("prompt") or "")
                n_raw = form.get("n")
                try:
                    n_val = int(n_raw) if n_raw not in (None, "") else 1
                except (TypeError, ValueError):
                    raise HTTPException(status_code=400, detail="Invalid n parameter")
                response_format = str(form.get("response_format") or "url")
                for field_name in ("image", "image[]", "images"):
                    for entry in form.getlist(field_name):
                        if entry is not None and entry != "":
                            upload_entries.append(entry)
                for entry in upload_entries:
                    images.append(await _decode_any_image_input(entry))
                param_carrier = SimpleNamespace(
                    size=form.get("size"),
                    quality=form.get("quality"),
                    aspect_ratio=form.get("aspect_ratio") or form.get("aspectRatio"),
                    image_size=form.get("image_size")
                    or form.get("imageSize")
                    or form.get("resolution"),
                )
            finally:
                closed_ids: set[int] = set()
                for entry in upload_entries:
                    close_fn = getattr(entry, "close", None)
                    if callable(close_fn):
                        closed_ids.add(id(entry))
                        try:
                            res = close_fn()
                            if inspect.isawaitable(res):
                                await res
                        except Exception:
                            pass
                multi_items = getattr(form, "multi_items", None)
                if callable(multi_items):
                    for _, val in multi_items():
                        if id(val) in closed_ids:
                            continue
                        close_fn = getattr(val, "close", None)
                        if callable(close_fn):
                            closed_ids.add(id(val))
                            try:
                                res = close_fn()
                                if inspect.isawaitable(res):
                                    await res
                            except Exception:
                                pass
                elif callable(getattr(form, "close", None)):
                    try:
                        res = form.close()
                        if inspect.isawaitable(res):
                            await res
                    except Exception:
                        pass
        else:
            body = await raw_request.json()
            if not isinstance(body, dict):
                raise HTTPException(status_code=400, detail="Request body must be a JSON object")
            raw_model = str(body.get("model") or "gemini-3.1-flash-image").strip()
            prompt = str(body.get("prompt") or "")
            try:
                n_val = int(body.get("n") or 1)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="Invalid n parameter")
            response_format = str(body.get("response_format") or "url")
            raw_imgs = body.get("images")
            if isinstance(raw_imgs, list):
                for item in raw_imgs:
                    images.append(await _decode_any_image_input(item))
            elif body.get("image"):
                images.append(await _decode_any_image_input(body.get("image")))
            param_carrier = body

        if not images:
            raise HTTPException(status_code=400, detail="At least one reference image is required")

        return await _run_openai_images_request(
            raw_model=raw_model,
            prompt=prompt,
            n=n_val,
            response_format=response_format,
            param_carrier=param_carrier,
            images=images,
            raw_request=raw_request,
            api_key=api_key,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/v1/chat/completions")
async def create_chat_completion(
    request: ChatCompletionRequest,
    raw_request: Request,
    api_key: str = Depends(verify_api_key_flexible),
):
    """OpenAI-compatible unified generation endpoint."""
    try:
        normalized = await _normalize_openai_request(request)
        assert_api_key_model_allowed(
            api_key,
            normalized.model,
            raw_model=request.model,
            canonical_model=get_canonical_model_name(normalized.model),
            base_aliases=get_base_model_aliases(),
        )
        if not normalized.prompt:
            raise HTTPException(status_code=400, detail="Prompt cannot be empty")
        request_base_url = _get_request_base_url(raw_request)

        if request.stream:
            return StreamingResponse(
                _iterate_openai_stream(normalized, request_base_url, raw_request=raw_request),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                },
            )

        payload = _parse_handler_result(
            await _collect_non_stream_result(
                normalized.model,
                normalized.prompt,
                normalized.images,
                base_url_override=request_base_url,
                video_media_id=normalized.video_media_id,
                raw_request=raw_request,
            )
        )
        return _build_openai_json_response(payload)

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/v1beta/models/{model}:generateContent")
@router.post("/models/{model}:generateContent")
async def generate_content(
    model: str,
    request: GeminiGenerateContentRequest,
    raw_request: Request,
    api_key: str = Depends(verify_api_key_flexible),
):
    """Gemini official generateContent endpoint."""
    try:
        normalized = await _normalize_gemini_request(model, request)
        canonical_model = get_canonical_model_name(normalized.model)
        assert_api_key_model_allowed(
            api_key,
            normalized.model,
            raw_model=model,
            canonical_model=canonical_model,
            base_aliases=get_base_model_aliases(),
        )
        if not normalized.prompt:
            raise HTTPException(status_code=400, detail="Prompt cannot be empty")

        request_base_url = _get_request_base_url(raw_request)

        payload = _enrich_payload_with_direct_url(
            _parse_handler_result(
                await _collect_non_stream_result(
                    normalized.model,
                    normalized.prompt,
                    normalized.images,
                    base_url_override=request_base_url,
                    video_media_id=normalized.video_media_id,
                    raw_request=raw_request,
                )
            )
        )
        if "error" in payload:
            return _build_gemini_error_response_from_handler(payload)

        return JSONResponse(
            content=await _build_gemini_success_payload(payload, canonical_model)
        )

    except HTTPException as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content=_build_gemini_error_payload(exc.status_code, str(exc.detail)),
        )
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content=_build_gemini_error_payload(500, str(exc)),
        )


@router.post("/v1beta/models/{model}:streamGenerateContent")
@router.post("/models/{model}:streamGenerateContent")
async def stream_generate_content(
    model: str,
    request: GeminiGenerateContentRequest,
    raw_request: Request,
    alt: Optional[str] = Query(None),
    api_key: str = Depends(verify_api_key_flexible),
):
    """Gemini official streamGenerateContent endpoint."""
    try:
        normalized = await _normalize_gemini_request(model, request)
        canonical_model = get_canonical_model_name(normalized.model)
        assert_api_key_model_allowed(
            api_key,
            normalized.model,
            raw_model=model,
            canonical_model=canonical_model,
            base_aliases=get_base_model_aliases(),
        )
        if not normalized.prompt:
            raise HTTPException(status_code=400, detail="Prompt cannot be empty")

        request_base_url = _get_request_base_url(raw_request)

        return StreamingResponse(
            _iterate_gemini_stream(
                normalized, canonical_model, request_base_url, raw_request=raw_request
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
    except HTTPException as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content=_build_gemini_error_payload(exc.status_code, str(exc.detail)),
        )
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content=_build_gemini_error_payload(500, str(exc)),
        )


@router.post("/v1beta/models/{model}:predict")
@router.post("/models/{model}:predict")
async def predict_images(
    model: str,
    request: GeminiPredictRequest,
    raw_request: Request,
    api_key: str = Depends(verify_api_key_flexible),
):
    """Standard Google AI Studio / Vertex Imagen :predict endpoint."""
    try:
        if not request.instances:
            raise HTTPException(status_code=400, detail="instances cannot be empty")
        first = request.instances[0] if isinstance(request.instances[0], dict) else {}
        prompt = str(first.get("prompt") or "").strip()
        if not prompt:
            raise HTTPException(status_code=400, detail="Prompt cannot be empty")

        images: List[bytes] = []
        img_field = first.get("image")
        if isinstance(img_field, dict):
            b64_val = img_field.get("bytesBase64Encoded") or img_field.get("bytes_base64_encoded")
            if b64_val:
                images.append(await _decode_any_image_input(b64_val))
        elif isinstance(img_field, str) and img_field.strip():
            images.append(await _decode_any_image_input(img_field))

        params = request.parameters if isinstance(request.parameters, dict) else {}
        raw_count = params.get("sampleCount") or params.get("sample_count") or 1
        try:
            sample_count = int(raw_count)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Invalid sampleCount parameter")
        if sample_count < 1 or sample_count > 4:
            raise HTTPException(status_code=400, detail="sampleCount must be between 1 and 4")

        resolved_model = _resolve_request_model(model, request, images=images)
        canonical_model = get_canonical_model_name(resolved_model)
        assert_api_key_model_allowed(
            api_key,
            resolved_model,
            raw_model=model,
            canonical_model=canonical_model,
            base_aliases=get_base_model_aliases(),
        )
        model_cfg = MODEL_CONFIG.get(resolved_model)
        if not model_cfg or model_cfg.get("type") != "image":
            raise HTTPException(
                status_code=400,
                detail=f"Model '{model}' is not an image generation model",
            )

        request_base_url = _get_request_base_url(raw_request)

        async def _generate_one_predict() -> Dict[str, Any]:
            return _enrich_payload_with_direct_url(
                _parse_handler_result(
                    await _collect_non_stream_result(
                        resolved_model,
                        prompt,
                        images,
                        base_url_override=request_base_url,
                        raw_request=raw_request,
                    )
                )
            )

        payloads = await _gather_tasks_fail_fast(
            [_generate_one_predict() for _ in range(sample_count)]
        )
        for payload in payloads:
            if "error" in payload:
                return _build_gemini_error_response_from_handler(payload)

        async def _materialize_prediction(payload: Dict[str, Any]) -> Dict[str, Any]:
            url = _extract_url_from_openai_payload(payload)
            if not url:
                raise HTTPException(
                    status_code=502,
                    detail="Image generation returned no image URL",
                )
            if url.startswith("data:image"):
                match = DATA_URL_RE.match(url)
                if not match:
                    raise HTTPException(status_code=400, detail="Invalid data URL")
                return {
                    "bytesBase64Encoded": match.group("data"),
                    "mimeType": match.group("mime"),
                }
            img_bytes = await retrieve_image_data(url)
            if img_bytes:
                mime_type = _detect_image_mime_type(
                    img_bytes,
                    fallback=_guess_mime_type(url, "image/png"),
                )
                return {
                    "bytesBase64Encoded": base64.b64encode(img_bytes).decode("ascii"),
                    "mimeType": mime_type,
                }
            return {
                "imageUri": url,
                "mimeType": _guess_mime_type(url, "image/png"),
            }

        predictions = await _gather_tasks_fail_fast(
            [_materialize_prediction(payload) for payload in payloads]
        )

        return JSONResponse(content={"predictions": predictions})

    except HTTPException as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content=_build_gemini_error_payload(exc.status_code, str(exc.detail)),
        )
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content=_build_gemini_error_payload(500, str(exc)),
        )
