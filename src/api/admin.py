"""Admin API routes"""

import asyncio
import hashlib
import importlib
import sys
import json
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends, HTTPException, Header, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing import Optional, List, Dict, Any, Literal
import secrets
import time
import re
import urllib.error
import urllib.request
from urllib.parse import urlparse
from curl_cffi.requests import AsyncSession
from ..core.auth import AuthManager
from ..core.database import Database
from ..core.config import config, normalize_captcha_method
from ..core.models import Token
from ..core.browser_runtime_status import (
    fail_runtime_prepare,
    finish_runtime_prepare,
    get_runtime_status,
    progress_runtime_prepare,
    start_runtime_prepare,
)
from ..core.monitoring import build_public_health_snapshot
from ..services.token_manager import TokenManager
from ..services.proxy_manager import ProxyManager
from ..services.concurrency_manager import ConcurrencyManager

try:
    import httpx
except ImportError:
    httpx = None

router = APIRouter()

# Dependency injection
token_manager: TokenManager = None
proxy_manager: ProxyManager = None
db: Database = None
concurrency_manager: Optional[ConcurrencyManager] = None
captcha_runtime_prepare_tasks: Dict[str, asyncio.Task] = {}

# Store active admin session tokens (in production, use Redis or database)
active_admin_tokens = set()
ADMIN_SESSION_COOKIE_NAME = "admin_session"
# 历史版本曾把「账号邮箱与 Cookie 不匹配」当作失败原因写进 last_st_refresh_result。
# 现在邮箱一律以 Flow 页面为准自动更正，这类旧记录不再回显给前端。
_LEGACY_IDENTITY_ERROR_MARKERS = (
    "登录账号与当前 Token",
    "请同步对应账号的",
)


def _sanitize_refresh_result(text: Any) -> str:
    """旧版本的邮箱不匹配报错直接丢弃，不再回显给前端。"""
    value = str(text or "").strip()
    if not value:
        return ""
    if any(marker in value for marker in _LEGACY_IDENTITY_ERROR_MARKERS):
        return ""
    return value



def _mask_token(token: Optional[str]) -> str:
    if not token:
        return ""
    if len(token) <= 24:
        return token
    return f"{token[:18]}...{token[-8:]}"


def _truncate_text(text: Any, limit: int = 240) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    return f"{value[:limit - 3]}..."


def _extract_error_summary(payload: Any) -> str:
    """从响应体里提取用户可读的错误摘要。"""
    if payload is None:
        return ""

    if isinstance(payload, str):
        raw = payload.strip()
        if not raw:
            return ""
        try:
            return _extract_error_summary(json.loads(raw))
        except Exception:
            return _truncate_text(raw)

    if isinstance(payload, dict):
        for key in ("error_summary", "error_message", "detail", "message"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return _truncate_text(value)

        error_value = payload.get("error")
        if isinstance(error_value, dict):
            for key in ("message", "detail", "reason", "code"):
                value = error_value.get(key)
                if isinstance(value, str) and value.strip():
                    return _truncate_text(value)
        elif isinstance(error_value, str) and error_value.strip():
            return _truncate_text(error_value)

        for nested_key in ("response", "data"):
            nested = payload.get(nested_key)
            if isinstance(nested, (dict, list, str)):
                summary = _extract_error_summary(nested)
                if summary:
                    return summary

        return ""

    if isinstance(payload, list):
        for item in payload:
            summary = _extract_error_summary(item)
            if summary:
                return summary
        return ""

    return _truncate_text(payload)


def _guess_client_hints_from_user_agent(user_agent: str) -> Dict[str, str]:
    """根据 UA 补全常见的 sec-ch-* 头。"""
    ua = (user_agent or "").strip()
    if not ua:
        return {}

    headers: Dict[str, str] = {}
    major_match = re.search(r"(?:Chrome|Chromium|Edg|EdgA|EdgiOS)/(\d+)", ua)
    is_mobile = any(token in ua for token in ("Android", "iPhone", "iPad", "Mobile"))
    headers["sec-ch-ua-mobile"] = "?1" if is_mobile else "?0"

    if "Windows" in ua:
        headers["sec-ch-ua-platform"] = '"Windows"'
    elif "Macintosh" in ua or "Mac OS X" in ua:
        headers["sec-ch-ua-platform"] = '"macOS"'
    elif "Android" in ua:
        headers["sec-ch-ua-platform"] = '"Android"'
    elif "iPhone" in ua or "iPad" in ua:
        headers["sec-ch-ua-platform"] = '"iOS"'
    elif "Linux" in ua:
        headers["sec-ch-ua-platform"] = '"Linux"'

    if major_match:
        major = major_match.group(1)
        if "Edg/" in ua:
            headers["sec-ch-ua"] = (
                f'"Not:A-Brand";v="99", "Microsoft Edge";v="{major}", "Chromium";v="{major}"'
            )
        else:
            headers["sec-ch-ua"] = (
                f'"Not:A-Brand";v="99", "Google Chrome";v="{major}", "Chromium";v="{major}"'
            )

    return headers


def _validate_browser_proxy_url_local(proxy_url: str) -> tuple[bool, Optional[str]]:
    if not proxy_url:
        return True, None
    normalized = proxy_url.strip()
    if not re.match(r"^(http|https|socks5h?|socks5)://", normalized):
        normalized = f"http://{normalized}"
    if not re.match(
        r"^(socks5h?|socks5|http|https)://(?:([^:]+):([^@]+)@)?([^:]+):(\d+)$",
        normalized,
    ):
        return False, "代理格式错误"
    return True, None


def _normalize_runtime_method(method: Optional[str]) -> str:
    normalized = (method or "").strip().lower()
    if normalized not in {"browser", "camoufox", ""}:
        return "camoufox"
    return "browser"


async def _prepare_captcha_runtime(method: str):
    runtime_method = _normalize_runtime_method(method)
    try:
        start_runtime_prepare(
            runtime_method,
            "正在准备 Camoufox + WebDriver BiDi 浏览器环境...",
        )
        from ..services.browser_captcha import BrowserCaptchaService

        service = await BrowserCaptchaService.get_instance(db)
        await service.reload_browser_count()
        await service.warmup_browser_slots()
        finish_runtime_prepare(
            runtime_method,
            "Camoufox 浏览器环境已就绪。",
        )
    except Exception as e:
        fail_runtime_prepare(
            runtime_method, f"浏览器环境准备失败: {type(e).__name__}: {e}"
        )
    finally:
        captcha_runtime_prepare_tasks.pop(runtime_method, None)


def _schedule_captcha_runtime_prepare(method: str) -> bool:
    runtime_method = _normalize_runtime_method(method)
    task = captcha_runtime_prepare_tasks.get(runtime_method)
    if task and not task.done():
        progress_runtime_prepare(
            runtime_method, "浏览器环境准备任务仍在进行中，请稍候..."
        )
        return False
    captcha_runtime_prepare_tasks[runtime_method] = asyncio.create_task(
        _prepare_captcha_runtime(runtime_method)
    )
    return True

def set_dependencies(
    tm: TokenManager,
    pm: ProxyManager,
    database: Database,
    cm: Optional[ConcurrencyManager] = None,
):
    """Set service instances"""
    global token_manager, proxy_manager, db, concurrency_manager
    token_manager = tm
    proxy_manager = pm
    db = database
    concurrency_manager = cm


# ========== Request Models ==========


class LoginRequest(BaseModel):
    username: str
    password: str


class AddTokenRequest(BaseModel):
    st: str = ""
    project_id: Optional[str] = None  # 用户可选输入project_id
    project_name: Optional[str] = None
    remark: Optional[str] = None
    captcha_proxy_url: Optional[str] = None
    extension_route_key: Optional[str] = None
    image_enabled: bool = True
    video_enabled: bool = True
    image_concurrency: int = -1
    video_concurrency: int = -1
    protocol_mode: Literal["protocol"] = "protocol"
    google_cookies: str = Field(min_length=1, max_length=524288)
    # 可选：留空时由 Cookie/会话自动识别，识别失败则使用占位邮箱
    login_account: Optional[str] = Field(default=None, max_length=254)
    login_password: Optional[str] = None
    proxy_url: Optional[str] = None
    auto_refresh_enabled: bool = True
    refresh_interval_minutes: int = Field(default=30, ge=1, le=10080)

    @field_validator("login_account")
    @classmethod
    def validate_login_account(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized:
            return None
        if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", normalized):
            raise ValueError("login_account must be a valid email address")
        return normalized


class UpdateTokenRequest(BaseModel):
    st: str = ""
    project_id: Optional[str] = None  # 用户可选输入project_id
    project_name: Optional[str] = None
    remark: Optional[str] = None
    captcha_proxy_url: Optional[str] = None
    extension_route_key: Optional[str] = None
    image_enabled: Optional[bool] = None
    video_enabled: Optional[bool] = None
    image_concurrency: Optional[int] = None
    video_concurrency: Optional[int] = None
    protocol_mode: Optional[Literal["protocol"]] = None
    google_cookies: Optional[str] = Field(default=None, max_length=524288)
    login_account: Optional[str] = None
    login_password: Optional[str] = None
    proxy_url: Optional[str] = None
    auto_refresh_enabled: Optional[bool] = None
    refresh_interval_minutes: Optional[int] = None

    @field_validator("login_account")
    @classmethod
    def validate_login_account(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", normalized):
            raise ValueError("login_account must be a valid email address")
        return normalized


class ProxyConfigRequest(BaseModel):
    proxy_enabled: bool
    proxy_url: Optional[str] = None
    media_proxy_enabled: Optional[bool] = None
    media_proxy_url: Optional[str] = None


class ProxyTestRequest(BaseModel):
    proxy_url: str
    test_url: Optional[str] = "https://flow.google.com/"
    timeout_seconds: Optional[int] = 15


class CaptchaScoreTestRequest(BaseModel):
    website_url: Optional[str] = "https://flow.google.com/"
    website_key: Optional[str] = "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV"
    action: Optional[str] = "homepage"
    verify_url: Optional[str] = "https://antcpt.com/score_detector/verify.php"
    enterprise: Optional[bool] = False


class GenerationConfigRequest(BaseModel):
    image_timeout: Optional[int] = None
    video_timeout: Optional[int] = None
    max_retries: Optional[int] = None


class CallLogicConfigRequest(BaseModel):
    call_mode: str


class ChangePasswordRequest(BaseModel):
    username: Optional[str] = None
    old_password: str
    new_password: str


class UpdateAPIKeyRequest(BaseModel):
    new_api_key: str


class UpdateDebugConfigRequest(BaseModel):
    enabled: bool


class UpdateAdminConfigRequest(BaseModel):
    error_ban_threshold: int


class ST2ATRequest(BaseModel):
    """Current Flow cookie validation request."""

    st: str = ""
    google_cookies: Optional[str] = None


class ImportTokenItem(BaseModel):
    """Current Flow account import item."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    login_account: Optional[str] = Field(default=None, max_length=254)
    google_cookies: str = Field(min_length=1, max_length=524288)
    remark: Optional[str] = Field(default=None, max_length=512)
    project_id: Optional[str] = Field(default=None, max_length=255)
    project_name: Optional[str] = Field(default=None, max_length=255)
    is_active: bool = True
    captcha_proxy_url: Optional[str] = Field(default=None, max_length=4096)
    extension_route_key: Optional[str] = Field(default=None, max_length=255)
    image_enabled: bool = True
    video_enabled: bool = True
    image_concurrency: int = Field(default=-1, ge=-1)
    video_concurrency: int = Field(default=-1, ge=-1)
    protocol_mode: Literal["protocol"] = "protocol"
    proxy_url: Optional[str] = Field(default=None, max_length=4096)
    auto_refresh_enabled: bool = True
    refresh_interval_minutes: int = Field(default=30, ge=1, le=10080)

    @field_validator("login_account")
    @classmethod
    def validate_login_account(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized:
            return None
        if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", normalized):
            raise ValueError("login_account must be a valid email address")
        return normalized


class ImportTokensRequest(BaseModel):
    """Current Flow account import request."""

    tokens: List[ImportTokenItem] = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def validate_unique_accounts(self):
        identities = [
            item.login_account for item in self.tokens if item.login_account
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("duplicate login_account entries are not allowed")
        return self


class TokenRefreshConfigRequest(BaseModel):
    enabled: Optional[bool] = None
    refresh_interval_minutes: Optional[int] = Field(default=None, ge=1, le=10080)
    # 同步改写全部账号的刷新间隔，避免旧账号继续沿用历史默认值
    apply_to_tokens: bool = False


class PluginTokenUpdateRequest(BaseModel):
    """Current Chrome extension credential payload."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    google_cookies: str = Field(min_length=1, max_length=524288)
    # 可选：留空时按 Cookie 会话定位账号，定位不到则使用占位邮箱
    login_account: Optional[str] = Field(default=None, max_length=254)
    protocol_mode: Literal["protocol"] = "protocol"

    @field_validator("login_account")
    @classmethod
    def validate_login_account(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized:
            return None
        if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", normalized):
            raise ValueError("login_account must be a valid email address")
        return normalized


# ========== Auth Middleware ==========


async def verify_admin_token(request: Request, authorization: str = Header(None)):
    """Verify admin session token (NOT API key)"""
    header_token = ""
    if authorization and authorization.startswith("Bearer "):
        header_token = authorization[7:].strip()

    cookie_token = get_admin_token_from_cookie(request) or ""

    if header_token and header_token in active_admin_tokens:
        return header_token

    if cookie_token and cookie_token in active_admin_tokens:
        return cookie_token

    if header_token or cookie_token:
        raise HTTPException(status_code=401, detail="Invalid or expired admin token")

    raise HTTPException(status_code=401, detail="Missing authorization")


def get_admin_token_from_cookie(request: Request) -> Optional[str]:
    token = str(request.cookies.get(ADMIN_SESSION_COOKIE_NAME) or "").strip()
    return token or None


def is_admin_session_token_valid(token: Optional[str]) -> bool:
    normalized = str(token or "").strip()
    return bool(normalized) and normalized in active_admin_tokens


# ========== Auth Endpoints ==========


@router.post("/api/admin/login")
async def admin_login(request: LoginRequest, response: Response):
    """Admin login - returns session token (NOT API key)"""
    admin_config = await db.get_admin_config()

    if not AuthManager.verify_admin(request.username, request.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Generate independent session token
    session_token = f"admin-{secrets.token_urlsafe(32)}"

    # Store in active tokens
    active_admin_tokens.add(session_token)

    response.set_cookie(
        key=ADMIN_SESSION_COOKIE_NAME,
        value=session_token,
        httponly=True,
        samesite="lax",
        secure=False,
        path="/",
    )

    return {
        "success": True,
        "token": session_token,  # Session token (NOT API key)
        "username": admin_config.username,
    }


@router.post("/api/admin/logout")
async def admin_logout(response: Response, token: str = Depends(verify_admin_token)):
    """Admin logout - invalidate session token"""
    active_admin_tokens.discard(token)
    response.delete_cookie(ADMIN_SESSION_COOKIE_NAME, path="/")
    return {"success": True, "message": "退出登录成功"}


@router.post("/api/admin/change-password")
async def change_password(
    request: ChangePasswordRequest, token: str = Depends(verify_admin_token)
):
    """Change admin password"""
    admin_config = await db.get_admin_config()

    # Verify old password
    if not AuthManager.verify_admin(admin_config.username, request.old_password):
        raise HTTPException(status_code=400, detail="旧密码错误")

    # Update password and username in database
    update_params = {"password": request.new_password}
    if request.username:
        update_params["username"] = request.username

    await db.update_admin_config(**update_params)

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()

    # 🔑 Invalidate all admin session tokens (force re-login for security)
    active_admin_tokens.clear()

    return {"success": True, "message": "密码修改成功,请重新登录"}


# ========== Token Management ==========


@router.get("/api/tokens")
async def get_tokens(token: str = Depends(verify_admin_token)):
    """Get all tokens with statistics"""
    token_rows = await db.get_all_tokens_with_stats()
    to_iso = lambda value: value.isoformat() if hasattr(value, "isoformat") else value
    now = datetime.now(timezone.utc)

    def normalize_dt(value):
        if not value:
            return None
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except Exception:
                return None
        if getattr(value, "tzinfo", None) is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    return [
        {
        "id": row.get("id"),
        "st": row.get("st"),  # Session Token for editing
        "at": row.get("at"),  # Access Token for editing (从ST转换而来)
            "at_expires": to_iso(row.get("at_expires"))
            if row.get("at_expires")
            else None,  # 🆕 AT过期时间
            "at_expired": bool(
                normalize_dt(row.get("at_expires"))
                and normalize_dt(row.get("at_expires")) <= now
            ),
        "at_expiring_within_1h": bool(
            normalize_dt(row.get("at_expires"))
            and normalize_dt(row.get("at_expires")) > now
            and (normalize_dt(row.get("at_expires")) - now).total_seconds() < 3600
        ),
        "token": row.get("at"),  # 兼容前端 token.token 的访问方式
        "email": row.get("email"),
        "name": row.get("name"),
        "remark": row.get("remark"),
        "is_active": bool(row.get("is_active")),
            "created_at": to_iso(row.get("created_at"))
            if row.get("created_at")
            else None,
            "last_used_at": to_iso(row.get("last_used_at"))
            if row.get("last_used_at")
            else None,
        "use_count": row.get("use_count"),
        "credits": row.get("credits"),  # 🆕 余额
        "user_paygate_tier": row.get("user_paygate_tier"),
        "current_project_id": row.get("current_project_id"),  # 🆕 项目ID
        "current_project_name": row.get("current_project_name"),  # 🆕 项目名称
        "captcha_proxy_url": row.get("captcha_proxy_url") or "",
        "extension_route_key": row.get("extension_route_key") or "",
        "protocol_mode": row.get("protocol_mode") or "session",
        "google_cookies": row.get("google_cookies") or "",
        "login_account": row.get("login_account") or "",
        "login_password": row.get("login_password") or "",
        "proxy_url": row.get("proxy_url") or "",
        "auto_refresh_enabled": bool(row.get("auto_refresh_enabled", True)),
        "refresh_interval_minutes": row.get("refresh_interval_minutes") or 30,
            "last_st_refresh_at": to_iso(row.get("last_st_refresh_at"))
            if row.get("last_st_refresh_at")
            else None,
        "last_st_refresh_result": _sanitize_refresh_result(
            row.get("last_st_refresh_result")
        ),
        "image_enabled": bool(row.get("image_enabled")),
        "video_enabled": bool(row.get("video_enabled")),
        "image_concurrency": row.get("image_concurrency"),
        "video_concurrency": row.get("video_concurrency"),
        "image_count": row.get("image_count", 0),
        "video_count": row.get("video_count", 0),
        "error_count": row.get("error_count", 0),
        "today_error_count": row.get("today_error_count", 0),
        "consecutive_error_count": row.get("consecutive_error_count", 0),
            "last_error_at": to_iso(row.get("last_error_at"))
            if row.get("last_error_at")
            else None,
        "ban_reason": row.get("ban_reason"),
        "banned_at": to_iso(row.get("banned_at")) if row.get("banned_at") else None,
        }
        for row in token_rows
    ]  # 直接返回数组,兼容前端


@router.post("/api/tokens")
async def add_token(request: AddTokenRequest, token: str = Depends(verify_admin_token)):
    """Add a new token"""
    try:
        new_token = await token_manager.add_token(
            st=request.st,
            project_id=request.project_id,  # 🆕 支持用户指定project_id
            project_name=request.project_name,
            remark=request.remark,
            captcha_proxy_url=request.captcha_proxy_url.strip()
            if request.captcha_proxy_url is not None
            else None,
            extension_route_key=request.extension_route_key.strip()
            if request.extension_route_key is not None
            else None,
            image_enabled=request.image_enabled,
            video_enabled=request.video_enabled,
            image_concurrency=request.image_concurrency,
            video_concurrency=request.video_concurrency,
            protocol_mode=request.protocol_mode,
            google_cookies=request.google_cookies,
            login_account=request.login_account,
            login_password=request.login_password,
            proxy_url=request.proxy_url,
            auto_refresh_enabled=request.auto_refresh_enabled,
            refresh_interval_minutes=request.refresh_interval_minutes,
        )

        # 热更新并发限制，避免必须重启服务
        if concurrency_manager:
            await concurrency_manager.reset_token(
                new_token.id,
                image_concurrency=new_token.image_concurrency,
                video_concurrency=new_token.video_concurrency,
            )

        return {
            "success": True,
            "message": "Token添加成功",
            "token": {
                "id": new_token.id,
                "email": new_token.email,
                "credits": new_token.credits,
                "project_id": new_token.current_project_id,
                "project_name": new_token.current_project_name,
            },
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"添加Token失败: {str(e)}")


@router.put("/api/tokens/{token_id}")
async def update_token(
    token_id: int, request: UpdateTokenRequest, token: str = Depends(verify_admin_token)
):
    """Validate current Flow cookies and update an account."""
    try:
        existing = await token_manager.get_token(token_id)
        session_value = request.st or (existing.st if existing else "")
        google_cookies = request.google_cookies or (
            existing.google_cookies if existing else ""
        )
        result = await token_manager.flow_client.st_to_at(
            session_value,
            google_cookies=google_cookies,
            token_id=token_id,
        )
        at = result["access_token"]
        expires = result.get("expires")

        # 解析过期时间
        from datetime import datetime

        at_expires = None
        if expires:
            try:
                at_expires = datetime.fromisoformat(expires.replace("Z", "+00:00"))
            except:
                pass

        # 更新token (包含AT、ST、AT过期时间、project_id和project_name)
        await token_manager.update_token(
            token_id=token_id,
            st=session_value,
            at=at,
            at_expires=at_expires,  # 🆕 更新AT过期时间
            email=request.login_account,
            project_id=request.project_id,
            project_name=request.project_name,
            remark=request.remark,
            captcha_proxy_url=request.captcha_proxy_url.strip()
            if request.captcha_proxy_url is not None
            else None,
            extension_route_key=request.extension_route_key.strip()
            if request.extension_route_key is not None
            else None,
            image_enabled=request.image_enabled,
            video_enabled=request.video_enabled,
            image_concurrency=request.image_concurrency,
            video_concurrency=request.video_concurrency,
            protocol_mode=request.protocol_mode,
            google_cookies=request.google_cookies,
            login_account=request.login_account,
            login_password=request.login_password,
            proxy_url=request.proxy_url,
            auto_refresh_enabled=request.auto_refresh_enabled,
            refresh_interval_minutes=request.refresh_interval_minutes,
        )
        if request.google_cookies:
            await token_manager.mark_cookie_validated(
                token_id, request.login_account
            )

        # 热更新并发限制，确保管理台修改立即生效
        if concurrency_manager:
            updated_token = await token_manager.get_token(token_id)
            if updated_token:
                await concurrency_manager.reset_token(
                    token_id,
                    image_concurrency=updated_token.image_concurrency,
                    video_concurrency=updated_token.video_concurrency,
                )

        return {"success": True, "message": "Token更新成功"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/api/tokens/{token_id}")
async def delete_token(token_id: int, token: str = Depends(verify_admin_token)):
    """Delete token"""
    try:
        await token_manager.delete_token(token_id)
        if concurrency_manager:
            await concurrency_manager.remove_token(token_id)
        return {"success": True, "message": "Token删除成功"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/tokens/{token_id}/enable")
async def enable_token(token_id: int, token: str = Depends(verify_admin_token)):
    """Enable token"""
    await token_manager.enable_token(token_id)
    return {"success": True, "message": "Token已启用"}


@router.post("/api/tokens/{token_id}/disable")
async def disable_token(token_id: int, token: str = Depends(verify_admin_token)):
    """Disable token"""
    await token_manager.disable_token(token_id)
    return {"success": True, "message": "Token已禁用"}


@router.post("/api/tokens/{token_id}/refresh-credits")
async def refresh_credits(token_id: int, token: str = Depends(verify_admin_token)):
    """刷新Token余额 🆕"""
    try:
        credits = await token_manager.refresh_credits(token_id)
        return {"success": True, "message": "余额刷新成功", "credits": credits}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"刷新余额失败: {str(e)}")


@router.post("/api/tokens/{token_id}/refresh-at")
async def refresh_at(token_id: int, token: str = Depends(verify_admin_token)):
    """刷新完整 Google/Flow Cookie 并验证当前登录态。"""
    from ..core.logger import debug_logger
    from ..core.config import config
    
    debug_logger.log_info(
        f"[API] 手动刷新 AT 请求: token_id={token_id}, captcha_method={config.captcha_method}"
    )
    
    try:
        # 手动刷新完整 Cookie，不依赖 AT 标识是否到期。
        success = await token_manager.refresh_cookies(token_id)

        if success:
            # 获取更新后的token信息
            updated_token = await token_manager.get_token(token_id)
            
            message = "Cookie 刷新并验证成功"
            
            debug_logger.log_info(f"[API] AT 刷新成功: token_id={token_id}")
            
            return {
                "success": True,
                "message": message,
                "token": {
                    "id": updated_token.id,
                    "email": updated_token.email,
                    "at_expires": updated_token.at_expires.isoformat()
                    if updated_token.at_expires
                    else None,
                },
            }
        else:
            debug_logger.log_error(f"[API] AT 刷新失败: token_id={token_id}")
            
            snapshot = await token_manager.get_token(token_id)
            error_detail = _sanitize_refresh_result(
                snapshot.last_st_refresh_result if snapshot else None
            ) or "Cookie 刷新失败，请重新同步完整的 Google Cookies"
            
            raise HTTPException(status_code=500, detail=error_detail)
    except HTTPException:
        raise
    except Exception as e:
        debug_logger.log_error(f"[API] 刷新AT异常: {str(e)}")
        raise HTTPException(status_code=500, detail=f"刷新AT失败: {str(e)}")


@router.post("/api/tokens/st2at")
async def st_to_at(request: ST2ATRequest, token: str = Depends(verify_admin_token)):
    """Convert Session Token to Access Token (仅转换,不添加到数据库)"""
    try:
        result = await token_manager.flow_client.st_to_at(
            request.st,
            google_cookies=request.google_cookies,
        )
        return {
            "success": True,
            "message": "ST converted to AT successfully",
            "access_token": result["access_token"],
            "email": result.get("user", {}).get("email"),
            "expires": result.get("expires"),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/tokens/import")
async def import_tokens(
    request: ImportTokensRequest, token: str = Depends(verify_admin_token)
):
    """Validate and import current Flow accounts."""
    from datetime import datetime, timezone

    added = 0
    updated = 0
    errors = []
    existing_by_email = {}
    for existing_token in await token_manager.get_all_tokens():
        identity_values = {
            str(existing_token.email or "").strip().lower(),
            str(existing_token.login_account or "").strip().lower(),
        }
        for identity in identity_values:
            if identity and identity not in existing_by_email:
                existing_by_email[identity] = existing_token

    for idx, item in enumerate(request.tokens):
        try:
            google_cookies = item.google_cookies
            email = str(item.login_account or "").strip().lower()
            detected_email = ""
            if not email:
                # 未提供账号时，从 Cookie 读取 Flow 登录邮箱并保存最新登录态。
                identity = await token_manager.detect_account_identity(
                    google_cookies, proxy_url=item.proxy_url
                )
                google_cookies = identity["google_cookies"]
                email = identity["email"].strip().lower()
                detected_email = email
            cookie_hash = hashlib.sha256(google_cookies.encode("utf-8")).hexdigest()
            st = f"frontend-{cookie_hash[:32]}"

            try:
                result = await token_manager.flow_client.st_to_at(
                    st,
                    google_cookies=google_cookies,
                )
                at = result["access_token"]
                expires = result.get("expires")

                at_expires = None
                is_expired = False
                if expires:
                    try:
                        at_expires = datetime.fromisoformat(
                            expires.replace("Z", "+00:00")
                        )
                        now = datetime.now(timezone.utc)
                        is_expired = at_expires <= now
                    except (TypeError, ValueError):
                        pass

                existing = existing_by_email.get(email.lower())

                if existing:
                    await token_manager.update_token(
                        token_id=existing.id,
                        st=st,
                        at=at,
                        at_expires=at_expires,
                        email=email,
                        project_id=item.project_id,
                        project_name=item.project_name,
                        remark=item.remark,
                        captcha_proxy_url=item.captcha_proxy_url.strip()
                        if item.captcha_proxy_url is not None
                        else None,
                        extension_route_key=item.extension_route_key.strip()
                        if item.extension_route_key is not None
                        else None,
                        image_enabled=item.image_enabled,
                        video_enabled=item.video_enabled,
                        image_concurrency=item.image_concurrency,
                        video_concurrency=item.video_concurrency,
                        protocol_mode=item.protocol_mode,
                        google_cookies=google_cookies,
                        login_account=email,
                        proxy_url=item.proxy_url,
                        auto_refresh_enabled=item.auto_refresh_enabled,
                        refresh_interval_minutes=item.refresh_interval_minutes,
                    )
                    should_enable = item.is_active and not is_expired
                    if should_enable:
                        await token_manager.enable_token(existing.id)
                    else:
                        await token_manager.disable_token(existing.id)
                    if concurrency_manager:
                        await concurrency_manager.reset_token(
                            existing.id,
                            image_concurrency=item.image_concurrency,
                            video_concurrency=item.video_concurrency,
                        )
                    await token_manager.mark_cookie_validated(existing.id, email)
                    existing.is_active = should_enable
                    existing.email = email
                    existing.login_account = email
                    existing_by_email[email.lower()] = existing
                    updated += 1
                else:
                    new_token = await token_manager.add_token(
                        st=st,
                        project_id=item.project_id,
                        project_name=item.project_name,
                        remark=item.remark,
                        captcha_proxy_url=item.captcha_proxy_url.strip()
                        if item.captcha_proxy_url is not None
                        else None,
                        extension_route_key=item.extension_route_key.strip()
                        if item.extension_route_key is not None
                        else None,
                        image_enabled=item.image_enabled,
                        video_enabled=item.video_enabled,
                        image_concurrency=item.image_concurrency,
                        video_concurrency=item.video_concurrency,
                        protocol_mode=item.protocol_mode,
                        google_cookies=google_cookies,
                        login_account=email,
                        proxy_url=item.proxy_url,
                        auto_refresh_enabled=item.auto_refresh_enabled,
                        refresh_interval_minutes=item.refresh_interval_minutes,
                    )
                    should_enable = item.is_active and not is_expired
                    if not should_enable:
                        await token_manager.disable_token(new_token.id)
                        new_token.is_active = False
                    if concurrency_manager:
                        await concurrency_manager.reset_token(
                            new_token.id,
                            image_concurrency=item.image_concurrency,
                            video_concurrency=item.video_concurrency,
                        )
                    await token_manager.mark_cookie_validated(new_token.id, email)
                    existing_by_email[email.lower()] = new_token
                    added += 1

            except Exception as e:
                errors.append(f"第{idx + 1}项 ({email or detected_email}): {str(e)}")

        except Exception as e:
            errors.append(f"第{idx + 1}项: {str(e)}")

    return {
        "success": not errors or added + updated > 0,
        "added": added,
        "updated": updated,
        "errors": errors if errors else None,
        "message": f"导入完成: 新增 {added} 个, 更新 {updated} 个"
        + (f", {len(errors)} 个失败" if errors else ""),
    }



# ========== Camoufox Direct Browser Login (AIStudio2API Style) ==========


class BrowserLoginStartRequest(BaseModel):
    mode: Literal["auto", "window", "liveview"] = "auto"
    token_id: Optional[int] = None
    auto_save_token: bool = True
    proxy_url: Optional[str] = None
    captcha_proxy_url: Optional[str] = None
    remark: Optional[str] = None
    project_id: Optional[str] = None
    project_name: Optional[str] = None
    timeout_seconds: int = 300


async def _resolve_browser_login_proxy(
    explicit_proxy: Optional[str], token_id: Optional[int]
) -> Optional[str]:
    if explicit_proxy and explicit_proxy.strip():
        return explicit_proxy.strip()
    if token_id and token_manager:
        tok = await token_manager.get_token(token_id)
        if tok:
            if getattr(tok, "captcha_proxy_url", None):
                return str(tok.captcha_proxy_url).strip()
            if getattr(tok, "proxy_url", None):
                return str(tok.proxy_url).strip()
    if db:
        try:
            cap_cfg = await db.get_captcha_config()
            if cap_cfg and cap_cfg.browser_proxy_enabled and cap_cfg.browser_proxy_url:
                return str(cap_cfg.browser_proxy_url).strip()
        except Exception:
            pass
    if proxy_manager:
        try:
            p_cfg = await proxy_manager.get_proxy_config()
            if p_cfg and p_cfg.enabled and p_cfg.proxy_url:
                return str(p_cfg.proxy_url).strip()
        except Exception:
            pass
    return None


@router.post("/api/tokens/browser-login/start")
async def start_browser_login(
    request: BrowserLoginStartRequest, _admin_token: str = Depends(verify_admin_token)
):
    """启动隔离的 Camoufox 浏览器进行 Google Flow 登录，登录完成后自动导出 Cookie、StorageState 与启动缓存。"""
    from ..services.camoufox_bidi import CamoufoxLoginManager, CamoufoxLoginSession

    effective_proxy = await _resolve_browser_login_proxy(
        request.captcha_proxy_url or request.proxy_url, request.token_id
    )

    async def _on_login_complete(
        sess: CamoufoxLoginSession, exported: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not sess.auto_save_token or not token_manager:
            return {"action": "extracted_only"}

        google_cookies = str(exported.get("google_cookies") or "").strip()
        email = str(exported.get("email") or "").strip().lower()
        detected_project_id = (
            str(sess.project_id or exported.get("project_id") or "").strip() or None
        )

        if not email:
            identity = await token_manager.detect_account_identity(
                google_cookies, proxy_url=request.proxy_url or effective_proxy
            )
            google_cookies = identity["google_cookies"]
            email = str(identity.get("email") or "").strip().lower()

        target_token: Optional[Token] = None
        if sess.token_id:
            target_token = await token_manager.get_token(sess.token_id)
        if target_token is None and email:
            for item in await token_manager.get_all_tokens():
                if email in {
                    str(item.email or "").strip().lower(),
                    str(item.login_account or "").strip().lower(),
                }:
                    target_token = item
                    break

        cookie_hash = hashlib.sha256(google_cookies.encode("utf-8")).hexdigest()
        st_val = f"frontend-{cookie_hash[:32]}"

        if target_token is not None:
            from ..services.camoufox_bidi import persist_account_login_artifacts

            mod = sys.modules.get("src.services.browser_captcha")
            svc = getattr(mod, "BrowserCaptchaService", None) if mod else None
            svc_inst = getattr(svc, "_instance", None) if svc else None
            if svc_inst is not None and hasattr(svc_inst, "release_token_browsers"):
                await svc_inst.release_token_browsers(
                    target_token.id, reason="browser_relogin_updated"
                )
            if sess._temp_account_dir and sess._temp_account_dir.exists():
                persist_account_login_artifacts(
                    sess._temp_account_dir,
                    sess.user_data_root / f"token_{target_token.id}",
                )
            at_val = "flow-frontend-cookie"
            at_expires = datetime.now() + timedelta(minutes=55)
            await token_manager.update_token(
                token_id=target_token.id,
                st=st_val,
                at=at_val,
                at_expires=at_expires,
                email=email or target_token.email,
                project_id=detected_project_id or target_token.current_project_id,
                project_name=sess.project_name or target_token.current_project_name,
                remark=sess.remark if sess.remark is not None else target_token.remark,
                captcha_proxy_url=(
                    request.captcha_proxy_url.strip()
                    if request.captcha_proxy_url is not None
                    else target_token.captcha_proxy_url
                ),
                proxy_url=(
                    request.proxy_url.strip()
                    if request.proxy_url is not None
                    else target_token.proxy_url
                ),
                protocol_mode="protocol",
                google_cookies=google_cookies,
                login_account=email or target_token.login_account,
            )
            if exported.get("credits") is not None:
                try:
                    await token_manager.db.update_token(
                        target_token.id,
                        credits=int(exported["credits"]),
                        user_paygate_tier=exported.get("user_paygate_tier") or None,
                    )
                except Exception:
                    pass
            else:
                try:
                    await token_manager.refresh_credits(target_token.id)
                except Exception:
                    pass
            await token_manager.mark_cookie_validated(
                target_token.id, email or target_token.email
            )
            refreshed = await token_manager.get_token(target_token.id)
            return {
                "action": "updated",
                "token_id": target_token.id,
                "email": (refreshed.email if refreshed else email),
                "credits": (refreshed.credits if refreshed else 0),
                "project_id": (
                    refreshed.current_project_id if refreshed else detected_project_id
                ),
            }

        new_tok = await token_manager.add_token(
            st=st_val,
            project_id=detected_project_id,
            project_name=sess.project_name,
            remark=sess.remark,
            captcha_proxy_url=(
                request.captcha_proxy_url.strip()
                if request.captcha_proxy_url
                else None
            ),
            proxy_url=request.proxy_url.strip() if request.proxy_url else None,
            protocol_mode="protocol",
            google_cookies=google_cookies,
            login_account=email or None,
            preverified_credits=exported.get("credits"),
            preverified_tier=exported.get("user_paygate_tier"),
            skip_remote_verify=True,
        )
        if concurrency_manager:
            await concurrency_manager.reset_token(
                new_tok.id,
                image_concurrency=new_tok.image_concurrency,
                video_concurrency=new_tok.video_concurrency,
            )
        return {
            "action": "added",
            "token_id": new_tok.id,
            "email": new_tok.email,
            "credits": new_tok.credits,
            "project_id": new_tok.current_project_id,
        }

    manager = CamoufoxLoginManager.get_instance()
    session = await manager.start_session(
        mode=request.mode,
        proxy_url=effective_proxy,
        token_id=request.token_id,
        auto_save_token=request.auto_save_token,
        remark=request.remark,
        project_id=request.project_id,
        project_name=request.project_name,
        timeout_seconds=float(request.timeout_seconds or 300),
        on_complete=_on_login_complete,
    )
    return {"success": True, **session.to_dict()}


@router.get("/api/tokens/browser-login/status/{session_id}")
async def get_browser_login_status(
    session_id: str,
    include_frame: bool = True,
    _admin_token: str = Depends(verify_admin_token),
):
    """查询 Camoufox 登录会话状态及最新画面帧。"""
    from ..services.camoufox_bidi import CamoufoxLoginManager

    session = CamoufoxLoginManager.get_instance().get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="登录会话不存在或已过期")
    payload = {"success": True, **session.to_dict()}
    if include_frame and session.live_view.frame_b64:
        payload["frame_b64"] = session.live_view.frame_b64
        payload["frame_meta"] = session.live_view.frame_meta
    return payload


@router.post("/api/tokens/browser-login/input/{session_id}")
async def send_browser_login_input(
    session_id: str,
    request: Dict[str, Any],
    _admin_token: str = Depends(verify_admin_token),
):
    """向 Camoufox 登录页面发送鼠标/键盘/文本/滚轮操作。"""
    from ..services.camoufox_bidi import CamoufoxLoginManager

    session = CamoufoxLoginManager.get_instance().get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="登录会话不存在或已结束")
    session.live_view.enqueue_input(request)
    return {"success": True}


@router.post("/api/tokens/browser-login/cancel/{session_id}")
async def cancel_browser_login(
    session_id: str, _admin_token: str = Depends(verify_admin_token)
):
    """取消并关闭正在进行的 Camoufox 浏览器登录会话。"""
    from ..services.camoufox_bidi import CamoufoxLoginManager

    await CamoufoxLoginManager.get_instance().cancel_session(session_id)
    return {"success": True}


@router.websocket("/api/tokens/browser-login/ws/{session_id}")
async def browser_login_liveview_ws(websocket: WebSocket, session_id: str):
    """WebSocket 实时画面流与键鼠转发 (对齐 AIStudio2API LiveView)。"""
    from ..services.camoufox_bidi import CamoufoxLoginManager

    query_token = (websocket.query_params.get("token") or "").strip()
    cookie_token = (websocket.cookies.get(ADMIN_SESSION_COOKIE_NAME) or "").strip()
    if not (
        is_admin_session_token_valid(query_token)
        or is_admin_session_token_valid(cookie_token)
    ):
        await websocket.close(code=4401)
        return

    session = CamoufoxLoginManager.get_instance().get_session(session_id)
    if session is None:
        await websocket.close(code=4404)
        return

    await websocket.accept()
    last_status = ""
    last_frame_seq = -1
    last_dialog_seq = -1

    async def _receiver() -> None:
        try:
            while True:
                msg = await websocket.receive_json()
                if isinstance(msg, dict):
                    session.live_view.enqueue_input(msg)
        except (WebSocketDisconnect, Exception):
            pass

    recv_task = asyncio.create_task(_receiver())
    try:
        while True:
            if (
                session.status != last_status
                or session.live_view.frame_seq != last_frame_seq
                or session.live_view.dialog_seq != last_dialog_seq
            ):
                if session.status != last_status:
                    last_status = session.status
                    await websocket.send_json(
                        {
                            "type": "status",
                            "status": session.status,
                            "message": session.message,
                            "error": session.error,
                            "result": session.result,
                        }
                    )
                if session.live_view.dialog_seq != last_dialog_seq:
                    last_dialog_seq = session.live_view.dialog_seq
                    await websocket.send_json(
                        {"type": "dialog", "dialog": session.live_view.dialog}
                    )
                if (
                    session.live_view.frame_seq != last_frame_seq
                    and session.live_view.frame_b64
                ):
                    last_frame_seq = session.live_view.frame_seq
                    await websocket.send_json(
                        {
                            "type": "frame",
                            "meta": session.live_view.frame_meta,
                            "image_b64": session.live_view.frame_b64,
                        }
                    )
                if session.status in ("completed", "failed", "cancelled"):
                    break
            session.live_view._wake_event.clear()
            try:
                await asyncio.wait_for(session.live_view._wake_event.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        recv_task.cancel()
        try:
            await websocket.close()
        except Exception:
            pass

# ========== Config Management ==========


@router.get("/api/config/proxy")
async def get_proxy_config(token: str = Depends(verify_admin_token)):
    """Get proxy configuration"""
    config = await proxy_manager.get_proxy_config()
    return {
        "success": True,
        "config": {
            "enabled": config.enabled,
            "proxy_url": config.proxy_url,
            "media_proxy_enabled": config.media_proxy_enabled,
            "media_proxy_url": config.media_proxy_url,
        },
    }


@router.get("/api/proxy/config")
async def get_proxy_config_alias(token: str = Depends(verify_admin_token)):
    """Get proxy configuration (alias for frontend compatibility)"""
    config = await proxy_manager.get_proxy_config()
    return {
        "proxy_enabled": config.enabled,  # Frontend expects proxy_enabled
        "proxy_url": config.proxy_url,
        "media_proxy_enabled": config.media_proxy_enabled,
        "media_proxy_url": config.media_proxy_url,
    }


@router.post("/api/proxy/config")
async def update_proxy_config_alias(
    request: ProxyConfigRequest, token: str = Depends(verify_admin_token)
):
    """Update proxy configuration (alias for frontend compatibility)"""
    try:
        await proxy_manager.update_proxy_config(
            enabled=request.proxy_enabled,
            proxy_url=request.proxy_url,
            media_proxy_enabled=request.media_proxy_enabled,
            media_proxy_url=request.media_proxy_url,
        )
    except ValueError as e:
        return {"success": False, "message": str(e)}
    return {"success": True, "message": "代理配置更新成功"}


@router.post("/api/config/proxy")
async def update_proxy_config(
    request: ProxyConfigRequest, token: str = Depends(verify_admin_token)
):
    """Update proxy configuration"""
    try:
        await proxy_manager.update_proxy_config(
            enabled=request.proxy_enabled,
            proxy_url=request.proxy_url,
            media_proxy_enabled=request.media_proxy_enabled,
            media_proxy_url=request.media_proxy_url,
        )
    except ValueError as e:
        return {"success": False, "message": str(e)}
    return {"success": True, "message": "代理配置更新成功"}


@router.post("/api/proxy/test")
async def test_proxy_connectivity(
    request: ProxyTestRequest, token: str = Depends(verify_admin_token)
):
    """测试代理是否可访问目标站点（默认 https://flow.google.com/）"""
    proxy_input = (request.proxy_url or "").strip()
    test_url = (request.test_url or "https://flow.google.com/").strip()
    timeout_seconds = int(request.timeout_seconds or 15)
    timeout_seconds = max(5, min(timeout_seconds, 60))

    if not proxy_input:
        return {"success": False, "message": "代理地址为空", "test_url": test_url}

    try:
        proxy_url = proxy_manager.normalize_proxy_url(proxy_input)
    except ValueError as e:
        return {"success": False, "message": str(e), "test_url": test_url}

    start_time = time.time()
    try:
        proxies = {"http": proxy_url, "https": proxy_url}
        async with AsyncSession() as session:
            resp = await session.get(
                test_url,
                proxies=proxies,
                timeout=timeout_seconds,
                impersonate="chrome120",
                allow_redirects=True,
                verify=False,
            )

        elapsed_ms = int((time.time() - start_time) * 1000)
        status_code = resp.status_code
        final_url = str(resp.url)
        ok = 200 <= status_code < 400

        return {
            "success": ok,
            "message": "代理可用"
            if ok
            else f"代理可连通，但目标返回状态码 {status_code}",
            "test_url": test_url,
            "final_url": final_url,
            "status_code": status_code,
            "elapsed_ms": elapsed_ms,
        }
    except Exception as e:
        elapsed_ms = int((time.time() - start_time) * 1000)
        return {
            "success": False,
            "message": f"代理测试失败: {str(e)}",
            "test_url": test_url,
            "elapsed_ms": elapsed_ms,
        }


@router.get("/api/config/generation")
async def get_generation_config(token: str = Depends(verify_admin_token)):
    """Get generation timeout configuration"""
    config = await db.get_generation_config()
    return {
        "success": True,
        "config": {
            "image_timeout": config.image_timeout,
            "video_timeout": config.video_timeout,
            "max_retries": config.max_retries,
        },
    }


@router.post("/api/config/generation")
async def update_generation_config(
    request: GenerationConfigRequest, token: str = Depends(verify_admin_token)
):
    """Update generation timeout configuration"""
    await db.update_generation_config(
        image_timeout=request.image_timeout,
        video_timeout=request.video_timeout,
        max_retries=request.max_retries,
    )

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()

    return {"success": True, "message": "生成配置更新成功"}


@router.get("/api/call-logic/config")
async def get_call_logic_config(token: str = Depends(verify_admin_token)):
    """Get token call logic configuration."""
    config_obj = await db.get_call_logic_config()
    call_mode = getattr(config_obj, "call_mode", None)
    if call_mode not in ("default", "polling"):
        call_mode = (
            "polling"
            if getattr(config_obj, "polling_mode_enabled", False)
            else "default"
        )
    return {
        "success": True,
        "config": {
            "call_mode": call_mode,
            "polling_mode_enabled": call_mode == "polling",
        },
    }


@router.post("/api/call-logic/config")
async def update_call_logic_config(
    request: CallLogicConfigRequest, token: str = Depends(verify_admin_token)
):
    """Update token call logic configuration."""
    call_mode = (
        request.call_mode if request.call_mode in ("default", "polling") else None
    )
    if call_mode is None:
        raise HTTPException(status_code=400, detail="Invalid call_mode")

    await db.update_call_logic_config(call_mode)
    await db.reload_config_to_memory()

    return {
        "success": True,
        "message": "Token轮询模式保存成功",
        "config": {
            "call_mode": call_mode,
            "polling_mode_enabled": call_mode == "polling",
        },
    }


# ========== System Info ==========


@router.get("/api/system/info")
async def get_system_info(token: str = Depends(verify_admin_token)):
    """Get system information"""
    stats = await db.get_system_info_stats()

    return {
        "success": True,
        "info": {
            "total_tokens": stats["total_tokens"],
            "active_tokens": stats["active_tokens"],
            "total_credits": stats["total_credits"],
            "version": "1.0.0",
        },
    }


# ========== Additional Routes for Frontend Compatibility ==========


@router.post("/api/login")
async def login(request: LoginRequest, response: Response):
    """Login endpoint (alias for /api/admin/login)"""
    return await admin_login(request, response)


@router.post("/api/logout")
async def logout(response: Response, token: str = Depends(verify_admin_token)):
    """Logout endpoint (alias for /api/admin/logout)"""
    return await admin_logout(response, token)


@router.get("/health")
async def health_check():
    """Public health check endpoint - no auth required"""
    try:
        return await build_public_health_snapshot(db)
    except Exception:
        return {"backend_running": True, "has_active_tokens": False}


@router.get("/api/stats")
async def get_stats(token: str = Depends(verify_admin_token)):
    """Get statistics for dashboard"""
    return await db.get_dashboard_stats()


@router.get("/api/logs")
async def get_logs(limit: int = 100, token: str = Depends(verify_admin_token)):
    """Get lightweight request logs for list view"""
    limit = max(1, min(limit, 100))
    logs = await db.get_logs(limit=limit, include_payload=False)

    result = []
    for log in logs:
        raw_status_code = log.get("status_code")
        try:
            status_code = int(raw_status_code) if raw_status_code is not None else None
        except (TypeError, ValueError):
            status_code = None
        result.append(
            {
            "id": log.get("id"),
            "token_id": log.get("token_id"),
            "token_email": log.get("token_email"),
            "token_username": log.get("token_username"),
            "operation": log.get("operation"),
                "status_code": status_code
                if status_code is not None
                else raw_status_code,
            "duration": log.get("duration"),
            "status_text": log.get("status_text") or "",
            "progress": log.get("progress") or 0,
            "created_at": log.get("created_at"),
            "updated_at": log.get("updated_at"),
                "error_summary": _extract_error_summary(
                    log.get("response_body_excerpt")
                )
                if status_code is not None and status_code >= 400
                else "",
            }
        )
    return result


@router.get("/api/logs/{log_id}")
async def get_log_detail(log_id: int, token: str = Depends(verify_admin_token)):
    """Get single request log detail (payload loaded on demand)"""
    log = await db.get_log_detail(log_id)
    if not log:
        raise HTTPException(status_code=404, detail="日志不存在")

    error_summary = _extract_error_summary(log.get("response_body"))

    return {
        "id": log.get("id"),
        "token_id": log.get("token_id"),
        "token_email": log.get("token_email"),
        "token_username": log.get("token_username"),
        "operation": log.get("operation"),
        "status_code": log.get("status_code"),
        "duration": log.get("duration"),
        "status_text": log.get("status_text") or "",
        "progress": log.get("progress") or 0,
        "created_at": log.get("created_at"),
        "updated_at": log.get("updated_at"),
        "error_summary": error_summary,
        "request_body": log.get("request_body"),
        "response_body": log.get("response_body"),
    }


@router.delete("/api/logs")
async def clear_logs(token: str = Depends(verify_admin_token)):
    """Clear all logs"""
    try:
        await db.clear_all_logs()
        return {"success": True, "message": "所有日志已清空"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/admin/config")
async def get_admin_config(token: str = Depends(verify_admin_token)):
    """Get admin configuration"""
    admin_config = await db.get_admin_config()

    return {
        "admin_username": admin_config.username,
        "api_key": admin_config.api_key,
        "error_ban_threshold": admin_config.error_ban_threshold,
        "debug_enabled": config.debug_enabled,  # Return actual debug status
    }


@router.post("/api/admin/config")
async def update_admin_config(
    request: UpdateAdminConfigRequest, token: str = Depends(verify_admin_token)
):
    """Update admin configuration (error_ban_threshold)"""
    # Update error_ban_threshold in database
    await db.update_admin_config(error_ban_threshold=request.error_ban_threshold)

    return {"success": True, "message": "配置更新成功"}


@router.post("/api/admin/password")
async def update_admin_password(
    request: ChangePasswordRequest, token: str = Depends(verify_admin_token)
):
    """Update admin password"""
    return await change_password(request, token)


@router.post("/api/admin/apikey")
async def update_api_key(
    request: UpdateAPIKeyRequest, token: str = Depends(verify_admin_token)
):
    """Update API key (for external API calls, NOT for admin login)"""
    # Update API key in database
    await db.update_admin_config(api_key=request.new_api_key)

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()

    return {"success": True, "message": "API Key更新成功"}


@router.post("/api/admin/debug")
async def update_debug_config(
    request: UpdateDebugConfigRequest, token: str = Depends(verify_admin_token)
):
    """Update debug configuration"""
    try:
        # Update in-memory config only (not database)
        # This ensures debug mode is automatically disabled on restart
        config.set_debug_enabled(request.enabled)

        status = "enabled" if request.enabled else "disabled"
        return {
            "success": True,
            "message": f"Debug mode {status}",
            "enabled": request.enabled,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to update debug config: {str(e)}"
        )


@router.get("/api/generation/timeout")
async def get_generation_timeout(token: str = Depends(verify_admin_token)):
    """Get generation timeout configuration"""
    return await get_generation_config(token)


@router.post("/api/generation/timeout")
async def update_generation_timeout(
    request: GenerationConfigRequest, token: str = Depends(verify_admin_token)
):
    """Update generation timeout configuration"""
    await db.update_generation_config(
        image_timeout=request.image_timeout,
        video_timeout=request.video_timeout,
        max_retries=request.max_retries,
    )

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()

    return {"success": True, "message": "生成配置更新成功"}


# ========== AT Auto Refresh Config ==========


@router.get("/api/token-refresh/config")
async def get_token_refresh_config(token: str = Depends(verify_admin_token)):
    """Get AT/protocol refresh configuration."""
    refresh_config = await db.get_token_refresh_config()
    return {
        "success": True,
        "config": {
            "at_auto_refresh_enabled": True,
            "protocol_refresh_enabled": refresh_config.enabled,
            "refresh_interval_minutes": refresh_config.refresh_interval_minutes,
        },
    }


@router.post("/api/token-refresh/enabled")
async def update_token_refresh_enabled(
    request: Optional[dict] = None, token: str = Depends(verify_admin_token)
):
    """Update protocol ST refresh enabled; AT refresh remains always enabled."""
    enabled = (request or {}).get("enabled")
    if enabled is not None:
        await db.update_token_refresh_config(enabled=bool(enabled))
    return {"success": True, "message": "刷新配置已更新"}


@router.post("/api/token-refresh/config")
async def update_token_refresh_config(
    request: TokenRefreshConfigRequest, token: str = Depends(verify_admin_token)
):
    refresh_config = await db.update_token_refresh_config(
        enabled=request.enabled,
        refresh_interval_minutes=request.refresh_interval_minutes,
    )
    applied = 0
    if request.apply_to_tokens and request.refresh_interval_minutes is not None:
        applied = await db.update_all_tokens_refresh_interval(
            request.refresh_interval_minutes
        )
    return {
        "success": True,
        "applied_to_tokens": applied,
        "config": {
            "at_auto_refresh_enabled": True,
            "protocol_refresh_enabled": refresh_config.enabled,
            "refresh_interval_minutes": refresh_config.refresh_interval_minutes,
        },
    }


async def _sync_runtime_cache_config():
    from . import routes

    if routes.generation_handler and routes.generation_handler.file_cache:
        file_cache = routes.generation_handler.file_cache
        file_cache.set_timeout(config.cache_timeout)
        await file_cache.refresh_cleanup_task()


# ========== Cache Configuration Endpoints ==========


@router.get("/api/cache/config")
async def get_cache_config(token: str = Depends(verify_admin_token)):
    """Get cache configuration"""
    cache_config = await db.get_cache_config()

    # Calculate effective base URL
    effective_base_url = (
        cache_config.cache_base_url
        if cache_config.cache_base_url
        else f"http://127.0.0.1:8000"
    )

    return {
        "success": True,
        "config": {
            "enabled": cache_config.cache_enabled,
            "timeout": cache_config.cache_timeout,
            "base_url": cache_config.cache_base_url or "",
            "effective_base_url": effective_base_url,
        },
    }


@router.post("/api/cache/enabled")
async def update_cache_enabled(request: dict, token: str = Depends(verify_admin_token)):
    """Update cache enabled status"""
    enabled = request.get("enabled", False)
    await db.update_cache_config(enabled=enabled)

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()
    await _sync_runtime_cache_config()

    return {"success": True, "message": f"缓存已{'启用' if enabled else '禁用'}"}


@router.post("/api/cache/config")
async def update_cache_config_full(
    request: dict, token: str = Depends(verify_admin_token)
):
    """Update complete cache configuration"""
    enabled = request.get("enabled")
    timeout = request.get("timeout")
    base_url = request.get("base_url")

    if timeout is not None:
        try:
            timeout = int(timeout)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="缓存超时时间必须为整数")
        if timeout < 0:
            raise HTTPException(status_code=400, detail="缓存超时时间不能小于 0")

    await db.update_cache_config(enabled=enabled, timeout=timeout, base_url=base_url)

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()
    await _sync_runtime_cache_config()

    return {"success": True, "message": "缓存配置更新成功"}


@router.post("/api/cache/base-url")
async def update_cache_base_url(
    request: dict, token: str = Depends(verify_admin_token)
):
    """Update cache base URL"""
    base_url = request.get("base_url", "")
    await db.update_cache_config(base_url=base_url)

    # 🔥 Hot reload: sync database config to memory
    await db.reload_config_to_memory()
    await _sync_runtime_cache_config()

    return {"success": True, "message": "缓存Base URL更新成功"}


@router.post("/api/captcha/config")
async def update_captcha_config(
    request: dict, token: str = Depends(verify_admin_token)
):
    """Update Camoufox browser configuration"""
    captcha_method = normalize_captcha_method(
        request.get("captcha_method") or "camoufox"
    )
    browser_proxy_enabled = bool(request.get("browser_proxy_enabled", False))
    browser_proxy_url = str(request.get("browser_proxy_url") or "").strip()
    browser_count = request.get("browser_count", 1)
    browser_personal_fresh_restart_every_n_solves = request.get(
        "browser_personal_fresh_restart_every_n_solves", 10
    )

    if browser_proxy_enabled and browser_proxy_url:
        is_valid, error_msg = _validate_browser_proxy_url_local(browser_proxy_url)
        if not is_valid:
            return {"success": False, "message": error_msg}

    try:
        browser_count = max(1, min(20, int(browser_count or 1)))
    except Exception:
        return {"success": False, "message": "浏览器实例数量必须是整数"}
    try:
        browser_personal_fresh_restart_every_n_solves = max(
            0,
            int(
                browser_personal_fresh_restart_every_n_solves
                if browser_personal_fresh_restart_every_n_solves is not None
                else 10
            ),
        )
    except Exception:
        return {"success": False, "message": "重置码数必须是整数，0 表示禁用"}

    await db.update_captcha_config(
        captcha_method=captcha_method,
        browser_proxy_enabled=browser_proxy_enabled,
        browser_proxy_url=browser_proxy_url if browser_proxy_enabled else None,
        browser_count=browser_count,
        browser_personal_fresh_restart_every_n_solves=browser_personal_fresh_restart_every_n_solves,
    )
    await db.reload_config_to_memory()

    runtime_prepare_started = _schedule_captcha_runtime_prepare("browser")
    return {
        "success": True,
        "message": "Camoufox 浏览器配置更新成功",
        "runtime_prepare_started": runtime_prepare_started,
        "runtime_prepare_message": "已开始同步 Camoufox 浏览器槽位配置。",
        "runtime_status_method": "browser",
    }


@router.get("/api/captcha/runtime-status")
async def get_captcha_runtime_status(
    method: str = "browser", token: str = Depends(verify_admin_token)
):
    """Get background browser runtime preparation status."""
    runtime_method = _normalize_runtime_method(method)
    task = captcha_runtime_prepare_tasks.get(runtime_method)
    status = get_runtime_status(runtime_method)
    status["method"] = runtime_method
    status["task_running"] = bool(task and not task.done())
    return status


@router.get("/api/captcha/config")
async def get_captcha_config(token: str = Depends(verify_admin_token)):
    """Get Camoufox browser configuration"""
    captcha_config = await db.get_captcha_config()
    return {
        "captcha_method": "camoufox",
        "browser_proxy_enabled": captcha_config.browser_proxy_enabled,
        "browser_proxy_url": captcha_config.browser_proxy_url or "",
        "browser_count": captcha_config.browser_count,
        "browser_personal_fresh_restart_every_n_solves": captcha_config.browser_personal_fresh_restart_every_n_solves,
    }


@router.get("/api/captcha/browser-stats")
async def get_browser_captcha_stats(token: str = Depends(verify_admin_token)):
    """Inspect existing Camoufox browser slots."""
    module = sys.modules.get("src.services.browser_captcha")
    service = module.BrowserCaptchaService._instance if module else None
    if service is None:
        return {"captcha_method": "camoufox", "running": False, "browsers": []}
    return {
        "captcha_method": "camoufox",
        "running": not service._closed,
        **service.get_stats(),
    }


@router.post("/api/captcha/score-test")
async def test_captcha_score(
    _request: Optional[CaptchaScoreTestRequest] = None,
    _token: str = Depends(verify_admin_token),
):
    """使用 Camoufox 浏览器执行 reCAPTCHA 分数检测。"""
    req = _request or CaptchaScoreTestRequest()
    website_url = (req.website_url or "https://antcpt.com/score_detector/").strip()
    website_key = (
        req.website_key or "6LcR_okUAAAAAPYrPe-HK_0RULO1aZM15ENyM-Mf"
    ).strip()
    action = (req.action or "homepage").strip()
    verify_url = (
        req.verify_url or "https://antcpt.com/score_detector/verify.php"
    ).strip()
    enterprise = bool(req.enterprise)
    started_at = time.time()
    captcha_config = await db.get_captcha_config()
    browser_proxy_enabled = bool(captcha_config.browser_proxy_enabled)
    browser_proxy_url = captcha_config.browser_proxy_url or ""

    try:
        from ..services.browser_captcha import BrowserCaptchaService

        service = await BrowserCaptchaService.get_instance(db)
        score_payload, browser_id = await service.get_custom_score(
            website_url=website_url,
            website_key=website_key,
            verify_url=verify_url,
            action=action,
            enterprise=enterprise,
        )
        token_value = score_payload.get("token") if isinstance(score_payload, dict) else None
        verify_result = (
            score_payload.get("verify_result")
            if isinstance(score_payload, dict)
            and isinstance(score_payload.get("verify_result"), dict)
            else {}
        )
        fingerprint = await service.get_fingerprint(browser_id) if token_value else None
        verify_success = bool(verify_result.get("success"))
        return {
            "success": verify_success,
            "message": "分数校验成功" if verify_success else "分数校验未通过",
            "captcha_method": "camoufox",
            "website_url": website_url,
            "website_key": website_key,
            "action": action,
            "verify_url": verify_url,
            "enterprise": enterprise,
            "token_acquired": bool(token_value),
            "token_preview": _mask_token(token_value),
            "token_elapsed_ms": int((score_payload or {}).get("token_elapsed_ms") or 0),
            "verify_elapsed_ms": int((score_payload or {}).get("verify_elapsed_ms") or 0),
            "verify_http_status": (score_payload or {}).get("verify_http_status"),
            "score": verify_result.get("score"),
            "verify_result": verify_result,
            "browser_proxy_enabled": browser_proxy_enabled,
            "browser_proxy_url": browser_proxy_url if browser_proxy_enabled else "",
            "fingerprint": fingerprint,
            "elapsed_ms": int((time.time() - started_at) * 1000),
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"分数测试失败: {str(e)}",
            "captcha_method": "camoufox",
            "website_url": website_url,
            "website_key": website_key,
            "action": action,
            "verify_url": verify_url,
            "enterprise": enterprise,
            "token_acquired": False,
            "elapsed_ms": int((time.time() - started_at) * 1000),
        }

# ========== Plugin Configuration Endpoints ==========


async def _verify_plugin_connection_token(authorization: Optional[str]) -> None:
    plugin_config = await db.get_plugin_config()
    provided_token = ""
    normalized_authorization = str(authorization or "").strip()
    if normalized_authorization:
        scheme, separator, credentials = normalized_authorization.partition(" ")
        provided_token = (
            credentials.strip()
            if separator and scheme.lower() == "bearer"
            else normalized_authorization
        )
    expected_token = str(plugin_config.connection_token or "")
    if (
        not expected_token
        or not provided_token
        or not secrets.compare_digest(provided_token, expected_token)
    ):
        raise HTTPException(status_code=401, detail="Invalid connection token")


@router.get("/api/plugin/config")
async def get_plugin_config(request: Request, token: str = Depends(verify_admin_token)):
    """Get plugin configuration"""
    plugin_config = await db.get_plugin_config()

    # Get the actual domain and port from the request
    # This allows the connection URL to reflect the user's actual access path
    host_header = (
        request.headers.get("x-forwarded-host", "").split(",", 1)[0].strip()
        or request.headers.get("host", "").strip()
    )
    forwarded_scheme = (
        request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip().lower()
    )
    scheme = (
        forwarded_scheme
        if forwarded_scheme in {"http", "https"}
        else request.url.scheme
    )

    # Generate connection URL based on actual request
    if host_header:
        # Use the actual domain/IP and port from the request
        connection_url = f"{scheme}://{host_header}/api/plugin/update-token"
    else:
        # Fallback to config-based URL
        from ..core.config import config

        server_host = config.server_host
        server_port = config.server_port

        if server_host == "0.0.0.0":
            connection_url = f"http://127.0.0.1:{server_port}/api/plugin/update-token"
        else:
            connection_url = (
                f"http://{server_host}:{server_port}/api/plugin/update-token"
            )

    return {
        "success": True,
        "config": {
            "connection_token": plugin_config.connection_token,
            "connection_url": connection_url,
            "auto_enable_on_update": plugin_config.auto_enable_on_update,
        },
    }


@router.post("/api/plugin/config")
async def update_plugin_config(request: dict, token: str = Depends(verify_admin_token)):
    """Update plugin configuration"""
    connection_token = request.get("connection_token", "")
    auto_enable_on_update = request.get("auto_enable_on_update", True)  # 默认开启

    # Generate random token if empty
    if not connection_token:
        connection_token = secrets.token_urlsafe(32)

    await db.update_plugin_config(
        connection_token=connection_token, auto_enable_on_update=auto_enable_on_update
    )

    return {
        "success": True,
        "message": "插件配置更新成功",
        "connection_token": connection_token,
        "auto_enable_on_update": auto_enable_on_update,
    }


@router.post("/api/plugin/update-token")
async def plugin_update_token(
    request: PluginTokenUpdateRequest,
    authorization: Optional[str] = Header(None),
):
    """Validate and store credentials pushed by Token Updater 1.3+."""
    await _verify_plugin_connection_token(authorization)
    plugin_config = await db.get_plugin_config()

    google_cookies = request.google_cookies
    email = (request.login_account or "").strip().lower()

    try:
        result = await token_manager.flow_client.st_to_at(
            "",
            google_cookies=google_cookies,
        )
        google_cookies = result.get("google_cookies") or google_cookies
        at = result["access_token"]
        expires = result.get("expires")
        at_expires = None
        if expires:
            try:
                at_expires = datetime.fromisoformat(expires.replace("Z", "+00:00"))
            except (TypeError, ValueError):
                pass
    except Exception as cookie_error:
        raise HTTPException(
            status_code=400,
            detail=f"Google Cookie 验证失败：{str(cookie_error)}",
        ) from cookie_error

    cookie_hash = hashlib.sha256(google_cookies.encode("utf-8")).hexdigest()[:32]
    session_token = f"frontend-{cookie_hash}"

    # Step 2: 定位已有账号。邮箱可选：先用邮箱，未提供则按 Cookie 会话(st)反查，
    # 都没有则生成与 add_token 一致的占位邮箱，避免“必须填 Google 账号”的硬性要求。
    existing_token = await db.get_token_by_email(email) if email else None
    if existing_token is None:
        existing_token = await db.get_token_by_st(session_token)
    if existing_token is not None and not email:
        email = str(existing_token.email or existing_token.login_account or "")
    if not email:
        email = f"account-{cookie_hash[:16]}@flow.local"

    if existing_token:
        # Update existing token
        try:
            # Update token
            await token_manager.update_token(
                token_id=existing_token.id,
                st=session_token,
                at=at,
                at_expires=at_expires,
                email=email,
                protocol_mode=request.protocol_mode,
                google_cookies=google_cookies,
                login_account=email,
                auto_enable_on_update=plugin_config.auto_enable_on_update,
            )
            await token_manager.mark_cookie_validated(existing_token.id, email)

            # Check if auto-enable is enabled and token is disabled
            if plugin_config.auto_enable_on_update and not existing_token.is_active:
                await token_manager.enable_token(existing_token.id)
                return {
                    "success": True,
                    "message": f"Token updated and auto-enabled for {email}",
                    "action": "updated",
                    "auto_enabled": True,
                }

            return {
                "success": True,
                "message": f"Token updated for {email}",
                "action": "updated",
            }
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to update token: {str(e)}"
            )
    else:
        # Add new token
        try:
            new_token = await token_manager.add_token(
                st=session_token,
                remark="Added by Chrome Extension",
                protocol_mode=request.protocol_mode,
                google_cookies=google_cookies,
                login_account=email,
                auto_refresh_enabled=True,
                refresh_interval_minutes=30,
            )

            return {
                "success": True,
                "message": f"Token added for {new_token.email}",
                "action": "added",
                "token_id": new_token.id,
            }
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to add token: {str(e)}"
            )


@router.post("/api/plugin/check-tokens")
async def plugin_check_tokens(
    request: Optional[dict] = None, authorization: Optional[str] = Header(None)
):
    """Return token status for external syncers using the plugin connection token."""
    await _verify_plugin_connection_token(authorization)

    request = request or {}
    requested_emails = request.get("emails") if isinstance(request, dict) else None
    email_filter = set()
    if isinstance(requested_emails, list):
        email_filter = {
            str(email or "").strip().lower()
            for email in requested_emails
            if str(email or "").strip()
        }

    rows = await db.get_all_tokens_with_stats()
    tokens = []
    for row in rows:
        email = str(row.get("email") or "").strip()
        if email_filter and email.lower() not in email_filter:
            continue
        token_obj = None
        try:
            token_obj = Token(**row)
        except Exception:
            token_obj = None
        needs_refresh = token_manager.needs_at_refresh(token_obj) if token_obj else True
        tokens.append(
            {
            "id": row.get("id"),
            "email": email,
            "is_active": bool(row.get("is_active")),
            "needs_refresh": needs_refresh,
                "at_expires": row.get("at_expires").isoformat()
                if hasattr(row.get("at_expires"), "isoformat")
                else row.get("at_expires"),
                "last_used_at": row.get("last_used_at").isoformat()
                if hasattr(row.get("last_used_at"), "isoformat")
                else row.get("last_used_at"),
            "protocol_mode": row.get("protocol_mode") or "session",
            "auto_refresh_enabled": bool(row.get("auto_refresh_enabled", True)),
            "refresh_interval_minutes": row.get("refresh_interval_minutes") or 30,
            "last_st_refresh_at": (
                row.get("last_st_refresh_at").isoformat()
                if hasattr(row.get("last_st_refresh_at"), "isoformat")
                else row.get("last_st_refresh_at")
            ),
            "last_st_refresh_result": _sanitize_refresh_result(
                row.get("last_st_refresh_result")
            ),
            "credits": row.get("credits", 0),
            }
        )

    return {"success": True, "tokens": tokens}
