"""Structured Flow upstream error classification and retry/failover policy."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import re
from typing import Optional
from .redaction import redact_sensitive_text


class FlowErrorCategory(str, Enum):
    AUTH_EXPIRED = "auth_expired"
    WAF_RISK = "waf_risk"
    TIER_PERMISSION = "tier_permission"
    CONTENT_POLICY = "content_policy"
    RATE_LIMIT = "rate_limit"
    INVALID_INPUT = "invalid_input"
    NOT_FOUND = "not_found"
    TRANSIENT_UPSTREAM = "transient_upstream"
    UNKNOWN = "unknown"


CONTENT_SAFETY_MARKERS: tuple[str, ...] = (
    "public_error_unsafe_generation",
    "public_error_prominent_people_filter_failed",
    "prominent_people_filter_failed",
    "unsafe_generation",
    "content_policy",
    "safety_filter",
    "minors",
    "celebrity",
    "sexual",
    "violence",
    "hate_speech",
    "ip_infringement",
)

TIER_PERMISSION_MARKERS: tuple[str, ...] = (
    "public_error_model_access_denied",
    "model_access_denied",
    "requires a flow ultra",
    "需要 pro",
    "需要 ult",
    "permission_denied",
    "tier",
)

AUTH_EXPIRED_MARKERS: tuple[str, ...] = (
    "登录态已失效",
    "登录已失效",
    "identity_recheck_required",
    "code=401",
    "http error 401",
    "unauthenticated",
    "认证失败",
    "session is invalid",
    "access_token_refresh_needed",
)
WAF_RISK_MARKERS: tuple[str, ...] = (
    "public_error_unusual_activity",
    "recaptcha evaluation failed",
    "failed to obtain recaptcha token",
    "recaptcha 验证失败",
    "recaptcha 错误",
    "unusual_activity",
)

RATE_LIMIT_MARKERS: tuple[str, ...] = (
    "429",
    "too many requests",
    "too much traffic",
    "resource_exhausted",
    "daily limit",
    "daily generation quota",
    "quota reached",
    "quota exceeded",
)

NON_TOKEN_INPUT_MARKERS: tuple[str, ...] = (
    "首尾帧模型需要",
    "最多支持",
    "当前提供了",
    "unsupported flow",
    "invalid_argument",
    "客户端连接已断开",
    "browser captcha queue wait timeout",
)

_DAILY_QUOTA_MODEL_RE = re.compile(
    r"daily\s+(?:limit|quota)\s+(?:for|of)\s+([A-Za-z0-9 ._-]+?)(?=\s+(?:reached|exceeded|hit)\b|[,;:(]|$)",
    re.IGNORECASE,
)
_RETRY_AFTER_SEC_RE = re.compile(
    r"(?:retry[-_\s]?after|retryAfter|try\s+again\s+in)\s*[:=]?\s*(\d+(?:\.\d+)?)\s*(ms|s|sec|secs|seconds|m|min|mins|minutes)?\b",
    re.IGNORECASE,
)
_QUOTA_RESET_TS_RE = re.compile(
    r"quotaResetTimeStamp[\"'\s:=]+([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2}))",
    re.IGNORECASE,
)

TRANSIENT_UPSTREAM_MARKERS: tuple[str, ...] = (
    "http error 500",
    "http error 502",
    "http error 503",
    "http error 504",
    "internal error",
    "reason=internal",
    "reason: internal",
    '"reason":"internal"',
    "server error",
    "upstream error",
    "timeout",
    "timed out",
    "connection reset",
    "connection aborted",
    "connection refused",
    "temporarily unavailable",
)



@dataclass(frozen=True, slots=True)
class ClassifiedFlowError:
    category: FlowErrorCategory
    http_status: int
    retryable_same_token: bool
    failover_other_token: bool
    counts_toward_ban: bool
    cooldown_seconds: float
    remediation_hint: str
    quota_model: Optional[str] = None
    sanitized_detail: str = ""
    charged: str = "no"  # "no" | "maybe" | "yes"
    next_action: str = "none"  # "retry" | "retry_after" | "wait" | "reauth" | "fix_input" | "reconcile" | "none"
    retry_after_seconds: Optional[float] = None


def extract_quota_model_name(message: str) -> Optional[str]:
    match = _DAILY_QUOTA_MODEL_RE.search(message or "")
    if not match:
        return None
    name = match.group(1).strip(" .,:;\"'")
    return name or None


def extract_retry_after_seconds(message: str) -> Optional[float]:
    """Parse Retry-After durations or Google quotaResetTimeStamp into bounded cooldown seconds."""
    text = str(message or "")
    if not text:
        return None
    ts_match = _QUOTA_RESET_TS_RE.search(text)
    if ts_match:
        raw_ts = ts_match.group(1)
        try:
            if raw_ts.endswith("Z"):
                raw_ts = raw_ts[:-1] + "+00:00"
            reset_dt = datetime.fromisoformat(raw_ts)
            delta = (reset_dt - datetime.now(timezone.utc)).total_seconds()
            if delta > 0:
                return float(max(5.0, min(3600.0, delta)))
        except Exception:
            pass
    sec_match = _RETRY_AFTER_SEC_RE.search(text)
    if sec_match:
        try:
            value = float(sec_match.group(1))
            unit = (sec_match.group(2) or "s").lower()
            if unit == "ms":
                value /= 1000.0
            elif unit.startswith("m"):
                value *= 60.0
            if value > 0:
                return float(max(5.0, min(3600.0, value)))
        except Exception:
            pass
    return None


def resolve_cooldown_bucket(
    model: Optional[str] = None,
    *,
    for_image: bool = False,
    for_video: bool = False,
    quota_model: Optional[str] = None,
) -> Optional[str]:
    """Normalize a model/operation into a coarse cooldown family bucket (e.g. image:GEM_PIX_2, video)."""
    raw = str(quota_model or model or "").strip().lower()
    if raw:
        if any(k in raw for k in ("gem_pix", "nano-banana-pro", "nano banana pro", "gemini-3.0-pro-image")):
            return "image:GEM_PIX_2"
        if any(k in raw for k in ("harbor_seal", "nano-banana-2-lite", "flash-lite-image")):
            return "image:HARBOR_SEAL"
        if any(k in raw for k in ("beluga", "narwhal", "nano-banana-2", "gemini-3.1-flash-image")):
            return "image:BELUGA"
        if any(k in raw for k in ("veo", "omni", "macaron", "video")):
            return "video"
        if for_image:
            return f"image:{raw}"
    if for_video:
        return "video"
    if for_image:
        return "image"
    return None
def classify_flow_error(
    error: Exception | str,
    *,
    target_resolution: Optional[str] = None,
    operation: Optional[str] = None,
    post_submit: bool = False,
    billed_operation: bool = False,
) -> ClassifiedFlowError:
    """Classify an upstream Flow / Camoufox failure without inflating WAF, bans, or double-billing."""
    raw_msg = str(error or "").strip()
    lower = raw_msg.lower()
    sanitized = redact_sensitive_text(raw_msg)
    retry_after = extract_retry_after_seconds(raw_msg)

    if any(m in lower or m in raw_msg for m in AUTH_EXPIRED_MARKERS):
        is_recheck = "identity_recheck_required" in lower
        hint = (
            "Google 要求对该账号进行二次身份核验（持续重定向 /about），请在后台点击「重登」并完成“验证是您本人”。"
            if is_recheck
            else "账号 Google 会话或 Cookie 可能暂时失效，已触发后台恢复或请重新登录后重试。"
        )
        return ClassifiedFlowError(
            category=FlowErrorCategory.AUTH_EXPIRED,
            http_status=401,
            retryable_same_token=False,
            failover_other_token=not (post_submit and billed_operation),
            counts_toward_ban=False,
            cooldown_seconds=300.0 if is_recheck else 120.0,
            remediation_hint=hint,
            sanitized_detail=sanitized,
            charged="maybe" if (post_submit and billed_operation) else "no",
            next_action="reconcile" if (post_submit and billed_operation) else "reauth",
        )

    if any(m in lower for m in CONTENT_SAFETY_MARKERS):
        return ClassifiedFlowError(
            category=FlowErrorCategory.CONTENT_POLICY,
            http_status=400,
            retryable_same_token=False,
            failover_other_token=False,
            counts_toward_ban=False,
            cooldown_seconds=0.0,
            remediation_hint="提示词或参考素材触发内容安全/人物过滤，请修改描述或减少真人参考图后重试。",
            sanitized_detail=sanitized,
            charged="no",
            next_action="fix_input",
        )

    is_403 = "403" in lower or "permission_denied" in lower
    is_4k_upsample = (
        str(operation or "").lower() == "upsample"
        and "4k" in str(target_resolution or "").lower()
    )
    if any(m in lower for m in TIER_PERMISSION_MARKERS) or (is_403 and is_4k_upsample):
        return ClassifiedFlowError(
            category=FlowErrorCategory.TIER_PERMISSION,
            http_status=403,
            retryable_same_token=False,
            failover_other_token=not (post_submit and billed_operation),
            counts_toward_ban=False,
            cooldown_seconds=0.0,
            remediation_hint="当前账号订阅等级或模型权限不足（如 4K 放大需更高订阅），已跳过同账号重试。",
            sanitized_detail=sanitized,
            charged="no",
            next_action="fix_input",
        )

    if any(m in lower for m in WAF_RISK_MARKERS):
        return ClassifiedFlowError(
            category=FlowErrorCategory.WAF_RISK,
            http_status=403,
            retryable_same_token=False,
            failover_other_token=not (post_submit and billed_operation),
            counts_toward_ban=False,
            cooldown_seconds=retry_after or 180.0,
            remediation_hint="命中 reCAPTCHA/风控校验，已冷却当前账号并重置打码会话。",
            sanitized_detail=sanitized,
            charged="maybe" if (post_submit and billed_operation) else "no",
            next_action="reconcile" if (post_submit and billed_operation) else "retry",
            retry_after_seconds=retry_after,
        )

    if any(m in lower for m in RATE_LIMIT_MARKERS):
        quota_model = extract_quota_model_name(raw_msg)
        cooldown = retry_after or (
            300.0 if quota_model or "daily" in lower or "quota" in lower else 180.0
        )
        hint = (
            f"模型 {quota_model} 达到每日配额上限，请切换其他模型或账号。"
            if quota_model
            else "上游限流或流量拥堵，建议稍后重试或切换其他账号。"
        )
        return ClassifiedFlowError(
            category=FlowErrorCategory.RATE_LIMIT,
            http_status=429,
            retryable_same_token=False,
            failover_other_token=not (post_submit and billed_operation),
            counts_toward_ban=False,
            cooldown_seconds=cooldown,
            remediation_hint=hint,
            quota_model=quota_model,
            sanitized_detail=sanitized,
            charged="no",
            next_action="retry_after",
            retry_after_seconds=retry_after or cooldown,
        )

    if any(m in lower or m in raw_msg for m in NON_TOKEN_INPUT_MARKERS):
        return ClassifiedFlowError(
            category=FlowErrorCategory.INVALID_INPUT,
            http_status=400,
            retryable_same_token=False,
            failover_other_token=False,
            counts_toward_ban=False,
            cooldown_seconds=0.0,
            remediation_hint="请求参数或素材数量不符合模型要求，请修正参数。",
            sanitized_detail=sanitized,
            charged="no",
            next_action="fix_input",
        )

    if "not_found" in lower or re.search(r"code\s*=\s*\[?5\]?", lower):
        return ClassifiedFlowError(
            category=FlowErrorCategory.NOT_FOUND,
            http_status=404,
            retryable_same_token=False,
            failover_other_token=False,
            counts_toward_ban=False,
            cooldown_seconds=0.0,
            remediation_hint="目标项目、资产或模型标识不存在。",
            sanitized_detail=sanitized,
            charged="no",
            next_action="fix_input",
        )

    if is_403:
        return ClassifiedFlowError(
            category=FlowErrorCategory.WAF_RISK,
            http_status=403,
            retryable_same_token=False,
            failover_other_token=not (post_submit and billed_operation),
            counts_toward_ban=False,
            cooldown_seconds=retry_after or 180.0,
            remediation_hint="上游返回 HTTP 403，已冷却当前账号避免继续抬高风险分。",
            sanitized_detail=sanitized,
            charged="maybe" if (post_submit and billed_operation) else "no",
            next_action="reconcile" if (post_submit and billed_operation) else "retry",
            retry_after_seconds=retry_after,
        )

    if any(m in lower for m in TRANSIENT_UPSTREAM_MARKERS):
        if post_submit and billed_operation:
            return ClassifiedFlowError(
                category=FlowErrorCategory.TRANSIENT_UPSTREAM,
                http_status=502,
                retryable_same_token=False,
                failover_other_token=False,
                counts_toward_ban=False,
                cooldown_seconds=0.0,
                remediation_hint="付费生成请求已提交但等待响应超时或连接中断；为避免重复扣费已停止自动重试，请先在项目历史中核对是否已生成。",
                sanitized_detail=sanitized,
                charged="maybe",
                next_action="reconcile",
            )
        return ClassifiedFlowError(
            category=FlowErrorCategory.TRANSIENT_UPSTREAM,
            http_status=502,
            retryable_same_token=True,
            failover_other_token=True,
            counts_toward_ban=True,
            cooldown_seconds=0.0,
            remediation_hint="上游服务短暂波动或超时，可稍后重试。",
            sanitized_detail=sanitized,
            charged="no",
            next_action="retry",
        )

    return ClassifiedFlowError(
        category=FlowErrorCategory.UNKNOWN,
        http_status=500,
        retryable_same_token=False,
        failover_other_token=False,
        counts_toward_ban=not (post_submit and billed_operation),
        cooldown_seconds=0.0,
        remediation_hint=(
            "付费提交后出现未分类异常，请先核对项目资产再决定是否重试。"
            if (post_submit and billed_operation)
            else "请检查请求日志与后台账号状态。"
        ),
        sanitized_detail=sanitized,
        charged="maybe" if (post_submit and billed_operation) else "no",
        next_action="reconcile" if (post_submit and billed_operation) else "none",
    )
