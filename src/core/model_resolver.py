"""Model name resolver - converts simplified model names + generationConfig params to internal MODEL_CONFIG keys.

When upstream services (e.g. New API) send requests with a generic model name
along with generationConfig containing aspectRatio / imageSize, this module
resolves them to the specific internal model name used by flow2api.

Example:
    model = "gemini-3.0-pro-image"
    generationConfig.imageConfig.aspectRatio = "16:9"
    generationConfig.imageConfig.imageSize = "2k"
    → resolved to "gemini-3.0-pro-image-landscape-2k"
"""

import re
from io import BytesIO
from typing import Optional, Dict, Any, Tuple
from ..core.logger import debug_logger

# ──────────────────────────────────────────────
# 简化模型名 → 基础模型名前缀 的映射
# ──────────────────────────────────────────────
IMAGE_BASE_MODELS = {
    # Gemini 3.0 Pro (GEM_PIX_2 / Nano Banana Pro)
    "gemini-3.0-pro-image": "gemini-3.0-pro-image",
    "gemini-3.0-pro-image-preview": "gemini-3.0-pro-image",
    "gemini-3-pro-image": "gemini-3.0-pro-image",
    "gemini-3-pro-image-preview": "gemini-3.0-pro-image",
    "nano-banana-pro": "gemini-3.0-pro-image",
    # Nano Banana 2.1 (BELUGA / Gemini 3.1 Flash Image)
    "gemini-3.1-flash-image": "gemini-3.1-flash-image",
    "gemini-3.1-flash-image-preview": "gemini-3.1-flash-image",
    "nano-banana-2.1": "gemini-3.1-flash-image",
    "nano-banana-2": "gemini-3.1-flash-image",
    # Nano Banana 2 Lite (HARBOR_SEAL / Gemini 3.1 Flash Lite Image)
    "gemini-3.1-flash-lite-image": "gemini-3.1-flash-lite-image",
    "gemini-3.1-flash-lite-image-preview": "gemini-3.1-flash-lite-image",
    "nano-banana-2-lite": "gemini-3.1-flash-lite-image",
}

# ──────────────────────────────────────────────
# aspectRatio 转换映射
# 支持 Gemini / AI Studio 标准格式 ("16:9")、Flow 枚举与英文名 ("landscape")
# ──────────────────────────────────────────────
ASPECT_RATIO_MAP = {
    # Gemini / AI Studio 标准 ratio 格式
    "16:9": "landscape",
    "9:16": "portrait",
    "1:1": "square",
    "4:3": "four-three",
    "3:4": "three-four",
    # Flow 原生枚举名
    "IMAGE_ASPECT_RATIO_LANDSCAPE": "landscape",
    "IMAGE_ASPECT_RATIO_16_9": "landscape",
    "IMAGE_ASPECT_RATIO_PORTRAIT": "portrait",
    "IMAGE_ASPECT_RATIO_SQUARE": "square",
    "IMAGE_ASPECT_RATIO_LANDSCAPE_FOUR_THREE": "four-three",
    "IMAGE_ASPECT_RATIO_PORTRAIT_THREE_FOUR": "three-four",
    "VIDEO_ASPECT_RATIO_LANDSCAPE": "landscape",
    "VIDEO_ASPECT_RATIO_PORTRAIT": "portrait",
    # 英文名直接映射
    "landscape": "landscape",
    "portrait": "portrait",
    "square": "square",
    "four-three": "four-three",
    "three-four": "three-four",
    "four_three": "four-three",
    "three_four": "three-four",
    # 大写形式
    "LANDSCAPE": "landscape",
    "PORTRAIT": "portrait",
    "SQUARE": "square",
}

# 每个基础模型支持的 aspectRatio 列表
# 如果请求的 ratio 不在支持列表中，降级到默认值
MODEL_SUPPORTED_ASPECTS = {
    "gemini-3.0-pro-image": [
        "landscape",
        "portrait",
        "square",
        "four-three",
        "three-four",
    ],
    "gemini-3.1-flash-image": [
        "landscape",
        "portrait",
        "square",
        "four-three",
        "three-four",
    ],
    "gemini-3.1-flash-lite-image": [
        "landscape",
        "portrait",
        "square",
        "four-three",
        "three-four",
    ],
}

# 每个基础模型支持的 imageSize（分辨率）列表
MODEL_SUPPORTED_SIZES = {
    "gemini-3.0-pro-image": ["1k", "2k", "4k"],
    "gemini-3.1-flash-image": ["1k", "2k", "4k"],
    "gemini-3.1-flash-lite-image": ["1k", "2k", "4k"],
}

# imageSize 归一化映射
IMAGE_SIZE_MAP = {
    "360p": "360p",
    "360P": "360p",
    "512px": "1k",
    "512PX": "1k",
    "720p": "720p",
    "720P": "720p",
    "1k": "1k",
    "1K": "1k",
    "standard": "1k",
    "STANDARD": "1k",
    "2k": "2k",
    "2K": "2k",
    "4k": "4k",
    "4K": "4k",
    "1080p": "1080p",
    "1080P": "1080p",
    "": "",
}

# 默认图片参数：16:9 横屏 + 2K 分辨率（与标准 AI Studio 参数风格一致）
DEFAULT_ASPECT = "landscape"
DEFAULT_IMAGE_SIZE = "2k"

OPENAI_IMAGE_SIZE_RE = re.compile(r"^(?P<w>\d{2,5})\s*[xX]\s*(?P<h>\d{2,5})$")
NUMERIC_ASPECT_RATIO_RE = re.compile(
    r"^(?P<w>\d+(?:\.\d+)?)\s*:\s*(?P<h>\d+(?:\.\d+)?)$"
)
DURATION_SECONDS_RE = re.compile(r"^(?P<sec>\d{1,3})\s*[sS]?$")

# OpenAI 常见 quality → imageSize 映射
# - 未传或 auto/medium 时使用默认 2k；low/standard 显式降到 1k；high/hd/ultra 升到 4k。
OPENAI_QUALITY_MAP = {
    "low": "1k",
    "standard": "1k",
    "medium": "2k",
    "high": "4k",
    "hd": "4k",
    "ultra": "4k",
}
# 用于把 OpenAI size（如 1024x1792）映射到最接近的 flow2api aspect 选项
ASPECT_RATIO_FLOAT_MAP = {
    "landscape": 16 / 9,
    "portrait": 9 / 16,
    "square": 1.0,
    "four-three": 4 / 3,
    "three-four": 3 / 4,
}


def _aspect_from_dimensions(
    width: int, height: int, *, video_mode: bool = False
) -> Optional[str]:
    if width <= 0 or height <= 0:
        return None

    ratio = width / height
    inferred = min(
        ASPECT_RATIO_FLOAT_MAP.items(),
        key=lambda item: abs(ratio - item[1]),
    )[0]

    if video_mode and inferred not in {"landscape", "portrait"}:
        return "portrait" if height > width else "landscape"
    return inferred


def _infer_aspect_ratio_from_images(
    images: Any,
    *,
    video_mode: bool = False,
) -> Optional[str]:
    if not isinstance(images, (list, tuple)):
        return None

    source_bytes = next(
        (
            bytes(item)
            for item in images
            if isinstance(item, (bytes, bytearray)) and item
        ),
        None,
    )
    if not source_bytes:
        return None

    try:
        from PIL import Image, ImageOps

        with Image.open(BytesIO(source_bytes)) as image:
            normalized = ImageOps.exif_transpose(image)
            width, height = normalized.size
    except Exception as exc:
        debug_logger.log_warning(
            f"[MODEL_RESOLVER] 参考图尺寸解析失败，跳过自动比例跟随: {exc}"
        )
        return None

    inferred = _aspect_from_dimensions(width, height, video_mode=video_mode)
    if inferred:
        debug_logger.log_info(
            f"[MODEL_RESOLVER] 未显式指定 aspectRatio，已按参考图尺寸自动推断: {width}x{height} -> {inferred}"
        )
    return inferred


# ──────────────────────────────────────────────
# 视频模型简化名映射
# ──────────────────────────────────────────────
VIDEO_BASE_MODELS = {
    # T2V models (Veo 3.1 Lite & Omni 1.2 Flash)
    "veo_3_1_t2v_lite": {
        "landscape": "veo_3_1_t2v_lite_landscape",
        "portrait": "veo_3_1_t2v_lite_portrait",
    },
    "veo_3_1_t2v_lite_4s": {
        "landscape": "veo_3_1_t2v_lite_4s_landscape",
        "portrait": "veo_3_1_t2v_lite_4s_portrait",
    },
    "veo_3_1_t2v_lite_6s": {
        "landscape": "veo_3_1_t2v_lite_6s_landscape",
        "portrait": "veo_3_1_t2v_lite_6s_portrait",
    },
    "veo_3_1_t2v_lite_8s": {
        "landscape": "veo_3_1_t2v_lite_8s_landscape",
        "portrait": "veo_3_1_t2v_lite_8s_portrait",
    },
    "omni": {
        "landscape": "omni",
        "portrait": "omni_portrait",
    },
    "omni-1.2-flash": {
        "landscape": "omni-1.2-flash-8s-landscape",
        "portrait": "omni-1.2-flash-8s-portrait",
    },
    "omni-1.2-flash-360p": {
        "landscape": "omni-1.2-flash-8s-landscape-360p",
        "portrait": "omni-1.2-flash-8s-portrait-360p",
    },
    "omni-1.2-flash-1080p": {
        "landscape": "omni-1.2-flash-8s-landscape-1080p",
        "portrait": "omni-1.2-flash-8s-portrait-1080p",
    },
    "omni-1.2-flash-4k": {
        "landscape": "omni-1.2-flash-8s-landscape-4k",
        "portrait": "omni-1.2-flash-8s-portrait-4k",
    },
    **{
        f"omni-1.2-flash-{d}s": {
            "landscape": f"omni-1.2-flash-{d}s-landscape",
            "portrait": f"omni-1.2-flash-{d}s-portrait",
        }
        for d in range(3, 16)
    },
    **{
        f"omni-1.2-flash-{d}s-{res}": {
            "landscape": f"omni-1.2-flash-{d}s-landscape-{res}",
            "portrait": f"omni-1.2-flash-{d}s-portrait-{res}",
        }
        for d in range(3, 16)
        for res in ("360p", "1080p", "4k")
    },
    "omni-1.2-flash-i2v": {
        "landscape": "omni-1.2-flash-i2v-8s-landscape",
        "portrait": "omni-1.2-flash-i2v-8s-portrait",
    },
    **{
        f"omni-1.2-flash-i2v-{res}": {
            "landscape": f"omni-1.2-flash-i2v-8s-landscape-{res}",
            "portrait": f"omni-1.2-flash-i2v-8s-portrait-{res}",
        }
        for res in ("360p", "1080p", "4k")
    },
    **{
        f"omni-1.2-flash-i2v-{d}s": {
            "landscape": f"omni-1.2-flash-i2v-{d}s-landscape",
            "portrait": f"omni-1.2-flash-i2v-{d}s-portrait",
        }
        for d in range(3, 16)
    },
    **{
        f"omni-1.2-flash-i2v-{d}s-{res}": {
            "landscape": f"omni-1.2-flash-i2v-{d}s-landscape-{res}",
            "portrait": f"omni-1.2-flash-i2v-{d}s-portrait-{res}",
        }
        for d in range(3, 16)
        for res in ("360p", "1080p", "4k")
    },
    "omni-1.2-flash-extend": {
        "landscape": "omni-1.2-flash-extend-8s-landscape",
        "portrait": "omni-1.2-flash-extend-8s-portrait",
    },
    **{
        f"omni-1.2-flash-extend-{res}": {
            "landscape": f"omni-1.2-flash-extend-8s-landscape-{res}",
            "portrait": f"omni-1.2-flash-extend-8s-portrait-{res}",
        }
        for res in ("360p", "1080p", "4k")
    },
    **{
        f"omni-1.2-flash-extend-{d}s": {
            "landscape": f"omni-1.2-flash-extend-{d}s-landscape",
            "portrait": f"omni-1.2-flash-extend-{d}s-portrait",
        }
        for d in range(3, 11)
    },
    **{
        f"omni-1.2-flash-extend-{d}s-{res}": {
            "landscape": f"omni-1.2-flash-extend-{d}s-landscape-{res}",
            "portrait": f"omni-1.2-flash-extend-{d}s-portrait-{res}",
        }
        for d in range(3, 11)
        for res in ("360p", "1080p", "4k")
    },
    "veo-3.1-lite": {
        "landscape": "veo-3.1-lite-8s-landscape",
        "portrait": "veo-3.1-lite-8s-portrait",
    },
    **{
        f"veo-3.1-lite-{res}": {
            "landscape": f"veo-3.1-lite-8s-landscape-{res}",
            "portrait": f"veo-3.1-lite-8s-portrait-{res}",
        }
        for res in ("1080p", "4k")
    },
    **{
        f"veo-3.1-lite-{d}s": {
            "landscape": f"veo-3.1-lite-{d}s-landscape",
            "portrait": f"veo-3.1-lite-{d}s-portrait",
        }
        for d in (4, 6, 8)
    },
    **{
        f"veo-3.1-lite-{d}s-{res}": {
            "landscape": f"veo-3.1-lite-{d}s-landscape-{res}",
            "portrait": f"veo-3.1-lite-{d}s-portrait-{res}",
        }
        for d in (4, 6, 8)
        for res in ("1080p", "4k")
    },
    "veo-3.1-lite-i2v": {
        "landscape": "veo-3.1-lite-i2v-8s-landscape",
        "portrait": "veo-3.1-lite-i2v-8s-portrait",
    },
    **{
        f"veo-3.1-lite-i2v-{res}": {
            "landscape": f"veo-3.1-lite-i2v-8s-landscape-{res}",
            "portrait": f"veo-3.1-lite-i2v-8s-portrait-{res}",
        }
        for res in ("1080p", "4k")
    },
    **{
        f"veo-3.1-lite-i2v-{d}s": {
            "landscape": f"veo-3.1-lite-i2v-{d}s-landscape",
            "portrait": f"veo-3.1-lite-i2v-{d}s-portrait",
        }
        for d in (4, 6, 8)
    },
    **{
        f"veo-3.1-lite-i2v-{d}s-{res}": {
            "landscape": f"veo-3.1-lite-i2v-{d}s-landscape-{res}",
            "portrait": f"veo-3.1-lite-i2v-{d}s-portrait-{res}",
        }
        for d in (4, 6, 8)
        for res in ("1080p", "4k")
    },
    "veo-3.1-lite-r2v": {
        "landscape": "veo-3.1-lite-r2v-8s-landscape",
        "portrait": "veo-3.1-lite-r2v-8s-portrait",
    },
    **{
        f"veo-3.1-lite-r2v-{res}": {
            "landscape": f"veo-3.1-lite-r2v-8s-landscape-{res}",
            "portrait": f"veo-3.1-lite-r2v-8s-portrait-{res}",
        }
        for res in ("1080p", "4k")
    },
    "veo-3.1-lite-r2v-8s": {
        "landscape": "veo-3.1-lite-r2v-8s-landscape",
        "portrait": "veo-3.1-lite-r2v-8s-portrait",
    },
    **{
        f"veo-3.1-lite-r2v-8s-{res}": {
            "landscape": f"veo-3.1-lite-r2v-8s-landscape-{res}",
            "portrait": f"veo-3.1-lite-r2v-8s-portrait-{res}",
        }
        for res in ("1080p", "4k")
    },
    "veo-3.1-lite-extend": {
        "landscape": "veo-3.1-lite-extend-8s-landscape",
        "portrait": "veo-3.1-lite-extend-8s-portrait",
    },
    **{
        f"veo-3.1-lite-extend-{res}": {
            "landscape": f"veo-3.1-lite-extend-8s-landscape-{res}",
            "portrait": f"veo-3.1-lite-extend-8s-portrait-{res}",
        }
        for res in ("1080p", "4k")
    },
    "veo-3.1-lite-extend-8s": {
        "landscape": "veo-3.1-lite-extend-8s-landscape",
        "portrait": "veo-3.1-lite-extend-8s-portrait",
    },
    **{
        f"veo-3.1-lite-extend-8s-{res}": {
            "landscape": f"veo-3.1-lite-extend-8s-landscape-{res}",
            "portrait": f"veo-3.1-lite-extend-8s-portrait-{res}",
        }
        for res in ("1080p", "4k")
    },
    # I2V Lite legacy aliases
    "veo_3_1_i2v_lite": {
        "landscape": "veo_3_1_i2v_lite_landscape",
        "portrait": "veo_3_1_i2v_lite_portrait",
    },
    "veo_3_1_i2v_lite_4s": {
        "landscape": "veo_3_1_i2v_lite_4s_landscape",
        "portrait": "veo_3_1_i2v_lite_4s_portrait",
    },
    "veo_3_1_i2v_lite_6s": {
        "landscape": "veo_3_1_i2v_lite_6s_landscape",
        "portrait": "veo_3_1_i2v_lite_6s_portrait",
    },
    "veo_3_1_i2v_lite_8s": {
        "landscape": "veo_3_1_i2v_lite_8s_landscape",
        "portrait": "veo_3_1_i2v_lite_8s_portrait",
    },
    "veo_3_1_interpolation_lite": {
        "landscape": "veo_3_1_interpolation_lite_landscape",
        "portrait": "veo_3_1_interpolation_lite_portrait",
    },
    "veo_3_1_interpolation_lite_4s": {
        "landscape": "veo_3_1_interpolation_lite_4s_landscape",
        "portrait": "veo_3_1_interpolation_lite_4s_portrait",
    },
    "veo_3_1_interpolation_lite_6s": {
        "landscape": "veo_3_1_interpolation_lite_6s_landscape",
        "portrait": "veo_3_1_interpolation_lite_6s_portrait",
    },
    "veo_3_1_interpolation_lite_8s": {
        "landscape": "veo_3_1_interpolation_lite_8s_landscape",
        "portrait": "veo_3_1_interpolation_lite_8s_portrait",
    },
}


VIDEO_DURATION_FAMILIES: Dict[str, Tuple[int, ...]] = {
    "veo-3.1-lite": (4, 6, 8),
    "veo-3.1-lite-i2v": (4, 6, 8),
    "omni-1.2-flash": tuple(range(3, 16)),
    "omni-1.2-flash-i2v": tuple(range(3, 16)),
    "omni-1.2-flash-extend": tuple(range(3, 11)),
}

CANONICAL_VIDEO_PREFIXES: Tuple[str, ...] = (
    "veo-3.1-lite-i2v",
    "veo-3.1-lite-r2v",
    "veo-3.1-lite-extend",
    "veo-3.1-lite",
    "omni-1.2-flash-i2v",
    "omni-1.2-flash-extend",
    "omni-1.2-flash",
)


def _strip_models_prefix(model: str) -> str:
    text = str(model or "").strip()
    if text.startswith("models/"):
        return text[len("models/"):]
    return text


def get_canonical_model_name(model: str) -> str:
    """Map a raw, alias, or legacy suffixed model ID to its canonical public model ID."""
    clean = _strip_models_prefix(model)
    if not clean:
        return clean
    if clean in IMAGE_BASE_MODELS:
        return IMAGE_BASE_MODELS[clean]
    for prefix in (
        "gemini-3.1-flash-lite-image",
        "gemini-3.1-flash-image",
        "gemini-3.0-pro-image",
    ):
        if clean == prefix or clean.startswith(f"{prefix}-"):
            return prefix
    for prefix in CANONICAL_VIDEO_PREFIXES:
        if clean == prefix or clean.startswith(f"{prefix}-"):
            return prefix
    if clean in ("omni", "omni_portrait") or clean.startswith("macaron_cookie_"):
        if "extend" in clean:
            return "omni-1.2-flash-extend"
        if "i2v" in clean:
            return "omni-1.2-flash-i2v"
        return "omni-1.2-flash"
    if clean.startswith("veo_3_1_"):
        if "extension" in clean or "extend" in clean:
            return "veo-3.1-lite-extend"
        if "r2v" in clean:
            return "veo-3.1-lite-r2v"
        if "i2v" in clean or "interpolation" in clean:
            return "veo-3.1-lite-i2v"
        return "veo-3.1-lite"
    return clean


def _extract_generation_params_full(
    request,
) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """从请求中提取 aspectRatio、imageSize/resolution 和 durationSeconds 参数。

    兼容：
    1. Gemini / AI Studio `generationConfig.imageConfig` / `videoConfig` / `responseFormat.{image,video}`
    2. Gemini / Imagen `parameters.{aspectRatio, imageSize, durationSeconds, resolution}`
    3. OpenAI `size` / `quality` / `seconds` / `extra_body.generationConfig` 及顶层扩展字段

    Returns:
        (aspect_ratio, image_size, duration_seconds)
    """

    def _normalize_str(value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        text = value.strip()
        return text if text else None

    def _read_value(obj: Any, *keys: str) -> Any:
        if obj is None:
            return None
        if isinstance(obj, dict):
            for key in keys:
                if key in obj and obj.get(key) is not None:
                    return obj.get(key)
            return None

        for key in keys:
            if hasattr(obj, key):
                value = getattr(obj, key, None)
                if value is not None:
                    return value

        extra = getattr(obj, "__pydantic_extra__", None) or {}
        for key in keys:
            if key in extra and extra.get(key) is not None:
                return extra.get(key)
        return None

    def _normalize_aspect_ratio(value: Any) -> Optional[str]:
        raw = _normalize_str(value)
        if not raw:
            return None
        if raw.lower() == "auto":
            return None

        # Flow enum names (e.g. IMAGE_ASPECT_RATIO_LANDSCAPE)
        direct_enum = ASPECT_RATIO_MAP.get(raw) or ASPECT_RATIO_MAP.get(raw.upper())
        if direct_enum:
            return direct_enum

        # OpenAI pixel dimensions (e.g. 1024x1792, 1536x1024)
        dim_match = OPENAI_IMAGE_SIZE_RE.match(raw)
        if dim_match:
            try:
                return _aspect_from_dimensions(
                    int(dim_match.group("w")), int(dim_match.group("h"))
                )
            except Exception:
                return None

        token = (
            raw.replace("：", ":")
            .replace("/", ":")
            .replace("x", ":")
            .replace("X", ":")
            .replace(" ", "")
            .strip()
        )

        mapped = (
            ASPECT_RATIO_MAP.get(token)
            or ASPECT_RATIO_MAP.get(token.lower())
            or ASPECT_RATIO_MAP.get(token.upper())
        )
        if mapped:
            return mapped

        # Arbitrary W:H ratio (e.g. 3:2, 2:3, 4:5, 5:4, 21:9) -> snap to closest Flow ratio
        ratio_match = NUMERIC_ASPECT_RATIO_RE.match(token)
        if ratio_match:
            try:
                w = float(ratio_match.group("w"))
                h = float(ratio_match.group("h"))
                if w > 0 and h > 0:
                    return _aspect_from_dimensions(int(round(w * 100)), int(round(h * 100)))
            except Exception:
                pass

        return token

    def _normalize_image_size(value: Any) -> Optional[str]:
        raw = _normalize_str(value)
        if not raw:
            return None
        if raw.lower() == "auto":
            return None

        token = raw.replace(" ", "").strip()
        for candidate in (token, token.lower(), token.upper()):
            mapped = IMAGE_SIZE_MAP.get(candidate)
            if mapped is not None:
                return mapped or None
        return token.lower()

    def _normalize_duration(value: Any) -> Optional[int]:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            sec = int(round(value))
            return sec if sec > 0 else None
        raw = _normalize_str(value)
        if not raw:
            return None
        m = DURATION_SECONDS_RE.match(raw)
        if not m:
            return None
        sec = int(m.group("sec"))
        return sec if sec > 0 else None

    def _aspect_from_openai_size(value: Any) -> Optional[str]:
        raw = _normalize_str(value)
        if not raw:
            return None
        if raw.lower() == "auto":
            return None
        match = OPENAI_IMAGE_SIZE_RE.match(raw)
        if match:
            try:
                return _aspect_from_dimensions(
                    int(match.group("w")), int(match.group("h"))
                )
            except Exception:
                return None
        # Also accept standard ratio strings passed in `size` (e.g. size="16:9")
        return _normalize_aspect_ratio(raw)

    def _image_size_from_openai_quality(value: Any) -> Optional[str]:
        raw = _normalize_str(value)
        if not raw:
            return None

        token = raw.strip().lower()
        if token == "auto":
            return None
        if token in IMAGE_SIZE_MAP:
            return _normalize_image_size(token)

        mapped = OPENAI_QUALITY_MAP.get(token)
        if mapped:
            return mapped
        return None

    def _apply_media_config(
        cfg_obj: Any,
        aspect_ratio: Optional[str],
        image_size: Optional[str],
        duration: Optional[int],
    ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        if cfg_obj is None:
            return aspect_ratio, image_size, duration
        if not aspect_ratio:
            aspect_ratio = _normalize_aspect_ratio(
                _read_value(cfg_obj, "aspectRatio", "aspect_ratio", "aspect")
            )
        if not image_size:
            image_size = _normalize_image_size(
                _read_value(cfg_obj, "imageSize", "image_size", "resolution")
            )
        if duration is None:
            duration = _normalize_duration(
                _read_value(
                    cfg_obj, "durationSeconds", "duration_seconds", "duration", "seconds"
                )
            )
        if not aspect_ratio:
            aspect_ratio = _aspect_from_openai_size(_read_value(cfg_obj, "size"))
        if not image_size:
            image_size = _image_size_from_openai_quality(
                _read_value(cfg_obj, "quality", "imageQuality", "image_quality")
            )
        return aspect_ratio, image_size, duration

    def _apply_response_format(
        resp_fmt: Any,
        aspect_ratio: Optional[str],
        image_size: Optional[str],
        duration: Optional[int],
    ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        if resp_fmt is None:
            return aspect_ratio, image_size, duration
        items = resp_fmt if isinstance(resp_fmt, list) else [resp_fmt]
        for item in items:
            img_part = _read_value(item, "image")
            if img_part is not None:
                aspect_ratio, image_size, duration = _apply_media_config(
                    img_part, aspect_ratio, image_size, duration
                )
            vid_part = _read_value(item, "video")
            if vid_part is not None:
                aspect_ratio, image_size, duration = _apply_media_config(
                    vid_part, aspect_ratio, image_size, duration
                )
        return aspect_ratio, image_size, duration

    def _apply_gen_config(
        gen_cfg: Any,
        aspect_ratio: Optional[str],
        image_size: Optional[str],
        duration: Optional[int],
    ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        if gen_cfg is None:
            return aspect_ratio, image_size, duration
        image_cfg = _read_value(gen_cfg, "imageConfig", "image_config")
        if image_cfg is not None:
            aspect_ratio, image_size, duration = _apply_media_config(
                image_cfg, aspect_ratio, image_size, duration
            )
        video_cfg = _read_value(gen_cfg, "videoConfig", "video_config")
        if video_cfg is not None:
            aspect_ratio, image_size, duration = _apply_media_config(
                video_cfg, aspect_ratio, image_size, duration
            )
        resp_fmt = _read_value(gen_cfg, "responseFormat", "response_format")
        if resp_fmt is not None:
            aspect_ratio, image_size, duration = _apply_response_format(
                resp_fmt, aspect_ratio, image_size, duration
            )
        return _apply_media_config(gen_cfg, aspect_ratio, image_size, duration)

    aspect_ratio: Optional[str] = None
    image_size: Optional[str] = None
    duration: Optional[int] = None

    if request is None:
        return None, None, None

    # 1) request.generationConfig / request.imageConfig / request.videoConfig / request.parameters
    aspect_ratio, image_size, duration = _apply_gen_config(
        _read_value(request, "generationConfig", "generation_config"),
        aspect_ratio,
        image_size,
        duration,
    )
    aspect_ratio, image_size, duration = _apply_media_config(
        _read_value(request, "imageConfig", "image_config"),
        aspect_ratio,
        image_size,
        duration,
    )
    aspect_ratio, image_size, duration = _apply_media_config(
        _read_value(request, "videoConfig", "video_config"),
        aspect_ratio,
        image_size,
        duration,
    )
    aspect_ratio, image_size, duration = _apply_media_config(
        _read_value(request, "parameters"),
        aspect_ratio,
        image_size,
        duration,
    )

    # 2) extra_body passthrough (OpenAI SDK extra_body={...})
    extra_body = _read_value(request, "extra_body", "extraBody")
    if isinstance(extra_body, dict):
        aspect_ratio, image_size, duration = _apply_gen_config(
            extra_body.get("generationConfig") or extra_body.get("generation_config"),
            aspect_ratio,
            image_size,
            duration,
        )
        aspect_ratio, image_size, duration = _apply_media_config(
            extra_body.get("imageConfig") or extra_body.get("image_config"),
            aspect_ratio,
            image_size,
            duration,
        )
        aspect_ratio, image_size, duration = _apply_media_config(
            extra_body, aspect_ratio, image_size, duration
        )

    # 3) Top-level request fields / __pydantic_extra__ (size, quality, aspect_ratio, image_size, duration, etc.)
    aspect_ratio, image_size, duration = _apply_media_config(
        request, aspect_ratio, image_size, duration
    )

    return aspect_ratio, image_size, duration


def _extract_generation_params(request) -> Tuple[Optional[str], Optional[str]]:
    """Backward-compatible helper returning (aspect_ratio, image_size)."""
    aspect_ratio, image_size, _ = _extract_generation_params_full(request)
    return aspect_ratio, image_size


def resolve_model_name(
    model: str, request=None, model_config: Dict[str, Any] = None, images: Any = None
) -> str:
    """将标准模型名 + generationConfig/OpenAI 参数解析为内部 MODEL_CONFIG key。

    图片模型默认参数：16:9 (`landscape`) + 2K (`-2k`)。
    显式传 `imageSize: "1K"` / `quality: "standard"` 时回落到 1K 基础图；
    显式传 `imageSize: "4K"` / `quality: "hd"` 时升级到 4K。
    """
    raw_model = _strip_models_prefix(model)
    aspect_ratio, image_size, duration = (
        _extract_generation_params_full(request) if request else (None, None, None)
    )

    # ────── 图片模型解析 ──────
    if raw_model in IMAGE_BASE_MODELS:
        base = IMAGE_BASE_MODELS[raw_model]
        resolved_aspect = aspect_ratio
        if not resolved_aspect:
            resolved_aspect = _infer_aspect_ratio_from_images(images)
        if not resolved_aspect:
            resolved_aspect = DEFAULT_ASPECT

        supported_aspects = MODEL_SUPPORTED_ASPECTS.get(base, [])
        if resolved_aspect not in supported_aspects and supported_aspects:
            debug_logger.log_warning(
                f"[MODEL_RESOLVER] 模型 {base} 不支持 aspectRatio={resolved_aspect}，"
                f"降级到 {DEFAULT_ASPECT}"
            )
            resolved_aspect = DEFAULT_ASPECT

        resolved = f"{base}-{resolved_aspect}"

        effective_size = image_size if image_size is not None else DEFAULT_IMAGE_SIZE
        if effective_size and effective_size != "1k":
            supported_sizes = MODEL_SUPPORTED_SIZES.get(base, [])
            if effective_size in supported_sizes:
                resolved = f"{resolved}-{effective_size}"
            else:
                debug_logger.log_warning(
                    f"[MODEL_RESOLVER] 模型 {base} 不支持 imageSize={effective_size}，忽略"
                )

        if model_config and resolved not in model_config:
            debug_logger.log_warning(
                f"[MODEL_RESOLVER] 解析后的模型名 {resolved} 不在 MODEL_CONFIG 中，"
                f"回退到原始模型名 {raw_model}"
            )
            return raw_model

        debug_logger.log_info(
            f"[MODEL_RESOLVER] 模型名转换: {raw_model} → {resolved} "
            f"(aspectRatio={resolved_aspect}, imageSize={effective_size})"
        )
        return resolved

    # ────── 视频模型解析 ──────
    if raw_model in VIDEO_BASE_MODELS:
        target_base = raw_model
        if duration is not None and target_base in VIDEO_DURATION_FAMILIES:
            allowed_durations = VIDEO_DURATION_FAMILIES[target_base]
            chosen_duration = (
                duration
                if duration in allowed_durations
                else min(allowed_durations, key=lambda d: abs(d - duration))
            )
            dur_candidate = f"{target_base}-{chosen_duration}s"
            if dur_candidate in VIDEO_BASE_MODELS:
                target_base = dur_candidate

        resolved_aspect = aspect_ratio
        if not resolved_aspect:
            resolved_aspect = _infer_aspect_ratio_from_images(images, video_mode=True)
        if not resolved_aspect or resolved_aspect not in ("landscape", "portrait"):
            resolved_aspect = "landscape"

        if image_size in ("4k", "1080p", "360p"):
            if f"{target_base}-{image_size}" in VIDEO_BASE_MODELS:
                target_base = f"{target_base}-{image_size}"
            elif f"{target_base}_{image_size}" in VIDEO_BASE_MODELS:
                target_base = f"{target_base}_{image_size}"

        orientation_map = VIDEO_BASE_MODELS[target_base]
        resolved = orientation_map.get(resolved_aspect)

        if resolved and model_config and resolved in model_config:
            debug_logger.log_info(
                f"[MODEL_RESOLVER] 视频模型名转换: {raw_model} → {resolved} "
                f"(aspectRatio={resolved_aspect}, duration={duration or 'default'}, resolution={image_size or 'default'})"
            )
            return resolved

        debug_logger.log_warning(
            f"[MODEL_RESOLVER] 视频模型 {raw_model} 解析失败 (aspect={resolved_aspect})，"
            f"使用原始模型名"
        )
        return raw_model

    # ────── 兼容旧版带后缀的 MODEL_CONFIG key ──────
    if model_config and raw_model in model_config:
        if image_size in ("2k", "4k", "1080p", "360p") and not (
            raw_model.endswith(f"-{image_size}") or raw_model.endswith(f"_{image_size}")
        ):
            for sep in ("-", "_"):
                candidate = f"{raw_model}{sep}{image_size}"
                if candidate in model_config:
                    return candidate
        return raw_model

    # 未知模型名，原样返回（由下游 MODEL_CONFIG 校验报错）
    return raw_model


def get_base_model_aliases() -> Dict[str, str]:
    """返回简化/别名模型及其参数说明，用于 /v1/models/aliases 展示与权限校验。"""
    aliases: Dict[str, str] = {}

    for alias, base in IMAGE_BASE_MODELS.items():
        aspects = MODEL_SUPPORTED_ASPECTS.get(base, [])
        sizes = MODEL_SUPPORTED_SIZES.get(base, [])
        desc_parts = [
            f"aspects: {', '.join(aspects)}",
            f"sizes: {', '.join(sizes)}",
            "default: 16:9, 2K",
        ]
        aliases[alias] = f"Image generation (alias) - {'; '.join(desc_parts)}"

    for alias in VIDEO_BASE_MODELS:
        aliases[alias] = (
            "Video generation (alias) - supports aspectRatio/durationSeconds/resolution via generationConfig"
        )

    return aliases
