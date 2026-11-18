"""Prompt cleanup, @mention resolution, jitter helpers, and Base64 media guards."""

from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
from functools import cache
import random
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

SD_BANNED_KEYWORDS: tuple[str, ...] = (
    "8k",
    "4k",
    "ultra hd",
    "high resolution",
    "masterpiece",
    "highly detailed",
    "ultra detailed",
    "trending on artstation",
    "hyperrealistic",
    "ultra realistic",
    "photorealistic",
    "best quality",
    "award winning",
)

_RE_TRAILING_SPACE_COMMA = re.compile(r"\s+,")
_RE_DOUBLE_COMMA = re.compile(r"(?:,\s*){2,}")
_RE_MULTI_SPACE = re.compile(r"\s{2,}")

# Matches @Name or @"Multi Word Name" only at string start or after whitespace/punctuation
# (never inside email addresses like alice@example.com).
_MENTION_PATTERN = re.compile(
    r'(?:(?<=^)|(?<=[\s(\[{"\'，。、；：]))@(?:"([^"\n]{1,80})"|([A-Za-z0-9_\-\u4e00-\u9fff]{1,64}))'
)

MAX_IMAGE_BASE64_CHARS = 50 * 1024 * 1024 * 4 // 3 + 16
MAX_VIDEO_BASE64_CHARS = 120 * 1024 * 1024 * 4 // 3 + 16
_DATA_URI_PREFIX_RE = re.compile(r"^data:[^;]+;base64,", re.IGNORECASE)


def jitter_seconds(base: float, *, low_ratio: float = 0.75, high_ratio: float = 1.25) -> float:
    """Return a non-negative randomized delay around `base` (anti-bot cadence)."""
    safe_base = max(0.0, float(base))
    if safe_base <= 0.0:
        return 0.0
    low = max(0.0, safe_base * min(low_ratio, high_ratio))
    high = max(low, safe_base * max(low_ratio, high_ratio))
    return random.uniform(low, high)


async def jitter_sleep(
    base: float,
    *,
    low_ratio: float = 0.75,
    high_ratio: float = 1.25,
) -> float:
    """Sleep for a jittered duration around `base` seconds and return the actual wait."""
    delay = jitter_seconds(base, low_ratio=low_ratio, high_ratio=high_ratio)
    if delay > 0.0:
        await asyncio.sleep(delay)
    return delay


@cache
def _compiled_banned_patterns(
    keywords: tuple[str, ...],
) -> tuple[tuple[str, re.Pattern[str]], ...]:
    return tuple(
        (kw, re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE))
        for kw in sorted(keywords, key=len, reverse=True)
    )


def strip_sd_banned_keywords(
    prompt: str,
    keywords: tuple[str, ...] = SD_BANNED_KEYWORDS,
) -> Tuple[str, List[str]]:
    """Strip legacy Stable-Diffusion boilerplate tags that degrade Flow/Veo quality."""
    text = str(prompt or "")
    if not text.strip():
        return text, []

    removed: List[str] = []
    cleaned = text
    for keyword, pattern in _compiled_banned_patterns(keywords):
        if pattern.search(cleaned):
            removed.append(keyword)
            cleaned = pattern.sub("", cleaned)

    if not removed:
        return text, []

    cleaned = _RE_TRAILING_SPACE_COMMA.sub(",", cleaned)
    cleaned = _RE_DOUBLE_COMMA.sub(", ", cleaned)
    cleaned = _RE_MULTI_SPACE.sub(" ", cleaned).strip(" ,")
    return cleaned or text.strip(), removed


@dataclass(frozen=True, slots=True)
class MentionResolutionResult:
    prompt: str
    character_ids: List[str]
    image_media_ids: List[str]
    resolved_mentions: List[Dict[str, str]]
    unresolved_mentions: List[str]


def parse_prompt_mentions(prompt: str) -> List[Tuple[str, str]]:
    """Return ordered `(raw_token, clean_name)` pairs for `@Name` or `@\"Name\"` tokens."""
    out: List[Tuple[str, str]] = []
    for match in _MENTION_PATTERN.finditer(str(prompt or "")):
        raw_name = (match.group(1) or match.group(2) or "").strip()
        if raw_name:
            out.append((match.group(0), raw_name))
    return out


def resolve_prompt_mentions(
    prompt: str,
    *,
    characters: Optional[Sequence[Dict[str, Any]]] = None,
    workflows: Optional[Sequence[Dict[str, Any]]] = None,
) -> MentionResolutionResult:
    """Resolve `@CharacterName` / `@AssetName` tokens against a Flow project's catalog.

    Raises:
        ValueError: If an `@Character` matches a character that has zero reference images
            (cannot anchor generation cleanly), or if multiple distinct entries have the
            exact same name.
    """
    mentions = parse_prompt_mentions(prompt)
    if not mentions:
        return MentionResolutionResult(
            prompt=prompt,
            character_ids=[],
            image_media_ids=[],
            resolved_mentions=[],
            unresolved_mentions=[],
        )

    char_by_name: Dict[str, List[Dict[str, Any]]] = {}
    for ch in characters or []:
        if not isinstance(ch, dict) or ch.get("archived"):
            continue
        name = str(ch.get("displayName") or "").strip().casefold()
        if name and ch.get("id"):
            char_by_name.setdefault(name, []).append(ch)

    wf_by_name: Dict[str, List[Dict[str, Any]]] = {}
    for wf in workflows or []:
        if not isinstance(wf, dict):
            continue
        name = str(wf.get("displayName") or "").strip().casefold()
        media_id = str(wf.get("primaryMediaId") or "").strip()
        if name and media_id:
            wf_by_name.setdefault(name, []).append(wf)

    character_ids: List[str] = []
    image_media_ids: List[str] = []
    resolved: List[Dict[str, str]] = []
    unresolved: List[str] = []

    for raw_tok, clean_name in mentions:
        key = clean_name.casefold()
        char_matches = char_by_name.get(key, [])
        if len(char_matches) > 1:
            raise ValueError(
                f"Ambiguous character mention {raw_tok}: {len(char_matches)} characters share that name"
            )
        if len(char_matches) == 1:
            ch = char_matches[0]
            if not ch.get("imageRefs"):
                raise ValueError(
                    f"Character {clean_name!r} has no reference images yet; attach image_refs or generate face/body first"
                )
            cid = str(ch["id"])
            if cid not in character_ids:
                character_ids.append(cid)
            resolved.append({"mention": raw_tok, "kind": "character", "id": cid, "name": str(ch.get("displayName") or clean_name)})
            continue

        wf_matches = wf_by_name.get(key, [])
        if len(wf_matches) > 1:
            raise ValueError(
                f"Ambiguous asset mention {raw_tok}: {len(wf_matches)} project assets share that name"
            )
        if len(wf_matches) == 1:
            wf = wf_matches[0]
            mid = str(wf["primaryMediaId"])
            if mid not in image_media_ids:
                image_media_ids.append(mid)
            resolved.append({"mention": raw_tok, "kind": "asset", "id": mid, "name": str(wf.get("displayName") or clean_name)})
            continue

        unresolved.append(clean_name)

    return MentionResolutionResult(
        prompt=prompt,
        character_ids=character_ids,
        image_media_ids=image_media_ids,
        resolved_mentions=resolved,
        unresolved_mentions=unresolved,
    )


def validate_base64_media_payload(
    encoded: str,
    *,
    expected_kind: str = "image",
    max_b64_chars: Optional[int] = None,
) -> bytes:
    """Validate size bounds, decode base64 safely, and verify magic bytes before caching/writing."""
    raw_str = str(encoded or "").strip()
    if not raw_str:
        raise ValueError(f"Empty base64 {expected_kind} payload")
    raw_str = _DATA_URI_PREFIX_RE.sub("", raw_str, count=1).strip()

    limit = (
        int(max_b64_chars)
        if max_b64_chars is not None
        else (MAX_VIDEO_BASE64_CHARS if expected_kind == "video" else MAX_IMAGE_BASE64_CHARS)
    )
    if len(raw_str) > limit:
        raise ValueError(
            f"Base64 {expected_kind} payload exceeds size cap ({len(raw_str)} > {limit} chars)"
        )

    try:
        decoded = base64.b64decode(raw_str, validate=True)
    except Exception:
        # Fallback for base64 with embedded whitespace/newlines while still catching garbage
        compact = "".join(raw_str.split())
        try:
            decoded = base64.b64decode(compact, validate=True)
        except Exception as exc:
            raise ValueError(f"Invalid base64 {expected_kind} payload") from exc

    if not decoded:
        raise ValueError(f"Decoded {expected_kind} payload is empty")

    if expected_kind == "image":
        is_valid_image = (
            decoded.startswith(b"\xff\xd8\xff")  # JPEG
            or decoded.startswith(b"\x89PNG\r\n\x1a\n")  # PNG
            or (decoded[:4] == b"RIFF" and decoded[8:12] == b"WEBP")  # WEBP
            or decoded[:6] in (b"GIF87a", b"GIF89a")  # GIF
            or decoded.startswith(b"BM")  # BMP
        )
        if not is_valid_image:
            raise ValueError("Decoded payload is not a valid image (magic-byte check failed)")
    elif expected_kind == "video":
        is_valid_video = len(decoded) >= 12 and (
            decoded[4:8] == b"ftyp"  # MP4/MOV ISO BMFF
            or decoded.startswith(b"\x1a\x45\xdf\xa3")  # WebM/Matroska
        )
        if not is_valid_video:
            raise ValueError("Decoded payload is not a valid video (magic-byte check failed)")

    return decoded
