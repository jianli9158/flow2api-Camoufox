"""Camoufox + WebDriver BiDi native runtime (ported from AIStudio2API camoufoxnative).

Provides:
- Zero-Playwright / Zero-nodriver native Camoufox browser process management
- Pure asyncio WebDriver BiDi client over localhost WebSocket
- Persistent per-account/per-slot Windows Firefox fingerprint (`camoufox-fingerprint.json` + `CAMOU_CONFIG_*`)
- 1 FPS idle frame rate (`gfx.display.frame-rate-divisor = 60`) + `renderLayers = False` CPU/GPU optimization
- Trusted BiDi `input.performActions` UI gesture + in-page `XMLHttpRequest` / `ProtectedSend` (`bidiChannel` 64KB chunked streaming)
- In-page `batchexecute` & `StreamChat` RPC execution sharing 100% browser TLS/H2/Cookie/WIZ_global_data context
- Local unauthenticated SOCKS5 proxy relay forwarding to authenticated upstream HTTP/HTTPS/SOCKS5 proxies
"""

from __future__ import annotations

import asyncio
import base64
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union
from urllib.parse import parse_qsl, quote, urlencode, urlparse
import uuid
import zipfile

from ..core.config import config
from ..core.logger import debug_logger
from .browser_cookie_utils import (
    build_browser_cookie_targets,
    build_cookie_signature,
    is_partitioned_cookie,
)
from .recaptcha_runtime import (
    TRUSTED_RECAPTCHA_HOOK_SOURCE,
    build_recaptcha_execute_script,
)

try:
    import websockets
except ImportError:
    websockets = None

try:
    import aiohttp
except ImportError:
    aiohttp = None


CAMOUFOX_RELEASE_VERSION = "152.0.4-beta.29"
CAMOUFOX_FIREFOX_MAJOR = 152
FLOW_ORIGIN = "https://flow.google.com"
FLOW_PROJECTS_URL = "https://flow.google.com/projects"
FLOW_WEBSITE_KEY = "6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV"

BIDI_ENDPOINT_PATTERN = re.compile(r"WebDriver BiDi listening on (ws://[^\s]+)")
FIREFOX_VERSION_PATTERN = re.compile(r"\b1[0-9]{2}\.0\b")
BROWSER_WINDOW_URL = "chrome://browser/content/browser.xhtml"

IDLE_FRAME_DIVISOR = 60
INTERACTIVE_FRAME_RATE = 60
PROTECTED_GRANT_BATCH = 64

PROTECTED_SEND_KEYS = [
    {"type": "keyDown", "value": "?"},
    {"type": "keyUp", "value": "?"},
]

_ASSETS_DIR = Path(__file__).resolve().parent / "camoufox_assets"


def _load_json_asset(filename: str, default: Any) -> Any:
    path = _ASSETS_DIR / filename
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        debug_logger.log_warning(f"[Camoufox] 加载资源 {filename} 失败: {exc}")
        return default


_WINDOWS_PROFILE_DATA: Dict[str, Any] = _load_json_asset(
    "windows_profile.json", {"fonts": [], "voices": []}
)
_WINDOWS_FONTS_DATA: Dict[str, Any] = _load_json_asset(
    "windows_fonts.json", {"prefs": {}, "families": [], "substitutes": {}}
)
_WINDOWS_WEBGL_PROFILES: List[Dict[str, Any]] = _load_json_asset(
    "windows_webgl.json", []
)

WINDOWS_ESSENTIAL_FONTS = [
    "Arial",
    "Times New Roman",
    "Courier New",
    "Verdana",
    "Georgia",
    "Trebuchet MS",
    "Tahoma",
    "Segoe UI",
    "Calibri",
    "Cambria Math",
    "Nirmala UI",
    "Consolas",
]
WINDOWS_MARKER_FONTS = ["Segoe UI", "Tahoma", "Cambria Math", "Nirmala UI"]


# ============================================================================
# 1. Windows Fingerprint & Camoufox Environment Builder
# ============================================================================


def _random_seed() -> int:
    return random.randint(1, 0xFFFFFFFF)


def _normalize_locale(locale: Optional[str]) -> str:
    value = str(locale or "").strip()
    return value if value else "en-US"


def _locale_values(locale: str) -> List[str]:
    if "-" not in locale:
        return [locale]
    language, _ = locale.split("-", 1)
    return [locale, language.lower()]


def _apply_locale_timezone(
    cfg: Dict[str, Any], locale: str, timezone: str
) -> None:
    locale = _normalize_locale(locale)
    timezone = str(timezone or "").strip() or "America/New_York"
    if "-" in locale:
        language, region = locale.split("-", 1)
        found = True
    else:
        language, region, found = locale, "", False
    cfg["navigator.language"] = locale
    cfg["navigator.languages"] = _locale_values(locale)
    cfg["headers.Accept-Language"] = (
        f"{locale},{language.lower()};q=0.9" if found else locale
    )
    cfg["locale:language"] = language.lower()
    if found and region:
        cfg["locale:region"] = region.upper()
    else:
        cfg.pop("locale:region", None)
    cfg["locale:all"] = ", ".join(_locale_values(locale))
    cfg["timezone"] = timezone


def _windows_font_subset() -> List[str]:
    all_fonts = list(_WINDOWS_PROFILE_DATA.get("fonts") or [])
    if not all_fonts:
        return list(WINDOWS_ESSENTIAL_FONTS)
    result: List[str] = []
    rest: List[str] = []
    for font in all_fonts:
        if font in WINDOWS_ESSENTIAL_FONTS:
            result.append(font)
        else:
            rest.append(font)
    percent = random.randint(30, 78)
    count = int(round((percent / 100.0) * len(rest)))
    random.shuffle(rest)
    result.extend(rest[:count])
    for marker in WINDOWS_MARKER_FONTS:
        if marker not in result:
            result.append(marker)
    return result


def _windows_font_aliases(fonts: Iterable[str]) -> List[str]:
    declared = {str(f).lower() for f in fonts if f}
    allowed: Dict[str, bool] = {}
    names: List[str] = []
    for family in _WINDOWS_FONTS_DATA.get("families") or []:
        if not isinstance(family, list):
            continue
        hit = any(str(name).lower() in declared for name in family)
        for name in family:
            key = str(name).lower()
            if key in declared:
                continue
            if key in allowed:
                allowed[key] = allowed[key] and hit
                continue
            allowed[key] = hit
            names.append(str(name))
    return [name for name in names if allowed.get(name.lower(), False)]


_SUBSTITUTE_FONTS_LOCK = threading.Lock()
_SUBSTITUTE_FONT_DIRECTORIES: Set[str] = set()
_SUBSTITUTE_POSTSCRIPT_SUFFIX = "-Windows"


def _utf16_be(text: str) -> bytes:
    return text.encode("utf-16-be")


def _font_checksum(data: bytes) -> int:
    total = 0
    limit = (len(data) // 4) * 4
    for offset in range(0, limit, 4):
        total = (total + struct.unpack_from(">I", data, offset)[0]) & 0xFFFFFFFF
    return total


def _read_font_name_table(path: Path) -> Optional[bytes]:
    try:
        with path.open("rb") as f:
            header = f.read(12)
            if len(header) < 12 or header[:4] not in (b"\x00\x01\x00\x00", b"OTTO", b"true"):
                return None
            num_tables = struct.unpack_from(">H", header, 4)[0]
            directory = f.read(16 * num_tables)
            if len(directory) < 16 * num_tables:
                return None
            for idx in range(0, len(directory), 16):
                tag = directory[idx : idx + 4]
                if tag != b"name":
                    continue
                offset, length = struct.unpack_from(">II", directory, idx + 8)
                f.seek(offset)
                table = f.read(length)
                return table if len(table) == length else None
    except Exception:
        return None
    return None


def _parse_font_names(table: bytes) -> Tuple[List[List[int]], bytes]:
    count, string_offset = struct.unpack_from(">HH", table, 2)
    records: List[List[int]] = []
    for idx in range(count):
        rec = list(struct.unpack_from(">6H", table, 6 + 12 * idx))
        records.append(rec)
    return records, table[string_offset:]


def _font_family_names(table: bytes) -> Tuple[str, List[str]]:
    records, storage = _parse_font_names(table)
    family = ""
    names: List[str] = []
    for platform_id, encoding_id, language_id, name_id, length, offset in records:
        if platform_id != 3 or encoding_id != 1 or name_id != 1:
            continue
        raw = storage[offset : offset + length]
        try:
            name = raw.decode("utf-16-be")
        except Exception:
            continue
        if language_id == 0x0409:
            family = name
        names.append(name)
    return family, names


def _rewrite_font_names(path: Path, missing_names: List[str]) -> None:
    data = path.read_bytes()
    count = struct.unpack_from(">H", data, 4)[0]
    tables: List[List[Any]] = []
    name_index = -1
    for idx in range(count):
        base = 12 + 16 * idx
        tag = data[base : base + 4]
        offset, length = struct.unpack_from(">II", data, base + 8)
        if tag == b"name":
            name_index = idx
        tables.append([tag, data[offset : offset + length]])
    if name_index < 0:
        return

    records, storage = _parse_font_names(tables[name_index][1])
    text = bytearray(storage)
    for rec in records:
        # rec = [platform, encoding, language, nameID, length, offset]
        if rec[3] != 6:
            continue
        suffix = (
            _SUBSTITUTE_POSTSCRIPT_SUFFIX.encode("ascii")
            if rec[0] == 1
            else _utf16_be(_SUBSTITUTE_POSTSCRIPT_SUFFIX)
        )
        orig_bytes = storage[rec[5] : rec[5] + rec[4]]
        rec[5] = len(text)
        rec[4] = len(orig_bytes) + len(suffix)
        text.extend(orig_bytes)
        text.extend(suffix)

    language = 0x0200
    for name in missing_names:
        while any(
            r[0] == 3 and r[1] == 1 and r[2] == language and r[3] == 1
            for r in records
        ):
            language += 1
        encoded = _utf16_be(name)
        records.append([3, 1, language, 1, len(encoded), len(text)])
        text.extend(encoded)
        language += 1

    records.sort(key=lambda r: (r[0], r[1], r[2], r[3]))
    name_header = bytearray(struct.pack(">3H", 0, len(records), 6 + 12 * len(records)))
    for rec in records:
        name_header.extend(struct.pack(">6H", *rec))
    tables[name_index][1] = bytes(name_header) + bytes(text)

    output = bytearray(data[: 12 + 16 * count])
    head_offset = -1
    for idx, (tag, table_data) in enumerate(tables):
        while len(output) % 4 != 0:
            output.append(0)
        entry_pos = 12 + 16 * idx
        struct.pack_into(">II", output, entry_pos + 8, len(output), len(table_data))
        if tag == b"head":
            head_offset = len(output)
        output.extend(table_data)
    while len(output) % 4 != 0:
        output.append(0)

    if head_offset >= 0:
        struct.pack_into(">I", output, head_offset + 8, 0)
    for idx, (_tag, table_data) in enumerate(tables):
        entry_pos = 12 + 16 * idx
        tbl_off = struct.unpack_from(">I", output, entry_pos + 8)[0]
        padded_len = (len(table_data) + 3) & ~3
        csum = _font_checksum(bytes(output[tbl_off : tbl_off + padded_len]))
        struct.pack_into(">I", output, entry_pos + 4, csum)
    if head_offset >= 0:
        adj = (0xB1B0AFBA - _font_checksum(bytes(output))) & 0xFFFFFFFF
        struct.pack_into(">I", output, head_offset + 8, adj)

    fd, tmp_name = tempfile.mkstemp(prefix=".font-", dir=str(path.parent.parent))
    try:
        with os.fdopen(fd, "wb") as tmp_file:
            tmp_file.write(output)
        os.chmod(tmp_name, 0o644)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.unlink(tmp_name)
            except Exception:
                pass


def add_windows_substitute_names(directory: Union[str, Path]) -> None:
    """Port of AIStudio2API `addWindowsSubstituteNames` (`fontnames.go`).

    Appends `-Windows` to bundled font PostScript names so they do not collide with
    macOS system fonts, and adds Windows platform family alias records for CJK/Windows
    substitute families in `Camoufox.app/Contents/Resources/fonts`.
    """
    dir_path = Path(directory)
    dir_key = str(dir_path.resolve()) if dir_path.exists() else str(dir_path)
    with _SUBSTITUTE_FONTS_LOCK:
        if dir_key in _SUBSTITUTE_FONT_DIRECTORIES or not dir_path.is_dir():
            return
        substitutes = _WINDOWS_FONTS_DATA.get("substitutes") or {}
        aliases: Dict[str, List[str]] = {}
        for alias, target in substitutes.items():
            aliases.setdefault(str(target), []).append(str(alias))
        try:
            for entry in dir_path.iterdir():
                if not entry.is_file() or entry.is_symlink():
                    continue
                table = _read_font_name_table(entry)
                if table is None:
                    continue
                family, names = _font_family_names(table)
                missing = sorted(
                    alias for alias in aliases.get(family, []) if alias not in names
                )
                if not missing:
                    continue
                _rewrite_font_names(entry, missing)
            _SUBSTITUTE_FONT_DIRECTORIES.add(dir_key)
        except Exception as exc:
            debug_logger.log_warning(f"[Camoufox] 修补 macOS 随包字体替换名失败: {exc}")


def windows_fontconfig(executable_path: Union[str, Path], profile_dir: Path) -> Optional[str]:
    """Port of AIStudio2API `windowsFontconfig` (`launcher.go`) for Linux."""
    exe_dir = Path(executable_path).resolve().parent
    conf_src = exe_dir / "fontconfig" / "windows" / "fonts.conf"
    if not conf_src.is_file():
        return None
    try:
        content = conf_src.read_text(encoding="utf-8")
        win_fonts_dir = str(exe_dir / "fonts" / "windows")
        content = content.replace('<dir prefix="cwd">fonts</dir>', f"<dir>{win_fonts_dir}</dir>")
        content = content.replace("<const>hintfull</const>", "<const>hintslight</const>")
        substitutes = _WINDOWS_FONTS_DATA.get("substitutes") or {}
        lines = ['\t<alias binding="same"><family>fantasy</family><prefer><family>Arial</family></prefer></alias>\n']
        for alias in sorted(substitutes.keys()):
            target = substitutes[alias]
            lines.append(
                f'\t<match target="pattern"><test qual="any" name="family"><string>{alias}</string></test>'
                f'<edit name="family" mode="assign" binding="same"><string>{target}</string></edit></match>\n'
            )
        content = content.replace("</fontconfig>", "".join(lines) + "</fontconfig>", 1)
        out_path = profile_dir / "fonts.conf"
        out_path.write_text(content, encoding="utf-8")
        return str(out_path)
    except Exception as exc:
        debug_logger.log_warning(f"[Camoufox] 生成 Linux fontconfig 失败: {exc}")
        return None

def _windows_voices(locale: str) -> List[Dict[str, Any]]:
    raw_voices = list(_WINDOWS_PROFILE_DATA.get("voices") or [])
    voices: List[Dict[str, Any]] = []
    for entry in raw_voices:
        if not isinstance(entry, str) or entry.count(":") < 2:
            continue
        name_lang, service = entry.rsplit(":", 1)
        name, language = name_lang.rsplit(":", 1)
        voices.append(
            {
                "name": name,
                "lang": language,
                "voiceUri": f"urn:moz-tts:sapi:{name}?{language}",
                "isDefault": False,
                "isLocalService": service == "local",
            }
        )
    if not voices:
        return []
    prefix = locale.lower().split("-", 1)[0] or "en"
    default_idx = next(
        (
            i
            for i, v in enumerate(voices)
            if locale and str(v["lang"]).lower() == locale.lower()
        ),
        -1,
    )
    if default_idx < 0:
        default_idx = next(
            (
                i
                for i, v in enumerate(voices)
                if str(v["lang"]).lower().split("-", 1)[0] == prefix
            ),
            0,
        )
    voices[default_idx]["isDefault"] = True
    return voices


def _windows_webgl() -> Dict[str, Any]:
    if not _WINDOWS_WEBGL_PROFILES:
        return {}
    total = sum(float(p.get("weight") or 0.0) for p in _WINDOWS_WEBGL_PROFILES)
    if total <= 0:
        return dict(_WINDOWS_WEBGL_PROFILES[0].get("config") or {})
    point = random.random() * total
    selected = _WINDOWS_WEBGL_PROFILES[-1]
    for profile in _WINDOWS_WEBGL_PROFILES:
        weight = float(profile.get("weight") or 0.0)
        if point < weight:
            selected = profile
            break
        point -= weight
    return dict(selected.get("config") or {})


def _normalize_camoufox_config(cfg: Dict[str, Any]) -> None:
    for key in list(cfg.keys()):
        val = cfg[key]
        derived = key.startswith(("navigator.", "screen.", "window.", "battery:"))
        if derived and key not in ("window.screenX", "window.screenY") and not val:
            cfg.pop(key, None)
    for key in ("window.history.length", "canvas:aaOffset", "canvas:aaCapOffset"):
        cfg.pop(key, None)
    if not cfg.get("fonts"):
        cfg["fonts"] = _windows_font_subset()
    language = str(cfg.get("navigator.language") or "en-US")
    cfg["voices"] = _windows_voices(language)
    if "webGl:renderer" not in cfg:
        cfg.update(_windows_webgl())
    if not any(k.startswith("mediaDevices:") for k in cfg):
        cfg["mediaDevices:enabled"] = True
        cfg["mediaDevices:micros"] = 1
        cfg["mediaDevices:webcams"] = 1
        cfg["mediaDevices:speakers"] = 0
    for seed_key in ("fonts:spacing_seed", "audio:seed", "canvas:seed"):
        if not cfg.get(seed_key):
            cfg[seed_key] = _random_seed()


def build_camoufox_fingerprint_config(
    ff_version: int = CAMOUFOX_FIREFOX_MAJOR,
    locale: str = "en-US",
    timezone: str = "America/New_York",
) -> Dict[str, Any]:
    """Generate a complete Windows Firefox Camoufox fingerprint config."""
    locale = _normalize_locale(locale)
    resolutions = [
        (1920, 1080, 1920, 1040),
        (1536, 864, 1536, 824),
        (1600, 900, 1600, 860),
        (1440, 900, 1440, 860),
        (1366, 768, 1366, 728),
    ]
    sw, sh, aw, ah = random.choice(resolutions)
    outer_w = sw
    outer_h = ah
    inner_w = outer_w
    inner_h = max(600, outer_h - random.choice([71, 79, 85]))
    version_str = f"{ff_version}.0"
    user_agent = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:{version_str}) Gecko/20100101 Firefox/{version_str}"

    cfg: Dict[str, Any] = {
        "navigator.userAgent": user_agent,
        "navigator.appCodeName": "Mozilla",
        "navigator.appName": "Netscape",
        "navigator.appVersion": "5.0 (Windows)",
        "navigator.oscpu": "Windows NT 10.0; Win64; x64",
        "navigator.platform": "Win32",
        "navigator.hardwareConcurrency": random.choice([8, 12, 16]),
        "navigator.product": "Gecko",
        "navigator.maxTouchPoints": 0,
        "screen.availHeight": ah,
        "screen.availWidth": aw,
        "screen.availTop": 0,
        "screen.availLeft": 0,
        "screen.height": sh,
        "screen.width": sw,
        "screen.colorDepth": 24,
        "screen.pixelDepth": 24,
        "window.outerHeight": outer_h,
        "window.outerWidth": outer_w,
        "window.innerHeight": inner_h,
        "window.innerWidth": inner_w,
        "window.screenX": 0,
        "window.screenY": 0,
        "headers.User-Agent": user_agent,
        "headers.Accept-Encoding": "gzip, deflate, br, zstd",
        "battery:charging": True,
        "battery:chargingTime": 0,
        "allowMainWorld": True,
        "showcursor": False,
        "fonts": _windows_font_subset(),
        "fonts:spacing_seed": _random_seed(),
    }
    _apply_locale_timezone(cfg, locale, timezone)
    _normalize_camoufox_config(cfg)
    return cfg


def load_or_create_account_camoufox_config(
    directory: Union[str, Path],
    ff_version: int = CAMOUFOX_FIREFOX_MAJOR,
    locale: str = "en-US",
    timezone: str = "America/New_York",
) -> Dict[str, Any]:
    """Load or create persistent `camoufox-fingerprint.json` in `directory`."""
    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    fp_path = dir_path / "camoufox-fingerprint.json"
    locale = _normalize_locale(locale)
    timezone = str(timezone or "").strip() or "America/New_York"

    if fp_path.is_file():
        try:
            saved = json.loads(fp_path.read_text(encoding="utf-8"))
            cfg = saved.get("config")
            if isinstance(cfg, dict) and cfg:
                version_str = f"{ff_version}.0"
                if saved.get("firefox_version") != ff_version:
                    for key in (
                        "navigator.userAgent",
                        "navigator.appVersion",
                        "headers.User-Agent",
                    ):
                        if isinstance(cfg.get(key), str):
                            cfg[key] = FIREFOX_VERSION_PATTERN.sub(
                                version_str, cfg[key]
                            )
                if saved.get("locale") != locale or saved.get("timezone") != timezone:
                    _apply_locale_timezone(cfg, locale, timezone)
                _normalize_camoufox_config(cfg)
                saved_payload = {
                    "firefox_version": ff_version,
                    "locale": locale,
                    "timezone": timezone,
                    "config": cfg,
                }
                fp_path.write_text(
                    json.dumps(saved_payload, ensure_ascii=False), encoding="utf-8"
                )
                return cfg
        except Exception as exc:
            debug_logger.log_warning(
                f"[Camoufox] 读取指纹 {fp_path} 失败，将重新生成: {exc}"
            )

    cfg = build_camoufox_fingerprint_config(ff_version, locale, timezone)
    saved_payload = {
        "firefox_version": ff_version,
        "locale": locale,
        "timezone": timezone,
        "config": cfg,
    }
    try:
        fp_path.write_text(
            json.dumps(saved_payload, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as exc:
        debug_logger.log_warning(f"[Camoufox] 写入指纹 {fp_path} 失败: {exc}")
    return cfg


def build_camoufox_env(
    fingerprint_config: Dict[str, Any],
    base_env: Optional[Dict[str, str]] = None,
    *,
    headless: bool = True,
    live_view: bool = False,
    profile_dir: Optional[Path] = None,
    executable_path: Optional[Union[str, Path]] = None,
) -> Dict[str, str]:
    """Chunk `fingerprint_config` into `CAMOU_CONFIG_1`, `CAMOU_CONFIG_2`, ... (matching AIStudio2API `launchBrowser`)."""
    cfg = dict(fingerprint_config)
    cfg["voices:blockIfNotDefined"] = True
    sys_name = platform.system().lower()
    if sys_name != "windows":
        cfg["AudioContext:maxChannelCount"] = 2
        cfg["AudioContext:sampleRate"] = 48000
        raw_fonts = [str(f) for f in (cfg.get("fonts") or []) if f]
        cfg["fonts"] = raw_fonts + _windows_font_aliases(raw_fonts)
        if cfg.get("window.outerHeight") and cfg.get("window.innerHeight") is None:
            try:
                cfg["window.innerHeight"] = int(cfg["window.outerHeight"]) - 57
            except Exception:
                pass

    env = {
        k: v
        for k, v in (base_env or os.environ).items()
        if not k.startswith("CAMOU_CONFIG_")
    }
    encoded = json.dumps(cfg, ensure_ascii=False, separators=(",", ":"))
    chunk_size = 2047
    for idx, offset in enumerate(range(0, len(encoded), chunk_size), start=1):
        env[f"CAMOU_CONFIG_{idx}"] = encoded[offset : offset + chunk_size]
    env["MOZ_HEADLESS_WIDTH"] = str(
        int(cfg.get("window.outerWidth") or 1280)
    )
    env["MOZ_HEADLESS_HEIGHT"] = str(
        int(cfg.get("window.outerHeight") or 800)
    )
    if headless and not live_view:
        env["MOZ_REMOTE_ALLOW_SYSTEM_ACCESS"] = "1"
    if sys_name == "linux" and executable_path and profile_dir is not None:
        fc_path = windows_fontconfig(executable_path, profile_dir)
        if fc_path:
            env["FONTCONFIG_FILE"] = fc_path
    if profile_dir is not None:
        env["TMP"] = str(profile_dir)
        env["TEMP"] = str(profile_dir)
        env["TMPDIR"] = str(profile_dir)
    return env


# ============================================================================
# 2. Camoufox Binary Discovery & Auto-Download
# ============================================================================


def _camoufox_binary_relative_path() -> Path:
    sys_name = platform.system().lower()
    if sys_name == "darwin":
        return Path("Camoufox.app") / "Contents" / "MacOS" / "camoufox"
    if sys_name == "windows":
        return Path("camoufox.exe")
    return Path("camoufox-bin")


def _camoufox_archive_url() -> Tuple[str, str]:
    sys_name = platform.system().lower()
    machine = platform.machine().lower()
    os_map = {"darwin": "mac", "windows": "win", "linux": "lin"}
    arch_map = {
        "x86_64": "x86_64",
        "amd64": "x86_64",
        "arm64": "arm64",
        "aarch64": "arm64",
        "i686": "i686",
        "x86": "i686",
    }
    os_tag = os_map.get(sys_name, "lin")
    arch_tag = arch_map.get(machine, "x86_64")
    archive = f"camoufox-{CAMOUFOX_RELEASE_VERSION}-{os_tag}.{arch_tag}.zip"
    url = f"https://github.com/daijro/camoufox/releases/download/v{CAMOUFOX_RELEASE_VERSION}/{archive}"
    return url, archive


def find_camoufox_executable() -> Optional[str]:
    """Find an existing Camoufox binary on the host machine."""
    rel_bin = _camoufox_binary_relative_path()
    candidates: List[Path] = []

    for env_key in ("CAMOUFOX_PATH", "BROWSER_EXECUTABLE_PATH"):
        raw = os.environ.get(env_key, "").strip()
        if raw:
            p = Path(raw)
            if p.is_dir():
                candidates.append(p / rel_bin)
            else:
                candidates.append(p)

    # Local project runtime & AIStudio2API shared runtime paths
    home = Path.home()
    candidates.extend(
        [
            Path(os.getcwd()) / "runtime" / "camoufox" / rel_bin,
            home
            / "Library"
            / "Application Support"
            / "AIStudio2API"
            / "runtime"
            / "camoufox"
            / rel_bin,
            home / "runtime" / "camoufox" / rel_bin,
            home / ".cache" / "camoufox" / rel_bin,
            home / "Library" / "Caches" / "camoufox" / rel_bin,
            home / ".local" / "share" / "camoufox" / rel_bin,
            Path("/Applications/Camoufox.app/Contents/MacOS/camoufox"),
        ]
    )

    for candidate in candidates:
        try:
            if candidate.is_file() and os.access(candidate, os.X_OK):
                return str(candidate)
        except Exception:
            continue

    which_bin = shutil.which("camoufox") or shutil.which("camoufox-bin")
    if which_bin:
        return which_bin
    return None


async def ensure_camoufox_executable(
    progress_callback: Optional[Callable[[str], None]] = None,
) -> str:
    """Locate Camoufox binary or download it into `./runtime/camoufox`."""
    existing = find_camoufox_executable()
    if existing:
        return existing

    target_dir = Path(os.getcwd()) / "runtime" / "camoufox"
    target_bin = target_dir / _camoufox_binary_relative_path()
    if target_bin.is_file() and os.access(target_bin, os.X_OK):
        return str(target_bin)

    url, archive_name = _camoufox_archive_url()
    if progress_callback:
        progress_callback(f"正在下载 Camoufox ({archive_name})...")
    debug_logger.log_info(f"[Camoufox] 未找到本地可执行文件，开始下载: {url}")

    def _download_and_extract() -> str:
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        staging_dir = Path(
            tempfile.mkdtemp(
                prefix="camoufox-install-", dir=str(target_dir.parent)
            )
        )
        zip_path = staging_dir / archive_name
        try:
            import urllib.request

            req = urllib.request.Request(
                url, headers={"User-Agent": "flow2api-camoufox-installer/1.0"}
            )
            with urllib.request.urlopen(req, timeout=300) as resp, open(
                zip_path, "wb"
            ) as out_file:
                shutil.copyfileobj(resp, out_file)

            extract_dir = staging_dir / "extracted"
            extract_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zip_path, "r") as zf:
                for info in zf.infolist():
                    dest = (extract_dir / info.filename).resolve()
                    if not str(dest).startswith(str(extract_dir.resolve())):
                        continue
                    mode = (info.external_attr >> 16) & 0xFFFF
                    if info.is_dir():
                        dest.mkdir(parents=True, exist_ok=True)
                        continue
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    # Check symlink
                    if (mode & 0o170000) == 0o120000:
                        link_target = zf.read(info).decode("utf-8")
                        if dest.exists() or dest.is_symlink():
                            dest.unlink()
                        os.symlink(link_target, dest)
                    else:
                        with zf.open(info) as src, open(dest, "wb") as dst:
                            shutil.copyfileobj(src, dst)
                        if mode & 0o111:
                            os.chmod(dest, 0o755)

            if target_dir.exists():
                shutil.rmtree(target_dir, ignore_errors=True)
            shutil.move(str(extract_dir), str(target_dir))
            if target_bin.is_file():
                os.chmod(target_bin, 0o755)
            return str(target_bin)
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)

    return await asyncio.to_thread(_download_and_extract)


# ============================================================================
# 3. Local Unauthenticated SOCKS5 Proxy Relay (for authenticated upstream proxies)
# ============================================================================


class LocalSocks5Relay:
    """Local `127.0.0.1:0` SOCKS5 server forwarding connections to upstream HTTP/SOCKS5 proxy."""

    def __init__(self, upstream_proxy_url: str):
        self.upstream_proxy_url = upstream_proxy_url.strip()
        self.parsed = urlparse(
            self.upstream_proxy_url
            if "://" in self.upstream_proxy_url
            else f"http://{self.upstream_proxy_url}"
        )
        self._server: Optional[asyncio.AbstractServer] = None
        self.host = "127.0.0.1"
        self.port = 0

    async def start(self) -> Tuple[str, int]:
        self._server = await asyncio.start_server(
            self._handle_client, self.host, 0
        )
        sockets = self._server.sockets or []
        if sockets:
            self.port = sockets[0].getsockname()[1]
        return self.host, self.port

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        upstream_writer: Optional[asyncio.StreamWriter] = None
        try:
            header = await asyncio.wait_for(reader.readexactly(2), timeout=10.0)
            if header[0] != 5:
                return
            methods = await asyncio.wait_for(
                reader.readexactly(header[1]), timeout=10.0
            )
            if 0 not in methods:
                writer.write(b"\x05\xff")
                await writer.drain()
                return
            writer.write(b"\x05\x00")
            await writer.drain()

            req = await asyncio.wait_for(reader.readexactly(4), timeout=10.0)
            if req[0] != 5 or req[1] != 1:
                writer.write(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00")
                await writer.drain()
                return
            atyp = req[3]
            if atyp == 1:
                raw_ip = await asyncio.wait_for(reader.readexactly(4), timeout=10.0)
                target_host = socket.inet_ntop(socket.AF_INET, raw_ip)
            elif atyp == 4:
                raw_ip = await asyncio.wait_for(reader.readexactly(16), timeout=10.0)
                target_host = socket.inet_ntop(socket.AF_INET6, raw_ip)
            elif atyp == 3:
                length = (
                    await asyncio.wait_for(reader.readexactly(1), timeout=10.0)
                )[0]
                target_host = (
                    await asyncio.wait_for(reader.readexactly(length), timeout=10.0)
                ).decode("utf-8", errors="replace")
            else:
                writer.write(b"\x05\x08\x00\x01\x00\x00\x00\x00\x00\x00")
                await writer.drain()
                return
            target_port = struct.unpack(
                "!H", await asyncio.wait_for(reader.readexactly(2), timeout=10.0)
            )[0]

            upstream_reader, upstream_writer = await self._dial_upstream(
                target_host, target_port
            )
            writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
            await writer.drain()

            async def _pipe(
                r: asyncio.StreamReader, w: asyncio.StreamWriter
            ) -> None:
                try:
                    while True:
                        chunk = await r.read(32768)
                        if not chunk:
                            break
                        w.write(chunk)
                        await w.drain()
                except Exception:
                    pass
                finally:
                    try:
                        w.close()
                    except Exception:
                        pass

            await asyncio.gather(
                _pipe(reader, upstream_writer), _pipe(upstream_reader, writer)
            )
        except Exception:
            try:
                writer.write(b"\x05\x01\x00\x01\x00\x00\x00\x00\x00\x00")
                await writer.drain()
            except Exception:
                pass
        finally:
            for w in (upstream_writer, writer):
                if w is not None:
                    try:
                        w.close()
                    except Exception:
                        pass

    async def _dial_upstream(
        self, target_host: str, target_port: int
    ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        scheme = (self.parsed.scheme or "http").lower()
        proxy_host = self.parsed.hostname or "127.0.0.1"
        proxy_port = self.parsed.port or (1080 if "socks" in scheme else 8080)
        username = self.parsed.username or ""
        password = self.parsed.password or ""

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(proxy_host, proxy_port), timeout=15.0
        )
        try:
            if scheme.startswith("socks5"):
                if username or password:
                    writer.write(b"\x05\x02\x00\x02")
                else:
                    writer.write(b"\x05\x01\x00")
                await writer.drain()
                resp = await asyncio.wait_for(reader.readexactly(2), timeout=10.0)
                if resp[1] == 2:
                    u_bytes = username.encode("utf-8")
                    p_bytes = password.encode("utf-8")
                    writer.write(
                        bytes([1, len(u_bytes)])
                        + u_bytes
                        + bytes([len(p_bytes)])
                        + p_bytes
                    )
                    await writer.drain()
                    auth_resp = await asyncio.wait_for(
                        reader.readexactly(2), timeout=10.0
                    )
                    if auth_resp[1] != 0:
                        raise RuntimeError("SOCKS5 upstream auth failed")
                elif resp[1] != 0:
                    raise RuntimeError(f"SOCKS5 upstream method rejected: {resp[1]}")

                host_bytes = target_host.encode("utf-8")
                writer.write(
                    b"\x05\x01\x00\x03"
                    + bytes([len(host_bytes)])
                    + host_bytes
                    + struct.pack("!H", target_port)
                )
                await writer.drain()
                conn_resp = await asyncio.wait_for(
                    reader.readexactly(4), timeout=15.0
                )
                if conn_resp[1] != 0:
                    raise RuntimeError(
                        f"SOCKS5 upstream connect failed: {conn_resp[1]}"
                    )
                atyp = conn_resp[3]
                if atyp == 1:
                    await reader.readexactly(6)
                elif atyp == 4:
                    await reader.readexactly(18)
                elif atyp == 3:
                    ln = (await reader.readexactly(1))[0]
                    await reader.readexactly(ln + 2)
                return reader, writer

            # HTTP CONNECT proxy
            connect_lines = [
                f"CONNECT {target_host}:{target_port} HTTP/1.1",
                f"Host: {target_host}:{target_port}",
            ]
            if username or password:
                token = base64.b64encode(
                    f"{username}:{password}".encode("utf-8")
                ).decode("ascii")
                connect_lines.append(f"Proxy-Authorization: Basic {token}")
            connect_lines.append("\r\n")
            writer.write("\r\n".join(connect_lines).encode("utf-8"))
            await writer.drain()

            status_line = await asyncio.wait_for(reader.readline(), timeout=15.0)
            if b" 200 " not in status_line:
                raise RuntimeError(
                    f"HTTP proxy CONNECT failed: {status_line.decode('utf-8', errors='replace').strip()}"
                )
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=10.0)
                if line in (b"\r\n", b"\n", b""):
                    break
            return reader, writer
        except BaseException:
            writer.close()
            raise


# ============================================================================
# 4. Pure-Python WebDriver BiDi Client
# ============================================================================


def _from_bidi_remote_value(remote: Any) -> Any:
    """Recursively decode a WebDriver BiDi RemoteValue into a native Python value."""
    if not isinstance(remote, dict):
        return remote
    rtype = remote.get("type")
    if rtype in ("undefined", "null"):
        return None
    if rtype in ("string", "boolean", "bigint"):
        return remote.get("value")
    if rtype == "number":
        val = remote.get("value")
        if isinstance(val, (int, float)):
            return val
        return 0
    if rtype == "array":
        items = remote.get("value")
        if isinstance(items, list):
            return [_from_bidi_remote_value(item) for item in items]
        return []
    if rtype in ("object", "map"):
        pairs = remote.get("value")
        out: Dict[str, Any] = {}
        if isinstance(pairs, list):
            for pair in pairs:
                if isinstance(pair, list) and len(pair) == 2:
                    raw_key, raw_val = pair
                    key = (
                        _from_bidi_remote_value(raw_key)
                        if isinstance(raw_key, dict)
                        else str(raw_key)
                    )
                    out[str(key)] = _from_bidi_remote_value(raw_val)
        return out
    return remote.get("value")


def _to_bidi_local_value(value: Any) -> Dict[str, Any]:
    """Encode a native Python value into a WebDriver BiDi LocalValue."""
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "boolean", "value": value}
    if isinstance(value, (int, float)):
        return {"type": "number", "value": value}
    if isinstance(value, str):
        return {"type": "string", "value": value}
    if isinstance(value, (list, tuple)):
        return {
            "type": "array",
            "value": [_to_bidi_local_value(item) for item in value],
        }
    if isinstance(value, dict):
        return {
            "type": "object",
            "value": [
                [str(k), _to_bidi_local_value(v)] for k, v in value.items()
            ],
        }
    return {"type": "string", "value": str(value)}


class BiDiChannel:
    """Receives `script.message` events for a registered BiDi channel."""

    def __init__(self, client: "BiDiClient", name: str):
        self.client = client
        self.name = name
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self._closed = False

    def push(self, data: Any) -> None:
        if not self._closed:
            self._queue.put_nowait(data)

    def fail(self, exc: Exception) -> None:
        if not self._closed:
            self._closed = True
            self._queue.put_nowait(exc)

    async def next_text(self, timeout: float = 90.0) -> str:
        item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
        if isinstance(item, Exception):
            raise item
        val = _from_bidi_remote_value(item)
        return str(val or "")

    def close(self) -> None:
        self._closed = True
        self.client._channels.pop(self.name, None)


class BiDiClient:
    """Async WebDriver BiDi client over WebSocket (supports `websockets` or `aiohttp`)."""

    def __init__(self, ws_send: Callable[[str], Any], ws_close: Callable[[], Any]):
        self._ws_send = ws_send
        self._ws_close = ws_close
        self._send_lock = asyncio.Lock()
        self._next_id = 0
        self._pending: Dict[int, asyncio.Future] = {}
        self._channels: Dict[str, BiDiChannel] = {}
        self._event_listeners: List[Callable[[str, Dict[str, Any]], None]] = []
        self._closed = False
        self._failure: Optional[Exception] = None
        self._reader_task: Optional[asyncio.Task] = None

    @staticmethod
    def _normalize_endpoint(endpoint: str) -> str:
        parsed = urlparse(endpoint.strip())
        if parsed.path in ("", "/"):
            return endpoint.strip().rstrip("/") + "/session"
        return endpoint.strip()

    @classmethod
    async def connect(cls, endpoint: str, timeout: float = 30.0) -> "BiDiClient":
        endpoint = cls._normalize_endpoint(endpoint)
        if websockets is not None:
            ws = await asyncio.wait_for(
                websockets.connect(
                    endpoint,
                    max_size=64 * 1024 * 1024,
                    ping_interval=None,
                ),
                timeout=timeout,
            )
            client = cls(ws.send, ws.close)

            async def _read_loop() -> None:
                try:
                    async for raw in ws:
                        text = (
                            raw.decode("utf-8", errors="replace")
                            if isinstance(raw, (bytes, bytearray))
                            else str(raw)
                        )
                        client._handle_message(text)
                except Exception as exc:
                    client._fail_all(exc)
                finally:
                    client._fail_all(RuntimeError("BiDi WebSocket closed"))

            client._reader_task = asyncio.create_task(_read_loop())
            return client

        if aiohttp is not None:
            session = aiohttp.ClientSession()
            try:
                ws = await asyncio.wait_for(
                    session.ws_connect(
                        endpoint, max_msg_size=64 * 1024 * 1024, heartbeat=None
                    ),
                    timeout=timeout,
                )
            except BaseException:
                await session.close()
                raise

            async def _close_all() -> None:
                try:
                    await ws.close()
                finally:
                    await session.close()

            client = cls(ws.send_str, _close_all)

            async def _read_loop() -> None:
                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            client._handle_message(msg.data)
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                            client._handle_message(
                                msg.data.decode("utf-8", errors="replace")
                            )
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
                except Exception as exc:
                    client._fail_all(exc)
                finally:
                    client._fail_all(RuntimeError("BiDi WebSocket closed"))
                    await session.close()

            client._reader_task = asyncio.create_task(_read_loop())
            return client

        raise RuntimeError(
            "缺少 WebSocket 客户端依赖：请安装 websockets 或 aiohttp"
        )

    def _fail_all(self, exc: Exception) -> None:
        if self._closed:
            return
        self._closed = True
        self._failure = exc
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(exc)
        self._pending.clear()
        for ch in list(self._channels.values()):
            ch.fail(exc)
        self._channels.clear()

    def _handle_message(self, raw_text: str) -> None:
        try:
            envelope = json.loads(raw_text)
        except Exception:
            return
        if not isinstance(envelope, dict):
            return
        msg_id = envelope.get("id")
        if msg_id is not None:
            fut = self._pending.pop(int(msg_id), None)
            if fut is not None and not fut.done():
                fut.set_result(envelope)
            return
        method = str(envelope.get("method") or "")
        params = envelope.get("params")
        if not isinstance(params, dict):
            params = {}
        if method == "script.message":
            ch_name = str(params.get("channel") or "")
            ch = self._channels.get(ch_name)
            if ch is not None:
                ch.push(params.get("data"))
        for listener in list(self._event_listeners):
            try:
                listener(method, params)
            except Exception:
                pass

    def add_listener(
        self, callback: Callable[[str, Dict[str, Any]], None]
    ) -> None:
        if callback not in self._event_listeners:
            self._event_listeners.append(callback)

    def remove_listener(
        self, callback: Callable[[str, Dict[str, Any]], None]
    ) -> None:
        if callback in self._event_listeners:
            self._event_listeners.remove(callback)

    def open_channel(self, name: str) -> BiDiChannel:
        ch = BiDiChannel(self, name)
        self._channels[name] = ch
        return ch
    async def command(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 60.0,
    ) -> Dict[str, Any]:
        if self._closed:
            raise self._failure or RuntimeError("BiDi connection is closed")
        loop = asyncio.get_running_loop()
        self._next_id += 1
        req_id = self._next_id
        fut: asyncio.Future = loop.create_future()
        self._pending[req_id] = fut
        payload = json.dumps(
            {"id": req_id, "method": method, "params": params or {}},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        try:
            async with self._send_lock:
                await asyncio.wait_for(self._ws_send(payload), timeout=timeout)
            envelope = await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._pending.pop(req_id, None)

        if envelope.get("type") == "error" or "error" in envelope:
            err_code = envelope.get("error") or "unknown"
            err_msg = envelope.get("message") or ""
            raise RuntimeError(f"BiDi {method} failed: {err_code} ({err_msg})")
        result = envelope.get("result")
        return result if isinstance(result, dict) else {}

    async def evaluate(
        self,
        context_id: str,
        expression: str,
        *,
        await_promise: bool = True,
        sandbox: str = "",
        user_activation: bool = False,
        timeout: float = 45.0,
    ) -> Any:
        target: Dict[str, Any] = {"context": context_id}
        if sandbox:
            target["sandbox"] = sandbox
        params: Dict[str, Any] = {
            "expression": expression,
            "target": target,
            "awaitPromise": await_promise,
            "resultOwnership": "none",
        }
        if user_activation:
            params["userActivation"] = True
        res = await self.command("script.evaluate", params, timeout=timeout)
        if res.get("type") == "exception":
            details = res.get("exceptionDetails") or {}
            text = details.get("text") or "JS evaluation exception"
            raise RuntimeError(f"BiDi script.evaluate exception: {text}")
        return _from_bidi_remote_value(res.get("result"))

    async def call_function(
        self,
        context_id: str,
        function_declaration: str,
        arguments: Optional[List[Dict[str, Any]]] = None,
        *,
        await_promise: bool = True,
        sandbox: str = "",
        user_activation: bool = False,
        timeout: float = 60.0,
    ) -> Any:
        target: Dict[str, Any] = {"context": context_id}
        if sandbox:
            target["sandbox"] = sandbox
        params: Dict[str, Any] = {
            "functionDeclaration": function_declaration,
            "target": target,
            "awaitPromise": await_promise,
            "resultOwnership": "none",
            "arguments": arguments or [],
        }
        if user_activation:
            params["userActivation"] = True
        res = await self.command("script.callFunction", params, timeout=timeout)
        if res.get("type") == "exception":
            details = res.get("exceptionDetails") or {}
            text = details.get("text") or "JS callFunction exception"
            raise RuntimeError(f"BiDi script.callFunction exception: {text}")
        return _from_bidi_remote_value(res.get("result"))

    async def close(self) -> None:
        self._fail_all(RuntimeError("BiDi client closed"))
        try:
            await self._ws_close()
        except Exception:
            pass
        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
            try:
                await self._reader_task
            except BaseException:
                pass


# ============================================================================
# 5. Firefox Profile & Preferences Writer (matching AIStudio2API launcher.go)
# ============================================================================


def _firefox_pref_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    return json.dumps(str(value), ensure_ascii=False)


def write_camoufox_user_js(
    profile_dir: Path,
    fingerprint_config: Dict[str, Any],
    *,
    proxy_url: Optional[str] = None,
    relay_addr: Optional[Tuple[str, int]] = None,
    headless: bool = True,
    live_view: bool = False,
    cache_dir: Optional[Path] = None,
    executable_path: Optional[Union[str, Path]] = None,
) -> None:
    """Write `user.js` preferences matching AIStudio2API `launchBrowser` + `firefoxPreferences`."""
    prefs: Dict[str, Any] = {
        "remote.active-protocols": 1,
        "devtools.chrome.enabled": True,
        "devtools.debugger.remote-enabled": True,
        "browser.shell.checkDefaultBrowser": False,
        "webgl.force-enabled": True,
        "extensions.webextensions.remote": False,
        "network.process.enabled": False,
        "media.rdd-process.enabled": False,
        "media.utility-process.enabled": False,
        "media.peerconnection.enabled": True,
        "javascript.options.mem.nursery.max_kb": 4096,
        "gfx.webrender.max-shared-surface-size": 512,
        "gfx.display.frame-rate-divisor": 1,
    }
    sys_name = platform.system().lower()
    if sys_name != "windows":
        prefs["dom.webgpu.enabled"] = True
        prefs["widget.non-native-theme.scrollbar.style"] = 4
        prefs["dom.quotaManager.temporaryStorage.fixedLimit"] = 50 << 20
        prefs["ui.prefersReducedMotion"] = 0
        font_prefs = _WINDOWS_FONTS_DATA.get("prefs")
        if isinstance(font_prefs, dict):
            for k, v in font_prefs.items():
                prefs[k] = int(v) if isinstance(v, float) and v.is_integer() else v

    if sys_name == "darwin":
        exe = executable_path or find_camoufox_executable()
        if exe:
            fonts_dir = Path(exe).resolve().parent.parent / "Resources" / "fonts"
            add_windows_substitute_names(fonts_dir)
        # gfx.font_loader.delay = 0 makes Firefox load localized & weighted font family names synchronously
        prefs["gfx.font_loader.delay"] = 0
        # Draw in parent process where bundled Resources/fonts are registered with CoreText
        prefs["layers.gpu-process.enabled"] = False
        prefs["webgl.allow-in-parent"] = True
    # Proxy configuration
    if relay_addr is not None:
        r_host, r_port = relay_addr
        prefs.update(
            {
                "network.proxy.type": 1,
                "network.proxy.socks": r_host,
                "network.proxy.socks_port": int(r_port),
                "network.proxy.socks_version": 5,
                "network.proxy.socks_remote_dns": True,
                "network.proxy.no_proxies_on": "localhost, 127.0.0.1, ::1",
            }
        )
    elif proxy_url:
        parsed = urlparse(
            proxy_url if "://" in proxy_url else f"http://{proxy_url}"
        )
        scheme = (parsed.scheme or "http").lower()
        p_host = parsed.hostname or "127.0.0.1"
        p_port = parsed.port or (1080 if "socks" in scheme else 8080)
        prefs["network.proxy.type"] = 1
        prefs["network.proxy.no_proxies_on"] = "localhost, 127.0.0.1, ::1"
        if "socks" in scheme:
            prefs["network.proxy.socks"] = p_host
            prefs["network.proxy.socks_port"] = int(p_port)
            prefs["network.proxy.socks_version"] = 4 if "socks4" in scheme else 5
            prefs["network.proxy.socks_remote_dns"] = scheme != "socks5"
        else:
            prefs["network.proxy.http"] = p_host
            prefs["network.proxy.http_port"] = int(p_port)
            prefs["network.proxy.ssl"] = p_host
            prefs["network.proxy.ssl_port"] = int(p_port)

    lines = [
        f'user_pref({json.dumps(k)}, {_firefox_pref_literal(v)});'
        for k, v in sorted(prefs.items())
    ]
    (profile_dir / "user.js").write_text("\n".join(lines) + "\n", encoding="utf-8")
    prefs_js_path = profile_dir / "prefs.js"
    if prefs_js_path.is_file():
        try:
            stale_prefixes = (
                'user_pref("camoufox.blocked"',
                'user_pref("dom.ipc.processCount',
                'user_pref("javascript.options.baselinejit_threshold"',
                'user_pref("javascript.options.ion_threshold"',
                'user_pref("javascript.options.compact_on_user_inactive_delay"',
                'user_pref("javascript.options.mem.gc_',
                'user_pref("browser.sessionhistory.',
                'user_pref("browser.sessionstore.',
                'user_pref("image.mem.surfacecache.',
                'user_pref("layout.frame_rate"',
                'user_pref("media.autoplay.default"',
                'user_pref("network.dns.disablePrefetch"',
                'user_pref("network.http.speculative-parallel-limit"',
                'user_pref("network.prefetch-next"',
                'user_pref("remote.prefs.recommended"',
                'user_pref("browser.cache.disk.',
                'user_pref("gfx.webrender.enable-subpixel-aa"',
                'user_pref("gfx.display.frame-rate-divisor", 60)',
            )
            raw_prefs = prefs_js_path.read_text(encoding="utf-8").splitlines()
            cleaned_prefs = [
                ln for ln in raw_prefs if not ln.startswith(stale_prefixes)
            ]
            if len(cleaned_prefs) != len(raw_prefs):
                prefs_js_path.write_text(
                    "\n".join(cleaned_prefs) + "\n", encoding="utf-8"
                )
        except Exception:
            pass


# ============================================================================
# 6. In-Page Protected Pump & Flow Batchexecute/StreamChat Scripts
# ============================================================================

# Installed in the Flow tab's main world: captures trusted user-activation gestures
# and provides `window.__flow2apiProtectedSend` + `window.__flow2apiExecuteRpc`
FLOW_PROTECTED_PUMP_SOURCE = r"""
(() => {
    if (window.__flow2apiProtectedRuntimeInstalled) return true;
    const state = {
        grants: 0,
        queue: [],
        waking: false,
        wake() {
            if (state.waking || state.grants <= 0 || state.queue.length === 0) return;
            state.waking = true;
            setTimeout(() => {
                state.waking = false;
                while (state.grants > 0 && state.queue.length > 0) {
                    state.grants--;
                    const run = state.queue.shift();
                    try { run(); } catch (_) {}
                }
            }, 0);
        }
    };

    window.addEventListener('keydown', (event) => {
        if (!event.isTrusted) return;
        if (event.key === '?' || event.key === 'Shift') {
            event.preventDefault();
            event.stopImmediatePropagation();
        }
        state.grants = 64;
        state.wake();
    }, true);

    Object.defineProperty(window, '__flow2apiProtectedRuntimeInstalled', {
        value: state,
        configurable: false
    });
    return true;
})()
"""


# ============================================================================
# 7. CamoufoxWorker: Per-Slot Persistent Browser & BiDi Session
# ============================================================================


class CamoufoxWorker:
    """Single Camoufox browser instance controlled via WebDriver BiDi (AIStudio2API style).

    Compatible with `TokenBrowser` interface expected by `BrowserCaptchaService`.
    """

    def __init__(self, token_id: int, user_data_dir: str, db: Any = None):
        self.token_id = token_id
        self.user_data_dir = Path(user_data_dir)
        self.user_data_dir.mkdir(parents=True, exist_ok=True)
        self.db = db

        self._semaphore = asyncio.Semaphore(1)
        self._shared_browser_lock = asyncio.Lock()
        self._frame_lock = asyncio.Lock()
        self._frame_boosts = 0

        self._process: Optional[asyncio.subprocess.Process] = None
        self._bidi: Optional[BiDiClient] = None
        self._context_id: Optional[str] = None
        self._relay: Optional[LocalSocks5Relay] = None
        self._profile_dir: Optional[Path] = None

        self._shared_browser_pid: Optional[int] = None
        self._shared_proxy_url: Optional[str] = None
        self._shared_bound_token_id: Optional[int] = None
        self._shared_bound_cookie_signature: Optional[str] = None
        self._browser_proxy_active: bool = False

        self._runtime_generation: int = 0
        self._solve_inflight: int = 0
        self._solve_count: int = 0
        self._error_count: int = 0
        self._consecutive_browser_failures: int = 0
        self._shared_launch_count: int = 0
        self._shared_reuse_count: int = 0
        self._last_idle_since: float = time.monotonic()

        self._last_fingerprint: Optional[Dict[str, Any]] = None
        self._last_harvest_session_id: Optional[str] = None
        self._harvest_streamchat_post: Optional[str] = None
        self._headless: bool = True
        self._is_persistent_profile: bool = False
        self._warmed_projects: set[str] = set()
        self._last_stream_chat_at: float = 0.0
        self._cached_wiz: Dict[str, str] = {}
        self._consecutive_risk_failures: int = 0
    def is_busy(self) -> bool:
        return self._solve_inflight > 0

    def note_idle(self) -> None:
        if self._solve_inflight <= 0:
            self._last_idle_since = time.monotonic()

    def idle_seconds(self) -> float:
        if self.is_busy():
            return 0.0
        return max(0.0, time.monotonic() - self._last_idle_since)

    def has_shared_browser(self) -> bool:
        return bool(
            self._process is not None
            and self._process.returncode is None
            and self._bidi is not None
            and not self._bidi._closed
            and self._context_id
        )

    def get_last_fingerprint(self) -> Optional[Dict[str, Any]]:
        if not self._last_fingerprint:
            return None
        return dict(self._last_fingerprint)

    def get_last_harvest_session_id(self) -> Optional[str]:
        return self._last_harvest_session_id

    def note_recaptcha_rejected(self) -> None:
        """Mark resident projects as needing fresh human warmup on next retry without killing the browser."""
        self._warmed_projects.clear()

    @staticmethod
    def _normalize_token_key(token_id: Optional[int]) -> Optional[int]:
        try:
            normalized = int(token_id or 0)
        except Exception:
            normalized = 0
        return normalized if normalized > 0 else None

    async def _resolve_proxy_runtime_config(
        self, token_proxy_url: Optional[str] = None
    ) -> Tuple[Optional[str], str]:
        raw_proxy_url = str(token_proxy_url or "").strip()
        if raw_proxy_url:
            if "://" not in raw_proxy_url:
                raw_proxy_url = f"http://{raw_proxy_url}"
            return raw_proxy_url, "token_captcha_proxy"

        if self.db and hasattr(self.db, "get_captcha_config"):
            try:
                captcha_cfg = await self.db.get_captcha_config()
                if getattr(captcha_cfg, "browser_proxy_enabled", False):
                    cfg_url = str(
                        getattr(captcha_cfg, "browser_proxy_url", "") or ""
                    ).strip()
                    if cfg_url:
                        if "://" not in cfg_url:
                            cfg_url = f"http://{cfg_url}"
                        return cfg_url, "global_browser_proxy"
            except Exception:
                pass
        return None, "none"

    async def _chrome_evaluate(self, expression: str) -> Any:
        """Execute privileged chrome JS inside `browser.xhtml` (for 1fps divisor & renderLayers)."""
        if not self._bidi:
            return None
        tree = await self._bidi.command(
            "browsingContext.getTree", {"maxDepth": 0, "moz:scope": "chrome"}
        )
        for item in tree.get("contexts") or []:
            if isinstance(item, dict) and item.get("url") == BROWSER_WINDOW_URL:
                win_ctx = str(item.get("context") or "")
                if win_ctx:
                    return await self._bidi.evaluate(win_ctx, expression)
        return None

    async def _set_tab_render_layers(self, enabled: bool) -> None:
        _ = enabled
        return

    @asynccontextmanager
    async def _boost_frames(self):
        """Ensure Camoufox frame rate is at 60fps during interactive operations without polluting prefs.js with 1fps."""
        if not self._headless or not self._bidi:
            yield
            return
        async with self._frame_lock:
            if self._frame_boosts == 0:
                try:
                    await self._chrome_evaluate(
                        "Services.prefs.setIntPref('gfx.display.frame-rate-divisor', 1)"
                    )
                except Exception:
                    pass
            self._frame_boosts += 1
        try:
            yield
        finally:
            async with self._frame_lock:
                self._frame_boosts = max(0, self._frame_boosts - 1)

    def _resolve_account_dir(self, token_id: Optional[int]) -> Path:
        """Resolve the shared account directory `browser_data_rt/token_<id>` so LoginSession and Worker share identical fingerprint, cache, and Firefox profile."""
        if re.match(r"^browser_\d+$", self.user_data_dir.name):
            root_dir = self.user_data_dir.parent
        elif self.user_data_dir.name.startswith("token_"):
            return self.user_data_dir
        else:
            root_dir = self.user_data_dir
        if token_id:
            account_dir = root_dir / f"token_{int(token_id)}"
            legacy_dir = self.user_data_dir / f"token_{int(token_id)}"
            if legacy_dir != account_dir and legacy_dir.is_dir():
                account_dir.mkdir(parents=True, exist_ok=True)
                for sub_name in ("camoufox-cache", "storage-state.json"):
                    src_item = legacy_dir / sub_name
                    dst_item = account_dir / sub_name
                    if src_item.exists() and not dst_item.exists():
                        try:
                            if src_item.is_dir():
                                shutil.copytree(src_item, dst_item, dirs_exist_ok=True)
                            else:
                                shutil.copy2(src_item, dst_item)
                        except Exception:
                            pass
            return account_dir
        return self.user_data_dir / "shared"

    async def _launch_camoufox(
        self, token_proxy_url: Optional[str] = None, token_id: Optional[int] = None
    ) -> None:
        exe_path = await ensure_camoufox_executable()
        raw_proxy_url, _ = await self._resolve_proxy_runtime_config(token_proxy_url)
        self._browser_proxy_active = bool(raw_proxy_url)

        account_dir = self._resolve_account_dir(token_id)
        account_dir.mkdir(parents=True, exist_ok=True)
        fp_config = load_or_create_account_camoufox_config(
            account_dir, CAMOUFOX_FIREFOX_MAJOR, "en-US", "America/New_York"
        )

        # Start local SOCKS5 relay if upstream proxy has credentials or is SOCKS5
        relay_addr: Optional[Tuple[str, int]] = None
        if raw_proxy_url:
            parsed_proxy = urlparse(raw_proxy_url)
            if parsed_proxy.username or parsed_proxy.password or "socks" in (
                parsed_proxy.scheme or ""
            ).lower():
                self._relay = LocalSocks5Relay(raw_proxy_url)
                relay_addr = await self._relay.start()

        if token_id:
            profile_dir = account_dir / "firefox-profile"
            profile_dir.mkdir(parents=True, exist_ok=True)
            _clear_stale_firefox_profile_locks(profile_dir)
            self._is_persistent_profile = True
        else:
            profile_dir = Path(
                tempfile.mkdtemp(prefix=f"flow2api-camoufox-{self.token_id}-")
            )
            self._is_persistent_profile = False
        self._profile_dir = profile_dir
        self._account_dir = account_dir
        self._warmed_projects.clear()
        pass
        cache_dir = account_dir / "camoufox-cache"
        # Headless by default (like AIStudio2API Worker), unless explicitly disabled
        self._headless = bool(
            os.environ.get("CAMOUFOX_HEADLESS", "1").strip() not in ("0", "false", "False")
        )
        write_camoufox_user_js(
            profile_dir,
            fp_config,
            proxy_url=raw_proxy_url,
            relay_addr=relay_addr,
            headless=self._headless,
            live_view=False,
            cache_dir=cache_dir,
            executable_path=exe_path,
        )

        args = [exe_path]
        if self._headless:
            args.append("--headless")
        args.extend(["--remote-debugging-port=0", "-wait-for-browser", "-profile", str(profile_dir)])

        env = build_camoufox_env(
            fp_config,
            headless=self._headless,
            live_view=False,
            profile_dir=profile_dir,
            executable_path=exe_path,
        )
        self._process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
        )
        self._shared_browser_pid = self._process.pid

        # Wait for `WebDriver BiDi listening on ws://...`
        endpoint = await self._wait_for_bidi_endpoint(
            self._process, timeout=45.0
        )
        self._bidi = await BiDiClient.connect(endpoint, timeout=30.0)
        await self._bidi.command("session.new", {"capabilities": {}})
        try:
            await self._bidi.command(
                "session.subscribe", {"events": ["script.message"]}
            )
        except Exception:
            pass
        pass

        tree = await self._bidi.command(
            "browsingContext.getTree", {"maxDepth": 0}
        )
        contexts = tree.get("contexts") or []
        if not contexts or not isinstance(contexts[0], dict):
            raise RuntimeError("Camoufox BiDi 未返回初始标签页")
        self._context_id = str(contexts[0].get("context") or "")
        if not self._context_id:
            raise RuntimeError("Camoufox BiDi 初始 context ID 为空")

        ua = str(
            fp_config.get("navigator.userAgent")
            or fp_config.get("headers.User-Agent")
            or ""
        )
        accept_lang = str(
            fp_config.get("headers.Accept-Language") or "en-US,en;q=0.9"
        )
        self._last_fingerprint = {
            "user_agent": ua,
            "accept_language": accept_lang,
            "proxy_url": raw_proxy_url,
            "platform": str(fp_config.get("navigator.platform") or "Win32"),
            "language": str(fp_config.get("navigator.language") or "en-US"),
            "languages": list(
                fp_config.get("navigator.languages") or ["en-US", "en"]
            ),
            "timezone": str(fp_config.get("timezone") or "America/New_York"),
        }
        self._shared_proxy_url = raw_proxy_url
        self._shared_launch_count += 1
        self._shared_reuse_count = 0
        debug_logger.log_info(
            f"[Camoufox] Slot-{self.token_id} 进程已启动 (PID={self._shared_browser_pid}, BiDi={endpoint}, UA={ua[:80]})"
        )

    @staticmethod
    async def _wait_for_bidi_endpoint(
        proc: asyncio.subprocess.Process, timeout: float = 45.0
    ) -> str:
        recent_lines: deque[str] = deque(maxlen=25)
        deadline = time.monotonic() + timeout
        assert proc.stdout is not None
        while time.monotonic() < deadline:
            remaining = max(0.5, deadline - time.monotonic())
            line_bytes = await asyncio.wait_for(
                proc.stdout.readline(), timeout=remaining
            )
            if not line_bytes:
                break
            line = line_bytes.decode("utf-8", errors="replace").strip()
            if line:
                recent_lines.append(line)
            match = BIDI_ENDPOINT_PATTERN.search(line)
            if match:
                return match.group(1)

        # Drain remaining stdout in background if found; otherwise error
        tail = " | ".join(recent_lines)
        raise RuntimeError(f"Camoufox 未输出 WebDriver BiDi 端点: {tail}")

    async def _install_cookies_via_bidi(self, cookies: List[Dict[str, Any]]) -> None:
        if not self._bidi or not cookies:
            return
        for cookie in cookies:
            name = str(cookie.get("name") or "").strip()
            value = str(cookie.get("value") or "")
            if not name:
                continue
            domain = str(cookie.get("domain") or "").strip()
            url = str(cookie.get("url") or "").strip()
            if not domain and url:
                domain = urlparse(url).hostname or "flow.google.com"
            if not domain:
                domain = "flow.google.com"
            host_only = bool(cookie.get("hostOnly", False)) or (
                bool(url) and not str(cookie.get("domain") or "").startswith(".")
            )
            if host_only or name.startswith("__Host-"):
                domain = domain.lstrip(".")

            path = str(cookie.get("path") or "/")
            if name.startswith("__Host-"):
                path = "/"
            secure = bool(
                cookie.get("secure", True)
                or name.startswith(("__Secure-", "__Host-"))
            )
            http_only = bool(cookie.get("httpOnly", False))
            same_site_raw = str(cookie.get("sameSite") or "").strip().lower()
            same_site = (
                same_site_raw
                if same_site_raw in ("strict", "lax", "none")
                else ("none" if secure else "lax")
            )

            bidi_cookie: Dict[str, Any] = {
                "name": name,
                "value": {"type": "string", "value": value},
                "domain": domain,
                "path": path,
                "secure": secure,
                "httpOnly": http_only,
                "sameSite": same_site,
            }
            expires = cookie.get("expires")
            if isinstance(expires, (int, float)) and expires > 0:
                bidi_cookie["expiry"] = int(expires)

            params: Dict[str, Any] = {"cookie": bidi_cookie}
            partition_key = str(cookie.get("partitionKey") or "").strip()
            if partition_key:
                params["partition"] = {
                    "type": "storageKey",
                    "sourceOrigin": partition_key,
                }
            try:
                await self._bidi.command("storage.setCookie", params, timeout=10.0)
            except Exception as exc:
                if "partition" in params:
                    params.pop("partition", None)
                    try:
                        await self._bidi.command(
                            "storage.setCookie", params, timeout=10.0
                        )
                    except Exception:
                        debug_logger.log_warning(
                            f"[Camoufox] 设置 Cookie {name} 失败: {exc}"
                        )
                else:
                    debug_logger.log_warning(
                        f"[Camoufox] 设置 Cookie {name} 失败: {exc}"
                    )

    async def _export_live_google_cookies(self) -> List[Dict[str, Any]]:
        """Export current Google/Flow cookies from Camoufox via `storage.getCookies`."""
        if not self._bidi or self._bidi._closed:
            return []
        try:
            cookie_res = await asyncio.wait_for(
                self._bidi.command("storage.getCookies", {}), timeout=6.0
            )
        except Exception:
            return []
        raw_cookies = cookie_res.get("cookies") or []
        exported: List[Dict[str, Any]] = []
        for item in raw_cookies:
            if not isinstance(item, dict):
                continue
            domain = str(item.get("domain") or "").strip()
            if not (
                "google." in domain
                or domain.endswith("youtube.com")
            ):
                continue
            val_obj = item.get("value")
            val_str = (
                str(val_obj.get("value") or "")
                if isinstance(val_obj, dict)
                else str(val_obj or "")
            )
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            same_site_raw = str(item.get("sameSite") or "lax").strip().lower()
            same_site = (
                "None"
                if same_site_raw == "none"
                else ("Strict" if same_site_raw == "strict" else "Lax")
            )
            entry: Dict[str, Any] = {
                "name": name,
                "value": val_str,
                "domain": domain,
                "hostOnly": not domain.startswith("."),
                "path": str(item.get("path") or "/"),
                "secure": bool(item.get("secure", True)),
                "httpOnly": bool(item.get("httpOnly", False)),
                "sameSite": same_site,
            }
            expiry = item.get("expiry")
            if isinstance(expiry, (int, float)) and expiry > 0:
                entry["expires"] = int(expiry)
            exported.append(entry)
        return exported

    async def _sync_live_cookies_to_db(
        self, token_id: Optional[int] = None, *, update_db: bool = True
    ) -> Optional[str]:
        """Persist rotated Google/Flow cookies (`__Secure-1PSIDTS`, `SIDCC`, `OSID`, etc.) back to `storage-state.json` and SQLite."""
        bound_id = self._normalize_token_key(token_id) or self._shared_bound_token_id
        exported = await self._export_live_google_cookies()
        if not exported:
            return None
        cookie_names = {c.get("name") for c in exported}
        if not (
            cookie_names.intersection({"SID", "__Secure-1PSID", "__Secure-3PSID"})
            and cookie_names.intersection({"OSID", "__Secure-OSID"})
        ):
            return None
        serialized = json.dumps(exported, ensure_ascii=False, separators=(",", ":"))
        new_sig = (
            build_cookie_signature(serialized)
            or hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        )
        self._shared_bound_cookie_signature = new_sig

        # Update storage-state.json in account_dir and shared token_<id> dir
        dirs_to_update: List[Path] = []
        account_dir = getattr(self, "_account_dir", None)
        if isinstance(account_dir, Path) and account_dir.exists():
            dirs_to_update.append(account_dir)
        if bound_id is not None:
            shared_token_dir = self.user_data_dir.parent / f"token_{bound_id}"
            if shared_token_dir.exists() and shared_token_dir not in dirs_to_update:
                dirs_to_update.append(shared_token_dir)
        for d in dirs_to_update:
            ss_path = d / "storage-state.json"
            try:
                state_obj: Dict[str, Any] = {"cookies": exported, "origins": []}
                if ss_path.is_file():
                    old_obj = json.loads(ss_path.read_text(encoding="utf-8"))
                    if isinstance(old_obj, dict) and isinstance(old_obj.get("origins"), list):
                        state_obj["origins"] = old_obj["origins"]
                ss_path.write_text(
                    json.dumps(state_obj, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            except Exception:
                pass

        if update_db and bound_id is not None and self.db is not None:
            try:
                new_st = "frontend-" + hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:32]
                await self.db.update_token(
                    bound_id,
                    google_cookies=serialized,
                    st=new_st,
                    last_st_refresh_at=datetime.now(timezone.utc),
                    last_st_refresh_result="success",
                )
            except Exception as exc:
                debug_logger.log_warning(
                    f"[Camoufox] 同步 Token-{bound_id} 轮换 Cookie 到数据库失败: {exc}"
                )
        return serialized
    async def _load_token_session_binding(
        self, token_id: Optional[int]
    ) -> Tuple[Optional[int], Optional[str], Optional[str], Optional[str]]:
        token_key = self._normalize_token_key(token_id)
        if token_key is None:
            return None, None, None, None
        token = None
        if self.db:
            try:
                token = await self.db.get_token(token_key)
            except Exception as exc:
                debug_logger.log_warning(
                    f"[Camoufox] Slot-{self.token_id} 读取 token({token_key}) 失败: {exc}"
                )
        session_token = str(getattr(token, "st", "") or "").strip() if token else ""
        google_cookies = (
            str(getattr(token, "google_cookies", "") or "").strip() if token else ""
        )
        if not google_cookies and (
            session_token.startswith("{") or "=" in session_token
        ):
            google_cookies = session_token
        if not (google_cookies or session_token):
            ss_path = self._resolve_account_dir(token_key) / "storage-state.json"
            if ss_path.is_file():
                try:
                    google_cookies = ss_path.read_text(encoding="utf-8").strip()
                except Exception:
                    pass
        if google_cookies:
            cookie_signature = (
                build_cookie_signature(google_cookies)
                or hashlib.sha256(google_cookies.encode("utf-8")).hexdigest()
            )
        elif session_token:
            cookie_signature = (
                build_cookie_signature(
                    f"__Secure-next-auth.session-token={session_token}"
                )
                or hashlib.sha256(session_token.encode("utf-8")).hexdigest()
            )
        else:
            cookie_signature = None
        return token_key, session_token, google_cookies, cookie_signature

    async def _ensure_shared_token_binding(
        self, token_id: Optional[int]
    ) -> bool:
        (
            token_key,
            session_token,
            google_cookies,
            cookie_signature,
        ) = await self._load_token_session_binding(token_id)

        if token_key is None:
            self._shared_bound_token_id = None
            self._shared_bound_cookie_signature = None
            return True

        if not (google_cookies or session_token) or not cookie_signature:
            return False

        if (
            self._shared_bound_token_id == token_key
            and self._shared_bound_cookie_signature == cookie_signature
        ):
            return True

        if google_cookies:
            browser_cookies = build_browser_cookie_targets(
                google_cookies,
                default_url="https://flow.google.com/",
                fallback_urls=[
                    "https://flow.google.com/",
                    "https://www.google.com/",
                ],
            )
        else:
            browser_cookies = [
                {
                    "name": "__Secure-next-auth.session-token",
                    "value": session_token,
                    "url": target_url,
                    "secure": True,
                    "httpOnly": True,
                    "sameSite": "None",
                }
                for target_url in (
                    "https://flow.google.com/",
                    "https://www.google.com/",
                    "https://www.recaptcha.net/",
                )
            ]

        if not browser_cookies:
            return False

        try:
            if self._bidi:
                if not self._is_persistent_profile:
                    try:
                        await self._bidi.command("storage.deleteCookies", {})
                    except Exception:
                        pass
                account_dir = getattr(self, "_account_dir", None)
                has_native_session_cookies = False
                if self._is_persistent_profile:
                    try:
                        existing_live = await self._export_live_google_cookies()
                        existing_names = {c.get("name") for c in existing_live}
                        if (
                            existing_names.intersection({"SID", "__Secure-1PSID", "__Secure-3PSID"})
                            and existing_names.intersection({"OSID", "__Secure-OSID"})
                        ):
                            has_native_session_cookies = True
                    except Exception:
                        pass
                if not has_native_session_cookies:
                    ss_cookies_loaded = False
                    if isinstance(account_dir, Path):
                        ss_path = account_dir / "storage-state.json"
                        if ss_path.is_file():
                            try:
                                ss_obj = json.loads(ss_path.read_text(encoding="utf-8"))
                                raw_ss_cookies = ss_obj.get("cookies") if isinstance(ss_obj, dict) else None
                                if isinstance(raw_ss_cookies, list) and raw_ss_cookies:
                                    await self._install_cookies_via_bidi(raw_ss_cookies)
                                    ss_cookies_loaded = True
                            except Exception:
                                pass
                    if not ss_cookies_loaded and browser_cookies:
                        await self._install_cookies_via_bidi(browser_cookies)
            self._shared_bound_token_id = token_key
            self._shared_bound_cookie_signature = cookie_signature
            return True
        except Exception as exc:
            debug_logger.log_warning(
                f"[Camoufox] Slot-{self.token_id} 绑定账号 Cookie 失败: {exc}"
            )
            return False

    async def _get_or_create_shared_browser(
        self,
        token_proxy_url: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Tuple[Any, Any, Any]:
        expected_proxy_url, _ = await self._resolve_proxy_runtime_config(
            token_proxy_url=token_proxy_url
        )
        expected_token_key = self._normalize_token_key(token_id)

        async with self._shared_browser_lock:
            alive = self.has_shared_browser()
            if alive and self._shared_proxy_url != expected_proxy_url:
                await self._recycle_browser_locked(
                    reason="proxy_changed", rotate_profile=False
                )
                alive = False

            if alive:
                current_bound = self._shared_bound_token_id
                if expected_token_key != current_bound and not (
                    expected_token_key is None and current_bound is None
                ):
                    await self._recycle_browser_locked(
                        reason=f"token_binding_changed:{current_bound}->{expected_token_key}",
                        rotate_profile=False,
                    )
                    alive = False

            if alive:
                if not await self._ensure_shared_token_binding(token_id):
                    await self._recycle_browser_locked(
                        reason="token_binding_failed", rotate_profile=False
                    )
                    alive = False

            if alive:
                self._shared_reuse_count += 1
                return self._process, self._bidi, self._context_id

            await self._launch_camoufox(
                token_proxy_url=token_proxy_url, token_id=token_id
            )
            if not await self._ensure_shared_token_binding(token_id):
                await self._recycle_browser_locked(
                    reason="initial_token_binding_failed", rotate_profile=False
                )
                raise RuntimeError("Camoufox 账号登录态绑定失败")

            self.note_idle()
            return self._process, self._bidi, self._context_id

    async def _recycle_browser_locked(
        self, reason: str = "unknown", rotate_profile: bool = True
    ) -> None:
        _ = rotate_profile
        proc = self._process
        bidi = self._bidi
        relay = self._relay
        profile_dir = self._profile_dir
        account_dir = getattr(self, "_account_dir", None)
        bound_token_id = self._shared_bound_token_id

        reason_lower = str(reason or "").lower()
        if (
            bidi is not None
            and not bidi._closed
            and bound_token_id is not None
            and self._consecutive_browser_failures == 0
            and "relogin" not in reason_lower
            and "failed" not in reason_lower
            and "error" not in reason_lower
        ):
            try:
                await self._sync_live_cookies_to_db(bound_token_id)
            except Exception:
                pass
        self._process = None
        self._bidi = None
        self._context_id = None
        self._relay = None
        self._profile_dir = None
        self._account_dir = None
        self._shared_browser_pid = None
        self._shared_proxy_url = None
        self._shared_bound_token_id = None
        self._shared_bound_cookie_signature = None
        self._runtime_generation += 1
        self._consecutive_browser_failures = 0
        self._shared_reuse_count = 0

        if bidi is not None:
            try:
                await asyncio.wait_for(
                    bidi.command("browser.close", {}), timeout=4.0
                )
            except Exception:
                pass
            await bidi.close()

        if proc is not None:
            if proc.returncode is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(proc.wait(), timeout=4.0)
                except Exception:
                    try:
                        proc.kill()
                        await asyncio.wait_for(proc.wait(), timeout=2.0)
                    except Exception:
                        pass

        if relay is not None:
            await relay.close()
        if profile_dir is not None and profile_dir.exists():
            if account_dir is not None and account_dir.exists():
                save_startup_cache(profile_dir, account_dir)
            if self._is_persistent_profile:
                _clear_stale_firefox_profile_locks(profile_dir)
            else:
                shutil.rmtree(profile_dir, ignore_errors=True)
        if proc is not None or bidi is not None:
            debug_logger.log_info(
                f"[Camoufox] Slot-{self.token_id} 实例已回收 (reason={reason})"
            )

    async def recycle_browser(
        self, reason: str = "unknown", rotate_profile: bool = True
    ) -> None:
        async with self._shared_browser_lock:
            await self._recycle_browser_locked(
                reason=reason, rotate_profile=rotate_profile
            )

    async def force_close_pending_browser(
        self, request_ref: Optional[str] = None, close_all: bool = False
    ) -> None:
        if close_all:
            await self.recycle_browser(
                reason="force_close_all", rotate_profile=False
            )

    async def _send_trusted_gesture(self, context_id: str) -> None:
        """Dispatch a trusted BiDi keyboard/mouse gesture (like AIStudio2API `sendProtectedKey`)."""
        if not self._bidi:
            return
        try:
            await self._bidi.command(
                "input.performActions",
                {
                    "context": context_id,
                    "actions": [
                        {
                            "type": "pointer",
                            "id": "mouse",
                            "parameters": {"pointerType": "mouse"},
                            "actions": [
                                {"type": "pointerMove", "x": 380, "y": 280},
                                {"type": "pointerMove", "x": 540, "y": 360},
                            ],
                        },
                        {
                            "type": "key",
                            "id": "keyboard",
                            "actions": PROTECTED_SEND_KEYS,
                        },
                    ],
                },
                timeout=10.0,
            )
        except Exception as exc:
            debug_logger.log_warning(
                f"[Camoufox] 发送 BiDi trusted 输入动作失败: {exc}"
            )

    async def _dismiss_flow_overlays(self, context_id: str) -> None:
        """Dismiss Flow onboarding / consent dialogs (e.g. '体验并参与塑造 AI 创意工具') so the project editor and reCAPTCHA anchor operate normally."""
        if not self._bidi:
            return
        dismiss_expr = r"""
        (async () => {
            const keywords = [
                '下一步', '继续', '同意', '接受', '开始', '知道了', '跳过', '完成',
                'next', 'continue', 'start', 'got it', 'agree', 'accept', 'skip', 'done'
            ];
            const roots = Array.from(document.querySelectorAll(
                '.cdk-overlay-container, [role="dialog"], [role="alertdialog"], [aria-modal="true"]'
            ));
            let scrolledAny = false;
            for (const root of roots) {
                const allEls = [root, ...Array.from(root.querySelectorAll('*'))];
                for (const el of allEls) {
                    if (el.scrollHeight > el.clientHeight + 4 && el.scrollTop + el.clientHeight < el.scrollHeight - 2) {
                        el.scrollTop = el.scrollHeight;
                        el.dispatchEvent(new Event('scroll', { bubbles: true }));
                        el.dispatchEvent(new WheelEvent('wheel', { deltaY: 800, bubbles: true }));
                        scrolledAny = true;
                    }
                }
            }
            if (scrolledAny) {
                await new Promise(r => setTimeout(r, 280));
            }
            for (const root of roots) {
                const buttons = Array.from(root.querySelectorAll('button, [role="button"]'));
                for (const btn of buttons) {
                    if (btn.disabled || btn.getAttribute('aria-disabled') === 'true') continue;
                    const text = String(btn.innerText || btn.textContent || '').trim().toLowerCase();
                    if (!text || text.length > 24) continue;
                    if (keywords.some(k => text === k || text.includes(k))) {
                        btn.click();
                        return 'clicked:' + text;
                    }
                }
            }
            return scrolledAny ? 'scrolled' : '';
        })()
        """
        for _ in range(6):
            try:
                clicked = await self._bidi.evaluate(
                    context_id, dismiss_expr, await_promise=True
                )
                if not clicked:
                    break
                debug_logger.log_info(
                    f"[Camoufox] Slot-{self.token_id} 已自动处理 Flow 引导弹窗: {clicked}"
                )
                await asyncio.sleep(0.45)
            except Exception:
                break

    async def _perform_human_warmup(
        self, context_id: str, project_id: Optional[str]
    ) -> None:
        """Simulate realistic multi-point Bézier mouse trajectories, wheel scroll, and focus inside the Flow page so reCAPTCHA Enterprise scores >= 0.7-0.9."""
        if not self._bidi:
            return
        project_key = str(project_id or "__projects__").strip() or "__projects__"
        first_warmup = project_key not in self._warmed_projects
        rounds = 2 if first_warmup else 1
        for r in range(rounds):
            x0, y0 = random.randint(240, 480), random.randint(220, 420)
            x1, y1 = random.randint(520, 920), random.randint(260, 560)
            cx, cy = (x0 + x1) // 2 + random.randint(-90, 90), (y0 + y1) // 2 + random.randint(-70, 70)
            steps = 8 if first_warmup else 5
            pointer_actions: List[Dict[str, Any]] = [
                {"type": "pointerMove", "x": x0, "y": y0, "duration": 0}
            ]
            for i in range(1, steps + 1):
                t = i / steps
                inv = 1.0 - t
                px = int(inv * inv * x0 + 2 * inv * t * cx + t * t * x1)
                py = int(inv * inv * y0 + 2 * inv * t * cy + t * t * y1)
                pointer_actions.append(
                    {
                        "type": "pointerMove",
                        "x": max(40, min(1360, px)),
                        "y": max(40, min(780, py)),
                        "duration": random.randint(18, 36),
                    }
                )
            try:
                await self._bidi.command(
                    "input.performActions",
                    {
                        "context": context_id,
                        "actions": [
                            {
                                "type": "pointer",
                                "id": "mouse",
                                "parameters": {"pointerType": "mouse"},
                                "actions": pointer_actions,
                            },
                            {
                                "type": "wheel",
                                "id": "wheel",
                                "actions": [
                                    {
                                        "type": "scroll",
                                        "x": x1,
                                        "y": y1,
                                        "deltaX": 0,
                                        "deltaY": random.choice([-48, 36, 64, -32]),
                                        "duration": 25,
                                    }
                                ],
                            },
                            {
                                "type": "key",
                                "id": "keyboard",
                                "actions": PROTECTED_SEND_KEYS,
                            },
                        ],
                    },
                    timeout=10.0,
                )
            except Exception:
                await self._send_trusted_gesture(context_id)
            if first_warmup:
                await asyncio.sleep(1.1 if r == 0 else 0.9)
        if first_warmup:
            self._warmed_projects.add(project_key)

    async def _ensure_flow_page_ready(
        self,
        context_id: str,
        project_id: Optional[str],
        action: str = "IMAGE_GENERATION",
    ) -> bool:
        """Navigate or reuse the resident Flow page, warm up reCAPTCHA Enterprise, and suspend renderLayers when done."""
        if not self._bidi:
            return False

        target_url = (
            f"{FLOW_ORIGIN}/project/{project_id}"
            if project_id
            else FLOW_PROJECTS_URL
        )
        current_url = ""
        try:
            current_url = str(
                await self._bidi.evaluate(
                    context_id, "location.href", await_promise=False
                )
                or ""
            )
        except Exception:
            current_url = ""

        needs_nav = not current_url.startswith(f"{FLOW_ORIGIN}/") or (
            bool(project_id) and f"/project/{project_id}" not in current_url
        )

        async with self._boost_frames():
            await self._set_tab_render_layers(True)
            if needs_nav:
                try:
                    await self._bidi.command(
                        "browsingContext.navigate",
                        {
                            "context": context_id,
                            "url": target_url,
                            "wait": "interactive",
                        },
                        timeout=45.0,
                    )
                except Exception as nav_exc:
                    if "NS_ERROR_ABORT" not in str(nav_exc):
                        debug_logger.log_warning(
                            f"[Camoufox] Slot-{self.token_id} 导航 {target_url} 异常: {nav_exc}"
                        )

            # Wait for document ready and ensure hooks are installed
            for _ in range(30):
                try:
                    state = await self._bidi.evaluate(
                        context_id, "document.readyState", await_promise=False
                    )
                    if state in ("interactive", "complete"):
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.3)
            # Verify we did not get redirected to signed-out /about or accounts.google.com
            await asyncio.sleep(0.4)
            try:
                page_check = await self._bidi.evaluate(
                    context_id,
                    """({
                        href: String(location.href || ''),
                        hasSNlM0e: Boolean(window.WIZ_global_data && window.WIZ_global_data.SNlM0e),
                        hasEmail: Boolean(window.WIZ_global_data && window.WIZ_global_data.oPEP7c)
                    })""",
                    await_promise=False,
                )
                if isinstance(page_check, dict):
                    href_now = str(page_check.get("href") or "")
                    is_unauthed_flow = (
                        "accounts.google.com" in href_now
                        or href_now.rstrip("/").endswith("/about")
                        or (
                            href_now.startswith(f"{FLOW_ORIGIN}/")
                            and not page_check.get("hasSNlM0e")
                            and not page_check.get("hasEmail")
                            and "/about" in href_now
                        )
                    )
                    if is_unauthed_flow and "accounts.google.com" not in href_now:
                        # Attempt passive OSID cookie refresh via accounts.google.com
                        passive_url = (
                            "https://accounts.google.com/ServiceLogin?passive=1209600&osid=1"
                            f"&continue={quote(target_url, safe='')}"
                            f"&followup={quote(target_url, safe='')}"
                        )
                        try:
                            await self._bidi.command(
                                "browsingContext.navigate",
                                {
                                    "context": context_id,
                                    "url": passive_url,
                                    "wait": "interactive",
                                },
                                timeout=20.0,
                            )
                            await asyncio.sleep(1.8)
                            page_check = await self._bidi.evaluate(
                                context_id,
                                """({
                                    href: String(location.href || ''),
                                    hasSNlM0e: Boolean(window.WIZ_global_data && window.WIZ_global_data.SNlM0e),
                                    hasEmail: Boolean(window.WIZ_global_data && window.WIZ_global_data.oPEP7c)
                                })""",
                                await_promise=False,
                            )
                            href_now = str((page_check or {}).get("href") or "")
                            is_unauthed_flow = (
                                "accounts.google.com" in href_now
                                or href_now.rstrip("/").endswith("/about")
                                or (
                                    href_now.startswith(f"{FLOW_ORIGIN}/")
                                    and not (page_check or {}).get("hasSNlM0e")
                                    and not (page_check or {}).get("hasEmail")
                                    and "/about" in href_now
                                )
                            )
                        except Exception:
                            pass
                    if is_unauthed_flow:
                        raise RuntimeError(
                            "Flow 账号登录态已失效（页面已跳转至未登录页），请在后台点击该账号的「重登」重新登录"
                        )
            except RuntimeError:
                raise
            except Exception:
                pass

            try:
                await self._bidi.evaluate(
                    context_id,
                    FLOW_PROTECTED_PUMP_SOURCE,
                    await_promise=False,
                )
            except Exception:
                pass

            await self._dismiss_flow_overlays(context_id)
            await self._send_trusted_gesture(context_id)
            await self._sync_live_cookies_to_db()
            ready = False
            for _ in range(30):
                try:
                    ok = await self._bidi.evaluate(
                        context_id,
                        "Boolean(window.grecaptcha && window.grecaptcha.enterprise && typeof window.grecaptcha.enterprise.execute === 'function')",
                        await_promise=False,
                    )
                    if ok is True:
                        ready = True
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.5)

            if not ready:
                # Inject enterprise.js if Flow page deferred it
                primary_host = (
                    "https://www.recaptcha.net"
                    if self._browser_proxy_active
                    else "https://www.google.com"
                )
                inject_expr = f"""
                (() => {{
                    if (window.grecaptcha && window.grecaptcha.enterprise && typeof window.grecaptcha.enterprise.execute === 'function') return true;
                    const script = document.createElement('script');
                    script.src = {json.dumps(f"{primary_host}/recaptcha/enterprise.js?render={FLOW_WEBSITE_KEY}")};
                    script.async = true;
                    (document.head || document.documentElement).appendChild(script);
                    return false;
                }})()
                """
                try:
                    await self._bidi.evaluate(
                        context_id, inject_expr, await_promise=False
                    )
                except Exception:
                    pass
                for _ in range(25):
                    try:
                        ok = await self._bidi.evaluate(
                            context_id,
                            "Boolean(window.grecaptcha && window.grecaptcha.enterprise && typeof window.grecaptcha.enterprise.execute === 'function')",
                            await_promise=False,
                        )
                        if ok is True:
                            ready = True
                            break
                    except Exception:
                        pass
                    await asyncio.sleep(0.5)

            if ready:
                await self._perform_human_warmup(context_id, project_id)
            return ready

    async def get_token(
        self,
        project_id: str,
        website_key: str = FLOW_WEBSITE_KEY,
        action: str = "IMAGE_GENERATION",
        token_proxy_url: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Acquire a reCAPTCHA Enterprise token inside the Camoufox Flow page with trusted BiDi activation."""
        async with self._semaphore:
            self._solve_inflight += 1
            max_retries = max(
                1, int(getattr(config, "browser_captcha_max_retries", 5) or 5)
            )
            try:
                for attempt in range(max_retries):
                    try:
                        start_ts = time.time()
                        _, bidi, context_id = await self._get_or_create_shared_browser(
                            token_proxy_url=token_proxy_url, token_id=token_id
                        )
                        ready = await self._ensure_flow_page_ready(
                            context_id, project_id, action=action
                        )
                        if not ready:
                            raise RuntimeError("Camoufox 页面 grecaptcha.enterprise 未就绪")

                        async with self._boost_frames():
                            await self._perform_human_warmup(context_id, project_id)
                            script = build_recaptcha_execute_script(
                                website_key or FLOW_WEBSITE_KEY,
                                str(action or "IMAGE_GENERATION").strip().upper(),
                                25000,
                            )
                            result = await bidi.evaluate(
                                context_id,
                                script,
                                await_promise=True,
                                user_activation=True,
                                timeout=35.0,
                            )
                        await self._set_tab_render_layers(False)

                        if isinstance(result, dict) and result.get("ok"):
                            token = str(result.get("token") or "").strip()
                            if token:
                                self._solve_count += 1
                                self._consecutive_browser_failures = 0
                                session_id = (
                                    str(uuid.uuid4())
                                    if str(action or "").upper() == "CHAT_GENERATION"
                                    else None
                                )
                                self._last_harvest_session_id = session_id
                                await self._sync_live_cookies_to_db(token_id)
                                debug_logger.log_info(
                                    f"[Camoufox] Slot-{self.token_id} token 获取成功 "
                                    f"({(time.time() - start_ts) * 1000:.0f}ms, action={action}, reuse={self._shared_reuse_count})"
                                )
                                return token, session_id
                        err_detail = (
                            result.get("error")
                            if isinstance(result, dict)
                            else str(result)
                        )
                        raise RuntimeError(f"execute 未返回有效 token: {err_detail}")
                    except Exception as exc:
                        self._error_count += 1
                        self._consecutive_browser_failures += 1
                        debug_logger.log_warning(
                            f"[Camoufox] Slot-{self.token_id} 第 {attempt + 1}/{max_retries} 次取码失败: {exc}"
                        )
                        if "登录态已失效" in str(exc):
                            break
                        if self._consecutive_browser_failures >= 2 or (
                            self._bidi and self._bidi._closed
                        ):
                            await self.recycle_browser(
                                reason=f"camoufox_solve_failed_{attempt + 1}",
                                rotate_profile=False,
                            )
                        if attempt < max_retries - 1:
                            await asyncio.sleep(1.0)
                return None, None
            finally:
                self._solve_inflight = max(0, self._solve_inflight - 1)
                self.note_idle()

    async def execute_flow_frontend_rpc(
        self,
        *,
        rpc_id: str,
        argument: List[Any],
        project_id: Optional[str] = None,
        source_path: Optional[str] = None,
        timeout: int = 90,
        token_id: Optional[int] = None,
        token_proxy_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute a Flow `batchexecute` RPC directly inside the Camoufox page via `XMLHttpRequest`."""
        async with self._semaphore:
            self._solve_inflight += 1
            try:
                _, bidi, context_id = await self._get_or_create_shared_browser(
                    token_proxy_url=token_proxy_url, token_id=token_id
                )
                current_url = ""
                try:
                    current_url = str(
                        await bidi.evaluate(
                            context_id, "String(location.href || '')", await_promise=False
                        )
                        or ""
                    )
                except Exception:
                    current_url = ""
                if not current_url.startswith(f"{FLOW_ORIGIN}/"):
                    await bidi.command(
                        "browsingContext.navigate",
                        {
                            "context": context_id,
                            "url": f"{FLOW_ORIGIN}/robots.txt",
                            "wait": "complete",
                        },
                        timeout=25.0,
                    )

                resolved_source_path = source_path or (
                    f"/project/{project_id}" if project_id else "/projects"
                )
                f_req = json.dumps(
                    [[[rpc_id, json.dumps(argument, ensure_ascii=False, separators=(",", ":")), None, "generic"]]],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )

                rpc_fn = r"""
                async (cfg) => {
                    let wiz = window.WIZ_global_data || {};
                    let bl = wiz.cfb2h || cfg.cachedBl || '';
                    let fSid = wiz.FdrFJe || cfg.cachedFSid || '';
                    let at = wiz.SNlM0e || cfg.cachedAt || '';

                    const fetchBootstrap = async (pathOverride) => {
                        const targetPath = pathOverride || ((cfg.sourcePath && cfg.sourcePath.includes('/edit/')) ? '/projects' : (cfg.sourcePath || '/projects'));
                        const pageResp = await fetch(targetPath, { credentials: 'include' });
                        const html = await pageResp.text();
                        const blMatch = html.match(/boq_labs-ai-sandbox-frontend_[A-Za-z0-9_.-]+/);
                        const sidMatch = html.match(/"FdrFJe":"(-?[0-9]+)"/);
                        const atMatch = html.match(/"SNlM0e"\s*:\s*"((?:\\.|[^"\\])+)"/);
                        if (blMatch) bl = blMatch[0];
                        if (sidMatch) fSid = sidMatch[1];
                        if (atMatch) {
                            try { at = JSON.parse('"' + atMatch[1] + '"'); } catch (_) { at = atMatch[1]; }
                        }
                    };

                    if (!bl || !fSid || !at) {
                        await fetchBootstrap();
                        if (!at) {
                            await fetchBootstrap('/projects');
                        }
                    }
                    if (!bl || !fSid) {
                        return { ok: false, error: 'Flow WIZ_global_data metadata unavailable' };
                    }

                    const postOnce = (xsrfToken) => new Promise((resolve, reject) => {
                        const reqId = Math.floor(10000 + Math.random() * 80000);
                        const url = `/_/AiSandboxAngularFrontend/data/batchexecute?rpcids=${encodeURIComponent(cfg.rpcId)}&source-path=${encodeURIComponent(cfg.sourcePath)}&bl=${encodeURIComponent(bl)}&f.sid=${encodeURIComponent(fSid)}&hl=en&_reqid=${reqId}&rt=c`;
                        const params = new URLSearchParams();
                        params.set('f.req', cfg.fReq);
                        if (xsrfToken) params.set('at', xsrfToken);

                        const xhr = new XMLHttpRequest();
                        xhr.open('POST', url, true);
                        xhr.withCredentials = true;
                        xhr.timeout = cfg.timeoutMs;
                        xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded;charset=UTF-8');
                        xhr.setRequestHeader('x-same-domain', '1');
                        xhr.onload = () => resolve({ ok: true, status: xhr.status, text: xhr.responseText || '', bl, fSid, at: xsrfToken });
                        xhr.onerror = () => reject(new Error('XHR network error'));
                        xhr.ontimeout = () => reject(new Error('XHR timeout'));
                        xhr.send(params.toString());
                    });

                    let result = await postOnce(at);
                    const xsrfMatch = (result.text || '').match(/\["xsrf"\s*,\s*"((?:\\.|[^"\\])*)"/);
                    if (xsrfMatch) {
                        let refreshedAt = xsrfMatch[1];
                        try { refreshedAt = JSON.parse('"' + refreshedAt + '"'); } catch (_) {}
                        if (window.WIZ_global_data) window.WIZ_global_data.SNlM0e = refreshedAt;
                        at = refreshedAt;
                        result = await postOnce(refreshedAt);
                    }
                    return result;
                }
                """

                res = await bidi.call_function(
                    context_id,
                    rpc_fn,
                    arguments=[
                        _to_bidi_local_value(
                            {
                                "rpcId": rpc_id,
                                "sourcePath": resolved_source_path,
                                "fReq": f_req,
                                "timeoutMs": max(10000, int(timeout * 1000)),
                                "cachedBl": self._cached_wiz.get("bl") or "",
                                "cachedFSid": self._cached_wiz.get("fSid") or "",
                                "cachedAt": self._cached_wiz.get("at") or "",
                            }
                        ),
                    ],
                    await_promise=True,
                    user_activation=True,
                    timeout=max(30.0, float(timeout) + 15.0),
                )
                if not isinstance(res, dict) or not res.get("ok"):
                    err_msg = (
                        res.get("error")
                        if isinstance(res, dict)
                        else f"unexpected response: {res}"
                    )
                    raise RuntimeError(f"Camoufox 页面内 RPC 失败: {err_msg}")
                if res.get("bl") and res.get("fSid"):
                    self._cached_wiz = {
                        "bl": str(res.get("bl") or ""),
                        "fSid": str(res.get("fSid") or ""),
                        "at": str(res.get("at") or ""),
                    }
                await self._sync_live_cookies_to_db(token_id, update_db=False)
                return {
                    "status": int(res.get("status") or 0),
                    "text": str(res.get("text") or ""),
                }
            finally:
                self._solve_inflight = max(0, self._solve_inflight - 1)
                self.note_idle()

    async def submit_flow_request(
        self,
        project_id: str,
        website_key: str,
        action: str,
        url: str,
        at_token: str,
        json_data: Dict[str, Any],
        timeout: int,
        token_proxy_url: Optional[str] = None,
        token_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Compatible with `TokenBrowser.submit_flow_request`."""
        token, _ = await self.get_token(
            project_id=project_id,
            website_key=website_key,
            action=action,
            token_proxy_url=token_proxy_url,
            token_id=token_id,
        )
        if not token:
            raise RuntimeError("Camoufox 未获取到有效 reCAPTCHA token")

        async with self._semaphore:
            self._solve_inflight += 1
            try:
                _, bidi, context_id = await self._get_or_create_shared_browser(
                    token_proxy_url=token_proxy_url, token_id=token_id
                )
                fn_decl = r"""
                async (cfg) => {
                    const patchToken = (value, tok) => {
                        if (!value) return;
                        if (Array.isArray(value)) { value.forEach(i => patchToken(i, tok)); return; }
                        if (typeof value !== 'object') return;
                        if (value.recaptchaContext && typeof value.recaptchaContext === 'object') {
                            value.recaptchaContext.token = tok;
                            if (!value.recaptchaContext.applicationType) {
                                value.recaptchaContext.applicationType = 'RECAPTCHA_APPLICATION_TYPE_WEB';
                            }
                        }
                        Object.values(value).forEach(i => patchToken(i, tok));
                    };
                    const body = JSON.parse(cfg.payloadJson);
                    patchToken(body, cfg.token);
                    const resp = await fetch(cfg.url, {
                        method: 'POST',
                        headers: {
                            'authorization': `Bearer ${cfg.atToken}`,
                            'content-type': 'text/plain;charset=UTF-8'
                        },
                        credentials: 'include',
                        body: JSON.stringify(body)
                    });
                    const text = await resp.text();
                    return { ok: resp.ok, status: resp.status, text, token: cfg.token };
                }
                """
                res = await bidi.call_function(
                    context_id,
                    fn_decl,
                    arguments=[
                        _to_bidi_local_value(
                            {
                                "url": url,
                                "atToken": at_token,
                                "token": token,
                                "payloadJson": json.dumps(
                                    json_data, ensure_ascii=False
                                ),
                            }
                        )
                    ],
                    await_promise=True,
                    user_activation=True,
                    timeout=max(35.0, float(timeout) + 10.0),
                )
                if not isinstance(res, dict):
                    raise RuntimeError("Camoufox 页面内提交返回格式异常")
                return res
            finally:
                self._solve_inflight = max(0, self._solve_inflight - 1)
                self.note_idle()

    async def generate_native_image(
        self,
        *,
        project_id: str,
        prompt: str,
        aspect_ratio: str = "IMAGE_ASPECT_RATIO_LANDSCAPE",
        model_name: str = "BELUGA",
        image_inputs: Optional[List[Dict[str, Any]]] = None,
        timeout: int = 120,
        token_id: Optional[int] = None,
        token_proxy_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute native Flow image generation via in-page UI + StreamChat in headless Camoufox."""
        return await self._execute_native_stream_chat_generation(
            project_id=project_id,
            prompt=prompt,
            aspect_ratio=aspect_ratio,
            model_name=model_name,
            media_kind="image",
            image_inputs=image_inputs,
            timeout=timeout,
            token_id=token_id,
            token_proxy_url=token_proxy_url,
        )

    async def generate_native_video(
        self,
        *,
        project_id: str,
        prompt: str,
        aspect_ratio: str = "VIDEO_ASPECT_RATIO_LANDSCAPE",
        model_key: str = "",
        timeout: int = 180,
        token_id: Optional[int] = None,
        token_proxy_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute native Flow video generation via in-page UI + StreamChat in headless Camoufox."""
        return await self._execute_native_stream_chat_generation(
            project_id=project_id,
            prompt=prompt,
            aspect_ratio=aspect_ratio,
            model_name=model_key or "VEO_3_1",
            media_kind="video",
            image_inputs=None,
            timeout=timeout,
            token_id=token_id,
            token_proxy_url=token_proxy_url,
        )

    async def _execute_native_stream_chat_generation(
        self,
        *,
        project_id: str,
        prompt: str,
        aspect_ratio: str,
        model_name: str,
        media_kind: str = "image",
        image_inputs: Optional[List[Dict[str, Any]]] = None,
        timeout: int = 120,
        token_id: Optional[int] = None,
        token_proxy_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Unified single-pass headless Camoufox UI + StreamChat pipeline for image and video generation."""
        from .flow_frontend import FlowFrontendMixin

        if not project_id:
            raise ValueError("Camoufox 原生生成必须提供 project_id")
        async with self._semaphore:
            self._solve_inflight += 1
            try:
                _, bidi, context_id = await self._get_or_create_shared_browser(
                    token_proxy_url=token_proxy_url, token_id=token_id
                )
                if True:
                    try:
                        target_url = f"{FLOW_ORIGIN}/project/{project_id}"
                        await bidi.command(
                            "browsingContext.navigate",
                            {
                                "context": context_id,
                                "url": target_url,
                                "wait": "complete",
                            },
                            timeout=45.0,
                        )
                        await asyncio.sleep(2.0)
                        await bidi.evaluate(
                            context_id,
                            """(() => {
                                try {
                                    localStorage.removeItem("glue.CookieNotificationBar");
                                    localStorage.removeItem("_grecaptcha");
                                } catch (e) {}
                                return true;
                            })()""",
                            await_promise=False,
                        )
                        await bidi.command(
                            "browsingContext.navigate",
                            {
                                "context": context_id,
                                "url": target_url,
                                "wait": "complete",
                            },
                            timeout=45.0,
                        )
                        await asyncio.sleep(6.0)
                        await bidi.evaluate(
                            context_id,
                            """(() => {
                                window.__flow2apiStreamChatReqs = [];
                                if (!window.__flow2apiOrigFetch) {
                                    window.__flow2apiOrigFetch = window.fetch;
                                    window.fetch = async function(input, init) {
                                        const url = input instanceof Request ? input.url : String(input || "");
                                        if (url.includes("StreamChat")) {
                                            const entry = { url, status: 0, chunks: "", done: false, error: "" };
                                            window.__flow2apiStreamChatReqs.push(entry);
                                            try {
                                                const resp = await window.__flow2apiOrigFetch.apply(this, arguments);
                                                entry.status = resp.status;
                                                const clone = resp.clone();
                                                (async () => {
                                                    try {
                                                        const reader = clone.body.getReader();
                                                        const dec = new TextDecoder();
                                                        while (true) {
                                                            const { done, value } = await reader.read();
                                                            if (done) break;
                                                            entry.chunks += dec.decode(value, { stream: true });
                                                        }
                                                    } catch (e) {
                                                        entry.error = String(e);
                                                    } finally {
                                                        entry.done = true;
                                                    }
                                                })();
                                                return resp;
                                            } catch (e) {
                                                entry.error = String(e);
                                                entry.done = true;
                                                throw e;
                                            }
                                        }
                                        return window.__flow2apiOrigFetch.apply(this, arguments);
                                    };
                                }
                                return true;
                            })()""",
                            await_promise=False,
                        )

                        cookie_bar_rect = await bidi.evaluate(
                            context_id,
                            """(() => {
                                const el = document.querySelector(".glue-cookie-notification-bar__accept");
                                if (!el) return null;
                                const r = el.getBoundingClientRect();
                                if (r.width <= 0 || r.height <= 0) return null;
                                return { x: Math.round(r.left + r.width / 2), y: Math.round(r.top + r.height / 2) };
                            })()""",
                            await_promise=False,
                        )
                        if isinstance(cookie_bar_rect, dict):
                            await bidi.command(
                                "input.performActions",
                                {
                                    "context": context_id,
                                    "actions": [
                                        {
                                            "type": "pointer",
                                            "id": "mouse",
                                            "parameters": {"pointerType": "mouse"},
                                            "actions": [
                                                {
                                                    "type": "pointerMove",
                                                    "x": int(cookie_bar_rect["x"]),
                                                    "y": int(cookie_bar_rect["y"]),
                                                    "duration": 250,
                                                },
                                                {"type": "pointerDown", "button": 0},
                                                {"type": "pointerUp", "button": 0},
                                            ],
                                        }
                                    ],
                                },
                            )
                            await asyncio.sleep(0.6)

                        pm_rect = None
                        for _ in range(15):
                            pm_rect = await bidi.evaluate(
                                context_id,
                                """(() => {
                                    const wiz = window.WIZ_global_data || {};
                                    const pm = document.querySelector(".ProseMirror");
                                    if (!pm) return null;
                                    const r = pm.getBoundingClientRect();
                                    if (r.width <= 0 || r.height <= 0) return null;
                                    return {
                                        x: Math.round(r.left + r.width / 2),
                                        y: Math.round(r.top + r.height / 2),
                                        bl: String(wiz.cfb2h || ""),
                                        fSid: String(wiz.FdrFJe || ""),
                                        at: String(wiz.SNlM0e || "")
                                    };
                                })()""",
                                await_promise=False,
                            )
                            if isinstance(pm_rect, dict):
                                if pm_rect.get("bl") and pm_rect.get("fSid"):
                                    self._cached_wiz = {
                                        "bl": str(pm_rect.get("bl") or ""),
                                        "fSid": str(pm_rect.get("fSid") or ""),
                                        "at": str(pm_rect.get("at") or ""),
                                    }
                                break
                            await asyncio.sleep(0.5)
                        if not isinstance(pm_rect, dict):
                            raise RuntimeError("Camoufox 页面未找到 .ProseMirror 输入框")

                        await bidi.command(
                            "input.performActions",
                            {
                                "context": context_id,
                                "actions": [
                                    {
                                        "type": "pointer",
                                        "id": "mouse",
                                        "parameters": {"pointerType": "mouse"},
                                        "actions": [
                                            {
                                                "type": "pointerMove",
                                                "x": int(pm_rect["x"]),
                                                "y": int(pm_rect["y"]),
                                                "duration": 250,
                                            },
                                            {"type": "pointerDown", "button": 0},
                                            {"type": "pointerUp", "button": 0},
                                        ],
                                    }
                                ],
                            },
                        )
                        await asyncio.sleep(0.5)

                        ar_upper = str(aspect_ratio or "").upper()
                        if "PORTRAIT" in ar_upper:
                            ar_desc = "vertical portrait 9:16"
                        elif "SQUARE" in ar_upper:
                            ar_desc = "square 1:1"
                        else:
                            ar_desc = "landscape 16:9"
                        if media_kind == "video":
                            ar_prefix = f"Generate 1 {ar_desc} video of "
                        else:
                            model_hint = (
                                "using Nano Banana Pro "
                                if str(model_name or "").upper() == "GEM_PIX_2"
                                else ""
                            )
                            ar_prefix = f"Generate 1 {ar_desc} image {model_hint}of "
                        clean_prompt = " ".join(str(prompt or "").split())
                        full_prompt = ar_prefix + clean_prompt

                        key_actions: List[Dict[str, Any]] = []
                        for ch in full_prompt:
                            key_actions.append({"type": "keyDown", "value": ch})
                            key_actions.append({"type": "pause", "duration": 15})
                            key_actions.append({"type": "keyUp", "value": ch})
                            key_actions.append({"type": "pause", "duration": 25})
                        await bidi.command(
                            "input.performActions",
                            {
                                "context": context_id,
                                "actions": [
                                    {
                                        "type": "key",
                                        "id": "keyboard",
                                        "actions": key_actions,
                                    }
                                ],
                            },
                        )
                        await asyncio.sleep(1.0)

                        gen_rect = await bidi.evaluate(
                            context_id,
                            """(() => {
                                const btn = document.querySelector("button.generate-icon-button");
                                if (!btn || btn.disabled) return null;
                                const r = btn.getBoundingClientRect();
                                return { x: Math.round(r.left + r.width / 2), y: Math.round(r.top + r.height / 2) };
                            })()""",
                            await_promise=False,
                        )
                        if not isinstance(gen_rect, dict):
                            raise RuntimeError(
                                "Camoufox 页面未找到可点击的 generate-icon-button"
                            )
                        self._last_stream_chat_at = time.monotonic()
                        await bidi.command(
                            "input.performActions",
                            {
                                "context": context_id,
                                "actions": [
                                    {
                                        "type": "pointer",
                                        "id": "mouse",
                                        "parameters": {"pointerType": "mouse"},
                                        "actions": [
                                            {
                                                "type": "pointerMove",
                                                "x": int(gen_rect["x"]),
                                                "y": int(gen_rect["y"]),
                                                "duration": 250,
                                            },
                                            {"type": "pointerDown", "button": 0},
                                            {"type": "pointerUp", "button": 0},
                                        ],
                                    }
                                ],
                            },
                        )

                        poll_steps = max(30, int(timeout or 90))
                        sc_status = 0
                        full_chunks = ""
                        for _ in range(poll_steps):
                            await asyncio.sleep(1.0)
                            st = await bidi.evaluate(
                                context_id,
                                """(() => {
                                    const reqs = window.__flow2apiStreamChatReqs || [];
                                    if (!reqs.length) return null;
                                    const last = reqs[reqs.length - 1];
                                    return { status: last.status, done: last.done, len: (last.chunks || "").length, error: last.error };
                                })()""",
                                await_promise=False,
                            )
                            if isinstance(st, dict) and st.get("done"):
                                sc_status = int(st.get("status") or 0)
                                full_chunks = str(
                                    await bidi.evaluate(
                                        context_id,
                                        "window.__flow2apiStreamChatReqs[window.__flow2apiStreamChatReqs.length - 1].chunks",
                                        await_promise=False,
                                    )
                                    or ""
                                )
                                break
                        if full_chunks and (
                            "ask_for_permission" in full_chunks
                            or not FlowFrontendMixin._extract_stream_chat_media_ids(
                                FlowFrontendMixin._parse_batchexecute_frames(full_chunks)
                            )
                        ) and "PUBLIC_ERROR_" not in full_chunks:
                            perm_rect = None
                            for _ in range(12):
                                await asyncio.sleep(0.5)
                                perm_rect = await bidi.evaluate(
                                    context_id,
                                    """(() => {
                                        const msgs = Array.from(document.querySelectorAll("flow-permission-message"));
                                        if (!msgs.length) return null;
                                        const lastMsg = msgs[msgs.length - 1];
                                        lastMsg.scrollIntoView({ block: "center" });
                                        const rows = Array.from(lastMsg.querySelectorAll(".option-row"));
                                        const target = rows[1] || rows[0];
                                        if (!target) return null;
                                        const r = target.getBoundingClientRect();
                                        if (r.width <= 0 || r.height <= 0 || r.top < 0) return null;
                                        return { x: Math.round(r.left + r.width / 2), y: Math.round(r.top + r.height / 2) };
                                    })()""",
                                    await_promise=False,
                                )
                                if isinstance(perm_rect, dict):
                                    break
                            if isinstance(perm_rect, dict):
                                prev_count = int(
                                    await bidi.evaluate(
                                        context_id,
                                        "(window.__flow2apiStreamChatReqs || []).length",
                                        await_promise=False,
                                    )
                                    or 1
                                )
                                await bidi.command(
                                    "input.performActions",
                                    {
                                        "context": context_id,
                                        "actions": [
                                            {
                                                "type": "pointer",
                                                "id": "mouse",
                                                "parameters": {"pointerType": "mouse"},
                                                "actions": [
                                                    {
                                                        "type": "pointerMove",
                                                        "x": int(perm_rect["x"]),
                                                        "y": int(perm_rect["y"]),
                                                        "duration": 250,
                                                    },
                                                    {"type": "pointerDown", "button": 0},
                                                    {"type": "pause", "duration": 80},
                                                    {"type": "pointerUp", "button": 0},
                                                ],
                                            }
                                        ],
                                    },
                                )
                                full_chunks = ""
                                for _ in range(poll_steps):
                                    await asyncio.sleep(1.0)
                                    st2 = await bidi.evaluate(
                                        context_id,
                                        f"""(() => {{
                                            const reqs = window.__flow2apiStreamChatReqs || [];
                                            if (reqs.length <= {prev_count}) return null;
                                            const last = reqs[reqs.length - 1];
                                            return {{ status: last.status, done: last.done, len: (last.chunks || "").length, error: last.error }};
                                        }})()""",
                                        await_promise=False,
                                    )
                                    if isinstance(st2, dict) and st2.get("done"):
                                        sc_status = int(st2.get("status") or 0)
                                        full_chunks = str(
                                            await bidi.evaluate(
                                                context_id,
                                                "window.__flow2apiStreamChatReqs[window.__flow2apiStreamChatReqs.length - 1].chunks",
                                                await_promise=False,
                                            )
                                            or ""
                                        )
                                        break

                        if not full_chunks:
                            raise RuntimeError("Camoufox StreamChat 未返回响应流")
                        if "PUBLIC_ERROR_" in full_chunks:
                            err_match = re.search(
                                r"PUBLIC_ERROR_[A-Z0-9_]+", full_chunks
                            )
                            err_code = (
                                err_match.group(0)
                                if err_match
                                else "PUBLIC_ERROR_UNKNOWN"
                            )
                            self._consecutive_risk_failures += 1
                            raise RuntimeError(
                                f"Flow StreamChat 返回错误: {err_code} (status={sc_status})"
                            )

                        frames = FlowFrontendMixin._parse_batchexecute_frames(
                            full_chunks
                        )
                        media_ids = FlowFrontendMixin._extract_stream_chat_media_ids(
                            frames
                        )
                        if not media_ids and media_kind == "video":
                            for rec in FlowFrontendMixin._extract_frontend_video_records(
                                frames, project_id
                            ):
                                if rec and FlowFrontendMixin._is_uuid(rec[0]):
                                    media_ids.append(str(rec[0]))
                        if not media_ids:
                            debug_logger.log_warning(
                                f"[Camoufox] StreamChat ({media_kind}) 未提取到 media_id, preview={full_chunks[:600]!r}"
                            )
                            self._consecutive_risk_failures += 1
                            raise RuntimeError(
                                f"Camoufox StreamChat 未提取到 media_id (len={len(full_chunks)})"
                            )

                        resolved_media: List[Dict[str, Any]] = []
                        for mid in media_ids[:1]:
                            as29s_res = await bidi.call_function(
                                context_id,
                                """async function(cfg) {
                                    const wiz = window.WIZ_global_data || {};
                                    const bl = wiz.cfb2h || "boq_labs-ai-sandbox-frontend_20261105.10_p0";
                                    const fSid = wiz.FdrFJe || "";
                                    const at = wiz.SNlM0e || "";
                                    const q = new URLSearchParams({
                                        rpcids: "as29s",
                                        "source-path": `/project/${cfg.projectId}/edit/${cfg.mediaId}`,
                                        bl: bl,
                                        "f.sid": fSid,
                                        hl: "zh-CN",
                                        _reqid: String(Math.floor(100000 + Math.random() * 900000)),
                                        rt: "c"
                                    });
                                    const body = new URLSearchParams({
                                        "f.req": JSON.stringify([[["as29s", JSON.stringify([cfg.mediaId]), null, "generic"]]]),
                                        "at": at
                                    });
                                    const resp = await fetch(`https://flow.google.com/_/AiSandboxAngularFrontend/data/batchexecute?${q.toString()}`, {
                                        method: "POST",
                                        credentials: "include",
                                        headers: {
                                            "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
                                            "x-same-domain": "1"
                                        },
                                        body: body.toString()
                                    });
                                    return { status: resp.status, text: await resp.text() };
                                }""",
                                arguments=[
                                    _to_bidi_local_value(
                                        {"projectId": project_id, "mediaId": mid}
                                    )
                                ],
                                await_promise=True,
                                timeout=30.0,
                            )
                            as29s_text = (
                                str(as29s_res.get("text") or "")
                                if isinstance(as29s_res, dict)
                                else ""
                            )
                            as29s_frames = (
                                FlowFrontendMixin._parse_batchexecute_frames(
                                    as29s_text
                                )
                                if as29s_text
                                else []
                            )
                            fife_url = (
                                FlowFrontendMixin._find_flow_content_url(
                                    as29s_frames,
                                    "video" if media_kind == "video" else "image",
                                )
                                if as29s_frames
                                else ""
                            )
                            if not fife_url and as29s_text:
                                kind_token = "video" if media_kind == "video" else "image"
                                url_match = re.search(
                                    rf"https://(?:flow-content\.google|storage\.googleapis\.com/ai-sandbox-videofx)/{kind_token}/[^\s\"'\\]+",
                                    as29s_text,
                                )
                                fife_url = (
                                    url_match.group(0) if url_match else ""
                                )
                            if fife_url:
                                if media_kind == "video":
                                    resolved_media.append(
                                        {
                                            "name": mid,
                                            "projectId": project_id,
                                            "video": {
                                                "fifeUrl": fife_url,
                                                "mediaGenerationId": mid,
                                            },
                                        }
                                    )
                                else:
                                    resolved_media.append(
                                        {
                                            "name": mid,
                                            "projectId": project_id,
                                            "image": {
                                                "generatedImage": {
                                                    "fifeUrl": fife_url,
                                                    "mediaGenerationId": mid,
                                                }
                                            },
                                        }
                                    )
                        await self._sync_live_cookies_to_db(token_id)
                        self._consecutive_risk_failures = 0
                        return {
                            "media": resolved_media,
                            "mediaIds": media_ids,
                            "frames": frames,
                            "responseText": full_chunks,
                            "rawLength": len(full_chunks),
                            "session_id": f";{int(time.time() * 1000)}",
                            "fingerprint": self.get_last_fingerprint(),
                        }
                    finally:
                        try:
                            await self._sync_live_cookies_to_db(token_id)
                        except Exception:
                            pass
                        pass
            finally:
                self._solve_inflight = max(0, self._solve_inflight - 1)
                self.note_idle()

    async def refresh_google_cookies(
        self, *, token_id: int, token_proxy_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """Refresh bound account's Google/Flow cookies using Camoufox."""
        if not token_id:
            raise ValueError("Cookie 刷新必须指定 Token ID")
        async with self._semaphore:
            self._solve_inflight += 1
            try:
                _, bidi, context_id = await self._get_or_create_shared_browser(
                    token_proxy_url=token_proxy_url, token_id=token_id
                )
                eval_meta_js = r"""(() => {
                    const wiz = window.WIZ_global_data || {};
                    let email = String(wiz.oPEP7c || '');
                    if (!email && document.documentElement) {
                        const m = document.documentElement.innerHTML.match(/"oPEP7c"\s*:\s*"([^"@]+@[^"\\]+)"/);
                        if (m) email = m[1];
                    }
                    return {
                        url: String(location.href || ''),
                        email: email,
                        bl: String(wiz.cfb2h || ''),
                        fSid: String(wiz.FdrFJe || ''),
                        at: String(wiz.SNlM0e || '')
                    };
                })()"""
                async with self._boost_frames():
                    await bidi.command(
                        "browsingContext.navigate",
                        {
                            "context": context_id,
                            "url": FLOW_PROJECTS_URL,
                            "wait": "complete",
                        },
                        timeout=35.0,
                    )
                    page_meta = await bidi.evaluate(
                        context_id, eval_meta_js, await_promise=False
                    )
                    current_url = str((page_meta or {}).get("url") or "")
                    email = str((page_meta or {}).get("email") or "")
                    if not email and "accounts.google.com" not in current_url:
                        passive_url = (
                            "https://accounts.google.com/ServiceLogin?passive=1209600&osid=1"
                            f"&continue={quote(FLOW_PROJECTS_URL, safe='')}"
                            f"&followup={quote(FLOW_PROJECTS_URL, safe='')}"
                        )
                        try:
                            await bidi.command(
                                "browsingContext.navigate",
                                {
                                    "context": context_id,
                                    "url": passive_url,
                                    "wait": "complete",
                                },
                                timeout=25.0,
                            )
                            await asyncio.sleep(0.8)
                            page_meta = await bidi.evaluate(
                                context_id, eval_meta_js, await_promise=False
                            )
                            current_url = str((page_meta or {}).get("url") or "")
                            email = str((page_meta or {}).get("email") or "")
                        except Exception:
                            pass
                    if urlparse(current_url).hostname != "flow.google.com":
                        raise RuntimeError(
                            "Flow 登录已失效，请重新同步 Google Cookies"
                        )
                    if (page_meta or {}).get("bl") and (page_meta or {}).get("fSid"):
                        self._cached_wiz = {
                            "bl": str(page_meta.get("bl") or ""),
                            "fSid": str(page_meta.get("fSid") or ""),
                            "at": str(page_meta.get("at") or ""),
                        }
                serialized = await self._sync_live_cookies_to_db(
                    token_id, update_db=False
                )
                if not serialized:
                    exported = await self._export_live_google_cookies()
                    serialized = json.dumps(
                        exported, ensure_ascii=False, separators=(",", ":")
                    )
                return {
                    "google_cookies": serialized,
                    "email": email,
                }
            finally:
                self._solve_inflight = max(0, self._solve_inflight - 1)
                self.note_idle()

    async def get_custom_token(
        self,
        website_url: str,
        website_key: str,
        action: str = "homepage",
        enterprise: bool = False,
    ) -> Optional[str]:
        res = await self.get_custom_score(
            website_url=website_url,
            website_key=website_key,
            verify_url="",
            action=action,
            enterprise=enterprise,
        )
        return str((res or {}).get("token") or "").strip() or None

    async def get_custom_score(
        self,
        website_url: str,
        website_key: str,
        verify_url: str,
        action: str = "homepage",
        enterprise: bool = False,
    ) -> Dict[str, Any]:
        """Run score detector or custom reCAPTCHA verification inside Camoufox."""
        async with self._semaphore:
            self._solve_inflight += 1
            try:
                started_at = time.time()
                _, bidi, context_id = await self._get_or_create_shared_browser()
                async with self._boost_frames():
                    await self._set_tab_render_layers(True)
                    await bidi.command(
                        "browsingContext.navigate",
                        {
                            "context": context_id,
                            "url": website_url,
                            "wait": "complete",
                        },
                        timeout=35.0,
                    )
                    await self._send_trusted_gesture(context_id)
                    await asyncio.sleep(2.0)

                    execute_target = (
                        "grecaptcha.enterprise.execute"
                        if enterprise
                        else "grecaptcha.execute"
                    )
                    ready_target = (
                        "grecaptcha.enterprise.ready"
                        if enterprise
                        else "grecaptcha.ready"
                    )
                    token_expr = f"""
                    new Promise((resolve, reject) => {{
                        const timer = setTimeout(() => reject(new Error('timeout')), 25000);
                        try {{
                            {ready_target}(() => {{
                                {execute_target}({json.dumps(website_key)}, {{ action: {json.dumps(action)} }})
                                    .then(t => {{ clearTimeout(timer); resolve(t); }})
                                    .catch(e => {{ clearTimeout(timer); reject(e); }});
                            }});
                        }} catch (e) {{
                            clearTimeout(timer);
                            reject(e);
                        }}
                    }})
                    """
                    token = await bidi.evaluate(
                        context_id,
                        token_expr,
                        await_promise=True,
                        user_activation=True,
                        timeout=30.0,
                    )
                    token_elapsed_ms = int((time.time() - started_at) * 1000)

                    verify_result: Dict[str, Any] = {}
                    verify_elapsed_ms = 0
                    if verify_url:
                        v_start = time.time()
                        for _ in range(20):
                            dom_info = await bidi.evaluate(
                                context_id,
                                r"""
                                (() => {
                                    const bodyText = (document.body && document.body.innerText) || '';
                                    const match = bodyText.match(/Your score is\s*:\s*([0-9.]+)/i) || bodyText.match(/score\s*[:=]\s*([0-9.]+)/i);
                                    return {
                                        score: match ? parseFloat(match[1]) : null,
                                        raw_text: bodyText.slice(0, 2000),
                                        url: location.href,
                                        title: document.title
                                    };
                                })()
                                """,
                                await_promise=False,
                            )
                            if isinstance(dom_info, dict) and isinstance(
                                dom_info.get("score"), (int, float)
                            ):
                                verify_elapsed_ms = int(
                                    (time.time() - v_start) * 1000
                                )
                                verify_result = {
                                    "success": True,
                                    "score": dom_info["score"],
                                    "source": "camoufox_dom",
                                    "raw_text": dom_info.get("raw_text") or "",
                                    "page_url": dom_info.get("url") or "",
                                    "page_title": dom_info.get("title") or "",
                                }
                                break
                            await asyncio.sleep(0.5)
                await self._set_tab_render_layers(False)
                return {
                    "token": str(token or "") or None,
                    "token_elapsed_ms": token_elapsed_ms,
                    "verify_mode": "browser_page_dom",
                    "verify_elapsed_ms": verify_elapsed_ms,
                    "verify_http_status": 200 if verify_result.get("success") else None,
                    "verify_result": verify_result,
                }
            except Exception as exc:
                debug_logger.log_warning(
                    f"[Camoufox] 自定义分数测试失败: {exc}"
                )
                return {
                    "token": None,
                    "verify_mode": "browser_page",
                    "verify_elapsed_ms": 0,
                    "verify_http_status": None,
                    "verify_result": {"success": False, "error": str(exc)},
                }
            finally:
                self._solve_inflight = max(0, self._solve_inflight - 1)
                self.note_idle()


# ============================================================================
# 6. Startup Cache, Storage State & Direct Browser Login (AIStudio2API login.go + liveview.go)
# ============================================================================

PROFILE_STARTUP_DIR_NAME = "profile-startup"
STARTUP_CACHE_ENTRIES = ("compatibility.ini", "startupCache")


def restore_startup_cache(account_dir: Path, profile_dir: Path) -> None:
    """Copy cached `compatibility.ini` and `startupCache` from `account_dir/profile-startup` into `profile_dir`."""
    source_root = account_dir / PROFILE_STARTUP_DIR_NAME
    if not source_root.exists():
        return
    try:
        _copy_startup_cache_entries(source_root, profile_dir)
    except Exception as exc:
        debug_logger.log_warning(f"[Camoufox] 恢复启动缓存失败: {exc}")


def save_startup_cache(profile_dir: Path, account_dir: Path) -> None:
    """Persist `compatibility.ini` and `startupCache` from `profile_dir` back to `account_dir/profile-startup`."""
    if not (profile_dir / "startupCache").exists():
        return
    target = account_dir / PROFILE_STARTUP_DIR_NAME
    staging = account_dir / f"{PROFILE_STARTUP_DIR_NAME}.new"
    shutil.rmtree(staging, ignore_errors=True)
    try:
        _copy_startup_cache_entries(profile_dir, staging)
        shutil.rmtree(target, ignore_errors=True)
        staging.rename(target)
    except Exception as exc:
        shutil.rmtree(staging, ignore_errors=True)
        debug_logger.log_warning(f"[Camoufox] 保存启动缓存失败: {exc}")


def _copy_startup_cache_entries(source: Path, target: Path) -> None:
    for entry_name in STARTUP_CACHE_ENTRIES:
        src_entry = source / entry_name
        if not src_entry.exists():
            continue
        dst_entry = target / entry_name
        if src_entry.is_dir():
            shutil.copytree(src_entry, dst_entry, dirs_exist_ok=True)
        else:
            dst_entry.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_entry, dst_entry)


def _clear_stale_firefox_profile_locks(profile_dir: Path) -> None:
    """Remove stale Firefox profile lock files after a clean or forced exit."""
    for lock_name in (".parentlock", "lock", "parent.lock"):
        lock_path = profile_dir / lock_name
        try:
            if lock_path.exists() or lock_path.is_symlink():
                lock_path.unlink(missing_ok=True)
        except Exception:
            pass


def persist_account_login_artifacts(source_dir: Path, target_dir: Path) -> None:
    """Copy fingerprint, storage-state, startup cache, HTTP disk cache, and full Firefox profile from isolated login dir to account dir."""
    if not source_dir.exists() or source_dir.resolve() == target_dir.resolve():
        return
    target_dir.mkdir(parents=True, exist_ok=True)
    for file_name in ("camoufox-fingerprint.json", "storage-state.json"):
        src_file = source_dir / file_name
        if src_file.is_file():
            shutil.copy2(src_file, target_dir / file_name)
    for dir_name in (PROFILE_STARTUP_DIR_NAME, "camoufox-cache", "firefox-profile"):
        src_dir = source_dir / dir_name
        if src_dir.is_dir():
            dst_dir = target_dir / dir_name
            if dir_name == "firefox-profile" and dst_dir.exists():
                shutil.rmtree(dst_dir, ignore_errors=True)
            shutil.copytree(
                src_dir,
                dst_dir,
                dirs_exist_ok=True,
                ignore=shutil.ignore_patterns(".parentlock", "lock", "parent.lock"),
            )
            if dir_name == "firefox-profile":
                _clear_stale_firefox_profile_locks(dst_dir)
def has_display() -> bool:
    """Check whether the host machine can display a native GUI browser window."""
    if sys.platform in ("darwin", "win32"):
        return True
    return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))


WEBDRIVER_KEYS: Dict[str, str] = {
    "Cancel": "\uE001",
    "Help": "\uE002",
    "Backspace": "\uE003",
    "Tab": "\uE004",
    "Clear": "\uE005",
    "Enter": "\uE007",
    "Shift": "\uE008",
    "Control": "\uE009",
    "Alt": "\uE00A",
    "Pause": "\uE00B",
    "Escape": "\uE00C",
    "PageUp": "\uE00E",
    "PageDown": "\uE00F",
    "End": "\uE010",
    "Home": "\uE011",
    "ArrowLeft": "\uE012",
    "ArrowUp": "\uE013",
    "ArrowRight": "\uE014",
    "ArrowDown": "\uE015",
    "Insert": "\uE016",
    "Delete": "\uE017",
    "F1": "\uE031",
    "F2": "\uE032",
    "F3": "\uE033",
    "F4": "\uE034",
    "F5": "\uE035",
    "F6": "\uE036",
    "F7": "\uE037",
    "F8": "\uE038",
    "F9": "\uE039",
    "F10": "\uE03A",
    "F11": "\uE03B",
    "F12": "\uE03C",
    "Meta": "\uE03D",
    "ZenkakuHankaku": "\uE040",
}


class CamoufoxLiveView:
    """Streams live browser screenshots and forwards mouse/keyboard/wheel/paste inputs over WebSocket (AIStudio2API liveview.go)."""

    def __init__(self) -> None:
        self.status: str = "starting"
        self.message: str = "正在启动 Camoufox 浏览器..."
        self.frame_meta: Optional[Dict[str, Any]] = None
        self.frame_b64: str = ""
        self.frame_seq: int = 0
        self.dialog: Optional[Dict[str, Any]] = None
        self.dialog_seq: int = 0
        self.contexts: List[str] = []
        self._bidi: Optional[BiDiClient] = None
        self._inputs: deque[Dict[str, Any]] = deque()
        self._input_event = asyncio.Event()
        self._wake_event = asyncio.Event()
        self._closed = False
        self._tasks: List[asyncio.Task] = []

    def set_status(self, status: str, message: str = "") -> None:
        self.status = status
        if message:
            self.message = message
        self._wake_event.set()

    async def attach(self, bidi: BiDiClient, root_context_id: str) -> None:
        self._bidi = bidi
        self.contexts = [root_context_id]
        bidi.add_listener(self._on_bidi_event)
        try:
            await bidi.command(
                "session.subscribe",
                {
                    "events": [
                        "browsingContext.contextCreated",
                        "browsingContext.contextDestroyed",
                        "browsingContext.userPromptOpened",
                        "browsingContext.userPromptClosed",
                    ]
                },
                timeout=10.0,
            )
        except Exception:
            pass
        self._tasks = [
            asyncio.create_task(self._capture_loop()),
            asyncio.create_task(self._input_loop()),
        ]

    async def close(self) -> None:
        self._closed = True
        self._wake_event.set()
        self._input_event.set()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._bidi is not None:
            self._bidi.remove_listener(self._on_bidi_event)
            self._bidi = None

    def active_context(self) -> str:
        return self.contexts[-1] if self.contexts else ""

    def _on_bidi_event(
        self, method: str, params: Optional[Dict[str, Any]] = None
    ) -> None:
        params = params or {}
        ctx_id = str(params.get("context") or "")
        if method == "browsingContext.contextCreated":
            if params.get("parent") is None and ctx_id and ctx_id not in self.contexts:
                self.contexts.append(ctx_id)
        elif method == "browsingContext.contextDestroyed":
            if ctx_id in self.contexts and len(self.contexts) > 1:
                self.contexts.remove(ctx_id)
        elif method == "browsingContext.userPromptOpened":
            self.dialog = {
                "context": ctx_id,
                "kind": str(params.get("type") or "alert"),
                "message": str(params.get("message") or ""),
                "value": str(params.get("defaultValue") or ""),
            }
            self.dialog_seq += 1
            self._wake_event.set()
        elif method == "browsingContext.userPromptClosed":
            self.dialog = None
            self.dialog_seq += 1
            self._wake_event.set()

    def enqueue_input(self, item: Dict[str, Any]) -> None:
        if not isinstance(item, dict):
            return
        itype = str(item.get("type") or "")
        if itype == "dialog":
            asyncio.create_task(
                self._answer_dialog(
                    bool(item.get("accept", True)), str(item.get("text") or "")
                )
            )
            return
        # Coalesce consecutive pointer moves or wheel deltas
        if self._inputs:
            last = self._inputs[-1]
            if last.get("type") == itype:
                if (
                    itype == "pointer"
                    and item.get("action") == "move"
                    and last.get("action") == "move"
                ):
                    self._inputs[-1] = item
                    self._input_event.set()
                    return
                if (
                    itype == "wheel"
                    and last.get("x") == item.get("x")
                    and last.get("y") == item.get("y")
                ):
                    last["dx"] = float(last.get("dx") or 0) + float(item.get("dx") or 0)
                    last["dy"] = float(last.get("dy") or 0) + float(item.get("dy") or 0)
                    self._input_event.set()
                    return
        self._inputs.append(item)
        self._input_event.set()

    async def _answer_dialog(self, accept: bool, text: str) -> None:
        dialog = self.dialog
        bidi = self._bidi
        if not dialog or not bidi:
            return
        params: Dict[str, Any] = {
            "context": dialog["context"],
            "accept": accept,
        }
        if dialog.get("kind") == "prompt" and accept:
            params["userText"] = text
        try:
            await bidi.command("browsingContext.handleUserPrompt", params, timeout=5.0)
        except Exception:
            pass

    async def _capture_loop(self) -> None:
        while not self._closed:
            started = time.monotonic()
            bidi = self._bidi
            ctx_id = self.active_context()
            if bidi and ctx_id and not bidi._closed:
                try:
                    shot = await bidi.command(
                        "browsingContext.captureScreenshot",
                        {
                            "context": ctx_id,
                            "format": {"type": "image/jpeg", "quality": 0.78},
                        },
                        timeout=4.0,
                    )
                    b64_data = str(shot.get("data") or "")
                    if b64_data and b64_data != self.frame_b64:
                        meta = await bidi.evaluate(
                            ctx_id,
                            "(() => [location.href, document.title || '', innerWidth || 1280, innerHeight || 800, devicePixelRatio || 1])()",
                            await_promise=False,
                            timeout=2.5,
                        )
                        if isinstance(meta, list) and len(meta) >= 5:
                            self.frame_meta = {
                                "type": "frame",
                                "context": ctx_id,
                                "url": str(meta[0] or ""),
                                "title": str(meta[1] or ""),
                                "width": float(meta[2] or 1280),
                                "height": float(meta[3] or 800),
                                "scale": float(meta[4] or 1),
                            }
                        elif self.frame_meta is None:
                            self.frame_meta = {
                                "type": "frame",
                                "context": ctx_id,
                                "url": "",
                                "title": "",
                                "width": 1280.0,
                                "height": 800.0,
                                "scale": 1.0,
                            }
                        self.frame_b64 = b64_data
                        self.frame_seq += 1
                        self._wake_event.set()
                except Exception:
                    pass
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.05, 0.14 - elapsed))

    async def _input_loop(self) -> None:
        while not self._closed:
            await self._input_event.wait()
            self._input_event.clear()
            if self._closed:
                break
            bidi = self._bidi
            if not bidi or bidi._closed:
                continue
            batch: List[Dict[str, Any]] = []
            while self._inputs:
                batch.append(self._inputs.popleft())
            for item in batch:
                ctx_id = str(item.get("context") or "") or self.active_context()
                if not ctx_id:
                    continue
                source = self._build_action_source(item)
                if not source:
                    continue
                try:
                    await bidi.command(
                        "input.performActions",
                        {"context": ctx_id, "actions": [source]},
                        timeout=5.0,
                    )
                except Exception:
                    pass

    def _build_action_source(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        itype = str(item.get("type") or "")
        meta = self.frame_meta or {}
        max_w = max(1, int(meta.get("width") or 1280))
        max_h = max(1, int(meta.get("height") or 800))
        x = min(max(int(round(float(item.get("x") or 0))), 0), max_w - 1)
        y = min(max(int(round(float(item.get("y") or 0))), 0), max_h - 1)
        action = str(item.get("action") or "")

        if itype == "pointer":
            move = {"type": "pointerMove", "x": x, "y": y}
            button = int(item.get("button") or 0)
            if action == "move":
                acts = [move]
            elif action == "down":
                acts = [move, {"type": "pointerDown", "button": button}]
            elif action == "up":
                acts = [move, {"type": "pointerUp", "button": button}]
            elif action == "click":
                acts = [
                    move,
                    {"type": "pointerDown", "button": button},
                    {"type": "pointerUp", "button": button},
                ]
            else:
                return None
            return {
                "type": "pointer",
                "id": "mouse",
                "parameters": {"pointerType": "mouse"},
                "actions": acts,
            }
        if itype == "wheel":
            dx = int(round(float(item.get("dx") or 0)))
            dy = int(round(float(item.get("dy") or 0)))
            return {
                "type": "wheel",
                "id": "wheel",
                "actions": [{"type": "scroll", "x": x, "y": y, "deltaX": dx, "deltaY": dy}],
            }
        if itype == "key":
            key_name = str(item.get("key") or "")
            key_val = WEBDRIVER_KEYS.get(key_name) or (
                key_name if len(key_name) == 1 else ""
            )
            if not key_val or action not in ("down", "up"):
                return None
            return {
                "type": "key",
                "id": "keyboard",
                "actions": [
                    {
                        "type": "keyDown" if action == "down" else "keyUp",
                        "value": key_val,
                    }
                ],
            }
        if itype == "text":
            text = str(item.get("text") or "")
            key_actions: List[Dict[str, Any]] = []
            for ch in text:
                if ch == "\n":
                    val = WEBDRIVER_KEYS["Enter"]
                elif ch == "\t":
                    val = WEBDRIVER_KEYS["Tab"]
                elif ord(ch) < 0x20 or ord(ch) == 0x7F:
                    continue
                else:
                    val = ch
                key_actions.append({"type": "keyDown", "value": val})
                key_actions.append({"type": "keyUp", "value": val})
            if not key_actions:
                return None
            return {"type": "key", "id": "keyboard", "actions": key_actions}
        return None


FLOW_LOGIN_DETECT_SCRIPT = r"""
(async () => {
    const href = String(location.href || '');
    const host = String(location.hostname || '').toLowerCase();
    const result = {
        url: href,
        host: host,
        title: String(document.title || ''),
        is_flow_origin: host === 'flow.google.com' || host === 'labs.google',
        is_auth_page: host.includes('accounts.google') || href.includes('/about') || href.includes('ServiceLogin'),
        has_onboarding_dialog: false,
        workspace_ready: false,
        xsrf_token: '',
        email: '',
        project_id: '',
        local_storage: []
    };
    try {
        result.local_storage = Object.keys(localStorage).sort().map(name => ({
            name,
            value: String(localStorage.getItem(name) ?? '')
        }));
    } catch (e) {}

    const projMatch = href.match(/\/project\/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})/);
    if (projMatch) {
        result.project_id = projMatch[1];
    }

    if (result.is_flow_origin && !result.is_auth_page) {
        try {
            const consentHints = [
                '隐私权政策', 'privacy policy', '体验并参与塑造', '您的数据和', 'your data and',
                '下一步', '上一步', '同意', '接受'
            ];
            const keywords = [
                '下一步', '继续', '同意', '接受', '开始', '知道了', '跳过', '完成',
                'next', 'continue', 'start', 'got it', 'agree', 'accept', 'skip', 'done'
            ];
            const dialogs = Array.from(
                document.querySelectorAll('[role="dialog"], [role="alertdialog"], [aria-modal="true"], .cdk-overlay-pane')
            ).filter(el => {
                if (el.offsetWidth <= 50 || el.offsetHeight <= 50) return false;
                const t = String(el.innerText || '').toLowerCase();
                return consentHints.some(h => t.includes(h)) || Array.from(el.querySelectorAll('button, [role="button"]')).some(
                    b => keywords.some(k => String(b.innerText || b.textContent || '').trim().toLowerCase() === k)
                );
            });
            if (dialogs.length > 0) {
                result.has_onboarding_dialog = true;
                let didScroll = false;
                for (const dlg of dialogs) {
                    const scrollables = [dlg, ...Array.from(dlg.querySelectorAll('*'))];
                    for (const s of scrollables) {
                        if (s.scrollHeight > s.clientHeight + 4 && s.scrollTop + s.clientHeight < s.scrollHeight - 2) {
                            s.scrollTop = s.scrollHeight;
                            s.dispatchEvent(new Event('scroll', { bubbles: true }));
                            s.dispatchEvent(new WheelEvent('wheel', { deltaY: 800, bubbles: true }));
                            didScroll = true;
                        }
                    }
                }
                if (didScroll) {
                    await new Promise(r => setTimeout(r, 280));
                }
                for (const dlg of dialogs) {
                    const btns = Array.from(dlg.querySelectorAll('button, [role="button"]'));
                    for (const btn of btns) {
                        if (btn.disabled || btn.getAttribute('aria-disabled') === 'true') continue;
                        const txt = String(btn.innerText || btn.textContent || '').trim().toLowerCase();
                        if (txt && txt.length <= 24 && keywords.some(k => txt === k || txt.includes(k))) {
                            btn.click();
                            break;
                        }
                    }
                }
            }
            const bodyText = String(document.body?.innerText || '');
            const isLoading = bodyText.includes('正在加载') || bodyText.includes('Loading...');
            const hasProjectUI = Boolean(
                result.project_id
                || document.querySelector('a[href*="/project/"]')
                || bodyText.includes('新建项目')
                || bodyText.includes('New project')
            );
            result.workspace_ready = !result.has_onboarding_dialog && !isLoading && hasProjectUI;
        } catch (e) {}
    }
    try {
        const wiz = window.WIZ_global_data;
        if (wiz && typeof wiz === 'object') {
            if (typeof wiz.SNlM0e === 'string' && wiz.SNlM0e) {
                result.xsrf_token = wiz.SNlM0e;
            }
            for (const v of Object.values(wiz)) {
                if (typeof v === 'string' && /^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$/i.test(v.trim())) {
                    result.email = v.trim().toLowerCase();
                    break;
                }
            }
        }
    } catch (e) {}

    if (!result.email) {
        try {
            const values = [];
            for (const el of document.querySelectorAll('[aria-label],[data-email]')) {
                values.push(el.getAttribute('data-email') || '');
                values.push(el.getAttribute('aria-label') || '');
            }
            values.push(document.body?.innerText || '');
            const m = values.join('\n').match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
            if (m) {
                result.email = m[0].toLowerCase();
            }
        } catch (e) {}
    }

    if (result.is_flow_origin && !result.is_auth_page && !result.email) {
        try {
            const resp = await fetch('/fx/api/auth/session', { credentials: 'include' });
            if (resp.ok) {
                const data = await resp.json();
                const sessionEmail = data?.user?.email;
                if (typeof sessionEmail === 'string' && sessionEmail.includes('@')) {
                    result.email = sessionEmail.trim().toLowerCase();
                }
            }
        } catch (e) {}
    }
    return result;
})()
"""


class CamoufoxLoginSession:
    """Isolated Camoufox login session that waits for Google Flow authentication and extracts cookies, storageState, fingerprint, and startupCache."""

    def __init__(
        self,
        session_id: str,
        *,
        mode: str = "auto",
        proxy_url: Optional[str] = None,
        token_id: Optional[int] = None,
        auto_save_token: bool = True,
        remark: Optional[str] = None,
        project_id: Optional[str] = None,
        project_name: Optional[str] = None,
        timeout_seconds: float = 300.0,
        user_data_root: Optional[Path] = None,
    ) -> None:
        self.session_id = session_id
        self.mode = mode  # "window" | "liveview" | "auto"
        self.proxy_url = (proxy_url or "").strip() or None
        self.token_id = token_id
        self.auto_save_token = auto_save_token
        self.remark = remark
        self.project_id = project_id
        self.project_name = project_name
        self.timeout_seconds = max(30.0, float(timeout_seconds or 300.0))
        self.user_data_root = user_data_root or (
            Path(__file__).resolve().parent.parent.parent / "browser_data_rt"
        )
        self.live_view = CamoufoxLiveView()
        self.status: str = "starting"  # starting | waiting | saving | completed | failed | cancelled
        self.message: str = "正在启动 Camoufox 浏览器..."
        self.error: Optional[str] = None
        self.result: Optional[Dict[str, Any]] = None
        self.created_at: float = time.time()
        self.headless: bool = False
        self._process: Optional[asyncio.subprocess.Process] = None
        self._bidi: Optional[BiDiClient] = None
        self._relay: Optional[LocalSocks5Relay] = None
        self._temp_account_dir: Optional[Path] = None
        self._profile_dir: Optional[Path] = None
        self._task: Optional[asyncio.Task] = None
        self._cancel_event = asyncio.Event()

    def set_state(self, status: str, message: str, error: Optional[str] = None) -> None:
        self.status = status
        self.message = message
        if error is not None:
            self.error = error
        self.live_view.set_status(status, message)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "status": self.status,
            "message": self.message,
            "error": self.error,
            "mode": "liveview" if self.headless else "window",
            "headless": self.headless,
            "has_display": has_display(),
            "token_id": self.token_id,
            "auto_save_token": self.auto_save_token,
            "result": self.result,
            "frame_seq": self.live_view.frame_seq,
            "page_url": (self.live_view.frame_meta or {}).get("url") or "",
            "page_title": (self.live_view.frame_meta or {}).get("title") or "",
            "dialog": self.live_view.dialog,
        }

    async def start(
        self,
        on_complete: Optional[Callable[["CamoufoxLoginSession", Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None,
    ) -> None:
        self._task = asyncio.create_task(self._run(on_complete))

    async def cancel(self) -> None:
        self._cancel_event.set()
        if self.status not in ("completed", "failed"):
            self.set_state("cancelled", "浏览器登录已取消")
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        await self._cleanup(save_cache=False)

    async def _run(
        self,
        on_complete: Optional[Callable[["CamoufoxLoginSession", Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None,
    ) -> None:
        save_cache = False
        try:
            exe_path = await ensure_camoufox_executable()
            if self.mode == "liveview":
                self.headless = True
            elif self.mode == "window":
                if not has_display():
                    self.headless = True
                else:
                    self.headless = False
            else:
                # auto: use local visible window if display available, else headless liveview
                self.headless = not has_display()

            self._temp_account_dir = Path(
                tempfile.mkdtemp(prefix="flow2api-camoufox-login-account-")
            )
            # If relogging an existing token, release any active worker holding its profile and reuse its persistent artifacts
            if self.token_id:
                try:
                    from .browser_captcha import BrowserCaptchaService
                    svc = BrowserCaptchaService._instance
                    if svc is not None:
                        await svc.release_token_browsers(
                            int(self.token_id), reason="camoufox_relogin_start"
                        )
                except Exception:
                    pass
                existing_dir = self.user_data_root / f"token_{self.token_id}"
                if existing_dir.exists():
                    # Copy only the account's hardware fingerprint & startupCache so login starts with a clean cookie state (AIStudio2API login.go)
                    fp_file = existing_dir / "camoufox-fingerprint.json"
                    if fp_file.is_file():
                        shutil.copy2(fp_file, self._temp_account_dir / "camoufox-fingerprint.json")
                    startup_dir = existing_dir / PROFILE_STARTUP_DIR_NAME
                    if startup_dir.is_dir():
                        shutil.copytree(
                            startup_dir,
                            self._temp_account_dir / PROFILE_STARTUP_DIR_NAME,
                            dirs_exist_ok=True,
                        )
            fp_config = load_or_create_account_camoufox_config(
                self._temp_account_dir,
                CAMOUFOX_FIREFOX_MAJOR,
                "en-US",
                "America/New_York",
            )

            relay_addr: Optional[Tuple[str, int]] = None
            if self.proxy_url:
                parsed = urlparse(self.proxy_url)
                if parsed.username or parsed.password or "socks" in (
                    parsed.scheme or ""
                ).lower():
                    self._relay = LocalSocks5Relay(self.proxy_url)
                    relay_addr = await self._relay.start()

            self._profile_dir = self._temp_account_dir / "firefox-profile"
            self._profile_dir.mkdir(parents=True, exist_ok=True)
            _clear_stale_firefox_profile_locks(self._profile_dir)
            restore_startup_cache(self._temp_account_dir, self._profile_dir)
            cache_dir = self._temp_account_dir / "camoufox-cache"

            # Pass actual self.headless + live_view=True so macOS window mode disables separate GPU process
            write_camoufox_user_js(
                self._profile_dir,
                fp_config,
                proxy_url=self.proxy_url,
                relay_addr=relay_addr,
                headless=self.headless,
                live_view=True,
                cache_dir=cache_dir,
                executable_path=exe_path,
            )

            args = [exe_path, "--remote-debugging-port=0"]
            if self.headless:
                args.extend(["-no-remote", "-headless"])
            else:
                args.append("-wait-for-browser")
            args.extend(["-profile", str(self._profile_dir)])

            env = build_camoufox_env(
                fp_config,
                headless=self.headless,
                live_view=True,
                profile_dir=self._profile_dir,
                executable_path=exe_path,
            )
            self._process = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
            )
            endpoint = await CamoufoxWorker._wait_for_bidi_endpoint(
                self._process, timeout=45.0
            )
            self._bidi = await BiDiClient.connect(endpoint, timeout=30.0)
            capabilities: Dict[str, Any] = {
                "alwaysMatch": {
                    "unhandledPromptBehavior": {
                        "alert": "ignore",
                        "confirm": "ignore",
                        "prompt": "ignore",
                    }
                }
            }
            await self._bidi.command("session.new", {"capabilities": capabilities})

            tree = await self._bidi.command("browsingContext.getTree", {"maxDepth": 0})
            contexts = tree.get("contexts") or []
            if not contexts or not isinstance(contexts[0], dict):
                raise RuntimeError("Camoufox BiDi 未返回初始标签页")
            root_ctx = str(contexts[0].get("context") or "")
            if not root_ctx:
                raise RuntimeError("Camoufox BiDi 初始标签页无效")

            # Always attach LiveView so the Web UI can preview and interact even in window mode
            await self.live_view.attach(self._bidi, root_ctx)

            if self.headless:
                self.set_state(
                    "waiting",
                    "请在下方远程浏览器画面中登录 Google 账号并进入 Flow 页面，登录成功后将自动拉取 Cookie 与缓存...",
                )
            else:
                self.set_state(
                    "waiting",
                    "已弹出 Camoufox 浏览器窗口（也可在下方画面直接操作），请完成 Google 登录进入 Flow，成功后将自动抓取 Cookie 与缓存...",
                )

            try:
                await self._bidi.command(
                    "browsingContext.navigate",
                    {
                        "context": root_ctx,
                        "url": "https://flow.google.com/projects",
                        "wait": "none",
                    },
                    timeout=20.0,
                )
            except Exception as nav_exc:
                if "NS_ERROR_ABORT" not in str(nav_exc):
                    debug_logger.log_warning(f"[CamoufoxLogin] 初始导航提示: {nav_exc}")

            login_data = await self._wait_for_login()
            save_cache = True
            self.set_state(
                "saving",
                f"检测到 Flow 登录成功 ({login_data.get('email') or 'Google Account'})，正在导出 Cookie、StorageState 与浏览器缓存...",
            )

            # Give the page a brief moment to flush all cookies & localStorage
            await asyncio.sleep(1.2)
            exported = await self._export_login_state(login_data)

            # Close browser cleanly first so Firefox writes startupCache & cookies.sqlite to profile_dir
            await self._cleanup(save_cache=True)

            # Persist to browser_data_rt/token_<id> BEFORE on_complete if token_id is already known
            if self.token_id and self._temp_account_dir and self._temp_account_dir.exists():
                early_target_dir = self.user_data_root / f"token_{self.token_id}"
                persist_account_login_artifacts(self._temp_account_dir, early_target_dir)

            if on_complete is not None:
                saved_info = await on_complete(self, exported)
                exported.update(saved_info or {})

            # If a token_id was assigned/updated by on_complete, ensure login artifacts are persisted to browser_data_rt/token_<id>
            final_token_id = exported.get("token_id") or self.token_id
            if final_token_id and self._temp_account_dir and self._temp_account_dir.exists():
                try:
                    from .browser_captcha import BrowserCaptchaService
                    svc = BrowserCaptchaService._instance
                    if svc is not None:
                        await svc.release_token_browsers(
                            int(final_token_id), reason="camoufox_relogin_complete"
                        )
                except Exception:
                    pass
                target_dir = self.user_data_root / f"token_{final_token_id}"
                persist_account_login_artifacts(self._temp_account_dir, target_dir)
                exported["cache_persisted_dir"] = str(target_dir)
            self.result = exported
            self.set_state(
                "completed",
                f"浏览器登录并拉取缓存完成！账号: {exported.get('email') or '已同步'}",
            )
        except asyncio.CancelledError:
            if self.status not in ("completed", "cancelled"):
                self.set_state("cancelled", "浏览器登录已取消")
        except Exception as exc:
            debug_logger.log_error(f"[CamoufoxLogin] 登录流程失败: {exc}")
            self.set_state("failed", f"浏览器登录失败: {exc}", error=str(exc))
        finally:
            await self._cleanup(save_cache=save_cache)
            if self._temp_account_dir and self._temp_account_dir.exists():
                shutil.rmtree(self._temp_account_dir, ignore_errors=True)

    async def _wait_for_login(self) -> Dict[str, Any]:
        deadline = time.monotonic() + self.timeout_seconds
        origins: Dict[str, Any] = {}
        session_cookie_names = {
            "SID",
            "__Secure-1PSID",
            "__Secure-3PSID",
            "SAPISID",
            "__Secure-1PAPISID",
            "__Secure-3PAPISID",
        }
        while time.monotonic() < deadline and not self._cancel_event.is_set():
            if self._process and self._process.returncode is not None:
                raise RuntimeError("Camoufox 登录窗口已被关闭，未完成登录")
            bidi = self._bidi
            ctx_id = self.live_view.active_context()
            if not bidi or bidi._closed or not ctx_id:
                await asyncio.sleep(0.5)
                continue
            try:
                info = await bidi.evaluate(
                    ctx_id,
                    FLOW_LOGIN_DETECT_SCRIPT,
                    await_promise=True,
                    timeout=6.0,
                )
            except Exception:
                await asyncio.sleep(0.5)
                continue

            if isinstance(info, dict):
                page_url = str(info.get("url") or "")
                parsed = urlparse(page_url)
                host = (parsed.hostname or "").lower()
                if parsed.scheme == "https" and (
                    host == "google.com"
                    or host.endswith(".google.com")
                    or host == "labs.google"
                ):
                    ls_items = info.get("local_storage")
                    if isinstance(ls_items, list) and ls_items:
                        origin_key = f"{parsed.scheme}://{parsed.netloc}"
                        origins[origin_key] = {
                            "origin": origin_key,
                            "localStorage": ls_items,
                        }

                if info.get("is_flow_origin") and not info.get("is_auth_page"):
                    # Verify Google session cookies exist in browser storage
                    cookie_res = await bidi.command("storage.getCookies", {}, timeout=8.0)
                    raw_cookies = cookie_res.get("cookies") or []
                    found_session_cookies = {
                        str(c.get("name") or "")
                        for c in raw_cookies
                        if isinstance(c, dict)
                        and str(c.get("name") or "") in session_cookie_names
                    }
                    has_auth_signal = bool(
                        info.get("xsrf_token") or info.get("email")
                    )
                    if len(found_session_cookies) >= 2 and has_auth_signal:
                        if info.get("has_onboarding_dialog") or not info.get("workspace_ready"):
                            self.set_state(
                                "waiting",
                                "已登录进入 Flow，正在自动滚动并同意《隐私权政策》与引导条款（完成后自动保存）...",
                            )
                            await asyncio.sleep(0.7)
                            continue
                        return {
                            "email": str(info.get("email") or "").strip().lower(),
                            "xsrf_token": str(info.get("xsrf_token") or ""),
                            "project_id": str(info.get("project_id") or ""),
                            "page_url": page_url,
                            "origins": origins,
                        }
            await asyncio.sleep(0.6)
        raise RuntimeError("等待 Google Flow 登录超时，请重试")

    async def _export_login_state(self, login_data: Dict[str, Any]) -> Dict[str, Any]:
        assert self._bidi is not None
        in_page_meta: Dict[str, Any] = {}
        try:
            ctx = self.live_view.active_context()
            if ctx:
                meta_res = await self._bidi.evaluate(
                    ctx,
                    r"""(async () => {
                        const out = { email: '', project_id: '', credits: null, tier: '' };
                        try {
                            const wiz = window.WIZ_global_data || {};
                            const at = String(wiz.SNlM0e || '');
                            const bl = String(wiz.cfb2h || 'boq_labs-ai-sandbox-frontend_20260415.05_p0');
                            const fSid = String(wiz.FdrFJe || '');
                            const hl = String(document.documentElement.lang || navigator.language || 'en');
                            if (typeof wiz.oPEP7c === 'string' && wiz.oPEP7c.includes('@')) {
                                out.email = wiz.oPEP7c.trim().toLowerCase();
                            }
                            // Wait briefly for project cards or onboarding dialog to finish rendering
                            for (let w = 0; w < 8; w++) {
                                if (document.querySelector('a[href*="/project/"]') || /\/project\/[0-9a-f-]{36}/i.test(location.pathname)) break;
                                await new Promise(r => setTimeout(r, 400));
                            }
                            // Dismiss onboarding overlays if present
                            for (let i = 0; i < 4; i++) {
                                const btns = Array.from(document.querySelectorAll('.cdk-overlay-container button, [role="dialog"] button'));
                                const hit = btns.find(b => !b.disabled && ['下一步','继续','同意','接受','开始','知道了','跳过','next','continue','start','got it','agree','accept','skip'].some(k => (b.innerText || '').trim().toLowerCase().includes(k)));
                                if (!hit) break;
                                hit.click();
                                await new Promise(r => setTimeout(r, 350));
                            }
                            const m = location.pathname.match(/\/project\/([0-9a-f-]{36})/i);
                            if (m) out.project_id = m[1];
                            if (!out.project_id) {
                                for (const a of document.querySelectorAll('a[href*="/project/"]')) {
                                    const am = (a.getAttribute('href') || '').match(/\/project\/([0-9a-f-]{36})/i);
                                    if (am) { out.project_id = am[1]; break; }
                                }
                            }
                            if (at) {
                                const callRpc = async (rpcId, argJson) => {
                                    const qs = new URLSearchParams({
                                        'rpcids': rpcId,
                                        'source-path': location.pathname || '/projects',
                                        'bl': bl,
                                        'hl': hl,
                                        'soc-app': '1',
                                        'soc-platform': '1',
                                        'soc-device': '1',
                                        'rt': 'c'
                                    });
                                    if (fSid) qs.set('f.sid', fSid);
                                    const body = new URLSearchParams({
                                        'f.req': JSON.stringify([[[rpcId, argJson, null, 'generic']]]),
                                        'at': at
                                    });
                                    const resp = await fetch(`/_/LabsTailwindUi/data/batchexecute?${qs.toString()}`, {
                                        method: 'POST',
                                        credentials: 'include',
                                        headers: {
                                            'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
                                            'X-Same-Domain': '1'
                                        },
                                        body: body.toString()
                                    });
                                    return await resp.text();
                                };
                                try {
                                    const credText = await callRpc('i945dd', '["projects"]');
                                    const cm = credText.match(/\[(\d+),"PAYGATE_TIER_[A-Z_]+"\]/) || credText.match(/"PAYGATE_TIER_[A-Z_]+"/);
                                    const tm = credText.match(/PAYGATE_TIER_[A-Z_]+/);
                                    if (tm) out.tier = tm[0];
                                    const numMatch = credText.match(/\\?"\[(\d+),\\?"PAYGATE_TIER/);
                                    if (numMatch) out.credits = parseInt(numMatch[1], 10);
                                } catch (e) {}
                                if (!out.project_id) {
                                    try {
                                        const projText = await callRpc('jHPbke', JSON.stringify(['projects/*', [null, ['Flow Project']], [null, 22]]));
                                        const pm = projText.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i);
                                        if (pm) out.project_id = pm[0];
                                    } catch (e) {}
                                }
                            }
                        } catch (e) {}
                        return out;
                    })()""",
                    await_promise=True,
                    timeout=15.0,
                )
                if isinstance(meta_res, dict):
                    in_page_meta = meta_res
        except Exception as exc:
            debug_logger.log_warning(f"[CamoufoxLogin] 页面内预取项目与积分提示: {exc}")

        cookie_res = await self._bidi.command("storage.getCookies", {}, timeout=10.0)
        raw_cookies = cookie_res.get("cookies") or []
        flow_cookies: List[Dict[str, Any]] = []
        storage_cookies: List[Dict[str, Any]] = []

        for item in raw_cookies:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            domain = str(item.get("domain") or "").strip()
            path = str(item.get("path") or "/")
            if not name or not domain:
                continue
            clean_domain = domain.lstrip(".").lower()
            if not (
                clean_domain == "google.com"
                or clean_domain.endswith(".google.com")
                or clean_domain == "labs.google"
                or clean_domain.endswith(".labs.google")
            ):
                continue
            val_obj = item.get("value")
            value = (
                str(val_obj.get("value") or "")
                if isinstance(val_obj, dict)
                else str(val_obj or "")
            )
            expiry = item.get("expiry")
            expires = (
                float(expiry)
                if isinstance(expiry, (int, float)) and expiry > 0
                else -1.0
            )
            same_site_raw = str(item.get("sameSite") or "lax").strip().lower()
            same_site = (
                same_site_raw.capitalize()
                if same_site_raw in ("none", "lax", "strict")
                else "Lax"
            )
            http_only = bool(item.get("httpOnly", False))
            secure = bool(item.get("secure", True))
            host_only = not domain.startswith(".") and (
                clean_domain == "flow.google.com" or name.startswith("__Host-")
            )
            cookie_entry: Dict[str, Any] = {
                "name": name,
                "value": value,
                "domain": domain,
                "path": path,
                "expires": expires,
                "httpOnly": http_only,
                "secure": secure,
                "sameSite": same_site,
            }
            if host_only:
                cookie_entry["hostOnly"] = True
            flow_cookies.append(cookie_entry)
            storage_cookies.append(cookie_entry)

        if not flow_cookies:
            raise RuntimeError("未从 Camoufox 浏览器导出到有效的 Google/Flow Cookie")

        flow_cookies.sort(
            key=lambda c: (c.get("domain") or "", c.get("name") or "", c.get("path") or "")
        )
        origins_map = login_data.get("origins") or {}
        storage_origins = sorted(
            origins_map.values(), key=lambda o: str(o.get("origin") or "")
        )
        storage_state = {
            "cookies": storage_cookies,
            "origins": storage_origins,
        }
        if self._temp_account_dir and self._temp_account_dir.exists():
            (self._temp_account_dir / "storage-state.json").write_text(
                json.dumps(storage_state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        google_cookies_json = json.dumps(
            flow_cookies, ensure_ascii=False, separators=(",", ":")
        )
        return {
            "email": login_data.get("email") or in_page_meta.get("email") or "",
            "project_id": (
                self.project_id
                or login_data.get("project_id")
                or in_page_meta.get("project_id")
                or ""
            ),
            "credits": in_page_meta.get("credits"),
            "user_paygate_tier": in_page_meta.get("tier") or "",
            "page_url": login_data.get("page_url") or "https://flow.google.com/projects",
            "cookie_count": len(flow_cookies),
            "google_cookies": google_cookies_json,
        }

    async def _cleanup(self, save_cache: bool = False) -> None:
        await self.live_view.close()
        bidi = self._bidi
        proc = self._process
        relay = self._relay
        profile_dir = self._profile_dir
        temp_account_dir = self._temp_account_dir

        self._bidi = None
        self._process = None
        self._relay = None
        self._profile_dir = None

        if bidi is not None:
            try:
                await asyncio.wait_for(bidi.command("browser.close", {}), timeout=4.0)
            except Exception:
                pass
            await bidi.close()

        if proc is not None:
            if proc.returncode is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(proc.wait(), timeout=4.0)
                except Exception:
                    try:
                        proc.kill()
                        await asyncio.wait_for(proc.wait(), timeout=2.0)
                    except Exception:
                        pass

        if relay is not None:
            await relay.close()

        if profile_dir is not None and profile_dir.exists():
            if save_cache and temp_account_dir is not None and temp_account_dir.exists():
                save_startup_cache(profile_dir, temp_account_dir)
                _clear_stale_firefox_profile_locks(profile_dir)
            else:
                shutil.rmtree(profile_dir, ignore_errors=True)


class CamoufoxLoginManager:
    """Manages active Camoufox interactive login sessions for the Admin UI."""

    _instance: Optional["CamoufoxLoginManager"] = None

    def __init__(self) -> None:
        self._sessions: Dict[str, CamoufoxLoginSession] = {}
        self._lock = asyncio.Lock()

    @classmethod
    def get_instance(cls) -> "CamoufoxLoginManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def get_session(self, session_id: str) -> Optional[CamoufoxLoginSession]:
        return self._sessions.get(session_id)

    def get_active_session(self) -> Optional[CamoufoxLoginSession]:
        for sess in list(self._sessions.values()):
            if sess.status in ("starting", "waiting", "saving"):
                return sess
        return None

    async def start_session(
        self,
        *,
        mode: str = "auto",
        proxy_url: Optional[str] = None,
        token_id: Optional[int] = None,
        auto_save_token: bool = True,
        remark: Optional[str] = None,
        project_id: Optional[str] = None,
        project_name: Optional[str] = None,
        timeout_seconds: float = 300.0,
        on_complete: Optional[Callable[[CamoufoxLoginSession, Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None,
    ) -> CamoufoxLoginSession:
        async with self._lock:
            # Cancel any previous unfinished login session to avoid multiple login browsers
            for old_id, old_sess in list(self._sessions.items()):
                if old_sess.status in ("starting", "waiting"):
                    await old_sess.cancel()
                if time.time() - old_sess.created_at > 1800:
                    self._sessions.pop(old_id, None)

            session_id = uuid.uuid4().hex[:16]
            session = CamoufoxLoginSession(
                session_id,
                mode=mode,
                proxy_url=proxy_url,
                token_id=token_id,
                auto_save_token=auto_save_token,
                remark=remark,
                project_id=project_id,
                project_name=project_name,
                timeout_seconds=timeout_seconds,
            )
            self._sessions[session_id] = session
            await session.start(on_complete=on_complete)
            return session

    async def cancel_session(self, session_id: str) -> bool:
        sess = self._sessions.get(session_id)
        if not sess:
            return False
        await sess.cancel()
        return True
