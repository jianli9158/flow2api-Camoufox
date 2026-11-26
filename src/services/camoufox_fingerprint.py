"""Windows Firefox fingerprint generation, font substitution, binary management, and user.js writer for Camoufox."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import platform
import random
import re
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import threading
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse
import zipfile

_CAMOUFOX_INSTALL_LOCK = asyncio.Lock()
from ..core.logger import debug_logger


CAMOUFOX_RELEASE_VERSION = "152.0.4-beta.29"
CAMOUFOX_FIREFOX_MAJOR = 152
FIREFOX_VERSION_PATTERN = re.compile(r"\b1[0-9]{2}\.0\b")

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
    """Port of AIStudio2API `addWindowsSubstituteNames` (`fontnames.go`)."""
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
            raw_cfg = saved.get("config")
            if isinstance(raw_cfg, dict) and raw_cfg:
                cfg = dict(raw_cfg)
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
                if saved != saved_payload:
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

    async with _CAMOUFOX_INSTALL_LOCK:
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
                extract_root = extract_dir.resolve()
                with zipfile.ZipFile(zip_path, "r") as zf:
                    for info in zf.infolist():
                        dest = (extract_root / info.filename).resolve()
                        if not dest.is_relative_to(extract_root):
                            continue
                        mode = (info.external_attr >> 16) & 0xFFFF
                        if info.is_dir():
                            dest.mkdir(parents=True, exist_ok=True)
                            continue
                        if dest == extract_root:
                            continue
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        if (mode & 0o170000) == 0o120000:
                            link_target = zf.read(info).decode("utf-8")
                            if (
                                not link_target
                                or Path(link_target).is_absolute()
                                or not (dest.parent / link_target)
                                .resolve()
                                .is_relative_to(extract_root)
                            ):
                                continue
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
    _ = fingerprint_config, headless, live_view, cache_dir
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
        "browser.cache.disk.enable": False,
        "browser.cache.disk.smart_size.enabled": False,
        "browser.cache.disk.capacity": 30720,
        "browser.cache.disk.max_entry_size": 5120,
        "places.history.enabled": False,
        "browser.chrome.site_icons": False,
        "browser.chrome.favicons": False,
        "browser.sessionstore.resume_from_crash": False,
        "media.gmp-widevinecdm.enabled": False,
        "media.gmp-widevinecdm.visible": False,
        "media.gmp-widevinecdm.autoupdate": False,
        "media.gmp-provider.enabled": False,
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
        prefs["gfx.font_loader.delay"] = 0
        prefs["layers.gpu-process.enabled"] = False
        prefs["webgl.allow-in-parent"] = True

    prefs.update(
        {
            "network.proxy.type": 0,
            "network.proxy.http": "",
            "network.proxy.http_port": 0,
            "network.proxy.ssl": "",
            "network.proxy.ssl_port": 0,
            "network.proxy.socks": "",
            "network.proxy.socks_port": 0,
            "network.proxy.no_proxies_on": "localhost, 127.0.0.1, ::1",
        }
    )
    if relay_addr is not None:
        r_host, r_port = relay_addr
        prefs.update(
            {
                "network.proxy.type": 1,
                "network.proxy.socks": r_host,
                "network.proxy.socks_port": int(r_port),
                "network.proxy.socks_version": 5,
                "network.proxy.socks_remote_dns": True,
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
                'user_pref("network.proxy.',
                'user_pref("remote.prefs.recommended"',
                'user_pref("browser.cache.disk.',
                'user_pref("media.gmp-',
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


def _camoufox_subprocess_kwargs() -> Dict[str, Any]:
    """Return OS subprocess creation kwargs isolating Camoufox into its own process group."""
    if sys.platform == "win32":
        flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        return {"creationflags": flags} if flags else {}
    return {"start_new_session": True}


async def _terminate_camoufox_process(
    proc: Any, *, terminate_timeout: float = 4.0
) -> None:
    """Terminate Camoufox and its child process group safely (compatible with test mock Processes)."""
    if proc is None or getattr(proc, "returncode", None) is not None:
        return
    pid = getattr(proc, "pid", None)
    use_killpg = (
        sys.platform != "win32"
        and hasattr(os, "killpg")
        and isinstance(pid, int)
        and not isinstance(pid, bool)
        and pid > 0
    )

    def _signal_group_or_proc(sig: int, fallback_method: str) -> None:
        if use_killpg:
            try:
                os.killpg(pid, sig)
                return
            except (ProcessLookupError, PermissionError, TypeError, OSError):
                pass
        fn = getattr(proc, fallback_method, None)
        if callable(fn):
            fn()

    try:
        _signal_group_or_proc(signal.SIGTERM, "terminate")
    except Exception:
        pass
    try:
        await asyncio.wait_for(proc.wait(), timeout=terminate_timeout)
        return
    except Exception:
        pass
    try:
        _signal_group_or_proc(signal.SIGKILL, "kill")
        await asyncio.wait_for(proc.wait(), timeout=2.0)
    except Exception:
        pass


def _start_stdout_drain(
    proc: Optional[asyncio.subprocess.Process],
) -> Optional[asyncio.Task]:
    """Continuously drain subprocess stdout after BiDi startup to avoid 64KB OS pipe buffer deadlocks."""
    stdout = getattr(proc, "stdout", None) if proc is not None else None
    if stdout is None:
        return None

    async def _drain() -> None:
        try:
            at_eof = getattr(stdout, "at_eof", None)
            while not (callable(at_eof) and at_eof() is True):
                chunk = await stdout.read(65536)
                if not chunk:
                    break
        except (asyncio.CancelledError, Exception):
            pass

    return asyncio.create_task(_drain())
