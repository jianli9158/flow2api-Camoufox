"""Persistent browser profile directories, startup cache, login leases, and disk cache maintenance."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import fcntl as _fcntl
except ImportError:
    _fcntl = None  # type: ignore[assignment]

try:
    import msvcrt as _msvcrt
except ImportError:
    _msvcrt = None  # type: ignore[assignment]

from ..core.logger import debug_logger


PROFILE_STARTUP_DIR_NAME = "profile-startup"
STARTUP_CACHE_ENTRIES = ("compatibility.ini", "startupCache")

_RESERVED_PROFILE_NAMES = {"shared", "pending"}
_PROFILE_NAME_RE = re.compile(r"^[A-Za-z0-9._@-]+(?:/[A-Za-z0-9._@-]+)*$")
_ACTIVE_PROFILE_LOGIN_LEASES: Dict[Path, str] = {}
_ACTIVE_TOKEN_LOGIN_LEASES: Dict[int, str] = {}
_ACTIVE_PROFILE_KERNEL_LEASES: Dict[Path, Tuple["ProfileLease", str]] = {}
_PROFILE_LOGIN_LEASE_LOCK = threading.Lock()
PROFILE_KERNEL_LOCK_FILENAME = ".flow2api-profile.lock"


class ProfileLeaseBusyError(RuntimeError):
    """Raised when another worker or OS process holds the profile directory lock."""


class ProfileLease:
    """Cross-process + in-process OS kernel lock for a Camoufox account profile directory."""

    def __init__(
        self,
        account_dir: Path,
        lock_path: Path,
        fd: int,
        holder: str,
    ) -> None:
        self.account_dir = _canonical_lease_path(account_dir)
        self.lock_path = lock_path
        self.holder = holder
        self._fd: Optional[int] = fd
        self._released = False

    @property
    def active(self) -> bool:
        return not self._released and self._fd is not None

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        with _PROFILE_LOGIN_LEASE_LOCK:
            existing = _ACTIVE_PROFILE_KERNEL_LEASES.get(self.account_dir)
            if existing is not None and existing[0] is self:
                _ACTIVE_PROFILE_KERNEL_LEASES.pop(self.account_dir, None)
        fd = self._fd
        self._fd = None
        if fd is None:
            return
        try:
            if _fcntl is not None:
                _fcntl.flock(fd, _fcntl.LOCK_UN)
            elif _msvcrt is not None:
                os.lseek(fd, 0, os.SEEK_SET)
                _msvcrt.locking(fd, _msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
        finally:
            try:
                os.close(fd)
            except OSError:
                pass

    def __enter__(self) -> "ProfileLease":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()

    def __del__(self) -> None:
        try:
            self.release()
        except Exception:
            pass


def _read_lock_holder_diag(lock_path: Path) -> str:
    try:
        raw = lock_path.read_bytes()
        payload = raw.lstrip(b"\x00").decode("utf-8", errors="ignore").strip()
        if payload:
            info = json.loads(payload)
            if isinstance(info, dict):
                return f"pid={info.get('pid')}, holder={info.get('holder')}"
    except Exception:
        pass
    return "unknown holder"


def acquire_profile_lease(
    account_dir: Union[str, Path],
    holder: str = "worker",
    *,
    wait_seconds: float = 0.0,
    poll_interval: float = 0.15,
) -> ProfileLease:
    """Acquire exclusive in-process and OS kernel lock (`flock`/`msvcrt`) on `account_dir`.

    Byte 0 holds the kernel lock and diagnostic metadata lives at offset 1+. The lock
    file is intentionally never unlinked on release so concurrent processes share one inode.
    When `wait_seconds > 0`, retries briefly before raising `ProfileLeaseBusyError`.
    """
    target_dir = _canonical_lease_path(Path(account_dir))
    target_dir.mkdir(parents=True, exist_ok=True)
    lock_path = target_dir / PROFILE_KERNEL_LOCK_FILENAME
    deadline = time.monotonic() + max(0.0, float(wait_seconds or 0.0))
    step = max(0.02, min(0.5, float(poll_interval or 0.15)))

    while True:
        busy_err: Optional[ProfileLeaseBusyError] = None
        with _PROFILE_LOGIN_LEASE_LOCK:
            active_entry = _ACTIVE_PROFILE_KERNEL_LEASES.get(target_dir)
            if active_entry is not None and active_entry[0].active:
                busy_err = ProfileLeaseBusyError(
                    f"浏览器资料目录 {target_dir.name} 已被本进程持有 ({active_entry[1]})"
                )
            else:
                fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
                try:
                    if _fcntl is not None:
                        _fcntl.flock(fd, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
                    elif _msvcrt is not None:
                        os.lseek(fd, 0, os.SEEK_END)
                        if os.lseek(fd, 0, os.SEEK_CUR) == 0:
                            os.write(fd, b"\x00")
                        os.lseek(fd, 0, os.SEEK_SET)
                        _msvcrt.locking(fd, _msvcrt.LK_NBLCK, 1)
                except OSError as exc:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
                    diag = _read_lock_holder_diag(lock_path)
                    busy_err = ProfileLeaseBusyError(
                        f"浏览器资料目录 {target_dir.name} 已被其他进程锁定 ({diag})"
                    )
                    busy_err.__cause__ = exc
                else:
                    meta = json.dumps(
                        {
                            "pid": os.getpid(),
                            "holder": str(holder or "worker"),
                            "acquired_at": datetime.now(timezone.utc).isoformat(),
                        },
                        ensure_ascii=False,
                    ).encode("utf-8")
                    try:
                        os.lseek(fd, 0, os.SEEK_SET)
                        os.ftruncate(fd, 0)
                        os.write(fd, b"\x00" + meta)
                    except OSError:
                        pass

                    lease = ProfileLease(
                        target_dir, lock_path, fd, str(holder or "worker")
                    )
                    _ACTIVE_PROFILE_KERNEL_LEASES[target_dir] = (
                        lease,
                        str(holder or "worker"),
                    )
                    return lease

        if time.monotonic() >= deadline:
            raise busy_err
        time.sleep(min(step, max(0.01, deadline - time.monotonic())))

def get_active_profile_lease_holder(account_dir: Path) -> Optional[str]:
    key = _canonical_lease_path(account_dir)
    with _PROFILE_LOGIN_LEASE_LOCK:
        entry = _ACTIVE_PROFILE_KERNEL_LEASES.get(key)
        if entry is not None and entry[0].active:
            return entry[1]
        return None

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


def normalize_browser_profile_dir(profile_dir: Optional[str]) -> str:
    """Normalize a per-account browser profile directory key relative to `browser_data_rt/`."""
    raw = str(profile_dir or "").strip().replace("\\", "/")
    raw = raw.strip("/")
    if not raw:
        return ""
    if raw.startswith("browser_data_rt/"):
        raw = raw[len("browser_data_rt/") :].strip("/")
    if not raw:
        return ""
    parts = [p for p in raw.split("/") if p]
    if not parts or any(p in (".", "..") for p in parts):
        raise ValueError("浏览器资料目录路径不合法")
    cleaned = "/".join(parts)
    if not _PROFILE_NAME_RE.fullmatch(cleaned):
        raise ValueError("浏览器资料目录仅支持字母、数字及 . _ @ - / 字符")
    first_seg = parts[0].lower()
    if first_seg in _RESERVED_PROFILE_NAMES or re.match(r"^browser_\d+$", first_seg):
        raise ValueError(f"浏览器资料目录不能使用保留名称: {parts[0]}")
    if re.fullmatch(r"token_\d+", cleaned) or cleaned.startswith("profiles/"):
        return cleaned
    return f"profiles/{cleaned}"


def resolve_account_profile_dir(
    user_data_root: Path,
    *,
    token_id: Optional[int] = None,
    profile_dir_name: Optional[str] = None,
) -> Tuple[Path, str]:
    """Resolve `(absolute_account_dir, canonical_profile_key)` under `user_data_root`."""
    normalized = normalize_browser_profile_dir(profile_dir_name)
    if normalized:
        return user_data_root / normalized, normalized
    if token_id is not None and int(token_id) > 0:
        canonical = f"token_{int(token_id)}"
        return user_data_root / canonical, canonical
    return user_data_root / "pending", ""


def _profile_dir_summary(profile_root: Path, canonical: str) -> Dict[str, Any]:
    ff_dir = profile_root / "firefox-profile"
    cookies_db = ff_dir / "cookies.sqlite"
    fp_file = profile_root / "camoufox-fingerprint.json"
    ss_file = profile_root / "storage-state.json"
    mtime = 0.0
    for candidate in (cookies_db, ss_file, fp_file, ff_dir, profile_root):
        try:
            if candidate.exists():
                mtime = max(mtime, candidate.stat().st_mtime)
        except Exception:
            pass
    return {
        "profile_dir": canonical,
        "has_firefox_profile": ff_dir.is_dir(),
        "has_cookies_db": cookies_db.is_file(),
        "has_fingerprint": fp_file.is_file(),
        "has_storage_state": ss_file.is_file(),
        "active_login": bool(get_profile_login_holder(profile_root)),
        "updated_at": datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
        if mtime > 0
        else None,
    }


def list_browser_profile_dirs(user_data_root: Optional[Path] = None) -> List[Dict[str, Any]]:
    """List persistent account browser directories under `browser_data_rt/`."""
    root = user_data_root or (
        Path(__file__).resolve().parent.parent.parent / "browser_data_rt"
    )
    if not root.is_dir():
        return []
    entries: List[Dict[str, Any]] = []
    try:
        for child in sorted(root.iterdir(), key=lambda p: p.name):
            if not child.is_dir():
                continue
            if re.fullmatch(r"token_\d+", child.name):
                entries.append(_profile_dir_summary(child, child.name))
        profiles_dir = root / "profiles"
        if profiles_dir.is_dir():
            for child in sorted(profiles_dir.iterdir(), key=lambda p: p.name):
                if child.is_dir():
                    entries.append(_profile_dir_summary(child, f"profiles/{child.name}"))
    except Exception:
        pass
    return entries


def _canonical_lease_path(account_dir: Path) -> Path:
    try:
        return account_dir.resolve()
    except Exception:
        return account_dir


def get_profile_login_holder(account_dir: Path) -> Optional[str]:
    key = _canonical_lease_path(account_dir)
    with _PROFILE_LOGIN_LEASE_LOCK:
        return _ACTIVE_PROFILE_LOGIN_LEASES.get(key)


def is_token_in_login_session(token_id: Optional[int]) -> bool:
    if token_id is None:
        return False
    with _PROFILE_LOGIN_LEASE_LOCK:
        return int(token_id) in _ACTIVE_TOKEN_LOGIN_LEASES


def acquire_profile_login_lease(
    account_dir: Path, session_id: str, *, token_id: Optional[int] = None
) -> None:
    key = _canonical_lease_path(account_dir)
    with _PROFILE_LOGIN_LEASE_LOCK:
        existing = _ACTIVE_PROFILE_LOGIN_LEASES.get(key)
        if existing and existing != session_id:
            raise RuntimeError(f"浏览器资料目录 {account_dir.name} 已被登录会话 {existing} 占用")
        _ACTIVE_PROFILE_LOGIN_LEASES[key] = session_id
        if token_id is not None and int(token_id) > 0:
            _ACTIVE_TOKEN_LOGIN_LEASES[int(token_id)] = session_id


def release_profile_login_lease(
    account_dir: Optional[Path], session_id: str, *, token_id: Optional[int] = None
) -> None:
    with _PROFILE_LOGIN_LEASE_LOCK:
        if account_dir is not None:
            key = _canonical_lease_path(account_dir)
            if _ACTIVE_PROFILE_LOGIN_LEASES.get(key) == session_id:
                _ACTIVE_PROFILE_LOGIN_LEASES.pop(key, None)
        if token_id is not None and int(token_id) > 0:
            if _ACTIVE_TOKEN_LOGIN_LEASES.get(int(token_id)) == session_id:
                _ACTIVE_TOKEN_LOGIN_LEASES.pop(int(token_id), None)


def clear_site_cookies_in_profile(account_dir: Path) -> None:
    """Clear site cookies while keeping hardware fingerprint, HTTP cache, and Firefox profile settings."""
    ff_dir = account_dir / "firefox-profile"
    for fname in ("cookies.sqlite", "cookies.sqlite-wal", "cookies.sqlite-shm"):
        try:
            p = ff_dir / fname
            if p.exists():
                p.unlink(missing_ok=True)
        except Exception:
            pass
    try:
        ss_path = account_dir / "storage-state.json"
        if ss_path.is_file():
            ss_path.unlink(missing_ok=True)
    except Exception:
        pass


def persist_account_login_artifacts(source_dir: Path, target_dir: Path) -> None:
    """Copy fingerprint, storage-state, startup cache, HTTP disk cache, and full Firefox profile from isolated login dir to account dir."""
    if not source_dir.exists() or source_dir.resolve() == target_dir.resolve():
        return
    with acquire_profile_lease(
        target_dir,
        holder=f"persist_login_artifacts:{source_dir.name}",
        wait_seconds=5.0,
    ):
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
                    ignore=shutil.ignore_patterns(
                        ".parentlock",
                        "lock",
                        "parent.lock",
                        "gmp-widevinecdm",
                        "minidumps",
                        "crashes",
                        "datareporting",
                        "saved-telemetry-pings",
                    ),
                )
                if dir_name == "firefox-profile":
                    _clear_stale_firefox_profile_locks(dst_dir)

def prune_camoufox_profile_bloat(
    base_dir: Union[str, Path],
    *,
    max_cache_bytes: int = 32 * 1024 * 1024,
) -> Dict[str, int]:
    """Remove unneeded Widevine DRM binaries, crash dumps, and oversized cache2 entries from idle profiles."""
    root = Path(base_dir)
    stats = {"profiles_scanned": 0, "profiles_skipped_busy": 0, "bytes_reclaimed": 0}
    candidate_account_dirs: List[Path] = []
    if root.is_dir():
        for child in root.iterdir():
            if not child.is_dir():
                continue
            if child.name == "profiles":
                candidate_account_dirs.extend(p for p in child.iterdir() if p.is_dir())
            else:
                candidate_account_dirs.append(child)

    def _dir_size(path: Path) -> int:
        total = 0
        try:
            for dirpath, _, filenames in os.walk(path):
                for fname in filenames:
                    try:
                        total += os.path.getsize(os.path.join(dirpath, fname))
                    except OSError:
                        pass
        except OSError:
            pass
        return total

    def _has_lock_files(path: Path) -> bool:
        return any(
            (path / lk).exists() or (path / lk).is_symlink()
            for lk in (".parentlock", "lock", "parent.lock")
        )

    for acc_dir in candidate_account_dirs:
        ff_dir = acc_dir / "firefox-profile"
        if not ff_dir.is_dir():
            continue
        stats["profiles_scanned"] += 1
        if (
            get_profile_login_holder(acc_dir)
            or get_active_profile_lease_holder(acc_dir)
            or _has_lock_files(ff_dir)
        ):
            stats["profiles_skipped_busy"] += 1
            continue

        try:
            lease = acquire_profile_lease(
                acc_dir, holder="profile_bloat_pruner", wait_seconds=0.0
            )
        except ProfileLeaseBusyError:
            stats["profiles_skipped_busy"] += 1
            continue

        with lease:
            for removable_rel in (
                "gmp-widevinecdm",
                "minidumps",
                "crashes",
                "datareporting",
                "saved-telemetry-pings",
                "sessionstore-backups",
                "cache2/doomed",
                "cache2/trash",
            ):
                target = ff_dir / removable_rel
                if target.exists():
                    reclaimed = _dir_size(target)
                    shutil.rmtree(target, ignore_errors=True)
                    stats["bytes_reclaimed"] += reclaimed

            for fav_entry in ff_dir.glob("favicons.sqlite*"):
                try:
                    if fav_entry.is_dir():
                        reclaimed = _dir_size(fav_entry)
                        shutil.rmtree(fav_entry, ignore_errors=True)
                        stats["bytes_reclaimed"] += reclaimed
                    elif fav_entry.is_file() or fav_entry.is_symlink():
                        fsize = fav_entry.stat().st_size if fav_entry.is_file() else 0
                        fav_entry.unlink(missing_ok=True)
                        stats["bytes_reclaimed"] += fsize
                except OSError:
                    pass

            for db_path in ff_dir.glob("*.sqlite"):
                if not db_path.is_file():
                    continue
                wal_path = Path(f"{db_path}-wal")
                if not wal_path.is_file():
                    continue
                try:
                    wal_before = wal_path.stat().st_size
                    if wal_before <= 0:
                        continue
                    db_before = db_path.stat().st_size
                except OSError:
                    continue
                conn = None
                try:
                    conn = sqlite3.connect(str(db_path), timeout=2.0)
                    conn.execute("PRAGMA busy_timeout = 1000")
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                except (sqlite3.Error, OSError):
                    continue
                finally:
                    if conn is not None:
                        try:
                            conn.close()
                        except Exception:
                            pass
                try:
                    db_after = db_path.stat().st_size if db_path.is_file() else 0
                    wal_after = wal_path.stat().st_size if wal_path.is_file() else 0
                    stats["bytes_reclaimed"] += max(
                        0, (db_before + wal_before) - (db_after + wal_after)
                    )
                except OSError:
                    pass

            entries_dir = ff_dir / "cache2" / "entries"
            if entries_dir.is_dir():
                file_records: List[Tuple[float, int, Path]] = []
                total_entries_bytes = 0
                try:
                    for entry in entries_dir.iterdir():
                        if not entry.is_file():
                            continue
                        try:
                            st = entry.stat()
                            total_entries_bytes += st.st_size
                            file_records.append((st.st_mtime, st.st_size, entry))
                        except OSError:
                            pass
                except OSError:
                    file_records = []
                if total_entries_bytes > max_cache_bytes:
                    file_records.sort(key=lambda r: r[0])
                    remaining_bytes = total_entries_bytes
                    for _, fsize, fpath in file_records:
                        if remaining_bytes <= max_cache_bytes:
                            break
                        try:
                            fpath.unlink(missing_ok=True)
                            remaining_bytes -= fsize
                            stats["bytes_reclaimed"] += fsize
                        except OSError:
                            pass

    now = time.time()
    legacy_tmp_root = Path(__file__).resolve().parent.parent / "tmp"
    if legacy_tmp_root.is_dir():
        try:
            for entry in legacy_tmp_root.glob("*browser_profile_*"):
                if not entry.is_dir():
                    continue
                if (
                    get_profile_login_holder(entry)
                    or get_active_profile_lease_holder(entry)
                    or _has_lock_files(entry)
                    or _has_lock_files(entry / "firefox-profile")
                ):
                    continue
                try:
                    is_empty = not any(entry.iterdir())
                    is_stale = (now - entry.stat().st_mtime) >= 3600.0
                    if is_empty or is_stale:
                        reclaimed = _dir_size(entry)
                        shutil.rmtree(entry, ignore_errors=True)
                        stats["bytes_reclaimed"] += reclaimed
                except OSError:
                    pass
        except OSError:
            pass

    sys_tmp_root = Path(tempfile.gettempdir())
    if sys_tmp_root.is_dir():
        try:
            for entry in sys_tmp_root.glob("flow2api-camoufox-*"):
                if not entry.is_dir():
                    continue
                if (
                    get_profile_login_holder(entry)
                    or get_active_profile_lease_holder(entry)
                    or _has_lock_files(entry)
                ):
                    continue
                try:
                    if (now - entry.stat().st_mtime) >= 7200.0:
                        reclaimed = _dir_size(entry)
                        shutil.rmtree(entry, ignore_errors=True)
                        stats["bytes_reclaimed"] += reclaimed
                except OSError:
                    pass
        except OSError:
            pass

    return stats
