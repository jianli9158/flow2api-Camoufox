"""FFmpeg/FFprobe helpers for last-frame extraction and seamless video chaining."""

from __future__ import annotations

import asyncio
import math
from pathlib import Path
import shutil
import tempfile
from typing import List, Sequence, Tuple


def check_ffmpeg_available() -> Tuple[str, str]:
    """Verify ffmpeg and ffprobe are on PATH; return `(ffmpeg_bin, ffprobe_bin)`."""
    ffmpeg_bin = shutil.which("ffmpeg")
    ffprobe_bin = shutil.which("ffprobe")
    if not ffmpeg_bin or not ffprobe_bin:
        missing = [
            name
            for name, path in (("ffmpeg", ffmpeg_bin), ("ffprobe", ffprobe_bin))
            if not path
        ]
        raise RuntimeError(
            f"Video chaining requires {', '.join(missing)} installed and available on PATH"
        )
    return ffmpeg_bin, ffprobe_bin


async def _run_proc(cmd: Sequence[str], *, timeout: float = 120.0) -> Tuple[int, bytes, bytes]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return int(proc.returncode or 0), stdout or b"", stderr or b""
    except BaseException:
        if proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            await proc.wait()
        raise


async def probe_video_duration(ffprobe_bin: str, video_path: Path) -> float:
    """Return video duration in seconds using ffprobe; raise RuntimeError if unreadable/non-positive."""
    rc, stdout, stderr = await _run_proc(
        [
            ffprobe_bin,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(video_path),
        ],
        timeout=30.0,
    )
    if rc != 0:
        err = stderr.decode("utf-8", errors="ignore")[-400:].strip()
        raise RuntimeError(
            f"ffprobe duration probe failed (rc={rc}): {err or 'unknown error'}"
        )
    raw_duration = (stdout.decode("utf-8", errors="ignore") or "").strip()
    try:
        val = float(raw_duration)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"ffprobe returned unreadable duration for {video_path}: {raw_duration!r}"
        ) from exc
    if not math.isfinite(val) or val <= 0.0:
        raise RuntimeError(
            f"ffprobe returned non-positive duration ({val}) for {video_path}"
        )
    return val


async def extract_tail_frame(
    ffmpeg_bin: str,
    ffprobe_bin: str,
    video_path: Path,
    output_image_path: Path,
    *,
    offset_ms: int = 200,
) -> bytes:
    """Extract a high-quality frame slightly before the end (`duration - offset_ms`) to avoid black fade."""
    duration = await probe_video_duration(ffprobe_bin, video_path)
    if not math.isfinite(duration) or duration <= 0.0:
        raise RuntimeError(
            f"Cannot extract tail frame from non-positive duration ({duration}) for {video_path}"
        )
    safe_offset = max(0, int(offset_ms)) / 1000.0
    seek_sec = max(0.0, duration - safe_offset)

    rc, _, stderr = await _run_proc(
        [
            ffmpeg_bin,
            "-y",
            "-ss",
            f"{seek_sec:.3f}",
            "-i",
            str(video_path),
            "-frames:v",
            "1",
            "-q:v",
            "2",
            str(output_image_path),
        ],
        timeout=45.0,
    )
    if rc != 0 or not output_image_path.is_file() or output_image_path.stat().st_size <= 0:
        err = stderr.decode("utf-8", errors="ignore")[-400:]
        raise RuntimeError(f"ffmpeg tail-frame extraction failed (rc={rc}): {err}")
    return output_image_path.read_bytes()


async def concat_videos(
    ffmpeg_bin: str,
    clip_paths: Sequence[Path],
    output_path: Path,
) -> Path:
    """Concatenate MP4 clips seamlessly via concat demuxer (with re-encode fallback)."""
    paths = [Path(p) for p in clip_paths]
    if not paths:
        raise ValueError("No video clips provided for concatenation")
    if len(paths) == 1:
        shutil.copy2(paths[0], output_path)
        return output_path

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".txt",
        dir=str(output_path.parent),
        delete=False,
    ) as list_file:
        list_path = Path(list_file.name)
        for p in paths:
            escaped = str(p.resolve()).replace("'", "'\\''")
            list_file.write(f"file '{escaped}'\n")

    try:
        rc, _, stderr = await _run_proc(
            [
                ffmpeg_bin,
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(list_path),
                "-c",
                "copy",
                str(output_path),
            ],
            timeout=120.0,
        )
        if rc != 0 or not output_path.is_file() or output_path.stat().st_size <= 0:
            # Fallback to re-encode if clip streams differ slightly
            rc2, _, stderr2 = await _run_proc(
                [
                    ffmpeg_bin,
                    "-y",
                    "-f",
                    "concat",
                    "-safe",
                    "0",
                    "-i",
                    str(list_path),
                    "-c:v",
                    "libx264",
                    "-pix_fmt",
                    "yuv420p",
                    "-c:a",
                    "aac",
                    str(output_path),
                ],
                timeout=240.0,
            )
            if rc2 != 0 or not output_path.is_file() or output_path.stat().st_size <= 0:
                err = (stderr2 or stderr).decode("utf-8", errors="ignore")[-400:]
                raise RuntimeError(f"ffmpeg concat failed (rc={rc2}): {err}")
        return output_path
    finally:
        list_path.unlink(missing_ok=True)
