#!/usr/bin/env python3
"""Reliable, resumable recording-session ingestion into Cloudflare R2.

This intentionally uses rclone for transport and keeps orchestration state on the
local computer.  It does not attempt to preserve filesystem permissions or other
filesystem-specific metadata.
"""

from __future__ import annotations

import argparse
import ctypes
import dataclasses
import datetime as dt
import hashlib
import json
import math
import mimetypes
import os
import platform
import re
import secrets
import shutil
import signal
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from typing import Any

try:
    import certifi
except ImportError:  # The one-command installer supplies it in the private venv.
    certifi = None


APP_NAME = "fpv-upload"
SCHEMA_VERSION = 6
CONTENT_SAMPLE_BYTES = 128 * 1024
RECORDING_ID_SAMPLE_BYTES = 1024 * 1024
RECORDING_ID_ROLES = ("imu", "tel", "l_vts", "r_vts")
REMOTE_OPERATION_TIMEOUT_SECONDS = 30 * 60


def default_state_dir() -> Path:
    system = platform.system()
    if system == "Windows":
        return Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")) / "FPV Upload"
    if system == "Darwin":
        return Path.home() / "Library" / "Application Support" / "FPV Upload"
    return Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state")) / "fpv-upload"


DEFAULT_STATE_DIR = default_state_dir()
ENVIRONMENT_CATALOG_PATH = Path(__file__).with_name("environment-catalog.json")
DEFAULT_CONFIG_PATH = Path(__file__).with_name("config.json")
UPLOAD_ENV_PATH = Path(__file__).with_name(".env")
MAX_CATALOG_BYTES = 64 * 1024
MIN_CREDENTIAL_VALIDITY_SECONDS = 30 * 60 * 60
VIDEO_EXTENSIONS = {
    ".3g2", ".3gp", ".avi", ".m2ts", ".m4v", ".mkv", ".mov", ".mp4", ".mts", ".webm"
}
IGNORED_CARD_METADATA = {
    ".DS_Store", ".Spotlight-V100", ".TemporaryItems", ".Trashes", ".fseventsd",
    "$RECYCLE.BIN", "System Volume Information",
}
TRINET_FILE_RE = re.compile(
    r"^(take\d+)(?:_([lr]))?\.(mp4|vts|imu|tel)$", re.IGNORECASE
)
TRINET_VIDEO_STEM_RE = re.compile(r"^(take\d+)(?:[_-].*)?$", re.IGNORECASE)
MIN_TAKE_VIDEO_BYTES = 1024 * 1024
def open_https(request: urllib.request.Request | str, timeout: int):
    """Open HTTPS using an explicit maintained CA bundle when installed."""
    context = ssl.create_default_context(cafile=certifi.where() if certifi else None)
    return urllib.request.urlopen(request, timeout=timeout, context=context)


PROFILES: dict[str, dict[str, Any]] = {
    "safe": {
        "transfers": 2,
        "checkers": 6,
        "upload_concurrency": 2,
        "chunk_size_mib": 64,
        "upload_cutoff_mib": 256,
    },
    "balanced": {
        "transfers": 3,
        "checkers": 8,
        "upload_concurrency": 4,
        "chunk_size_mib": 64,
        "upload_cutoff_mib": 256,
    },
    "fast": {
        "transfers": 4,
        "checkers": 12,
        "upload_concurrency": 6,
        "chunk_size_mib": 64,
        "upload_cutoff_mib": 256,
    },
    "many-small-files": {
        "transfers": 8,
        "checkers": 16,
        "upload_concurrency": 2,
        "chunk_size_mib": 32,
        "upload_cutoff_mib": 256,
    },
    "huge-video": {
        "transfers": 2,
        "checkers": 6,
        "upload_concurrency": 6,
        "chunk_size_mib": 128,
        "upload_cutoff_mib": 256,
    },
}


@dataclasses.dataclass(frozen=True)
class FileEntry:
    path: str
    size: int
    mtime_ns: int
    sample_sha256: str = ""


@dataclasses.dataclass(frozen=True)
class TrinetTake:
    take_label: str
    device_id: str
    recording_id: str
    recording_uid: str
    content_sha256: str
    boot_ns: int
    files: tuple[str, ...]
    legacy_recording_uid: str = ""


@dataclasses.dataclass(frozen=True)
class TrinetSummary:
    device_ids: tuple[str, ...]
    recording_set_id: str
    takes: tuple[TrinetTake, ...]


@dataclasses.dataclass(frozen=True)
class Inventory:
    entries: tuple[FileEntry, ...]
    total_bytes: int
    fingerprint: str
    trinet: TrinetSummary | None = None

    @property
    def file_count(self) -> int:
        return len(self.entries)

    @property
    def largest_file(self) -> int:
        return max((item.size for item in self.entries), default=0)

    def as_dict(self) -> dict[str, Any]:
        result = {
            "fingerprint": self.fingerprint,
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
            "files": [dataclasses.asdict(item) for item in self.entries],
        }
        if self.trinet is not None:
            result["trinet"] = dataclasses.asdict(self.trinet)
        return result


@dataclasses.dataclass(frozen=True)
class VendorSession:
    source: Path
    source_name: str
    content_id: str
    entries: tuple[FileEntry, ...]
    identity_files: tuple[dict[str, Any], ...]

    @property
    def total_bytes(self) -> int:
        return sum(item.size for item in self.entries)


@dataclasses.dataclass(frozen=True)
class DurationSummary:
    accounted_seconds: float
    raw_video_seconds: float | None
    method: str
    video_count: int
    group_count: int | None
    groups: tuple[dict[str, Any], ...]

    def as_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


class IngestError(RuntimeError):
    pass


def now_local() -> dt.datetime:
    return dt.datetime.now().astimezone()


def iso_now() -> str:
    return now_local().isoformat(timespec="seconds")


def human_bytes(value: float) -> str:
    amount = float(value)
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    for unit in units:
        if abs(amount) < 1024.0 or unit == units[-1]:
            return f"{amount:.1f} {unit}" if unit != "B" else f"{int(amount)} B"
        amount /= 1024.0
    return f"{amount:.1f} PiB"


def human_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def slugify(value: str, fallback: str = "session") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return (cleaned[:60] or fallback).lower()


def remote_join(base: str, *parts: str) -> str:
    value = base.rstrip("/")
    for part in parts:
        clean = part.strip("/")
        if clean:
            value += "/" + clean
    return value


def sampled_file_sha256(path: Path, size: int) -> str:
    """Hash bounded samples so resume identity is content-aware without a full reread."""
    digest = hashlib.sha256()
    if size <= CONTENT_SAMPLE_BYTES * 3:
        offsets = (0,)
    else:
        offsets = (0, max(0, (size // 2) - (CONTENT_SAMPLE_BYTES // 2)), size - CONTENT_SAMPLE_BYTES)
    try:
        with path.open("rb", buffering=0) as handle:
            for offset in offsets:
                handle.seek(offset)
                chunk = handle.read(CONTENT_SAMPLE_BYTES if len(offsets) > 1 else size)
                digest.update(str(offset).encode("ascii"))
                digest.update(b"\0")
                digest.update(chunk)
    except OSError as exc:
        raise IngestError(f"Could not read source file {path}: {exc}") from exc
    return digest.hexdigest()


def full_files_sha256(paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    try:
        for path in paths:
            with path.open("rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    digest.update(chunk)
    except OSError as exc:
        raise IngestError(f"Could not hash Trinet identity sidecar {path}: {exc}") from exc
    return digest.hexdigest()


def recording_identity_file_sha256(path: Path, size: int) -> str:
    """Hash bounded first/middle/last samples using the stereo bucket ID rule."""
    digest = hashlib.sha256()
    if size <= RECORDING_ID_SAMPLE_BYTES * 3:
        ranges = ((0, size),)
    else:
        ranges = (
            (0, RECORDING_ID_SAMPLE_BYTES),
            (max(0, size // 2 - RECORDING_ID_SAMPLE_BYTES // 2), RECORDING_ID_SAMPLE_BYTES),
            (size - RECORDING_ID_SAMPLE_BYTES, RECORDING_ID_SAMPLE_BYTES),
        )
    try:
        with path.open("rb") as handle:
            for offset, length in ranges:
                handle.seek(offset)
                chunk = handle.read(length)
                if len(chunk) != length:
                    raise IngestError(f"Could not read recording identity sample: {path} at {offset}.")
                digest.update(str(offset).encode("ascii") + b"\0" + chunk)
    except OSError as exc:
        raise IngestError(f"Could not hash recording identity file {path}: {exc}") from exc
    return digest.hexdigest()


def stereo_recording_id(source: Path, files: Mapping[str, FileEntry]) -> str:
    """Return the migration-compatible ID derived from IMU, TEL, and both VTS files."""
    identity = hashlib.sha256(b"fpv-sv-stereo-recording-id-v2-sampled-1m\n")
    for role in RECORDING_ID_ROLES:
        item = files[role]
        sample_hash = recording_identity_file_sha256(source / item.path, item.size)
        identity.update(
            role.encode("ascii") + b"\0" + str(item.size).encode("ascii") +
            b"\0" + sample_hash.encode("ascii") + b"\n"
        )
    return "rec-" + identity.hexdigest()[:32]


def inspect_trinet_take(source: Path, take_label: str, files: Mapping[str, FileEntry]) -> TrinetTake:
    """Validate one take's six files and derive its content identity."""
    expected = {"l_mp4", "r_mp4", "l_vts", "r_vts", "imu", "tel"}
    missing = sorted(expected - files.keys())
    if missing:
        raise IngestError(
            f"{take_label} is incomplete; missing {', '.join(missing)}. "
            "Nothing was uploaded. Copy the complete take to the SD card and retry."
        )
    imu_path = source / files["imu"].path
    tel_path = source / files["tel"].path
    try:
        with imu_path.open("rb") as handle:
            imu_header = handle.read(64)
        with tel_path.open("rb") as handle:
            tel_header = handle.read(32)
    except OSError as exc:
        raise IngestError(f"Could not read Trinet headers for {take_label}: {exc}") from exc
    if len(imu_header) != 64 or imu_header[:8] != b"TRIMU001":
        raise IngestError(f"{files['imu'].path} has an invalid Trinet IMU header.")
    if len(tel_header) != 32 or tel_header[:8] != b"TRTEL01\0":
        raise IngestError(f"{files['tel'].path} has an invalid Trinet telemetry header.")
    device_id = imu_header[0x28:0x38].hex()
    if not device_id or device_id == "0" * 32:
        raise IngestError(f"{files['imu'].path} contains an invalid device ID.")
    if tel_header[0x18:0x20] != imu_header[0x28:0x30]:
        raise IngestError(
            f"Device ID mismatch between {files['imu'].path} and {files['tel'].path}."
        )
    # Sidecars alone can repeat after a recorder reset while video differs.
    # Sidecars are hashed in full; videos use size plus the sampled hash to avoid
    # rereading multi-GB files. SD-card timestamps are excluded (no RTC).
    legacy_content_sha256 = full_files_sha256((imu_path, tel_path))
    identity = hashlib.sha256()
    for role in sorted(expected):
        item = files[role]
        identity.update(role.encode("ascii"))
        identity.update(b"\0")
        identity.update(str(item.size).encode("ascii"))
        identity.update(b"\0")
        if role.endswith("_mp4"):
            identity.update(sampled_file_sha256(source / item.path, item.size).encode("ascii"))
        else:
            identity.update(full_files_sha256((source / item.path,)).encode("ascii"))
        identity.update(b"\n")
    content_sha256 = identity.hexdigest()
    recording_id = stereo_recording_id(source, files)
    return TrinetTake(
        take_label=take_label,
        device_id=device_id,
        recording_id=recording_id,
        recording_uid=f"{device_id}_{recording_id.removeprefix('rec-')}",
        content_sha256=content_sha256,
        boot_ns=int.from_bytes(imu_header[0x14:0x1C], "little"),
        files=tuple(sorted((entry.path for entry in files.values()), key=str.casefold)),
        legacy_recording_uid=f"{device_id}_{legacy_content_sha256[:32]}",
    )


def inspect_trinet_recording(
    source: Path, entries: Iterable[FileEntry], skip_invalid: bool = False
) -> TrinetSummary | None:
    """Validate a flat Trinet recording directory and derive stable content identities.

    With skip_invalid, incomplete or corrupt takes are reported and left out
    instead of rejecting the whole card.
    """
    matched: dict[str, dict[str, FileEntry]] = {}
    saw_trinet_name = False
    for entry in entries:
        relative = Path(entry.path)
        match = TRINET_FILE_RE.fullmatch(relative.name)
        if not match:
            continue
        saw_trinet_name = True
        if relative.parent != Path("."):
            continue
        take_label, eye, extension = match.groups()
        role = f"{eye.lower()}_{extension.lower()}" if eye else extension.lower()
        take = matched.setdefault(take_label.lower(), {})
        if role in take:
            raise IngestError(
                f"Duplicate Trinet {role} file for {take_label} in {source}. Nothing was uploaded."
            )
        take[role] = entry
    if not saw_trinet_name:
        return None
    if not matched:
        raise IngestError(
            "Trinet take files must be directly inside the recording folder. Nothing was uploaded."
        )

    takes: list[TrinetTake] = []
    for take_label in sorted(matched, key=lambda value: int(re.search(r"\d+", value).group())):
        files = matched[take_label]
        try:
            tiny = [files[role].path for role in ("l_mp4", "r_mp4")
                    if role in files and files[role].size < MIN_TAKE_VIDEO_BYTES]
            if skip_invalid and tiny:
                raise IngestError(f"{', '.join(tiny)} smaller than 1 MB.")
            takes.append(inspect_trinet_take(source, take_label, files))
        except IngestError as exc:
            if not skip_invalid:
                raise
            print(f"SKIPPED {take_label}: {exc}", file=sys.stderr)
    if not takes:
        raise IngestError(f"No valid Trinet takes found in {source}. Nothing was uploaded.")
    device_ids = {take.device_id for take in takes}
    seen_uids: set[str] = set()
    for take in takes:
        if take.recording_uid in seen_uids:
            raise IngestError(
                f"Duplicate recording content found in {source}: {take.recording_uid}. "
                "Nothing was uploaded."
            )
        seen_uids.add(take.recording_uid)
    set_digest = hashlib.sha256()
    for take in takes:
        set_digest.update(take.recording_uid.encode("ascii"))
        set_digest.update(b"\n")
    return TrinetSummary(
        device_ids=tuple(sorted(device_ids)),
        recording_set_id=f"recording-{set_digest.hexdigest()[:32]}",
        takes=tuple(takes),
    )


def is_ignored_card_path(relative: Path) -> bool:
    return relative.name.startswith("._") or any(
        part in IGNORED_CARD_METADATA for part in relative.parts
    )


def scan_source(source: Path, skip_invalid_takes: bool = False) -> Inventory:
    source = source.resolve()
    if not source.is_dir():
        raise IngestError(f"Source is not a readable directory: {source}")

    entries: list[FileEntry] = []
    symlinks: list[str] = []
    try:
        for path in sorted(source.rglob("*"), key=lambda p: p.as_posix()):
            relative = path.relative_to(source).as_posix()
            if is_ignored_card_path(Path(relative)):
                continue
            if path.is_symlink():
                symlinks.append(relative)
                continue
            if path.is_file():
                try:
                    relative.encode("utf-8")
                except UnicodeEncodeError as exc:
                    raise IngestError(
                        f"Source filename is not valid UTF-8 and cannot be recorded safely: {relative!r}"
                    ) from exc
                before = path.stat()
                sample_sha256 = sampled_file_sha256(path, before.st_size)
                after = path.stat()
                if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                    raise IngestError(f"Source file changed while it was being inventoried: {path}")
                entries.append(
                    FileEntry(relative, after.st_size, after.st_mtime_ns, sample_sha256)
                )
    except OSError as exc:
        raise IngestError(f"Could not inventory source {source}: {exc}") from exc

    if symlinks:
        preview = ", ".join(symlinks[:5])
        raise IngestError(
            f"Source contains {len(symlinks)} symbolic link(s), which would not be "
            f"preserved as recording data: {preview}"
        )
    if not entries:
        raise IngestError(f"Source contains no files: {source}")

    digest = hashlib.sha256()
    for item in entries:
        digest.update(item.path.encode("utf-8", errors="surrogateescape"))
        digest.update(b"\0")
        digest.update(str(item.size).encode("ascii"))
        digest.update(b"\0")
        digest.update(str(item.mtime_ns).encode("ascii"))
        digest.update(b"\0")
        digest.update(item.sample_sha256.encode("ascii"))
        digest.update(b"\n")
    trinet = inspect_trinet_recording(source, entries, skip_invalid_takes)
    return Inventory(
        tuple(entries), sum(item.size for item in entries), digest.hexdigest(), trinet
    )


def scan_vendor_session(source: Path) -> VendorSession:
    """Hash nonvideo files, falling back to bounded video samples when needed."""
    source = source.expanduser().resolve()
    if not source.is_dir():
        raise IngestError(f"Vendor session is not a readable directory: {source}")
    entries: list[FileEntry] = []
    identity_files: list[dict[str, Any]] = []
    digest = hashlib.sha256(b"fpv-stereo-nepal-content-id-v1-nonvideo\n")
    for path in sorted(source.rglob("*"), key=lambda item: item.as_posix().casefold()):
        relative_path = path.relative_to(source)
        if is_ignored_card_path(relative_path):
            continue
        if path.is_symlink():
            raise IngestError(f"Symbolic links are not accepted: {relative_path.as_posix()}")
        if not path.is_file():
            continue
        relative = relative_path.as_posix()
        try:
            relative.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise IngestError(f"Filename is not valid UTF-8: {relative!r}") from exc
        before = path.stat()
        entries.append(FileEntry(relative, before.st_size, before.st_mtime_ns))
        if path.suffix.lower() not in VIDEO_EXTENSIONS:
            file_sha256 = full_files_sha256((path,))
            after = path.stat()
            if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise IngestError(f"Source file changed while identity was calculated: {path}")
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            digest.update(str(after.st_size).encode("ascii"))
            digest.update(b"\0")
            digest.update(file_sha256.encode("ascii"))
            digest.update(b"\n")
            identity_files.append({"path": relative, "size": after.st_size, "sha256": file_sha256})
    if not entries:
        raise IngestError(f"Vendor session contains no uploadable files: {source}")
    if not identity_files:
        digest = hashlib.sha256(b"fpv-stereo-nepal-content-id-v2-video-samples\n")
        sampled_entries = []
        for item in entries:
            path = source / item.path
            sample = sampled_file_sha256(path, item.size)
            after = path.stat()
            if (after.st_size, after.st_mtime_ns) != (item.size, item.mtime_ns):
                raise IngestError(f"Source file changed while identity was calculated: {path}")
            digest.update(item.path.encode("utf-8") + b"\0" + str(item.size).encode("ascii")
                          + b"\0" + sample.encode("ascii") + b"\n")
            sampled_entries.append(dataclasses.replace(item, sample_sha256=sample))
        entries = sampled_entries
    return VendorSession(
        source=source,
        source_name=source.name,
        content_id="cid-" + digest.hexdigest()[:32],
        entries=tuple(entries),
        identity_files=tuple(identity_files),
    )


def discover_vendor_sessions(source: Path) -> list[VendorSession]:
    source = source.expanduser().resolve()
    if not source.is_dir():
        raise IngestError(f"Vendor upload source is not a directory: {source}")
    return [scan_vendor_session(source)]


def parse_environment_catalog(payload: Any) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    try:
        if payload["schema_version"] != 3:
            raise ValueError("schema_version must be 3")
        revision = int(payload["revision"])
        updated_at = str(payload["updated_at"])
        default_site_hours = float(payload["default_environment_site_hours"])
        environments = payload["environments"]
        site_entries = payload["sites"]
        if (
            revision < 1
            or default_site_hours <= 0
            or not environments
            or not isinstance(site_entries, list)
        ):
            raise ValueError("revision, hours, environments, and sites must be valid")
    except (KeyError, TypeError, ValueError) as exc:
        raise IngestError(f"Invalid environment catalog header: {exc}") from exc
    sites: dict[str, dict[str, str]] = {}
    for item in site_entries:
        try:
            site_id = str(item["id"])
            if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", site_id):
                raise ValueError("site ID must be a lowercase slug")
            if site_id in sites:
                raise ValueError(f"duplicate site ID {site_id}")
            sites[site_id] = {"id": site_id, "name": str(item["name"])}
        except (KeyError, TypeError, ValueError) as exc:
            raise IngestError(f"Invalid site catalog entry: {item!r}") from exc
    catalog: dict[str, dict[str, Any]] = {}
    for item in environments:
        try:
            environment_id = str(item["id"])
            if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", environment_id):
                raise ValueError("environment ID must be a lowercase slug")
            if environment_id in catalog:
                raise ValueError(f"duplicate environment ID {environment_id}")
            catalog[environment_id] = {
                "id": environment_id,
                "name": str(item["name"]),
                "allowed_hours": float(item["allowed_hours"]),
                "target_site_count": int(item["target_site_count"]),
            }
        except (KeyError, TypeError, ValueError) as exc:
            raise IngestError(f"Invalid environment catalog entry: {item!r}") from exc
    if not catalog:
        raise IngestError("Environment catalog contains no environments.")
    return catalog, {
        "revision": revision,
        "updated_at": updated_at,
        "default_environment_site_hours": default_site_hours,
        "sites": sites,
    }


def load_bundled_environment_catalog() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    try:
        payload = json.loads(ENVIRONMENT_CATALOG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IngestError(f"Could not load bundled environment catalog: {exc}") from exc
    return parse_environment_catalog(payload)


def fetch_environment_catalog(
    catalog_url: str | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    if not catalog_url:
        raise IngestError(
            "catalog_url is required. Configure the dashboard's /api/catalog endpoint; "
            "uploads fail closed without the shared catalog."
        )
    parsed_url = urllib.parse.urlparse(catalog_url)
    if parsed_url.scheme not in {"https", "http"}:
        raise IngestError("catalog_url must be an HTTP or HTTPS URL.")
    if parsed_url.scheme != "https" and parsed_url.hostname not in {
        "127.0.0.1", "localhost", "::1"
    }:
        raise IngestError("catalog_url must use HTTPS except during localhost testing.")
    headers = {"Accept": "application/json", "User-Agent": f"{APP_NAME}/{SCHEMA_VERSION}"}
    access_id = os.environ.get("R2_INGEST_ACCESS_CLIENT_ID")
    access_secret = os.environ.get("R2_INGEST_ACCESS_CLIENT_SECRET")
    if bool(access_id) != bool(access_secret):
        raise IngestError(
            "Set both R2_INGEST_ACCESS_CLIENT_ID and R2_INGEST_ACCESS_CLIENT_SECRET, or neither."
        )
    if access_id and access_secret:
        headers["CF-Access-Client-Id"] = access_id
        headers["CF-Access-Client-Secret"] = access_secret
    request = urllib.request.Request(catalog_url, headers=headers, method="GET")
    try:
        with open_https(request, timeout=30) as response:
            body = response.read(MAX_CATALOG_BYTES + 1)
            etag = response.headers.get("ETag", "")
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        # Python.org macOS builds may have no CA bundle configured even though
        # the system trust store is healthy. curl uses the platform trust store;
        # this fallback keeps TLS verification enabled rather than bypassing it.
        if "CERTIFICATE_VERIFY_FAILED" not in str(exc) or not shutil.which("curl"):
            raise IngestError(
                f"Could not fetch the shared environment catalog from {catalog_url}: {exc}. "
                "No upload was started."
            ) from exc
        with tempfile.TemporaryDirectory(prefix="r2-ingest-catalog-") as temp_dir:
            body_path = Path(temp_dir) / "catalog.json"
            headers_path = Path(temp_dir) / "headers.txt"
            command = [
                "curl", "--fail", "--silent", "--show-error", "--max-time", "30",
                "--dump-header", str(headers_path), "--output", str(body_path),
                "--header", "Accept: application/json",
                "--header", f"User-Agent: {APP_NAME}/{SCHEMA_VERSION}",
            ]
            if access_id and access_secret:
                command.extend([
                    "--header", f"CF-Access-Client-Id: {access_id}",
                    "--header", f"CF-Access-Client-Secret: {access_secret}",
                ])
            command.append(catalog_url)
            result = subprocess.run(
                command, capture_output=True, text=True, check=False, timeout=35
            )
            if result.returncode != 0:
                raise IngestError(
                    f"Could not fetch the shared environment catalog from {catalog_url}: "
                    f"{result.stderr.strip() or 'curl failed'}. No upload was started."
                ) from exc
            body = body_path.read_bytes()
            etag = ""
            for line in headers_path.read_text(encoding="utf-8", errors="replace").splitlines():
                if line.lower().startswith("etag:"):
                    etag = line.split(":", 1)[1].strip()
    if len(body) > MAX_CATALOG_BYTES:
        raise IngestError("Shared environment catalog exceeds 64 KiB.")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IngestError(f"Dashboard returned an invalid environment catalog: {exc}") from exc
    catalog, metadata = parse_environment_catalog(payload)
    metadata["url"] = catalog_url
    metadata["etag"] = etag
    return catalog, metadata


def fetch_assignment(assignment_id: str) -> dict[str, Any]:
    if not re.fullmatch(r"asg-[a-f0-9]{12}", assignment_id):
        raise IngestError("Assignment ID must look like asg- followed by 12 letters/numbers.")
    broker_url = os.environ.get("FPV_UPLOAD_BROKER_URL", "").strip()
    broker_token = os.environ.get("FPV_UPLOAD_TOKEN", "").strip()
    if not broker_url or not broker_token:
        raise IngestError("The upload token is not configured. Run setup again.")
    url = urllib.parse.urljoin(broker_url, f"/assignments/{urllib.parse.quote(assignment_id)}")
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {broker_token}",
            "User-Agent": f"{APP_NAME}/{SCHEMA_VERSION}",
        },
        method="GET",
    )
    try:
        with open_https(request, timeout=30) as response:
            body = response.read(MAX_CATALOG_BYTES + 1)
    except urllib.error.HTTPError as exc:
        detail = exc.read(4096).decode("utf-8", errors="replace")
        raise IngestError(f"Assignment ID was rejected ({exc.code}): {detail}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise IngestError(f"Could not validate assignment ID; no upload was started: {exc}") from exc
    if len(body) > MAX_CATALOG_BYTES:
        raise IngestError("Assignment response was unexpectedly large.")
    try:
        value = json.loads(body.decode("utf-8"))
        required = {
            "id", "status", "project_id", "project_name", "l2_target_id", "l2_target_name",
            "environment_l1", "environment_l2", "environment_l3", "environment_id",
            "environment_name", "operator_id", "operator_name", "task_id", "task_name",
        }
        if not isinstance(value, dict) or not required.issubset(value):
            raise ValueError("missing required fields")
        if value["id"] != assignment_id or value["status"] != "active":
            raise ValueError("ID is inactive or mismatched")
        for key in required - {"status"}:
            if not isinstance(value[key], str) or not value[key].strip():
                raise ValueError(f"invalid {key}")
        for key in ("project_id", "l2_target_id", "environment_id", "operator_id", "task_id"):
            if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", value[key]):
                raise ValueError(f"invalid {key}")
        if value.get("episode_camera_shutter") not in {None, "rolling", "global"}:
            raise ValueError("invalid camera shutter")
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise IngestError(f"Credential service returned an invalid assignment: {exc}") from exc
    return value


def confirm_assignment(
    assignment: dict[str, Any], *, yes: bool, sources: list[Path] | None = None
) -> bool:
    print("\nUpload assignment — confirm before upload")
    print(f"Assignment ID: {assignment['id']}")
    print(f"Project:       {assignment['project_name']}")
    print(f"Taxonomy:      {assignment['environment_l1']} > {assignment['environment_l2']} > {assignment['environment_l3']}")
    print(
        f"Environment:   {assignment['environment_name']} "
        f"({assignment['environment_id']})"
    )
    print(f"Operator:      {assignment['operator_name']} ({assignment['operator_id']})")
    print(f"Task:          {assignment['task_name']} ({assignment['task_id']})")
    if sources is not None:
        print(f"Folders:       {len(sources)}")
        for index, source in enumerate(sources, 1):
            print(f"  {index:>3}. {source}")
    print("\nNo upload has started yet.")

    if yes:
        print("Assignment confirmation: accepted via --yes.")
        return True
    if not sys.stdin.isatty():
        raise IngestError(
            "Assignment confirmation requires an interactive terminal. "
            "After checking the assignment, rerun with --yes for unattended use."
        )
    answer = input("Is this the correct environment, operator, and task? [y/N] ").strip().lower()
    return answer in {"y", "yes"}


def validate_classification(
    environment_id: str,
    site_value: str | int,
    catalog: dict[str, dict[str, Any]],
    sites: dict[str, dict[str, str]],
) -> tuple[dict[str, Any], dict[str, str]]:
    environment = catalog.get(environment_id)
    if environment is None:
        choices = ", ".join(catalog)
        raise IngestError(f"Unknown environment '{environment_id}'. Choose one of: {choices}")
    raw_site = str(site_value)
    site_id = f"site-{int(raw_site):02d}" if raw_site.isdigit() else raw_site
    site = sites.get(site_id)
    if site is None:
        choices = ", ".join(sites)
        raise IngestError(f"Unknown site '{raw_site}'. Choose one of: {choices}")
    return environment, site


def leaf_video_groups(
    video_entries: Iterable[FileEntry],
) -> list[tuple[str, list[FileEntry]]]:
    by_directory: dict[Path, list[FileEntry]] = {}
    for entry in video_entries:
        by_directory.setdefault(Path(entry.path).parent, []).append(entry)
    directories = set(by_directory)
    leaf_directories = [
        directory
        for directory in directories
        if not any(directory != other and directory in other.parents for other in directories)
    ]
    groups: list[tuple[str, list[FileEntry]]] = []
    for directory in sorted(leaf_directories, key=lambda path: path.as_posix().casefold()):
        take_groups: dict[str, list[FileEntry]] = {}
        unmatched: list[FileEntry] = []
        for entry in by_directory[directory]:
            match = TRINET_VIDEO_STEM_RE.fullmatch(Path(entry.path).stem)
            if match:
                take_groups.setdefault(match.group(1).lower(), []).append(entry)
            else:
                unmatched.append(entry)
        if take_groups:
            for take_label in sorted(take_groups, key=lambda value: int(re.search(r"\d+", value).group())):
                groups.append((f"{directory.as_posix()}:{take_label}", sorted(take_groups[take_label], key=lambda item: item.path.casefold())))
            if unmatched:
                groups.append((directory.as_posix(), sorted(unmatched, key=lambda item: item.path.casefold())))
        else:
            groups.append((directory.as_posix(), sorted(unmatched, key=lambda item: item.path.casefold())))
    return groups


def ffprobe_duration_seconds(path: Path) -> float:
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise IngestError(f"Could not run ffprobe for {path}: {exc}") from exc
    if result.returncode != 0:
        raise IngestError(
            f"ffprobe could not read video duration for {path}: "
            f"{result.stderr.strip() or 'unknown error'}"
        )
    try:
        seconds = float(result.stdout.strip())
    except ValueError as exc:
        raise IngestError(f"ffprobe returned an invalid duration for {path}") from exc
    if seconds < 0 or not seconds < float("inf"):
        raise IngestError(f"ffprobe returned an unusable duration for {path}: {seconds}")
    return seconds


def calculate_duration(
    source: Path,
    inventory: Inventory,
    policy: str,
    manual_hours: float | None,
) -> DurationSummary:
    video_entries = [
        entry for entry in inventory.entries if Path(entry.path).suffix.lower() in VIDEO_EXTENSIONS
    ]
    if manual_hours is not None:
        if manual_hours < 0:
            raise IngestError("--duration-hours cannot be negative.")
        return DurationSummary(
            accounted_seconds=manual_hours * 3600,
            raw_video_seconds=None,
            method="manual",
            video_count=len(video_entries),
            group_count=None,
            groups=(),
        )
    if not shutil.which("ffprobe"):
        raise IngestError(
            "ffprobe is required for automatic recording-hour accounting. "
            "Install ffmpeg or provide --duration-hours."
        )
    if not video_entries:
        raise IngestError(
            "No recognized video files were found. Provide --duration-hours 0 if this is intentional."
        )

    if policy == "grouped-views":
        groups = []
        for directory, candidates in leaf_video_groups(video_entries):
            selected = candidates[0]
            seconds = ffprobe_duration_seconds(source / selected.path)
            groups.append(
                {
                    "key": directory,
                    "selected_path": selected.path,
                    "accounted_seconds": seconds,
                    "candidate_paths": [entry.path for entry in candidates],
                }
            )
        accounted = sum(float(group["accounted_seconds"]) for group in groups)
        raw_seconds = None
    elif policy == "sum-all":
        probed = [
            (entry, ffprobe_duration_seconds(source / entry.path)) for entry in video_entries
        ]
        raw_seconds = sum(seconds for _entry, seconds in probed)
        accounted = raw_seconds
        groups = []
    elif policy == "longest-file":
        probed = [
            (entry, ffprobe_duration_seconds(source / entry.path)) for entry in video_entries
        ]
        raw_seconds = sum(seconds for _entry, seconds in probed)
        accounted = max(seconds for _entry, seconds in probed)
        groups = []
    else:
        raise IngestError(f"Unknown duration policy: {policy}")
    return DurationSummary(
        accounted_seconds=accounted,
        raw_video_seconds=raw_seconds,
        method=policy,
        video_count=len(video_entries),
        group_count=len(groups) if policy == "grouped-views" else None,
        groups=tuple(groups),
    )


def physical_memory_mib() -> int:
    if platform.system() == "Darwin" and shutil.which("sysctl"):
        result = subprocess.run(
            ["sysctl", "-n", "hw.memsize"], capture_output=True, text=True, check=False
        )
        if result.returncode == 0 and result.stdout.strip().isdigit():
            return int(result.stdout.strip()) // (1024 * 1024)
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        pages = os.sysconf("SC_PHYS_PAGES")
        return int(page_size * pages) // (1024 * 1024)
    except (AttributeError, OSError, ValueError):
        return 4096


def profile_memory_mib(profile: dict[str, Any]) -> int:
    return (
        int(profile["transfers"])
        * int(profile["upload_concurrency"])
        * int(profile["chunk_size_mib"])
    )


def choose_profile(
    inventory: Inventory,
    requested: str,
    max_memory_mib: int,
) -> tuple[str, dict[str, Any], list[str]]:
    reasons: list[str] = []
    if requested != "auto":
        selected = requested
        reasons.append(f"Operator selected profile '{requested}'.")
    else:
        average = inventory.total_bytes / max(1, inventory.file_count)
        if inventory.file_count >= 1000 and average < 100 * 1024 * 1024:
            selected = "many-small-files"
            reasons.append("Many predominantly small files were detected.")
        elif inventory.largest_file >= 50 * 1024**3 and inventory.file_count <= 20:
            selected = "huge-video"
            reasons.append("A small set containing a 50 GiB+ object was detected.")
        else:
            selected = "balanced"
            reasons.append("Mixed recording data matched the balanced profile.")

    profile = dict(PROFILES[selected])
    initial_memory = profile_memory_mib(profile)
    while profile_memory_mib(profile) > max_memory_mib:
        if profile["upload_concurrency"] > 2:
            profile["upload_concurrency"] -= 1
        elif profile["transfers"] > 1:
            profile["transfers"] -= 1
        else:
            break
    if profile_memory_mib(profile) < initial_memory:
        reasons.append(
            f"Concurrency was reduced to stay within the {max_memory_mib} MiB memory budget."
        )
    return selected, profile, reasons


def choose_direct_profile(
    inventory: Inventory,
    requested: str,
    max_memory_mib: int,
) -> tuple[str, dict[str, Any], list[str]]:
    """Choose concurrency for the Worker transport's fixed 32 MiB R2 parts."""
    selected, profile, reasons = choose_profile(inventory, requested, 1 << 30)
    profile["chunk_size_mib"] = min(int(profile["chunk_size_mib"]), 32)
    initial_memory = profile_memory_mib(profile)
    while profile_memory_mib(profile) > max_memory_mib:
        if profile["upload_concurrency"] > 2:
            profile["upload_concurrency"] -= 1
        elif profile["transfers"] > 1:
            profile["transfers"] -= 1
        else:
            break
    if profile_memory_mib(profile) < initial_memory:
        reasons.append(
            f"Concurrency was reduced to stay within the {max_memory_mib} MiB memory budget."
        )
    return selected, profile, reasons


class FileLock:
    """Small cross-platform advisory lock used to isolate one source folder."""

    def __init__(self, path: Path, busy_message: str):
        self.path = path
        self.busy_message = busy_message
        self.file = None
        self.acquired = False

    def acquire(self, timeout: float = 0) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.path.open("a+b")
        deadline = time.monotonic() + max(0, timeout)
        while True:
            try:
                if platform.system() == "Windows":
                    import msvcrt

                    self.file.seek(0)
                    if self.file.read(1) == b"":
                        self.file.write(b"0")
                        self.file.flush()
                    self.file.seek(0)
                    msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self.acquired = True
                return
            except (OSError, ImportError) as exc:
                if time.monotonic() >= deadline:
                    self.file.close()
                    self.file = None
                    raise IngestError(self.busy_message) from exc
                time.sleep(0.1)

    def release(self) -> None:
        if self.file is None or self.file.closed:
            return
        if self.acquired:
            try:
                if platform.system() == "Windows":
                    import msvcrt

                    self.file.seek(0)
                    msvcrt.locking(self.file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(self.file.fileno(), fcntl.LOCK_UN)
            except (OSError, ImportError):
                pass
        self.file.close()
        self.file = None
        self.acquired = False


class StateStore:
    def __init__(self, state_dir: Path):
        self.state_dir = state_dir.expanduser().resolve()
        self.reports_dir = self.state_dir / "reports"
        self.logs_dir = self.state_dir / "logs"
        self.locks_dir = self.state_dir / "locks"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.locks_dir.mkdir(parents=True, exist_ok=True)
        initialize_lock = FileLock(
            self.state_dir / "initialize.lock",
            "Another FPV upload command is initializing local state. Try again in a moment.",
        )
        initialize_lock.acquire(timeout=30)
        try:
            self.db = sqlite3.connect(self.state_dir / "state.sqlite3", timeout=30)
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA busy_timeout=30000")
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA foreign_keys=ON")
            self._initialize()
        except Exception:
            self.close()
            raise
        finally:
            initialize_lock.release()

    def close(self) -> None:
        db = getattr(self, "db", None)
        if db is not None:
            db.close()

    @contextmanager
    def source_lock(self, source: Path):
        canonical = os.path.normcase(str(source.expanduser().resolve()))
        lock_id = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        lock = FileLock(
            self.locks_dir / f"source-{lock_id}.lock",
            f"This source folder is already being uploaded: {source}. "
            "Different SD cards may upload in parallel, but do not start two commands "
            "for the same folder.",
        )
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    def _initialize(self) -> None:
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                source_path TEXT NOT NULL,
                source_fingerprint TEXT NOT NULL,
                source_label TEXT NOT NULL,
                environment_id TEXT,
                environment_name TEXT,
                site_id TEXT,
                site_name TEXT,
                assignment_id TEXT,
                assignment_json TEXT,
                allowed_hours REAL,
                catalog_revision INTEGER,
                catalog_updated_at TEXT,
                catalog_etag TEXT,
                duration_seconds REAL,
                duration_method TEXT,
                duration_json TEXT,
                dashboard_destination TEXT,
                created_at TEXT NOT NULL,
                destination TEXT NOT NULL,
                data_destination TEXT NOT NULL,
                control_destination TEXT NOT NULL,
                status TEXT NOT NULL,
                profile_name TEXT NOT NULL,
                profile_json TEXT NOT NULL,
                inventory_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_error TEXT
            );
            CREATE INDEX IF NOT EXISTS sessions_source_fingerprint
                ON sessions(source_path, source_fingerprint, created_at);
            CREATE TABLE IF NOT EXISTS attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL,
                command_json TEXT NOT NULL,
                result_json TEXT,
                log_path TEXT NOT NULL,
                report_json_path TEXT,
                report_md_path TEXT,
                FOREIGN KEY(session_id) REFERENCES sessions(id),
                UNIQUE(session_id, sequence)
            );
            """
        )
        existing_columns = {
            row["name"] for row in self.db.execute("PRAGMA table_info(sessions)").fetchall()
        }
        migrations = {
            "environment_id": "ALTER TABLE sessions ADD COLUMN environment_id TEXT",
            "environment_name": "ALTER TABLE sessions ADD COLUMN environment_name TEXT",
            "site_id": "ALTER TABLE sessions ADD COLUMN site_id TEXT",
            "site_name": "ALTER TABLE sessions ADD COLUMN site_name TEXT",
            "assignment_id": "ALTER TABLE sessions ADD COLUMN assignment_id TEXT",
            "assignment_json": "ALTER TABLE sessions ADD COLUMN assignment_json TEXT",
            "allowed_hours": "ALTER TABLE sessions ADD COLUMN allowed_hours REAL",
            "catalog_revision": "ALTER TABLE sessions ADD COLUMN catalog_revision INTEGER",
            "catalog_updated_at": "ALTER TABLE sessions ADD COLUMN catalog_updated_at TEXT",
            "catalog_etag": "ALTER TABLE sessions ADD COLUMN catalog_etag TEXT",
            "duration_seconds": "ALTER TABLE sessions ADD COLUMN duration_seconds REAL",
            "duration_method": "ALTER TABLE sessions ADD COLUMN duration_method TEXT",
            "duration_json": "ALTER TABLE sessions ADD COLUMN duration_json TEXT",
            "dashboard_destination": "ALTER TABLE sessions ADD COLUMN dashboard_destination TEXT",
        }
        for column, statement in migrations.items():
            if column not in existing_columns:
                self.db.execute(statement)
        self.db.execute(
            "INSERT OR REPLACE INTO metadata(key, value) VALUES('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )
        self.db.commit()

    def recover_stale_attempts(self, source: Path) -> int:
        timestamp = iso_now()
        rows = self.db.execute(
            "SELECT DISTINCT attempts.session_id FROM attempts "
            "JOIN sessions ON sessions.id = attempts.session_id "
            "WHERE attempts.status = 'RUNNING' AND sessions.source_path=?",
            (str(source.resolve()),),
        ).fetchall()
        for row in rows:
            self.db.execute(
                "UPDATE attempts SET status='INTERRUPTED', ended_at=? "
                "WHERE status='RUNNING' AND session_id=?",
                (timestamp, row["session_id"]),
            )
            self.db.execute(
                "UPDATE sessions SET status='INTERRUPTED', updated_at=? "
                "WHERE id=? AND status != 'VERIFIED'",
                (timestamp, row["session_id"]),
            )
        self.db.commit()
        return len(rows)

    def find_session(
        self, source: Path, fingerprint: str, environment_id: str, site_id: str,
        assignment_id: str,
    ) -> sqlite3.Row | None:
        return self.db.execute(
            "SELECT * FROM sessions WHERE source_path=? AND source_fingerprint=? "
            "AND environment_id=? AND site_id=? AND assignment_id=? "
            "ORDER BY created_at DESC LIMIT 1",
            (str(source.resolve()), fingerprint, environment_id, site_id, assignment_id),
        ).fetchone()

    def find_session_by_id(self, session_id: str) -> sqlite3.Row | None:
        return self.db.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()

    def rebind_session_source(
        self, session_id: str, source: Path, inventory: Inventory, source_label: str
    ) -> None:
        self.db.execute(
            "UPDATE sessions SET source_path=?, source_fingerprint=?, source_label=?, "
            "inventory_json=?, updated_at=? WHERE id=?",
            (
                str(source.resolve()), inventory.fingerprint, source_label,
                json.dumps(inventory.as_dict(), ensure_ascii=False, sort_keys=True),
                iso_now(), session_id,
            ),
        )
        self.db.commit()

    def create_session(
        self,
        source: Path,
        inventory: Inventory,
        source_label: str,
        environment: dict[str, Any],
        site: dict[str, str],
        assignment: dict[str, Any],
        catalog_metadata: dict[str, Any],
        duration: DurationSummary,
        dashboard_destination: str,
        created_at: dt.datetime,
        destination: str,
        data_destination: str,
        control_destination: str,
        profile_name: str,
        profile: dict[str, Any],
    ) -> sqlite3.Row:
        session_id = destination.rstrip("/").split("/")[-1]
        timestamp = created_at.isoformat(timespec="seconds")
        self.db.execute(
            """
            INSERT INTO sessions(
                id, source_path, source_fingerprint, source_label,
                environment_id, environment_name, site_id, site_name, assignment_id,
                assignment_json, allowed_hours,
                catalog_revision, catalog_updated_at, catalog_etag,
                duration_seconds, duration_method, duration_json, dashboard_destination, created_at,
                destination, data_destination, control_destination, status,
                profile_name, profile_json, inventory_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'DISCOVERED', ?, ?, ?, ?)
            """,
            (
                session_id,
                str(source.resolve()),
                inventory.fingerprint,
                source_label,
                environment["id"],
                environment["name"],
                site["id"],
                site["name"],
                assignment["id"],
                json.dumps(assignment, ensure_ascii=False, sort_keys=True),
                environment["allowed_hours"],
                catalog_metadata["revision"],
                catalog_metadata["updated_at"],
                catalog_metadata.get("etag", ""),
                duration.accounted_seconds,
                duration.method,
                json.dumps(duration.as_dict(), ensure_ascii=False, sort_keys=True),
                dashboard_destination,
                timestamp,
                destination,
                data_destination,
                control_destination,
                profile_name,
                json.dumps(profile, sort_keys=True),
                json.dumps(inventory.as_dict(), ensure_ascii=False, sort_keys=True),
                timestamp,
            ),
        )
        self.db.commit()
        return self.get_session(session_id)

    def update_classification_snapshot(
        self,
        session_id: str,
        environment: dict[str, Any],
        site: dict[str, str],
        assignment: dict[str, Any],
        catalog_metadata: dict[str, Any],
    ) -> None:
        self.db.execute(
            """
            UPDATE sessions SET environment_name=?, site_name=?, assignment_json=?, allowed_hours=?,
                catalog_revision=?, catalog_updated_at=?, catalog_etag=?, updated_at=?
            WHERE id=?
            """,
            (
                environment["name"],
                site["name"],
                json.dumps(assignment, ensure_ascii=False, sort_keys=True),
                environment["allowed_hours"],
                catalog_metadata["revision"],
                catalog_metadata["updated_at"],
                catalog_metadata.get("etag", ""),
                iso_now(),
                session_id,
            ),
        )
        self.db.commit()

    def get_session(self, session_id: str) -> sqlite3.Row:
        row = self.db.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
        if row is None:
            raise IngestError(f"Unknown session: {session_id}")
        return row

    def update_session(self, session_id: str, status: str, error: str | None = None) -> None:
        self.db.execute(
            "UPDATE sessions SET status=?, last_error=?, updated_at=? WHERE id=?",
            (status, error, iso_now(), session_id),
        )
        self.db.commit()

    def update_profile(
        self, session_id: str, profile_name: str, profile: dict[str, Any]
    ) -> None:
        self.db.execute(
            "UPDATE sessions SET profile_name=?, profile_json=?, updated_at=? WHERE id=?",
            (profile_name, json.dumps(profile, sort_keys=True), iso_now(), session_id),
        )
        self.db.commit()

    def update_accounting(self, session_id: str, duration: DurationSummary) -> None:
        self.db.execute(
            """
            UPDATE sessions SET duration_seconds=?, duration_method=?, duration_json=?, updated_at=?
            WHERE id=?
            """,
            (
                duration.accounted_seconds,
                duration.method,
                json.dumps(duration.as_dict(), ensure_ascii=False, sort_keys=True),
                iso_now(),
                session_id,
            ),
        )
        self.db.commit()

    def start_attempt(self, session_id: str, command: list[str]) -> sqlite3.Row:
        row = self.db.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM attempts WHERE session_id=?",
            (session_id,),
        ).fetchone()
        sequence = int(row["sequence"])
        log_path = self.logs_dir / f"{session_id}-attempt-{sequence:03d}.log"
        self.db.execute(
            """
            INSERT INTO attempts(session_id, sequence, started_at, status, command_json, log_path)
            VALUES (?, ?, ?, 'RUNNING', ?, ?)
            """,
            (session_id, sequence, iso_now(), json.dumps(command), str(log_path)),
        )
        self.db.execute(
            "UPDATE sessions SET status='UPLOADING', updated_at=? WHERE id=?",
            (iso_now(), session_id),
        )
        self.db.commit()
        return self.db.execute(
            "SELECT * FROM attempts WHERE session_id=? AND sequence=?",
            (session_id, sequence),
        ).fetchone()

    def finish_attempt(
        self,
        attempt_id: int,
        status: str,
        result: dict[str, Any],
        report_json: Path,
        report_md: Path,
    ) -> None:
        self.db.execute(
            """
            UPDATE attempts SET ended_at=?, status=?, result_json=?,
                report_json_path=?, report_md_path=? WHERE id=?
            """,
            (
                iso_now(),
                status,
                json.dumps(result, ensure_ascii=False, sort_keys=True),
                str(report_json),
                str(report_md),
                attempt_id,
            ),
        )
        self.db.commit()

    def list_sessions(self, limit: int = 50) -> list[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM sessions ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()


def load_config(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        data = json.loads(path.expanduser().read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IngestError(f"Could not read configuration {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise IngestError("Configuration must contain one JSON object.")
    return data


def load_upload_environment(path: Path = UPLOAD_ENV_PATH) -> bool:
    """Load operator secrets; session credentials are minted immediately before upload."""
    if path.exists():
        try:
            lines = path.read_text(encoding="utf-8-sig").splitlines()
        except OSError as exc:
            raise IngestError(f"Could not read upload credentials {path}: {exc}") from exc
        for line_number, raw_line in enumerate(lines, 1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].lstrip()
            if "=" not in line:
                raise IngestError(f"Invalid .env line {line_number}: expected NAME=value")
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
                raise IngestError(f"Invalid .env name on line {line_number}")
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            os.environ.setdefault(key, value)

    forbidden = [
        name
        for name in ("R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_ENDPOINT")
        if os.environ.get(name, "").strip()
    ]
    if forbidden:
        raise IngestError(
            "Permanent R2 credentials are not accepted by the operator uploader. "
            "Remove them and use FPV_UPLOAD_TOKEN with FPV_UPLOAD_BROKER_URL."
        )

    broker_names = ("FPV_UPLOAD_BROKER_URL", "FPV_UPLOAD_TOKEN")
    broker_values = {name: os.environ.get(name, "").strip() for name in broker_names}
    broker_supplied = [name for name, value in broker_values.items() if value]
    if broker_supplied and len(broker_supplied) != len(broker_names):
        missing = ", ".join(name for name, value in broker_values.items() if not value)
        raise IngestError(f"Temporary credential settings are incomplete in {path}. Missing: {missing}")
    if broker_supplied:
        broker_url = urllib.parse.urlparse(broker_values["FPV_UPLOAD_BROKER_URL"])
        if broker_url.scheme != "https" and broker_url.hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise IngestError("FPV_UPLOAD_BROKER_URL must use HTTPS except during localhost testing.")
        return True

    return False


def configure_rclone(access_key_id: str, secret_access_key: str, endpoint_value: str, session_token: str = "") -> None:
    endpoint = urllib.parse.urlparse(endpoint_value)
    if endpoint.scheme != "https" or not endpoint.netloc or endpoint.path not in {"", "/"}:
        raise IngestError("R2_ENDPOINT must be the HTTPS account endpoint without a path.")
    remote_values = {
        "RCLONE_CONFIG": os.devnull,
        "RCLONE_CONFIG_R2_TYPE": "s3",
        "RCLONE_CONFIG_R2_PROVIDER": "Cloudflare",
        "RCLONE_CONFIG_R2_ACCESS_KEY_ID": access_key_id,
        "RCLONE_CONFIG_R2_SECRET_ACCESS_KEY": secret_access_key,
        "RCLONE_CONFIG_R2_ENDPOINT": endpoint_value.rstrip("/"),
        "RCLONE_CONFIG_R2_ACL": "private",
        "RCLONE_CONFIG_R2_NO_CHECK_BUCKET": "true",
    }
    if session_token:
        remote_values["RCLONE_CONFIG_R2_SESSION_TOKEN"] = session_token
    else:
        os.environ.pop("RCLONE_CONFIG_R2_SESSION_TOKEN", None)
    os.environ.update(remote_values)


def remote_bucket_and_key(remote_path: str) -> tuple[str, str]:
    if ":" not in remote_path:
        raise IngestError("Temporary credentials require an rclone remote such as r2:bucket/path.")
    _remote_name, value = remote_path.split(":", 1)
    bucket, separator, key = value.lstrip("/").partition("/")
    if not bucket or not separator or not key:
        raise IngestError(f"Could not determine R2 bucket and object key from {remote_path}.")
    return bucket, key


def configure_session_credentials(
    session: Mapping[str, Any] | sqlite3.Row,
    inventory: Inventory | None = None,
) -> dict[str, Any] | None:
    broker_url = os.environ.get("FPV_UPLOAD_BROKER_URL", "").strip()
    broker_token = os.environ.get("FPV_UPLOAD_TOKEN", "").strip()
    if not broker_url and not broker_token:
        raise IngestError(
            "Temporary upload credentials are not configured. Run setup and paste "
            "FPV_UPLOAD_TOKEN into the .env file beside the uploader."
        )
    if not broker_url or not broker_token:
        raise IngestError("Both FPV_UPLOAD_BROKER_URL and FPV_UPLOAD_TOKEN are required.")

    bucket, destination_key = remote_bucket_and_key(session["destination"])
    dashboard_bucket, dashboard_key = remote_bucket_and_key(
        remote_join(session["dashboard_destination"], f"{session['id']}.json")
    )
    if dashboard_bucket != bucket:
        raise IngestError("Upload data and dashboard summary must use the same R2 bucket.")
    recording_claims = []
    if inventory is not None and inventory.trinet is not None:
        recording_claims = [
            {
                "recordingUid": take.recording_uid,
                "deviceId": take.device_id,
                "environmentId": session["environment_id"],
                "siteId": session["site_id"],
                "sessionId": session["id"],
                **({"legacyRecordingUid": take.legacy_recording_uid}
                   if destination_key.startswith("raw/recordings/") and take.legacy_recording_uid else {}),
            }
            for take in inventory.trinet.takes
        ]
    request = urllib.request.Request(
        broker_url,
        data=json.dumps({
            **({"assignmentId": session["assignment_id"]} if "assignment_id" in session.keys() and session["assignment_id"] else {}),
            "sessionPrefix": destination_key.rstrip("/") + "/",
            "dashboardKey": dashboard_key,
            "recordings": recording_claims,
        }).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {broker_token}",
            "Content-Type": "application/json",
            "User-Agent": f"{APP_NAME}/{SCHEMA_VERSION}",
        },
        method="POST",
    )
    try:
        with open_https(request, timeout=30) as response:
            payload = json.loads(response.read(64 * 1024).decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read(4096).decode("utf-8", errors="replace")
        raise IngestError(f"Credential broker rejected this upload ({exc.code}): {detail}") from exc
    except (urllib.error.URLError, TimeoutError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IngestError(f"Could not obtain temporary upload credentials: {exc}") from exc
    try:
        has_assignment = "assignment_id" in session.keys() and bool(session["assignment_id"])
        write_allowed = has_assignment or destination_key.startswith("raw/recordings/")
        if (
            payload["bucket"] != bucket or payload["deleteAllowed"] is not False or
            payload["writeAllowed"] is not write_allowed
        ):
            raise ValueError("broker returned an unsafe or incorrect scope")
        expires_at = str(payload["expiresAt"])
        expiry = dt.datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        remaining_seconds = (expiry - dt.datetime.now(dt.timezone.utc)).total_seconds()
        if remaining_seconds < MIN_CREDENTIAL_VALIDITY_SECONDS:
            raise ValueError(
                "credential lifetime is too short for a 24-hour upload and verification"
            )
        configure_rclone(
            str(payload["accessKeyId"]),
            str(payload["secretAccessKey"]),
            str(payload["endpoint"]),
            str(payload["sessionToken"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise IngestError(f"Credential broker returned an invalid response: {exc}") from exc
    print(f"Temporary upload access: ACTIVE until {expires_at} (deletion denied)")
    return payload


def format_operator_progress(line: str, folder_index: int, folder_total: int) -> str | None:
    match = re.search(
        r"(?:Transferred:\s+)?[^:]+?\s*/\s*.+?,\s*([0-9.]+)%,\s*([^,]+?/s),\s*ETA\s+([^,\r\n]+)",
        line,
    )
    if not match:
        return None
    percent, speed, eta = (part.strip() for part in match.groups())
    return f"UPLOADING — Folder {folder_index} of {folder_total} — {percent}% — {speed} — ETA {eta}"


def run_streaming(
    command: list[str],
    log_path: Path,
    progress_context: tuple[int, int] | None = None,
) -> tuple[int, list[str], float]:
    started = time.monotonic()
    tail: list[str] = []
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log:
        log.write(f"[{iso_now()}] COMMAND {json.dumps(command)}\n")
        log.flush()
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        def forward_signal(signum: int, _frame: Any) -> None:
            if process.poll() is None:
                process.send_signal(signum)

        previous_int = signal.signal(signal.SIGINT, forward_signal)
        previous_term = signal.signal(signal.SIGTERM, forward_signal)
        try:
            assert process.stdout is not None
            for line in process.stdout:
                log.write(line)
                log.flush()
                formatted = (
                    format_operator_progress(line, *progress_context)
                    if progress_context is not None
                    else None
                )
                print((formatted + "\n") if formatted else line, end="", flush=True)
                tail.append(line.rstrip())
                if len(tail) > 80:
                    tail.pop(0)
            return_code = process.wait()
        finally:
            if process.stdout is not None:
                process.stdout.close()
            signal.signal(signal.SIGINT, previous_int)
            signal.signal(signal.SIGTERM, previous_term)
    return return_code, tail, time.monotonic() - started


def run_capture(command: list[str], timeout: int | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, check=False, timeout=timeout)


def installed_rclone_version() -> tuple[int, int, int] | None:
    if not shutil.which("rclone"):
        return None
    try:
        result = run_capture(["rclone", "version"], timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    match = re.search(r"rclone v(\d+)\.(\d+)(?:\.(\d+))?", result.stdout)
    if result.returncode != 0 or not match:
        return None
    return tuple(int(value or 0) for value in match.groups())


def rclone_size(
    remote: str, timeout: int = REMOTE_OPERATION_TIMEOUT_SECONDS
) -> dict[str, int] | None:
    result = run_capture(
        ["rclone", "size", remote, "--json", "--fast-list"],
        timeout=timeout,
    )
    if result.returncode != 0:
        return None
    try:
        parsed = json.loads(result.stdout)
        return {"count": int(parsed["count"]), "bytes": int(parsed["bytes"])}
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def marker_matches(
    marker: str,
    fingerprint: str,
    session_id: str | None = None,
    file_count: int | None = None,
    total_bytes: int | None = None,
) -> bool:
    # `rclone cat` traverses the parent directory first, which is intentionally
    # outside the exact dashboard-marker scope. copyto performs HeadObject/GetObject
    # against only this key and keeps the credential narrowly scoped.
    with tempfile.TemporaryDirectory(prefix="fpv-marker-readback-") as temp_dir:
        local = Path(temp_dir) / "marker.json"
        result = run_capture(
            ["rclone", "copyto", marker, str(local), "--retries", "10", "--low-level-retries", "10"],
            timeout=REMOTE_OPERATION_TIMEOUT_SECONDS,
        )
        if result.returncode != 0 or not local.is_file() or local.stat().st_size > MAX_CATALOG_BYTES:
            return False
        try:
            payload = json.loads(local.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return False
    expected = {
        "status": "VERIFIED",
        "source_fingerprint": fingerprint,
    }
    if session_id is not None:
        expected["session_id"] = session_id
    if file_count is not None:
        expected["file_count"] = file_count
    if total_bytes is not None:
        expected["total_bytes"] = total_bytes
    return all(payload.get(key) == value for key, value in expected.items())


def read_remote_json(remote: str) -> dict[str, Any] | None:
    """Read a small, exact object key without listing its parent prefix."""
    with tempfile.TemporaryDirectory(prefix="fpv-raw-readback-") as temp_dir:
        local = Path(temp_dir) / "object.json"
        result = run_capture(
            ["rclone", "copyto", remote, str(local), "--retries", "3", "--low-level-retries", "3"],
            timeout=REMOTE_OPERATION_TIMEOUT_SECONDS,
        )
        if result.returncode != 0 or not local.is_file():
            return None
        if local.stat().st_size > MAX_CATALOG_BYTES:
            raise IngestError(f"Remote control object is too large: {remote}")
        try:
            value = json.loads(local.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise IngestError(f"Remote control object is invalid: {remote}") from exc
        if not isinstance(value, dict):
            raise IngestError(f"Remote control object is invalid: {remote}")
        return value


def copy_small_file(local: Path, remote: str) -> tuple[bool, str]:
    # rcat performs a direct PUT to the exact key. copyto may issue CopyObject
    # merely to adjust mtime, which is unnecessary for control JSON and reports.
    try:
        with local.open("rb") as source:
            result = subprocess.run(
                [
                    "rclone", "rcat", remote, "--size", str(local.stat().st_size),
                    "--retries", "10", "--low-level-retries", "10", "--retries-sleep", "5s",
                ],
                stdin=source,
                capture_output=True,
                check=False,
                timeout=REMOTE_OPERATION_TIMEOUT_SECONDS,
            )
    except (OSError, subprocess.SubprocessError) as exc:
        return False, str(exc)
    message = (result.stderr or result.stdout).decode("utf-8", errors="replace").strip()
    return result.returncode == 0, message


def build_session_id(created_at: dt.datetime, label: str, fingerprint: str) -> str:
    # created_at is captured exactly once and persisted. Resumes never recalculate it,
    # so a job that crosses midnight remains under the same date and destination.
    nonce = secrets.token_hex(8)
    return f"{created_at.strftime('%Y%m%d-%H%M%S')}-{slugify(label)}-{fingerprint[:8]}-{nonce}"


def build_destination(remote: str, prefix: str, created_at: dt.datetime, session_id: str) -> str:
    return remote_join(
        remote,
        prefix,
        created_at.strftime("%Y"),
        created_at.strftime("%m"),
        created_at.strftime("%d"),
        session_id,
    )


def build_assignment_prefix(prefix: str, assignment: Mapping[str, Any]) -> str:
    """Build the canonical, readable hierarchy from an immutable Assignment."""
    return remote_join(
        prefix,
        f"l1={slugify(str(assignment['environment_l1']), 'l1')}",
        f"l2={assignment['l2_target_id']}",
        f"l3={slugify(str(assignment['environment_l3']), 'l3')}",
        f"site={assignment['environment_id']}",
        f"operator={assignment['operator_id']}",
        f"task={assignment['task_id']}",
    )


def build_video_id(inventory: Inventory) -> str:
    """Return a stable recording-set ID without rereading large video files."""
    if inventory.trinet is not None:
        identity = inventory.trinet.recording_set_id
    else:
        digest = hashlib.sha256()
        for item in inventory.entries:
            digest.update(item.path.encode("utf-8", errors="surrogateescape"))
            digest.update(b"\0")
            digest.update(str(item.size).encode("ascii"))
            digest.update(b"\0")
            digest.update(item.sample_sha256.encode("ascii"))
            digest.update(b"\n")
        identity = digest.hexdigest()
    return f"vid-{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:32]}"


def build_trinet_session_id(
    summary: TrinetSummary, environment_id: str, site_id: str, assignment_id: str
) -> str:
    classification = hashlib.sha256(
        f"{environment_id}\0{site_id}\0{assignment_id}".encode("utf-8")
    ).hexdigest()[:8]
    return f"{summary.recording_set_id}-{classification}"


def build_trinet_destination(
    remote: str,
    prefix: str,
    assignment: Mapping[str, Any],
    summary: TrinetSummary,
    video_id: str,
    session_id: str,
) -> str:
    # Trinet has no trustworthy wall clock. Classification and content identity,
    # rather than a date or SD-card volume name, define the durable object path.
    return remote_join(
        remote,
        build_assignment_prefix(prefix, assignment),
        (f"device={summary.device_ids[0]}" if len(summary.device_ids) == 1
         else "device=mixed-" + hashlib.sha256("\n".join(summary.device_ids).encode("ascii")).hexdigest()[:12]),
        f"video={video_id}",
        session_id,
    )


def rclone_card_metadata_filters() -> list[str]:
    filters = ["--exclude", "**/._*"]
    for name in sorted(IGNORED_CARD_METADATA, key=str.casefold):
        filters.extend(("--exclude", f"**/{name}", "--exclude", f"**/{name}/**"))
    return filters


def rclone_copy_command(
    source: Path,
    data_destination: str,
    profile: dict[str, Any],
    force_reupload: bool,
    dry_run: bool,
) -> list[str]:
    command = [
        "rclone",
        "copy",
        str(source),
        data_destination,
        "--transfers",
        str(profile["transfers"]),
        "--checkers",
        str(profile["checkers"]),
        "--s3-upload-cutoff",
        f"{profile['upload_cutoff_mib']}M",
        "--s3-chunk-size",
        f"{profile['chunk_size_mib']}M",
        "--s3-upload-concurrency",
        str(profile["upload_concurrency"]),
        "--retries",
        "20",
        "--low-level-retries",
        "20",
        "--retries-sleep",
        "10s",
        "--stats",
        "5s",
        "--stats-one-line",
        "--log-level",
        "INFO",
        "--fast-list",
    ]
    command.extend(rclone_card_metadata_filters())
    if force_reupload:
        command.append("--ignore-times")
    if dry_run:
        command.append("--dry-run")
    return command


def wrap_sleep_prevention(command: list[str]) -> tuple[list[str], bool]:
    if platform.system() == "Darwin" and shutil.which("caffeinate"):
        return ["caffeinate", "-dimsu", *command], True
    if platform.system() == "Linux" and shutil.which("systemd-inhibit"):
        return [
            "systemd-inhibit", "--what=sleep", "--mode=block",
            "--why=FPV recording upload", *command,
        ], True
    return command, False


def sleep_prevention_method() -> str | None:
    system = platform.system()
    if system == "Darwin" and shutil.which("caffeinate"):
        return "macOS caffeinate"
    if system == "Linux" and shutil.which("systemd-inhibit"):
        return "Linux systemd-inhibit"
    if system == "Windows":
        return "Windows execution-state API"
    return None


def set_windows_sleep_prevention(active: bool) -> bool:
    if platform.system() != "Windows":
        return False
    es_continuous = 0x80000000
    flags = es_continuous | 0x00000001 | 0x00000002 if active else es_continuous
    try:
        return bool(ctypes.windll.kernel32.SetThreadExecutionState(flags))
    except (AttributeError, OSError):
        return False


def check_sleep_prevention() -> tuple[bool, str]:
    method = sleep_prevention_method()
    if method == "Windows execution-state API":
        active = set_windows_sleep_prevention(True)
        if active:
            set_windows_sleep_prevention(False)
        return active, method if active else "Windows execution-state API failed"
    if method == "macOS caffeinate":
        command = ["caffeinate", "-dimsu", "true"]
    elif method == "Linux systemd-inhibit":
        command = [
            "systemd-inhibit",
            "--what=sleep",
            "--mode=block",
            "--why=FPV upload preflight",
            "true",
        ]
    else:
        return False, f"not available on {platform.system()}"
    try:
        result = run_capture(command, timeout=15)
    except (OSError, subprocess.SubprocessError) as exc:
        return False, str(exc)
    detail = method if result.returncode == 0 else (result.stderr.strip() or "activation failed")
    return result.returncode == 0, detail


def verify_session(
    source: Path,
    data_destination: str,
    inventory_before: Inventory,
    verification: str,
    checkers: int,
    log_path: Path,
) -> dict[str, Any]:
    started = time.monotonic()
    result: dict[str, Any] = {
        "mode": verification,
        "source_inventory_unchanged": False,
        "remote_inventory": None,
        "path_and_size_check": False,
        "content_hash_check": None,
        "exit_code": None,
        "tail": [],
    }
    inventory_after = scan_source(source)
    result["source_inventory_unchanged"] = (
        inventory_after.fingerprint == inventory_before.fingerprint
    )
    result["remote_inventory"] = rclone_size(data_destination)

    command = [
        "rclone",
        "check",
        str(source),
        data_destination,
        "--one-way",
        "--checkers",
        str(checkers),
        "--stats",
        "30s",
        "--fast-list",
    ]
    command.extend(rclone_card_metadata_filters())
    if verification == "standard":
        command.append("--size-only")
    elif verification == "download":
        command.append("--download")

    protected_command, _protected = wrap_sleep_prevention(command)
    return_code, tail, _duration = run_streaming(protected_command, log_path)
    result["exit_code"] = return_code
    result["tail"] = tail[-20:]
    result["path_and_size_check"] = return_code == 0
    if verification in {"enhanced", "download"}:
        result["content_hash_check"] = return_code == 0

    remote_inventory = result["remote_inventory"]
    result["counts_match"] = bool(
        remote_inventory
        and remote_inventory["count"] == inventory_before.file_count
        and remote_inventory["bytes"] == inventory_before.total_bytes
    )
    result["passed"] = bool(
        result["source_inventory_unchanged"]
        and result["path_and_size_check"]
        and result["counts_match"]
    )
    result["duration_seconds"] = round(time.monotonic() - started, 3)
    return result


def count_retry_lines(lines: Iterable[str]) -> int:
    pattern = re.compile(r"retry|re-try|low.level", re.IGNORECASE)
    return sum(1 for line in lines if pattern.search(line))


def report_paths(store: StateStore, session_id: str, sequence: int) -> tuple[Path, Path]:
    directory = store.reports_dir / session_id
    directory.mkdir(parents=True, exist_ok=True)
    return (
        directory / f"attempt-{sequence:03d}.json",
        directory / f"attempt-{sequence:03d}.md",
    )


def render_attempt_markdown(report: dict[str, Any]) -> str:
    verification = report.get("verification") or {}
    remote = verification.get("remote_inventory") or {}
    content_check = verification.get("content_hash_check")
    if content_check is None:
        content_check = "upload-time integrity checks only"
    lines = [
        f"# R2 upload report: {report['session_id']}",
        "",
        f"**Status:** {report['status']}",
        "",
        f"- Attempt: {report['attempt_sequence']}",
        f"- Source: `{report['source']}`",
        f"- Destination: `{report['destination']}`",
        f"- Assignment ID: `{report['assignment_id']}`",
        f"- Category: {report['environment_name']} (`{report['environment_id']}`)",
        f"- Physical environment: {report['site_name']} (`{report['site_id']}`)",
        f"- Operator: {report['operator_name']} (`{report['operator_id']}`)",
        f"- Camera shutter: {report.get('episode_camera_shutter') or 'pending device identification'}",
        f"- Shared catalog revision: {report['catalog_revision']}",
        f"- Accounted recording hours: {report['recording_duration_seconds'] / 3600:.3f}",
        f"- Duration method: {report['duration_method']}",
        f"- Session created: {report['session_created_at']}",
        f"- Attempt started: {report['started_at']}",
        f"- Attempt ended: {report['ended_at']}",
        f"- Duration: {human_duration(report['duration_seconds'])}",
        f"- Sleep prevention: {'active' if report['sleep_prevention'] else 'unavailable'}",
        f"- Temporary upload credential: {'yes (deletion denied)' if report.get('temporary_credentials') else 'no'}",
        f"- Credential expiry: {report.get('credential_expires_at') or 'not applicable'}",
        f"- Force re-upload: {'yes' if report['force_reupload'] else 'no'}",
        "",
        "## Inventory",
        "",
        f"- Source files: {report['source_file_count']}",
        f"- Source bytes: {report['source_total_bytes']} ({human_bytes(report['source_total_bytes'])})",
        f"- Remote objects: {remote.get('count', 'not checked')}",
        f"- Remote bytes: {remote.get('bytes', 'not checked')}",
        "",
        "## Upload configuration",
        "",
        f"- Profile: {report['profile_name']}",
        f"- File transfers: {report['profile']['transfers']}",
        f"- Multipart concurrency: {report['profile']['upload_concurrency']}",
        f"- Multipart part size: {report['profile']['chunk_size_mib']} MiB",
        f"- Multipart cutoff: {report['profile']['upload_cutoff_mib']} MiB",
        f"- Estimated multipart memory: {profile_memory_mib(report['profile'])} MiB",
        "",
        "## Checks",
        "",
        f"- Upload command exit code: {report['upload_exit_code']}",
        f"- Source inventory unchanged: {verification.get('source_inventory_unchanged', 'not checked')}",
        f"- Remote path/size check: {verification.get('path_and_size_check', 'not checked')}",
        f"- Total count/bytes match: {verification.get('counts_match', 'not checked')}",
        f"- Content hash check: {content_check}",
        f"- Verification duration: {verification.get('duration_seconds', 'not checked')} seconds",
        f"- Completion marker: {report.get('completion_marker', 'not written')}",
    ]
    if report.get("error"):
        lines.extend(["", "## Error", "", report["error"]])
    if report.get("next_action"):
        lines.extend(["", "## Next action", "", report["next_action"]])
    return "\n".join(lines) + "\n"


def write_report(
    store: StateStore,
    session_id: str,
    sequence: int,
    report: dict[str, Any],
) -> tuple[Path, Path]:
    json_path, md_path = report_paths(store, session_id, sequence)
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    md_path.write_text(render_attempt_markdown(report), encoding="utf-8")
    return json_path, md_path


def upload_control_files(
    session: sqlite3.Row,
    inventory: Inventory,
    report_json_path: Path,
    report_md_path: Path,
    sequence: int,
) -> tuple[bool, list[str]]:
    errors: list[str] = []
    with tempfile.TemporaryDirectory(prefix="r2-ingest-") as temp_dir:
        manifest_path = Path(temp_dir) / "source-manifest.json"
        duration = json.loads(session["duration_json"])
        assignment = json.loads(session["assignment_json"])
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "session_id": session["id"],
            "video_id": build_video_id(inventory),
            "assignment": assignment,
            "metadata_status": "PARTIAL_REQUIRED_FIELDS_PENDING",
            "environment_id": session["environment_id"],
            "environment_name": session["environment_name"],
            "site_id": session["site_id"],
            "site_name": session["site_name"],
            "allowed_hours": session["allowed_hours"],
            "catalog_revision": session["catalog_revision"],
            "catalog_updated_at": session["catalog_updated_at"],
            "duration": duration,
            "inventory": inventory.as_dict(),
        }
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        files = [
            (manifest_path, remote_join(session["control_destination"], "source-manifest.json")),
            (
                report_json_path,
                remote_join(session["control_destination"], f"attempt-{sequence:03d}.json"),
            ),
            (
                report_md_path,
                remote_join(session["control_destination"], f"attempt-{sequence:03d}.md"),
            ),
        ]
        for local, remote in files:
            ok, message = copy_small_file(local, remote)
            if not ok:
                errors.append(f"Could not upload {local.name}: {message}")
    return not errors, errors


def write_completion_marker(session: sqlite3.Row, inventory: Inventory) -> tuple[bool, str]:
    source_label = (
        session["source_label"]
        if "source_label" in session.keys()
        else Path(session["source_path"]).name
    )
    assignment = json.loads(session["assignment_json"])
    payload = {
        "schema_version": SCHEMA_VERSION,
        "status": "VERIFIED",
        "session_id": session["id"],
        "assignment_id": session["assignment_id"],
        "project_id": assignment["project_id"],
        "project_name": assignment["project_name"],
        "l2_target_id": assignment["l2_target_id"],
        "l2_target_name": assignment["l2_target_name"],
        "environment_l1": assignment["environment_l1"],
        "environment_l2": assignment["environment_l2"],
        "environment_l3": assignment["environment_l3"],
        "operator_id": assignment["operator_id"],
        "operator_name": assignment["operator_name"],
        "task_id": assignment["task_id"],
        "task_name": assignment["task_name"],
        "task": assignment.get("task"),
        "metadata_status": "PARTIAL_REQUIRED_FIELDS_PENDING",
        "metadata_pending": assignment.get("metadata_pending", []),
        "source_folder_name": source_label,
        "environment_id": session["environment_id"],
        "environment_name": session["environment_name"],
        "site_id": session["site_id"],
        "site_name": session["site_name"],
        "allowed_hours": session["allowed_hours"],
        "catalog_revision": session["catalog_revision"],
        "catalog_updated_at": session["catalog_updated_at"],
        "recording_duration_seconds": session["duration_seconds"],
        "duration_method": session["duration_method"],
        "session_created_at": session["created_at"],
        "verified_at": iso_now(),
        "source_fingerprint": inventory.fingerprint,
        "file_count": inventory.file_count,
        "total_bytes": inventory.total_bytes,
        "data_destination": session["data_destination"],
        "video_id": build_video_id(inventory),
        "object_key_layout": (
            "assignment-v2"
            if "/l1=" in session["data_destination"] and "/video=" in session["data_destination"]
            else "assignment-v1"
            if "/l1=" in session["data_destination"]
            else "legacy-v1"
        ),
    }
    if inventory.trinet is not None:
        payload["trinet"] = dataclasses.asdict(inventory.trinet)
    if assignment.get("episode_camera_shutter"):
        payload["episode_camera_shutter"] = assignment["episode_camera_shutter"]
    with tempfile.TemporaryDirectory(prefix="r2-ingest-") as temp_dir:
        marker_path = Path(temp_dir) / "_COMPLETE.json"
        marker_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        targets = [
            remote_join(session["control_destination"], "_COMPLETE.json"),
            remote_join(session["dashboard_destination"], f"{session['id']}.json"),
        ]
        for target in targets:
            ok, message = copy_small_file(marker_path, target)
            if not ok:
                return False, f"Could not write completion summary {target}: {message}"
        for target in targets:
            if not marker_matches(
                target,
                inventory.fingerprint,
                session["id"],
                inventory.file_count,
                inventory.total_bytes,
            ):
                return False, f"Completion summary could not be read back and validated: {target}"
        return True, ""


def upload(args: argparse.Namespace, config: dict[str, Any], store: StateStore) -> int:
    source = resolve_recording_source(Path(args.source).expanduser().resolve())
    args.source = str(source)
    with store.source_lock(source):
        recovered = store.recover_stale_attempts(source)
        if recovered:
            print(f"Recovered {recovered} interrupted attempt(s) for {source.name}.")
        return upload_locked(args, config, store)


def upload_locked(args: argparse.Namespace, config: dict[str, Any], store: StateStore) -> int:
    source = Path(args.source).expanduser().resolve()
    assignment_id = str(getattr(args, "assignment", "") or "")
    if not assignment_id:
        raise IngestError(
            "An Assignment ID is required. Create one in the dashboard, "
            "then add --assignment asg-xxxxxxxxxxxx."
        )
    assignment = getattr(args, "assignment_payload", None)
    confirmation_required = assignment is None
    if assignment is None:
        print(f"Validating assignment ID {assignment_id} ...")
        assignment = fetch_assignment(assignment_id)
    if confirmation_required and not confirm_assignment(
        assignment, yes=bool(getattr(args, "yes", False))
    ):
        print("Upload cancelled. Nothing was uploaded.")
        return 0
    args.environment = assignment["l2_target_id"]
    args.site = assignment["environment_id"]
    catalog_url = args.catalog_url or config.get("catalog_url")
    print(f"Fetching shared classification catalog from {catalog_url or '(not configured)'} ...")
    catalog, catalog_metadata = fetch_environment_catalog(catalog_url)
    print(
        f"Using catalog revision {catalog_metadata['revision']} "
        f"updated {catalog_metadata['updated_at']}"
    )
    print(f"Inventorying {source} ...")
    inventory = scan_source(source)

    environment_id = args.environment
    site_value = args.site
    assignment_sites = dict(catalog_metadata["sites"])
    assignment_sites[str(assignment["environment_id"])] = {
        "id": str(assignment["environment_id"]),
        "name": str(assignment["environment_name"]),
    }
    environment, site = validate_classification(
        str(environment_id), site_value, catalog, assignment_sites
    )
    site_id = site["id"]
    duration_policy = args.duration_policy or config.get("duration_policy", "grouped-views")
    duration = calculate_duration(source, inventory, duration_policy, args.duration_hours)

    remote = args.remote or config.get("remote")
    if not remote:
        raise IngestError("R2 destination is required via --remote or the configuration file.")
    prefix = args.prefix if args.prefix is not None else config.get("prefix", "raw")
    dashboard_destination = remote_join(
        remote, config.get("dashboard_index_prefix", "_dashboard/sessions")
    )
    if args.label:
        source_label = args.label
    elif inventory.trinet is not None:
        first_take = inventory.trinet.takes[0].take_label
        last_take = inventory.trinet.takes[-1].take_label
        take_range = first_take if first_take == last_take else f"{first_take}–{last_take}"
        device_label = (inventory.trinet.device_ids[0][:8] if len(inventory.trinet.device_ids) == 1
                        else f"{len(inventory.trinet.device_ids)} devices")
        source_label = f"Trinet {device_label} · {take_range}"
    else:
        source_label = source.name
    requested_profile = args.profile or config.get("profile", "auto")
    if requested_profile not in {*PROFILES, "auto"}:
        raise IngestError(f"Unknown profile: {requested_profile}")

    memory_limit = int(args.max_memory_mib or config.get("max_memory_mib", 1024))
    memory_limit = min(memory_limit, max(256, int(physical_memory_mib() * 0.15)))
    profile_name, selected_profile, reasons = choose_profile(
        inventory, requested_profile, memory_limit
    )

    trinet_session_id = None
    trinet_destination = None
    video_id = build_video_id(inventory)
    if inventory.trinet is not None:
        trinet_session_id = build_trinet_session_id(
            inventory.trinet, environment["id"], site_id, assignment_id
        )
        trinet_destination = build_trinet_destination(
            remote, prefix, assignment, inventory.trinet, video_id, trinet_session_id
        )

    session = (
        None
        if args.new_session
        else store.find_session(
            source, inventory.fingerprint, environment["id"], site_id, assignment_id
        )
    )
    if session is None and trinet_session_id is not None:
        session = store.find_session_by_id(trinet_session_id)
        if session is not None:
            if session["assignment_id"] != assignment_id:
                raise IngestError(
                    "This recording set already belongs to a different assignment ID. "
                    "No upload was started."
                )
            store.rebind_session_source(
                trinet_session_id, source, inventory, source_label
            )
            session = store.get_session(trinet_session_id)
    if session is None:
        created_at = now_local()
        if inventory.trinet is not None:
            assert trinet_session_id is not None and trinet_destination is not None
            session_id = trinet_session_id
            destination = trinet_destination
        else:
            session_id = build_session_id(created_at, source_label, inventory.fingerprint)
            classified_prefix = build_assignment_prefix(prefix, assignment)
            destination = remote_join(
                remote, classified_prefix, "device=unidentified", f"video={video_id}", session_id
            )
        session = store.create_session(
            source,
            inventory,
            source_label,
            environment,
            site,
            assignment,
            catalog_metadata,
            duration,
            dashboard_destination,
            created_at,
            destination,
            remote_join(destination, "data"),
            remote_join(destination, "_control"),
            profile_name,
            selected_profile,
        )
        print(f"Created session {session_id}")
    else:
        store.update_classification_snapshot(
            session["id"], environment, site, assignment, catalog_metadata
        )
        session = store.get_session(session["id"])
        if args.duration_hours is not None or args.duration_policy is not None:
            store.update_accounting(session["id"], duration)
            session = store.get_session(session["id"])
        else:
            duration = DurationSummary(**json.loads(session["duration_json"]))
        if args.profile is not None or args.max_memory_mib is not None:
            store.update_profile(session["id"], profile_name, selected_profile)
            session = store.get_session(session["id"])
            reasons.append("The persisted session profile was updated by an explicit CLI override.")
        else:
            profile_name = session["profile_name"]
            selected_profile = json.loads(session["profile_json"])
            reasons = [f"Reusing the persisted '{profile_name}' session profile."]
        print(f"Resuming session {session['id']} created at {session['created_at']}")

    print(f"Destination: {session['destination']}")
    print(f"Video ID: {video_id}")
    if inventory.trinet is not None:
        print(
            f"Trinet devices: {', '.join(inventory.trinet.device_ids)}; "
            f"takes: {', '.join(take.take_label for take in inventory.trinet.takes)}"
        )
    print(
        f"Assignment: {assignment_id}; L2 target: {environment['name']}; "
        f"environment: {site['name']} ({site_id}); operator: "
        f"{assignment['operator_name']} ({assignment['operator_id']}); "
        f"recording hours: {duration.accounted_seconds / 3600:.3f} "
        f"({duration.method})"
    )
    print(
        f"Inventory: {inventory.file_count} files, {human_bytes(inventory.total_bytes)}; "
        f"profile: {profile_name} ({profile_memory_mib(selected_profile)} MiB multipart budget)"
    )
    for reason in reasons:
        print(f"Profile note: {reason}")

    credential_info = configure_session_credentials(session, inventory)

    complete_marker = remote_join(session["control_destination"], "_COMPLETE.json")
    dashboard_marker = remote_join(
        session["dashboard_destination"], f"{session['id']}.json"
    )
    summaries_complete = marker_matches(
        complete_marker,
        inventory.fingerprint,
        session["id"],
        inventory.file_count,
        inventory.total_bytes,
    ) and marker_matches(
        dashboard_marker,
        inventory.fingerprint,
        session["id"],
        inventory.file_count,
        inventory.total_bytes,
    )
    if summaries_complete and not args.force_reupload:
        print("Existing completion records found; revalidating the actual cloud objects.")

    copy_command = rclone_copy_command(
        source,
        session["data_destination"],
        selected_profile,
        args.force_reupload,
        args.dry_run,
    )
    command, sleep_prevention = wrap_sleep_prevention(copy_command)
    windows_sleep_prevention = set_windows_sleep_prevention(True)
    sleep_prevention = sleep_prevention or windows_sleep_prevention
    if not sleep_prevention and not args.dry_run:
        raise IngestError(
            "Sleep prevention could not be activated. Run the doctor command and fix "
            "sleep prevention before uploading."
        )
    print(
        "Sleep prevention: "
        + (f"ACTIVE ({sleep_prevention_method()})" if sleep_prevention else "not required for preview")
    )
    attempt = store.start_attempt(session["id"], command)
    sequence = int(attempt["sequence"])
    started_monotonic = time.monotonic()
    upload_exit_code = 1
    upload_tail: list[str] = []
    verification: dict[str, Any] | None = None
    error: str | None = None
    status = "FAILED"
    next_action = "Rerun the same source. Completed matching objects will be skipped."
    completion_marker = "not written"
    folder_index = int(getattr(args, "batch_index", 1))
    folder_total = int(getattr(args, "batch_total", 1))

    try:
        print(f"UPLOADING — Folder {folder_index} of {folder_total} — {source.name}")
        upload_exit_code, upload_tail, _upload_duration = run_streaming(
            command, Path(attempt["log_path"]), (folder_index, folder_total)
        )
        if args.dry_run and upload_exit_code == 0:
            status = "DRY_RUN"
            next_action = "Review the report, then run again without --dry-run."
        elif upload_exit_code != 0:
            status = "RETRYABLE_FAILED"
            error = "The rclone upload command did not complete successfully."
        else:
            store.update_session(session["id"], "VERIFYING")
            print(f"VERIFYING — Folder {folder_index} of {folder_total}")
            verification = verify_session(
                source,
                session["data_destination"],
                inventory,
                args.verify,
                int(selected_profile["checkers"]),
                Path(attempt["log_path"]),
            )
            if verification["passed"]:
                status = "VERIFICATION_PASSED"
                next_action = "Control files and the completion marker will now be written."
            else:
                status = "FAILED_VERIFICATION"
                error = "One or more mandatory post-upload checks failed."
                next_action = "Review the report and log, then rerun the same session."
    except KeyboardInterrupt:
        status = "INTERRUPTED"
        error = "The operator interrupted the upload."
    except (OSError, subprocess.SubprocessError, IngestError) as exc:
        status = "RETRYABLE_FAILED"
        error = str(exc)
    finally:
        if windows_sleep_prevention:
            set_windows_sleep_prevention(False)

    duration = time.monotonic() - started_monotonic
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "session_id": session["id"],
        "assignment_id": session["assignment_id"],
        "project_id": assignment["project_id"],
        "project_name": assignment["project_name"],
        "environment_l1": assignment["environment_l1"],
        "environment_l2": assignment["environment_l2"],
        "environment_l3": assignment["environment_l3"],
        "operator_id": assignment["operator_id"],
        "operator_name": assignment["operator_name"],
        "task_id": assignment["task_id"],
        "task_name": assignment["task_name"],
        "metadata_status": "PARTIAL_REQUIRED_FIELDS_PENDING",
        "metadata_pending": assignment.get("metadata_pending", []),
        "environment_id": session["environment_id"],
        "environment_name": session["environment_name"],
        "site_id": session["site_id"],
        "site_name": session["site_name"],
        "allowed_hours": session["allowed_hours"],
        "catalog_revision": session["catalog_revision"],
        "catalog_updated_at": session["catalog_updated_at"],
        "recording_duration_seconds": session["duration_seconds"],
        "duration_method": session["duration_method"],
        "duration": json.loads(session["duration_json"]),
        "session_created_at": session["created_at"],
        "attempt_sequence": sequence,
        "started_at": attempt["started_at"],
        "ended_at": iso_now(),
        "duration_seconds": round(duration, 3),
        "status": status,
        "source": str(source),
        "destination": session["destination"],
        "data_destination": session["data_destination"],
        "source_fingerprint": inventory.fingerprint,
        "video_id": video_id,
        "source_file_count": inventory.file_count,
        "source_total_bytes": inventory.total_bytes,
        "profile_name": profile_name,
        "profile": selected_profile,
        "sleep_prevention": sleep_prevention,
        "temporary_credentials": credential_info is not None,
        "credential_expires_at": credential_info.get("expiresAt") if credential_info else None,
        "force_reupload": args.force_reupload,
        "verification_mode": args.verify,
        "upload_exit_code": upload_exit_code,
        "estimated_retry_log_lines": count_retry_lines(upload_tail),
        "verification": verification,
        "completion_marker": completion_marker,
        "error": error,
        "next_action": next_action,
        "log_path": attempt["log_path"],
    }
    report_json, report_md = write_report(store, session["id"], sequence, report)

    if status == "VERIFICATION_PASSED":
        controls_ok, control_errors = upload_control_files(
            session, inventory, report_json, report_md, sequence
        )
        if controls_ok:
            marker_ok, marker_error = write_completion_marker(session, inventory)
        else:
            marker_ok, marker_error = False, "; ".join(control_errors)
        if marker_ok:
            status = "VERIFIED"
            completion_marker = "written"
            error = None
            next_action = "Upload is verified. Retain or erase the source according to operator policy."
            store.update_session(session["id"], "VERIFIED")
        else:
            status = "UPLOADED_UNCONFIRMED"
            completion_marker = "failed"
            error = marker_error or "Could not write the remote completion marker."
            next_action = (
                "Rerun this session. Re-uploading or overwriting matching objects is safe."
            )
            store.update_session(session["id"], status, error)
        report.update(
            {
                "status": status,
                "completion_marker": completion_marker,
                "error": error,
                "next_action": next_action,
                "ended_at": iso_now(),
            }
        )
        report_json, report_md = write_report(store, session["id"], sequence, report)
        if marker_ok:
            # Refresh the remote report so its human-readable status is VERIFIED.
            copy_small_file(
                report_json,
                remote_join(session["control_destination"], f"attempt-{sequence:03d}.json"),
            )
            copy_small_file(
                report_md,
                remote_join(session["control_destination"], f"attempt-{sequence:03d}.md"),
            )
    else:
        store.update_session(session["id"], status, error)

    store.finish_attempt(int(attempt["id"]), status, report, report_json, report_md)
    print(f"\nFinal status: {status}")
    if status == "VERIFIED":
        print(f"VERIFIED — {session['source_label']}")
    print(f"Human-readable report: {report_md}")
    print(f"Machine-readable report: {report_json}")
    if error:
        print(f"Error: {error}", file=sys.stderr)
    return 0 if status in {"VERIFIED", "DRY_RUN"} else 1


def folder_contains_video(path: Path) -> bool:
    try:
        return any(
            item.is_file() and not is_ignored_card_path(item.relative_to(path)) and
            item.suffix.lower() in VIDEO_EXTENSIONS
            for item in path.rglob("*")
        )
    except OSError as exc:
        raise IngestError(f"Could not inspect batch folder {path}: {exc}") from exc


def resolve_recording_source(path: Path) -> Path:
    """Accept an SD root, Trinet directory, recording directory, or legacy folder."""
    path = path.expanduser().resolve()
    if not path.is_dir():
        return path
    candidates: list[Path] = []
    if path.name.casefold() == "recording":
        candidates.append(path)
    if path.name.casefold() == "trinet":
        candidates.extend((path / "recording", path))
    candidates.extend((path / "Trinet" / "recording", path / "recording", path / "Trinet", path))
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.is_dir() and folder_contains_video(candidate):
            if candidate != path:
                print(f"Detected Trinet recording folder: {candidate}")
            return candidate
    return path


def inventory_for_take(inventory: Inventory, take: TrinetTake) -> Inventory:
    """Keep one complete take as the atomic unit for unclassified ingest."""
    assert inventory.trinet is not None
    names = set(take.files)
    entries = tuple(item for item in inventory.entries if item.path in names)
    if len(entries) != len(names):
        raise IngestError(f"The inventory for {take.take_label} changed before upload.")
    digest = hashlib.sha256()
    digest.update(take.recording_uid.encode("ascii"))
    for item in entries:
        digest.update(item.path.encode("utf-8"))
        digest.update(str(item.size).encode("ascii"))
        digest.update(item.sample_sha256.encode("ascii"))
    summary = dataclasses.replace(inventory.trinet, device_ids=(take.device_id,), takes=(take,))
    return Inventory(entries, sum(item.size for item in entries), digest.hexdigest(), summary)


def take_video_duration_seconds(source: Path, take: TrinetTake) -> float | None:
    """Measure one camera's original video; stereo views count as one recording."""
    left = [name for name in take.files if name.lower() == f"{take.take_label.lower()}_l.mp4"]
    if len(left) != 1:
        return None
    try:
        seconds = ffprobe_duration_seconds(source / left[0])
    except IngestError as exc:
        print(f"DURATION UNKNOWN — {take.take_label}: {exc}", file=sys.stderr)
        return None
    if not math.isfinite(seconds) or seconds <= 0:
        print(f"DURATION UNKNOWN — {take.take_label}: ffprobe returned no positive duration.", file=sys.stderr)
        return None
    return seconds


def initial_metadata_for_take(take: TrinetTake, duration_seconds: float | None) -> dict[str, Any]:
    """Create the exact Oracle field set; unknown values stay blank until dashboard review."""
    return {
        "asset_id": f"recording-{take.device_id}-{take.recording_id}",
        "schema_version": "3.0.0",
        "capture_date": "",
        "recording_start_time": "",
        "recording_duration_s": duration_seconds or 0,
        "operator_id": "",
        "operator_age_range": "",
        "operator_gender": "",
        "operator_handedness": "",
        "task_environment": "",
        "task_subenvironment": "",
        "task_description": "",
        "environment_id": "",
        "environment_business_name": "",
        "environment_business_id": "",
        "environment_business_location": "",
        "environment_city": "",
        "environment_state_province": "",
        "environment_country_code": "",
        "environment_naics_primary_code": "",
        "collection_type": "exclusive",
        "vendor_company_name": "FPV Labs",
    }


def verify_take_source(source: Path, inventory: Inventory) -> None:
    for item in inventory.entries:
        path = source / item.path
        try:
            stat = path.stat()
            if (stat.st_size, stat.st_mtime_ns) != (item.size, item.mtime_ns):
                raise IngestError(f"Source changed during upload: {path}")
            if sampled_file_sha256(path, stat.st_size) != item.sample_sha256:
                raise IngestError(f"Source content changed during upload: {path}")
        except OSError as exc:
            raise IngestError(f"Could not verify source after upload: {path}: {exc}") from exc
    if inventory.trinet is not None:
        take = inventory.trinet.takes[0]
        refreshed = inspect_trinet_recording(source, inventory.entries)
        if refreshed is None or refreshed.takes[0].content_sha256 != take.content_sha256:
            raise IngestError(f"Recording content changed during upload: {take.take_label}")


def prepare_raw_destination(destination: str, inventory: Inventory) -> None:
    """Pin source identity before the first data write or a resumed transfer."""
    assert inventory.trinet is not None and len(inventory.trinet.takes) == 1
    take = inventory.trinet.takes[0]
    manifest_key = remote_join(destination, "_control", "source-manifest.json")
    expected = {
        "schema_version": 1,
        "recording_uid": take.recording_uid,
        "source_fingerprint": inventory.fingerprint,
        "content_sha256": take.content_sha256,
        "file_count": inventory.file_count,
        "total_bytes": inventory.total_bytes,
        "files": [
            {"path": item.path, "size": item.size, "sample_sha256": item.sample_sha256}
            for item in inventory.entries
        ],
    }
    current_size = rclone_size(destination)
    if current_size is None:
        raise IngestError(f"Could not inspect remote recording {destination}; nothing was uploaded.")
    existing = read_remote_json(manifest_key)
    if existing is not None:
        if existing != expected:
            raise IngestError(
                f"Recording {take.recording_uid} already exists with different source content. "
                "Nothing was uploaded; no remote files were changed."
            )
        return
    if current_size["count"] != 0:
        raise IngestError(
            f"Recording {take.recording_uid} contains remote objects without a valid source manifest. "
            "Nothing was uploaded; inspect the destination before retrying."
        )
    with tempfile.TemporaryDirectory(prefix="fpv-raw-manifest-") as temp_dir:
        document = Path(temp_dir) / "source-manifest.json"
        document.write_text(json.dumps(expected, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        ok, message = copy_small_file(document, manifest_key)
        if not ok or read_remote_json(manifest_key) != expected:
            raise IngestError(f"Could not pin source manifest for {take.take_label}: {message}")


def preview_source(source: Path, take: TrinetTake, eye: str) -> Path:
    matches = [source / name for name in take.files if name.casefold().endswith(f"_{eye.lower()}.mp4")]
    if len(matches) != 1:
        raise IngestError(f"Cannot identify the {eye} camera MP4 for {take.take_label}.")
    return matches[0]


def render_browser_preview(video: Path, output: Path) -> int:
    """Transcode the first five minutes to a browser-compatible, fast-start MP4."""
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        raise IngestError("Preview is pending: ffmpeg and ffprobe are required. Install ffmpeg and rerun upload-raw.")
    command = [
        "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
        "-i", str(video), "-t", "300", "-map", "0:v:0", "-map", "0:a:0?",
        "-sn", "-dn", "-vf", "fps=30,scale=w='trunc(min(1280,iw)/2)*2':h=-2",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "24",
        "-maxrate", "2200k", "-bufsize", "4400k", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", str(output),
    ]
    try:
        result = run_capture(command, timeout=60 * 60)
    except (OSError, subprocess.SubprocessError) as exc:
        raise IngestError(f"Preview is pending: could not transcode {video.name}: {exc}") from exc
    if result.returncode != 0 or not output.is_file() or output.stat().st_size == 0:
        raise IngestError(
            f"Preview is pending: ffmpeg could not transcode {video.name}: "
            f"{result.stderr.strip()[-500:] or 'no output was produced'}"
        )
    try:
        probe = run_capture([
            "ffprobe", "-v", "error", "-show_entries",
            "stream=codec_type,codec_name,pix_fmt:format=duration", "-of", "json", str(output),
        ], timeout=120)
        info = json.loads(probe.stdout) if probe.returncode == 0 else {}
        streams = info.get("streams", [])
        video_streams = [stream for stream in streams if stream.get("codec_type") == "video"]
        audio_streams = [stream for stream in streams if stream.get("codec_type") == "audio"]
        duration = float(info.get("format", {}).get("duration", "nan"))
        valid = (
            len(video_streams) == 1 and video_streams[0].get("codec_name") == "h264" and
            video_streams[0].get("pix_fmt") == "yuv420p" and
            all(stream.get("codec_name") == "aac" for stream in audio_streams) and
            0 < duration <= 301
        )
    except (OSError, subprocess.SubprocessError, ValueError, TypeError, json.JSONDecodeError) as exc:
        raise IngestError(f"Preview is pending: could not validate {video.name}: {exc}") from exc
    if not valid:
        raise IngestError(f"Preview is pending: {video.name} produced an invalid browser preview.")
    return output.stat().st_size


def preview_objects_match(destination: str, sizes: Any) -> bool:
    if not isinstance(sizes, dict):
        return False
    for eye in ("L",):
        expected = sizes.get(eye)
        if not isinstance(expected, int) or expected <= 0:
            return False
        actual = rclone_size(remote_join(destination, "_preview", f"{eye}.mp4"))
        if actual != {"count": 1, "bytes": expected}:
            return False
    return True


def upload_browser_previews(source: Path, take: TrinetTake, destination: str) -> dict[str, int]:
    sizes: dict[str, int] = {}
    with tempfile.TemporaryDirectory(prefix="fpv-preview-") as directory:
        for eye in ("L",):
            local = Path(directory) / f"{eye}.mp4"
            print(f"PREVIEW — {take.take_label} {eye}: transcoding first five minutes to H.264/AAC")
            sizes[eye] = render_browser_preview(preview_source(source, take, eye), local)
            remote = remote_join(destination, "_preview", f"{eye}.mp4")
            ok, message = copy_small_file(local, remote)
            if not ok or rclone_size(remote) != {"count": 1, "bytes": sizes[eye]}:
                raise IngestError(
                    f"Preview is pending: could not verify {take.take_label} {eye} in R2: {message}"
                )
    return sizes


def write_raw_markers(payload: dict[str, Any], marker: str, dashboard_marker: str, inventory: Inventory) -> None:
    take = inventory.trinet.takes[0]
    with tempfile.TemporaryDirectory(prefix="fpv-raw-control-") as temp_dir:
        document = Path(temp_dir) / "document.json"
        document.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        for target in (marker, dashboard_marker):
            ok, message = copy_small_file(document, target)
            if not ok or not marker_matches(target, inventory.fingerprint, take.recording_uid, inventory.file_count, inventory.total_bytes):
                raise IngestError(f"Could not verify completion record {target}: {message}")
            readback = read_remote_json(target)
            if (readback is None or readback.get("preview_status") != payload["preview_status"] or
                readback.get("preview_bytes") != payload["preview_bytes"]):
                raise IngestError(f"Could not verify completion record {target}: {message}")


class _LimitedFile:
    """Keep urllib's streaming request bounded to one multipart part."""
    def __init__(self, source: Any, length: int):
        self.source = source
        self.remaining = length

    def read(self, size: int = -1) -> bytes:
        if self.remaining <= 0:
            return b""
        data = self.source.read(min(size if size >= 0 else self.remaining, self.remaining))
        self.remaining -= len(data)
        return data


@contextmanager
def prevent_sleep_during_direct_upload() -> Iterable[None]:
    system = platform.system()
    guard: subprocess.Popen[bytes] | None = None
    windows_active = False
    try:
        if system == "Darwin" and shutil.which("caffeinate"):
            guard = subprocess.Popen(["caffeinate", "-dimsu", "-w", str(os.getpid())],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        elif system == "Linux" and shutil.which("systemd-inhibit"):
            guard = subprocess.Popen([
                "systemd-inhibit", "--what=sleep", "--mode=block",
                "--why=FPV recording upload", "sleep", "infinity",
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        elif system == "Windows":
            windows_active = set_windows_sleep_prevention(True)
        if (guard is not None and guard.poll() is not None) or (guard is None and not windows_active):
            raise IngestError("Sleep prevention is unavailable; run doctor before upload.")
        yield
    finally:
        if guard is not None:
            guard.terminate()
            try:
                guard.wait(timeout=10)
            except subprocess.TimeoutExpired:
                guard.kill()
                guard.wait(timeout=5)
        if windows_active:
            set_windows_sleep_prevention(False)


class DirectR2:
    """Scoped upload transport through the credential Worker's R2 binding."""
    def __init__(self, session: Mapping[str, Any], inventory: Inventory):
        broker_url = os.environ.get("FPV_UPLOAD_BROKER_URL", "").strip()
        broker_token = os.environ.get("FPV_UPLOAD_TOKEN", "").strip()
        if not broker_url or not broker_token:
            raise IngestError("FPV_UPLOAD_BROKER_URL and FPV_UPLOAD_TOKEN are required.")
        bucket, prefix = remote_bucket_and_key(session["destination"])
        if bucket != "fpv-sv-stereo" or inventory.trinet is None:
            raise IngestError("Direct upload requires a complete take in the stereo bucket.")
        claims = [{
            "recordingUid": take.recording_uid, "deviceId": take.device_id,
            "environmentId": session["environment_id"], "siteId": session["site_id"],
            "sessionId": session["id"], "legacyRecordingUid": take.legacy_recording_uid,
        } for take in inventory.trinet.takes]
        body = json.dumps({
            "sessionPrefix": prefix.rstrip("/") + "/",
            "dashboardKey": f"_dashboard/sessions/{session['id']}.json",
            "recordings": claims,
        }).encode("utf-8")
        endpoint = broker_url.rsplit("/", 1)[0] + "/direct-upload/session"
        request = urllib.request.Request(endpoint, data=body, method="POST", headers={
            "Authorization": f"Bearer {broker_token}", "Content-Type": "application/json",
            "User-Agent": f"{APP_NAME}/{SCHEMA_VERSION}",
        })
        try:
            with open_https(request, timeout=30) as response:
                result = json.load(response)
        except urllib.error.HTTPError as exc:
            raise IngestError(f"Direct upload session rejected ({exc.code}): {exc.read(2048).decode('utf-8', 'replace')}") from exc
        except (OSError, ValueError) as exc:
            raise IngestError(f"Could not create direct upload session: {exc}") from exc
        if result.get("bucket") != bucket or result.get("deleteAllowed") is not False or result.get("writeAllowed") is not True:
            raise IngestError("Direct upload session has an incorrect scope.")
        self.base = str(result["endpoint"]).rstrip("/")
        self.token = str(result["token"])
        self.expires_at = str(result["expiresAt"])
        if not self.base.startswith("https://") or not self.token:
            raise IngestError("Direct upload endpoint is invalid.")

    @classmethod
    def for_vendor(cls, destination: str, content_id: str) -> "DirectR2":
        broker_url = os.environ.get("FPV_UPLOAD_BROKER_URL", "").strip()
        broker_token = os.environ.get("FPV_UPLOAD_TOKEN", "").strip()
        if not broker_url or not broker_token:
            raise IngestError("FPV_UPLOAD_BROKER_URL and FPV_UPLOAD_TOKEN are required.")
        bucket, prefix = remote_bucket_and_key(destination)
        expected_bucket = os.environ.get("FPV_UPLOAD_BUCKET", "fpv-stereo-nepal").strip()
        if bucket != expected_bucket or not re.fullmatch(r"cid-[a-f0-9]{32}", content_id):
            raise IngestError("Vendor upload destination or content ID is invalid.")
        body = json.dumps({
            "sessionPrefix": prefix.rstrip("/") + "/",
            "dashboardKey": f"_uploads/{content_id}.json",
            "recordings": [],
        }).encode("utf-8")
        endpoint = broker_url.rsplit("/", 1)[0] + "/direct-upload/session"
        request = urllib.request.Request(endpoint, data=body, method="POST", headers={
            "Authorization": f"Bearer {broker_token}", "Content-Type": "application/json",
            "User-Agent": f"{APP_NAME}/{SCHEMA_VERSION}",
        })
        try:
            with open_https(request, timeout=30) as response:
                result = json.load(response)
        except urllib.error.HTTPError as exc:
            detail = exc.read(2048).decode("utf-8", "replace")
            raise IngestError(f"Vendor upload session rejected ({exc.code}): {detail}") from exc
        except (OSError, ValueError) as exc:
            raise IngestError(f"Could not create vendor upload session: {exc}") from exc
        if result.get("bucket") != bucket or result.get("deleteAllowed") is not False or result.get("writeAllowed") is not True:
            raise IngestError("Vendor upload session has an incorrect scope.")
        instance = cls.__new__(cls)
        instance.base = str(result["endpoint"]).rstrip("/")
        instance.token = str(result["token"])
        instance.expires_at = str(result["expiresAt"])
        if not instance.base.startswith("https://") or not instance.token:
            raise IngestError("Vendor upload endpoint is invalid.")
        return instance

    def _call(self, method: str, path: str, params: Mapping[str, str], data: Any = None,
              length: int | None = None, content_type: str | None = None,
              timeout: int = 600) -> tuple[int, bytes, Mapping[str, str]]:
        query = urllib.parse.urlencode(params)
        headers = {"Authorization": f"Bearer {self.token}", "User-Agent": f"{APP_NAME}/{SCHEMA_VERSION}"}
        if length is not None:
            headers["Content-Length"] = str(length)
        if content_type:
            headers["Content-Type"] = content_type
        for attempt in range(5):
            request = urllib.request.Request(f"{self.base}/{path}?{query}", data=data, method=method, headers=headers)
            try:
                with open_https(request, timeout=timeout) as response:
                    return response.status, response.read(1024 * 1024), response.headers
            except urllib.error.HTTPError as exc:
                if exc.code == 404 and method == "HEAD":
                    return 404, b"", {}
                detail = exc.read(2048).decode("utf-8", "replace")
                raise IngestError(f"Direct R2 {method} {path} failed ({exc.code}): {detail}") from exc
            except (urllib.error.URLError, OSError):
                # Retry dropped connections only when the body can be re-sent.
                if attempt == 4 or not (data is None or isinstance(data, bytes)):
                    raise
                time.sleep(2 ** attempt)
        raise AssertionError("unreachable")

    def head(self, key: str) -> int | None:
        status, _body, headers = self._call("HEAD", "object", {"key": key})
        return int(headers["Content-Length"]) if status == 200 else None

    def get_json(self, key: str) -> dict[str, Any] | None:
        if self.head(key) is None:
            return None
        _status, body, _headers = self._call("GET", "object", {"key": key})
        if len(body) > MAX_CATALOG_BYTES:
            raise IngestError(f"Remote control file is too large: {key}")
        value = json.loads(body)
        if not isinstance(value, dict):
            raise IngestError(f"Remote control file is invalid: {key}")
        return value

    def list(self, prefix: str) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        cursor = ""
        while True:
            params = {"prefix": prefix}
            if cursor:
                params["cursor"] = cursor
            _status, body, _headers = self._call("GET", "list", params)
            page = json.loads(body)
            found.extend(page["objects"])
            cursor = page.get("cursor") or ""
            if not cursor:
                return found

    def put_bytes(self, key: str, body: bytes, content_type: str = "application/json") -> None:
        self._call("PUT", "object", {"key": key}, body, len(body), content_type)
        if self.head(key) != len(body):
            raise IngestError(f"Direct R2 write verification failed: {key}")

    def put_file(self, key: str, source: Path, content_type: str = "application/octet-stream",
                 multipart_concurrency: int = 1) -> None:
        size = source.stat().st_size
        existing = self.head(key)
        if existing is not None:
            if existing != size:
                raise IngestError(f"Existing R2 object differs in size: {key}")
            return
        if size == 0:
            self._call("PUT", "object", {"key": key}, b"", 0, content_type)
        elif size <= 32 * 1024 * 1024:
            with source.open("rb") as stream:
                self._call("PUT", "object", {"key": key}, stream, size, content_type)
        else:
            _status, body, _headers = self._call("POST", "multipart", {
                "key": key, "action": "create"}, data=b"", length=0, content_type=content_type)
            upload_id = json.loads(body)["uploadId"]
            part_size = 32 * 1024 * 1024
            ranges = [(number, offset, min(part_size, size - offset))
                      for number, offset in enumerate(range(0, size, part_size), 1)]

            def upload_part(number: int, offset: int, length: int) -> dict[str, Any]:
                for attempt in range(5):
                    try:
                        with source.open("rb") as stream:
                            stream.seek(offset)
                            _status, response, _headers = self._call("PUT", "multipart", {
                                "key": key, "uploadId": upload_id, "partNumber": str(number)},
                                _LimitedFile(stream, length), length, timeout=900)
                        return json.loads(response)
                    except (IngestError, OSError) as exc:
                        if attempt == 4:
                            raise IngestError(f"Part {number} failed after retries: {exc}") from exc
                        time.sleep(min(2 ** attempt, 15))
                raise AssertionError("unreachable")

            try:
                parts_by_number: dict[int, dict[str, Any]] = {}
                completed_bytes = 0
                workers = max(1, min(int(multipart_concurrency), len(ranges)))
                with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="fpv-part") as executor:
                    futures = {
                        executor.submit(upload_part, number, offset, length): (number, length)
                        for number, offset, length in ranges
                    }
                    for future in as_completed(futures):
                        number, length = futures[future]
                        parts_by_number[number] = future.result()
                        completed_bytes += length
                        print(f"UPLOADING — {source.name}: {completed_bytes / size * 100:.1f}%", flush=True)
                parts = [parts_by_number[number] for number, _offset, _length in ranges]
                payload = json.dumps({"parts": parts}).encode("utf-8")
                self._call("POST", "multipart", {"key": key, "action": "complete", "uploadId": upload_id},
                           payload, len(payload), "application/json")
            except BaseException:
                try:
                    self._call("POST", "multipart", {
                        "key": key, "action": "abort", "uploadId": upload_id,
                    }, data=b"", length=0)
                except (IngestError, OSError):
                    pass
                raise
        if self.head(key) != size:
            raise IngestError(f"Direct R2 upload verification failed: {key}")


def upload_raw_direct_take(source: Path, take: TrinetTake, inventory: Inventory,
                           session: Mapping[str, Any], index: int, total: int,
                           profile: Mapping[str, Any] | None = None) -> None:
    """Upload one validated take through the v2 R2 binding, then publish markers."""
    client = DirectR2(session, inventory)
    duration_seconds = session["recording_duration_seconds"] if "recording_duration_seconds" in session else take_video_duration_seconds(source, take)
    _bucket, prefix = remote_bucket_and_key(session["destination"])
    prefix = prefix.rstrip("/") + "/"
    manifest_key = prefix + "_control/source-manifest.json"
    marker_key = prefix + "_control/_COMPLETE.json"
    metadata_key = prefix + "metadata.json"
    dashboard_key = f"_dashboard/sessions/{take.recording_uid}.json"
    expected_manifest = {
        "schema_version": 1, "recording_uid": take.recording_uid,
        "source_fingerprint": inventory.fingerprint,
        "content_sha256": take.content_sha256,
        "file_count": inventory.file_count, "total_bytes": inventory.total_bytes,
        "files": [{"path": item.path, "size": item.size, "sample_sha256": item.sample_sha256}
                  for item in inventory.entries],
    }
    existing_manifest = client.get_json(manifest_key)
    if existing_manifest is None:
        if client.list(prefix):
            raise IngestError(f"Recording {take.recording_uid} has objects without a source manifest; nothing was uploaded.")
        client.put_bytes(manifest_key, (json.dumps(expected_manifest, indent=2, sort_keys=True) + "\n").encode())
    elif existing_manifest != expected_manifest:
        raise IngestError(f"Recording {take.recording_uid} exists with different source content; nothing was uploaded.")

    def verified_data() -> bool:
        expected = {prefix + "data/" + item.path: item.size for item in inventory.entries}
        actual = {item["key"]: item["size"] for item in client.list(prefix + "data/")}
        if set(actual) - set(expected):
            raise IngestError(f"Unexpected data objects exist for {take.recording_uid}; inspect the destination.")
        if any(key in actual and actual[key] != size for key, size in expected.items()):
            raise IngestError(f"Existing data object size differs for {take.recording_uid}; inspect the destination.")
        return actual == expected

    marker = client.get_json(marker_key)
    dashboard = client.get_json(dashboard_key)
    if marker and dashboard and marker.get("source_fingerprint") == inventory.fingerprint and \
            dashboard.get("source_fingerprint") == inventory.fingerprint and \
            marker.get("preview_status") == "READY" and dashboard.get("preview_status") == "READY" and \
            verified_data() and all(
                isinstance(marker.get("preview_bytes", {}).get(eye), int) and
                client.head(prefix + f"_preview/{eye}.mp4") == marker["preview_bytes"][eye]
                for eye in ("L",)
            ):
        verify_take_source(source, inventory)
        print(f"DUPLICATE {take.take_label}: data and previews already verified; no data uploaded.")
        return

    direct_profile = profile or PROFILES["safe"]
    file_workers = max(1, min(int(direct_profile["transfers"]), len(inventory.entries)))
    multipart_workers = max(1, int(direct_profile["upload_concurrency"]))

    def upload_entry(item: FileEntry) -> None:
        key = prefix + "data/" + item.path
        print(f"UPLOADING — Take {index}/{total} — {item.path}", flush=True)
        client.put_file(key, source / item.path,
                        "video/mp4" if item.path.lower().endswith(".mp4") else "application/octet-stream",
                        multipart_workers)

    with ThreadPoolExecutor(max_workers=file_workers, thread_name_prefix="fpv-file") as executor:
        futures = [executor.submit(upload_entry, item) for item in inventory.entries]
        for future in as_completed(futures):
            future.result()
    verify_take_source(source, inventory)
    if not verified_data():
        raise IngestError(f"Cloud data verification failed for {take.take_label}; rerun to resume.")
    if client.get_json(metadata_key) is None:
        initial_metadata = initial_metadata_for_take(take, duration_seconds)
        client.put_bytes(metadata_key, (json.dumps(initial_metadata, indent=2) + "\n").encode())
    # A coordinator may edit the dashboard record while previews are pending.
    # Preserve their latest fields when this upload resumes; only refresh facts
    # about the source, verification, and preview state below.
    previous = {**(marker or {}), **(dashboard or {})}
    payload = {
        **previous,
        "schema_version": 3, "status": "VERIFIED", "session_id": take.recording_uid,
        "upload_session_id": inventory.trinet.recording_set_id,
        "upload_session_recording_count": session.get("upload_session_recording_count", len(inventory.trinet.takes)),
        "recording_uid": take.recording_uid, "source_folder_name": take.take_label,
        "recording_id": take.recording_id,
        "source_fingerprint": inventory.fingerprint, "file_count": inventory.file_count,
        "total_bytes": inventory.total_bytes,
        "data_destination": remote_join(session["destination"], "data"),
        "verified_at": (marker or {}).get("verified_at") or iso_now(),
        "recording_duration_seconds": duration_seconds or 0,
        "duration_method": "left-video-ffprobe" if duration_seconds is not None else "unknown",
        "metadata_status": previous.get("metadata_status", "PENDING"),
        "environment_id": previous.get("environment_id", ""),
        "site_id": previous.get("site_id", ""),
        "preview_status": "PENDING", "preview_bytes": {},
        "trinet": dataclasses.asdict(inventory.trinet),
    }

    def write_markers() -> None:
        document = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
        expected_readback = json.loads(document)
        for key in (marker_key, dashboard_key):
            client.put_bytes(key, document)
            value = client.get_json(key)
            if value != expected_readback:
                raise IngestError(f"Could not verify completion record {key}")

    write_markers()
    sizes: dict[str, int] = {}
    with tempfile.TemporaryDirectory(prefix="fpv-preview-") as directory:
        for eye in ("L",):
            key = prefix + f"_preview/{eye}.mp4"
            # ffmpeg output is not byte-reproducible, and the token cannot
            # overwrite, so keep a preview an interrupted run already uploaded.
            existing = client.head(key)
            if existing:
                print(f"PREVIEW — {take.take_label} {eye}: reusing uploaded preview")
                sizes[eye] = existing
                continue
            local = Path(directory) / f"{eye}.mp4"
            print(f"PREVIEW — {take.take_label} {eye}: transcoding first five minutes to H.264/AAC")
            sizes[eye] = render_browser_preview(preview_source(source, take, eye), local)
            client.put_file(key, local, "video/mp4")
    verify_take_source(source, inventory)
    payload["preview_status"] = "READY"
    payload["preview_bytes"] = sizes
    write_markers()
    print(f"VERIFIED {take.take_label}: {inventory.file_count} files, {human_bytes(inventory.total_bytes)}; preview ready")


def upload_raw(args: argparse.Namespace, config: dict[str, Any], store: StateStore) -> int:
    source = resolve_recording_source(Path(args.source).expanduser().resolve())
    with store.source_lock(source):
        inventory = scan_source(source, skip_invalid_takes=True)
        if inventory.trinet is None:
            raise IngestError("Raw upload requires a Trinet recording folder with complete takes.")
        covered = {name for take in inventory.trinet.takes for name in take.files}
        uncovered = [item.path for item in inventory.entries if item.path not in covered]
        # Top-level take files outside the accepted takes belong to skipped takes.
        skipped = sorted({match.group(1).lower() for path in uncovered
                          if (match := TRINET_FILE_RE.fullmatch(path))})
        extra = [path for path in uncovered if not TRINET_FILE_RE.fullmatch(path)]
        if extra:
            raise IngestError("Raw upload found files outside complete takes: " + ", ".join(extra[:10]) + ". Nothing was uploaded.")
        remote = args.remote or "r2:fpv-sv-stereo"
        bucket, _ = remote_bucket_and_key(remote_join(remote, "raw"))
        if bucket != "fpv-sv-stereo":
            raise IngestError("Raw uploads must target the fpv-sv-stereo bucket.")
        print(f"Devices: {', '.join(inventory.trinet.device_ids)}")
        print(f"Complete takes: {len(inventory.trinet.takes)}; source files: {inventory.file_count}")
        if skipped:
            print(f"Skipped takes (not uploaded): {', '.join(skipped)}")
        memory_limit = int(args.max_memory_mib or config.get("max_memory_mib", 1024))
        profile_name, direct_profile, profile_reasons = choose_direct_profile(
            inventory, args.profile or config.get("profile", "auto"), memory_limit
        )
        print(f"Upload profile: {profile_name} — {direct_profile['transfers']} parallel files × "
              f"{direct_profile['upload_concurrency']} multipart requests")
        for reason in profile_reasons:
            print(f"Profile note: {reason}")
        if not args.yes:
            answer = input("Upload these takes without business metadata now? [y/N] ").strip().lower()
            if answer not in {"y", "yes"}:
                print("Upload cancelled. Nothing was uploaded.")
                return 0
        durations = {} if args.dry_run else {
            take.recording_uid: take_video_duration_seconds(source, take)
            for take in inventory.trinet.takes
        }
        for index, take in enumerate(inventory.trinet.takes, 1):
            subset = inventory_for_take(inventory, take)
            destination = remote_join(remote, "raw", "recordings", take.device_id, take.recording_id)
            data_destination = remote_join(destination, "data")
            dashboard_destination = remote_join(remote, "_dashboard", "sessions")
            session = {
                "id": take.recording_uid,
                "upload_session_recording_count": len(inventory.trinet.takes),
                "recording_duration_seconds": durations.get(take.recording_uid),
                "destination": destination,
                "dashboard_destination": dashboard_destination,
                "environment_id": "",
                "site_id": "",
            }
            if args.dry_run:
                print(f"DRY RUN {index}/{len(inventory.trinet.takes)}: {take.take_label} -> {destination}")
                continue
            print(f"Take {index}/{len(inventory.trinet.takes)}: {take.take_label} -> {take.recording_uid}")
            if os.environ.get("FPV_UPLOAD_TRANSPORT", "direct").strip().lower() == "direct":
                with prevent_sleep_during_direct_upload():
                    upload_raw_direct_take(source, take, subset, session, index,
                                           len(inventory.trinet.takes), direct_profile)
                continue
            configure_session_credentials(session, subset)
            marker = remote_join(destination, "_control", "_COMPLETE.json")
            dashboard_marker = remote_join(dashboard_destination, f"{take.recording_uid}.json")
            prepare_raw_destination(destination, subset)
            complete = marker_matches(marker, subset.fingerprint, take.recording_uid, subset.file_count, subset.total_bytes) and marker_matches(
                dashboard_marker, subset.fingerprint, take.recording_uid, subset.file_count, subset.total_bytes
            )
            if complete:
                verify_take_source(source, subset)
                existing_check = [
                    "rclone", "check", str(source), data_destination,
                    "--one-way", "--size-only", "--fast-list", "--files-from-raw",
                ]
                with tempfile.TemporaryDirectory(prefix="fpv-raw-check-") as check_dir:
                    check_list = Path(check_dir) / "files.txt"
                    check_list.write_text("\n".join(take.files) + "\n", encoding="utf-8")
                    checked = run_capture([*existing_check, str(check_list)], timeout=REMOTE_OPERATION_TIMEOUT_SECONDS)
                if checked.returncode != 0 or rclone_size(data_destination) != {
                    "count": subset.file_count, "bytes": subset.total_bytes,
                }:
                    raise IngestError(
                        f"Existing completed recording {take.recording_uid} does not match the source. "
                        "Nothing was uploaded; inspect the remote copy before retrying."
                    )
                existing_marker = read_remote_json(marker) or {}
                dashboard_record = read_remote_json(dashboard_marker) or {}
                if (existing_marker.get("preview_status") == "READY" and
                    dashboard_record.get("preview_status") == "READY" and
                    existing_marker.get("preview_bytes") == dashboard_record.get("preview_bytes") and
                    preview_objects_match(destination, existing_marker.get("preview_bytes"))):
                    print(f"DUPLICATE {take.take_label}: all six files and browser preview already verified; no data uploaded.")
                    continue
                print(f"DUPLICATE DATA {take.take_label}: six files verified; preparing missing previews.")
            memory_limit = int(args.max_memory_mib or config.get("max_memory_mib", 1024))
            profile_name, profile, _reasons = choose_profile(subset, args.profile or "auto", memory_limit)
            file_list = store.reports_dir / f"raw-{take.recording_uid}-files.txt"
            file_list.parent.mkdir(parents=True, exist_ok=True)
            file_list.write_text("\n".join(take.files) + "\n", encoding="utf-8")
            filters = ["--files-from-raw", str(file_list)]
            command = rclone_copy_command(source, data_destination, profile, False, False)
            metadata_filters = rclone_card_metadata_filters()
            command = command[:-len(metadata_filters)] + filters + ["--immutable", "--size-only"]
            protected, sleep_active = wrap_sleep_prevention(command)
            windows_sleep = set_windows_sleep_prevention(True)
            if not (sleep_active or windows_sleep):
                raise IngestError("Sleep prevention is unavailable; run doctor before upload.")
            log_path = store.logs_dir / f"raw-{take.recording_uid}.log"
            try:
                if not complete:
                    result, tail, _elapsed = run_streaming(protected, log_path, (index, len(inventory.trinet.takes)))
                    if result != 0:
                        raise IngestError(f"Upload failed for {take.take_label}. Rerun the same source to resume. {tail[-1] if tail else ''}")
                    verify_take_source(source, subset)
                    check = ["rclone", "check", str(source), data_destination, "--one-way", "--size-only", "--fast-list", *filters]
                    checked, tail, _elapsed = run_streaming(check, log_path)
                    size = rclone_size(data_destination)
                    if checked != 0 or size != {"count": subset.file_count, "bytes": subset.total_bytes}:
                        raise IngestError(f"Cloud verification failed for {take.take_label}. Rerun to resume. {tail[-1] if tail else ''}")
                metadata_destination = remote_join(destination, "metadata.json")
                if read_remote_json(metadata_destination) is None:
                    with tempfile.TemporaryDirectory(prefix="fpv-metadata-") as metadata_dir:
                        document = Path(metadata_dir) / "metadata.json"
                        document.write_text(json.dumps(initial_metadata_for_take(take, durations[take.recording_uid]), indent=2) + "\n", encoding="utf-8")
                        ok, message = copy_small_file(document, metadata_destination)
                        if not ok or read_remote_json(metadata_destination) is None:
                            raise IngestError(f"Could not create metadata.json for {take.take_label}: {message}")
                payload = {
                    "schema_version": 3, "status": "VERIFIED", "session_id": take.recording_uid,
                    "upload_session_id": inventory.trinet.recording_set_id,
                    "upload_session_recording_count": len(inventory.trinet.takes),
                    "recording_uid": take.recording_uid, "source_folder_name": take.take_label,
                    "recording_id": take.recording_id,
                    "source_fingerprint": subset.fingerprint, "file_count": subset.file_count,
                    "total_bytes": subset.total_bytes, "data_destination": data_destination,
                    "verified_at": (existing_marker.get("verified_at") if complete else None) or iso_now(),
                    "recording_duration_seconds": durations[take.recording_uid] or 0,
                    "duration_method": "left-video-ffprobe" if durations[take.recording_uid] is not None else "unknown",
                    "metadata_status": "PENDING", "environment_id": "", "site_id": "",
                    "preview_status": "PENDING", "preview_bytes": {},
                    "trinet": dataclasses.asdict(subset.trinet),
                }
                write_raw_markers(payload, marker, dashboard_marker, subset)
                sizes = upload_browser_previews(source, take, destination)
                verify_take_source(source, subset)
                payload["preview_status"] = "READY"
                payload["preview_bytes"] = sizes
                write_raw_markers(payload, marker, dashboard_marker, subset)
                print(f"VERIFIED {take.take_label}: {subset.file_count} files, {human_bytes(subset.total_bytes)}; preview ready")
            finally:
                set_windows_sleep_prevention(False)
        if not args.dry_run:
            print("DATA UPLOAD DONE — all takes verified. Business metadata can be assigned in the dashboard.")
        return 0


def natural_name_key(path: Path) -> tuple[tuple[int, object], ...]:
    return tuple(
        (0, int(part)) if part.isdigit() else (1, part.casefold())
        for part in re.split(r"(\d+)", path.name)
    )


def resolve_batch_sources(parent: str | None, sources: Iterable[str]) -> list[Path]:
    supplied = [resolve_recording_source(Path(value)) for value in sources]
    if parent and supplied:
        raise IngestError("Use either --parent or individual session folders, not both.")
    if parent:
        parent_path = Path(parent).expanduser().resolve()
        if not parent_path.is_dir():
            raise IngestError(f"Batch parent is not a readable directory: {parent_path}")
        detected = resolve_recording_source(parent_path)
        if detected != parent_path:
            return [detected]
        try:
            covered_entries = [
                item for item in parent_path.iterdir()
                if not is_ignored_card_path(Path(item.name))
            ]
            loose_files = [item.name for item in covered_entries if not item.is_dir()]
            uncovered_folders = [
                item.name for item in covered_entries
                if item.is_dir() and not folder_contains_video(item)
            ]
            if loose_files or uncovered_folders:
                details: list[str] = []
                if loose_files:
                    details.append("loose files: " + ", ".join(sorted(loose_files)))
                if uncovered_folders:
                    details.append("folders without supported video: " + ", ".join(sorted(uncovered_folders)))
                raise IngestError(
                    "The parent is not fully covered by the batch (" + "; ".join(details) + "). "
                    "Nothing was uploaded. Organize all recording data into video-containing session "
                    "folders before using --parent."
                )
            supplied = sorted(
                (
                    item for item in covered_entries
                    if item.is_dir()
                ),
                key=natural_name_key,
            )
        except OSError as exc:
            raise IngestError(f"Could not list batch parent {parent_path}: {exc}") from exc
        if not supplied:
            raise IngestError(
                f"No immediate child folders containing video were found in {parent_path}."
            )
    elif not supplied:
        raise IngestError("Provide --parent or at least one individual session folder.")

    unique: list[Path] = []
    seen: set[Path] = set()
    for source in supplied:
        if source in seen:
            continue
        if not source.is_dir():
            raise IngestError(f"Batch session is not a readable directory: {source}")
        if not folder_contains_video(source):
            raise IngestError(f"Batch session contains no supported video files: {source}")
        seen.add(source)
        unique.append(source)
    return unique


def upload_batch(args: argparse.Namespace, config: dict[str, Any], store: StateStore) -> int:
    sources = resolve_batch_sources(args.parent, args.sources)
    assignment_id = str(getattr(args, "assignment", "") or "")
    if not assignment_id:
        raise IngestError("An Assignment ID is required. Create one in the dashboard first.")
    assignment = fetch_assignment(assignment_id)
    args.environment = assignment["l2_target_id"]
    args.site = assignment["environment_id"]
    catalog_url = args.catalog_url or config.get("catalog_url")
    catalog, metadata = fetch_environment_catalog(catalog_url)
    environment, site = validate_classification(
        str(args.environment), args.site, catalog, metadata["sites"]
    )

    if not confirm_assignment(assignment, yes=args.yes, sources=sources):
        print("Batch cancelled. Nothing was uploaded.")
        return 0

    for index, source in enumerate(sources, 1):
        print(f"\n=== Folder {index}/{len(sources)}: {source.name} ===")
        session_args = argparse.Namespace(**vars(args))
        session_args.source = str(source)
        session_args.label = None
        session_args.new_session = False
        session_args.batch_index = index
        session_args.batch_total = len(sources)
        session_args.assignment_payload = assignment
        try:
            result = upload(session_args, config, store)
        except IngestError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            result = 1
        if result != 0:
            print(
                f"Batch stopped at folder {index}: {source}\n"
                "Rerun the same batch command to resume; verified folders will be skipped.",
                file=sys.stderr,
            )
            return result

    if args.dry_run:
        print("\nDATA UPLOAD NOT DONE")
        print("SAFE TO DELETE: NO — this was only a preview; no upload was verified.")
        return 0

    clear_scope = str(Path(args.parent).expanduser().resolve()) if args.parent else "the listed folders"
    dashboard = catalog_url.removesuffix("/api/catalog") if catalog_url else ""
    print("\n============================================================")
    print(f"ALL {len(sources)} FOLDERS VERIFIED IN CLOUD STORAGE")
    print("DATA UPLOAD DONE")
    print(f"SAFE TO DELETE: YES — {clear_scope}")
    print("============================================================")
    if dashboard:
        print(f"Dashboard: {dashboard}")
    return 0


def inspect_source(args: argparse.Namespace, config: dict[str, Any]) -> int:
    source = resolve_recording_source(Path(args.source).expanduser().resolve())
    inventory = scan_source(source)
    memory_limit = int(args.max_memory_mib or config.get("max_memory_mib", 1024))
    memory_limit = min(memory_limit, max(256, int(physical_memory_mib() * 0.15)))
    requested = args.profile or config.get("profile", "auto")
    name, profile, reasons = choose_profile(inventory, requested, memory_limit)
    duration_policy = args.duration_policy or config.get("duration_policy", "grouped-views")
    duration = calculate_duration(source, inventory, duration_policy, args.duration_hours)
    print(f"Source: {source}")
    print(f"Files: {inventory.file_count}")
    print(f"Total: {human_bytes(inventory.total_bytes)}")
    print(f"Largest: {human_bytes(inventory.largest_file)}")
    print(f"Fingerprint: {inventory.fingerprint}")
    print(
        f"Accounted recording duration: {duration.accounted_seconds / 3600:.3f} hours "
        f"({duration.method})"
    )
    if duration.raw_video_seconds is not None:
        print(f"Raw sum of all video files: {duration.raw_video_seconds / 3600:.3f} hours")
        print(f"Logical clip groups: {duration.group_count}")
    print(f"Recommended profile: {name}")
    print(json.dumps(profile, indent=2, sort_keys=True))
    print(f"Estimated multipart memory: {profile_memory_mib(profile)} MiB")
    for reason in reasons:
        print(f"Reason: {reason}")
    return 0


def doctor(args: argparse.Namespace, config: dict[str, Any]) -> int:
    checks: list[tuple[str, bool, str]] = []
    rclone_path = shutil.which("rclone")
    rclone_version = installed_rclone_version()
    checks.append(
        (
            "rclone 1.59 or newer",
            bool(rclone_path and rclone_version and rclone_version >= (1, 59, 0)),
            (
                f"{'.'.join(str(part) for part in rclone_version)} at {rclone_path}"
                if rclone_path and rclone_version
                else rclone_path or "not found"
            ),
        )
    )
    ffprobe_path = shutil.which("ffprobe")
    checks.append(("ffprobe installed", bool(ffprobe_path), ffprobe_path or "not found"))
    sleep_ok, sleep_detail = check_sleep_prevention()
    checks.append(
        (
            "sleep prevention",
            sleep_ok,
            sleep_detail,
        )
    )
    remote = args.remote or config.get("remote")
    broker_url = os.environ.get("FPV_UPLOAD_BROKER_URL", "").strip()
    broker_token = os.environ.get("FPV_UPLOAD_TOKEN", "").strip()
    if broker_url and broker_token:
        health_url = urllib.parse.urljoin(broker_url, "/health")
        try:
            health_request = urllib.request.Request(
                health_url,
                headers={"Accept": "application/json", "User-Agent": f"{APP_NAME}/{SCHEMA_VERSION}"},
            )
            with open_https(health_request, timeout=15) as response:
                broker_health = json.loads(response.read(MAX_CATALOG_BYTES).decode("utf-8"))
            broker_ok = broker_health.get("status") == "ok" and broker_health.get("deleteAllowed") is False
            detail = (
                f"reachable; deletion denied; credential TTL "
                f"{int(broker_health.get('credentialTtlSeconds', 0)) // 3600} hours"
            ) if broker_ok else "unsafe or invalid health response"
        except (urllib.error.URLError, TimeoutError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            broker_ok, detail = False, str(exc)
        checks.append(("temporary credential service", broker_ok, detail))
        session_access_ok = False
        session_access_detail = "credential service health check failed"
        if broker_ok and remote and rclone_path:
            check_time = now_local()
            check_id = f"doctor-{check_time.strftime('%Y%m%d-%H%M%S')}-{secrets.token_hex(4)}"
            check_destination = build_destination(
                remote,
                "raw/doctor/doctor-site",
                check_time,
                check_id,
            )
            check_session = {
                "id": check_id,
                "destination": check_destination,
                "dashboard_destination": remote_join(
                    remote, config.get("dashboard_index_prefix", "_dashboard/sessions")
                ),
            }
            try:
                configure_session_credentials(check_session)
                remote_inventory = rclone_size(
                    remote_join(check_destination, "data"), timeout=120
                )
                session_access_ok = remote_inventory == {"count": 0, "bytes": 0}
                session_access_detail = (
                    "authenticated token minted scoped credentials and R2 accepted them"
                    if session_access_ok
                    else "R2 did not accept the scoped credential check"
                )
            except (IngestError, subprocess.SubprocessError) as exc:
                session_access_detail = str(exc)
        elif not remote:
            session_access_detail = "set config.remote"
        elif not rclone_path:
            session_access_detail = "rclone is not installed"
        checks.append(("R2 session access", session_access_ok, session_access_detail))
    else:
        checks.append(
            (
                "temporary credential service",
                False,
                f"paste FPV_UPLOAD_TOKEN into {UPLOAD_ENV_PATH}",
            )
        )
        checks.append(("R2 session access", False, "temporary credentials are required"))
    catalog_url = args.catalog_url or config.get("catalog_url")
    try:
        catalog, metadata = fetch_environment_catalog(catalog_url)
        site_total = len(metadata["sites"])
        checks.append(
            (
                "shared catalog reachable",
                True,
                f"revision {metadata['revision']}; {len(catalog)} environments; {site_total} sites",
            )
        )
    except IngestError as exc:
        checks.append(("shared catalog reachable", False, str(exc)))
    for label, passed, detail in checks:
        print(f"{'PASS' if passed else 'FAIL'}  {label}: {detail}")
    return 0 if all(item[1] for item in checks) else 1


def verify_vendor_source(session: VendorSession) -> None:
    for item in session.entries:
        path = session.source / item.path
        try:
            current = path.stat()
        except OSError as exc:
            raise IngestError(f"Source file disappeared during upload: {path}") from exc
        if (current.st_size, current.st_mtime_ns) != (item.size, item.mtime_ns):
            raise IngestError(f"Source file changed during upload: {path}")


def vendor_video_duration(path: Path) -> float | None:
    """Read MP4/MOV movie headers by seeking over video payloads."""
    try:
        if path.suffix.lower() in {".mp4", ".mov", ".m4v", ".3gp", ".3g2"}:
            with path.open("rb") as stream:
                end = path.stat().st_size
                stack = [(0, end)]
                visited = 0
                while stack and visited < 10000:
                    position, limit = stack.pop()
                    while position + 8 <= limit and visited < 10000:
                        visited += 1
                        stream.seek(position)
                        header = stream.read(8)
                        if len(header) != 8:
                            break
                        size, kind = int.from_bytes(header[:4], "big"), header[4:]
                        width = 8
                        if size == 1:
                            extra = stream.read(8)
                            if len(extra) != 8:
                                break
                            size, width = int.from_bytes(extra, "big"), 16
                        elif size == 0:
                            size = limit - position
                        if size < width or position + size > limit:
                            break
                        if kind == b"moov":
                            stack.append((position + width, position + size))
                        elif kind == b"mvhd":
                            data = stream.read(min(32, size - width))
                            if len(data) >= 20 and data[0] == 0:
                                scale = int.from_bytes(data[12:16], "big")
                                duration = int.from_bytes(data[16:20], "big")
                                unknown = 2**32 - 1
                            elif len(data) >= 32 and data[0] == 1:
                                scale = int.from_bytes(data[20:24], "big")
                                duration = int.from_bytes(data[24:32], "big")
                                unknown = 2**64 - 1
                            else:
                                return None
                            return duration / scale if scale and 0 < duration < unknown else None
                        position += size
        if shutil.which("ffprobe"):
            value = ffprobe_duration_seconds(path)
            return value if value > 0 else None
    except (OSError, IngestError, ValueError):
        pass
    return None


def vendor_duration_summary(session: VendorSession) -> dict[str, Any]:
    videos = [item for item in session.entries if Path(item.path).suffix.lower() in VIDEO_EXTENSIONS]
    seconds = 0.0
    measured = 0
    for item in videos:
        value = vendor_video_duration(session.source / item.path)
        if value is not None:
            seconds += value
            measured += 1
    return {"video_seconds": seconds, "video_count": len(videos),
            "measured_video_count": measured, "unknown_video_count": len(videos) - measured,
            "duration_method": "sum-all-camera-files"}


def vendor_destination(session: VendorSession, remote: str) -> str:
    return remote_join(remote, "raw", session.content_id)


def upload_vendor_session(
    session: VendorSession,
    remote: str,
    profile: Mapping[str, Any],
    classification: Mapping[str, str],
    index: int,
    total: int,
) -> None:
    destination = vendor_destination(session, remote)
    _bucket, prefix_value = remote_bucket_and_key(destination)
    prefix = prefix_value.rstrip("/") + "/"
    manifest_key = prefix + "_control/source-manifest.json"
    marker_key = prefix + "_control/_COMPLETE.json"
    metadata_key = prefix + "metadata.json"
    registry_key = f"_uploads/{session.content_id}.json"
    client = DirectR2.for_vendor(destination, session.content_id)
    status_key = prefix + "_control/upload-status.json"
    existing_status = client.get_json(status_key) or {}
    started_at = existing_status.get("started_at") or iso_now()
    progress: dict[str, Any] = {
        "schema_version": 1, "content_id": session.content_id,
        "source_folder": session.source_name, "card_label": session.source_name,
        "file_count": len(session.entries), "total_bytes": session.total_bytes,
        "uploaded_files": 0, "uploaded_bytes": 0, "started_at": started_at,
        "video_seconds": 0, "video_count": sum(Path(x.path).suffix.lower() in VIDEO_EXTENSIONS for x in session.entries),
        "measured_video_count": 0,
    }
    def publish_progress(state: str) -> None:
        progress.update(status=state, updated_at=iso_now())
        client.put_bytes(status_key, json.dumps(progress, ensure_ascii=False).encode("utf-8"))
    manifest = {
        "schema_version": 1,
        "vendor": "nepal",
        "source_folder": session.source_name,
        "content_id": session.content_id,
        "classification": dict(classification),
        "identity_method": ("sha256-v1-nonvideo-full-content" if session.identity_files else "sha256-v2-video-samples"),
        "identity_excludes_extensions": sorted(VIDEO_EXTENSIONS),
        "identity_files": list(session.identity_files),
        "file_count": len(session.entries),
        "total_bytes": session.total_bytes,
        "files": [{"path": item.path, "size": item.size,
                   **({"sample_sha256": item.sample_sha256} if item.sample_sha256 else {})}
                  for item in session.entries],
    }
    existing_registry = client.get_json(registry_key)
    if existing_registry is not None and existing_registry.get("session_prefix") != prefix:
        raise IngestError(
            f"Duplicate blocked: {session.source_name} has content ID {session.content_id}, already "
            f"stored at {existing_registry.get('session_prefix', 'another session')}."
        )
    if existing_registry is not None and existing_registry.get("classification") != dict(classification):
        raise IngestError(
            f"Existing L1/L2/L3 metadata differs for {session.source_name}; nothing was overwritten."
        )
    if existing_registry is None:
        registry_claim = {
            "schema_version": 1,
            "status": "CLAIMED",
            "vendor": "nepal",
            "source_folder": session.source_name,
            "content_id": session.content_id,
            "classification": dict(classification),
            "identity_method": ("sha256-v1-nonvideo-full-content" if session.identity_files else "sha256-v2-video-samples"),
            "session_prefix": prefix,
            "manifest_key": manifest_key,
            "completion_key": marker_key,
            "claimed_at": iso_now(),
        }
        client.put_bytes(
            registry_key,
            json.dumps(registry_claim, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        )
        existing_registry = registry_claim
    if not existing_status.get("started_at"):
        started_at = existing_registry.get("claimed_at") or started_at
        progress["started_at"] = started_at
    existing_manifest = client.get_json(manifest_key)
    if existing_manifest is None:
        if client.list(prefix):
            raise IngestError(
                f"Destination has objects but no source manifest: {prefix}. Nothing new was uploaded."
            )
        client.put_bytes(
            manifest_key,
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        )
    elif existing_manifest != manifest:
        raise IngestError(f"Existing manifest does not match {session.source_name}; nothing was overwritten.")

    metadata = {
        "l1": classification["l1"],
        "l2": classification["l2"],
        "l3": classification["l3"],
    }
    existing_metadata = client.get_json(metadata_key)
    if existing_metadata is None:
        client.put_bytes(
            metadata_key,
            json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        )
    elif existing_metadata != metadata:
        raise IngestError(
            f"Existing L1/L2/L3 metadata differs for {session.source_name}; nothing was overwritten."
        )

    expected_data = {
        prefix + "data/" + session.source_name + "/" + item.path: item.size
        for item in session.entries
    }
    completed = client.get_json(marker_key)
    if completed is not None and completed.get("content_id") == session.content_id:
        remote_objects = {item["key"]: int(item["size"]) for item in client.list(prefix + "data/")}
        if remote_objects != expected_data:
            raise IngestError(f"Completed remote data differs from {session.source_name}; inspect before retrying.")
        verify_vendor_source(session)
        if existing_status.get("status") != "VERIFIED" or "duration_method" not in existing_status:
            progress.update(vendor_duration_summary(session), uploaded_files=len(session.entries),
                            uploaded_bytes=session.total_bytes, finished_at=completed.get("verified_at"))
            publish_progress("VERIFIED")
        print(f"DUPLICATE {index}/{total}: {session.source_name} already verified as {session.content_id}")
        return

    progress["unknown_video_count"] = progress["video_count"]
    publish_progress("UPLOADING")
    transfers = max(1, int(profile.get("transfers", 4)))
    multipart = max(1, int(profile.get("upload_concurrency", 6)))
    print(
        f"UPLOAD {index}/{total}: {session.source_name} -> {prefix} "
        f"({len(session.entries)} files, {human_bytes(session.total_bytes)})"
    )

    def upload_one(item: FileEntry) -> str:
        source_path = session.source / item.path
        current = source_path.stat()
        if (current.st_size, current.st_mtime_ns) != (item.size, item.mtime_ns):
            raise IngestError(f"Source file changed before upload: {source_path}")
        content_type = mimetypes.guess_type(item.path)[0] or "application/octet-stream"
        client.put_file(
            prefix + "data/" + session.source_name + "/" + item.path,
            source_path,
            content_type,
            multipart,
        )
        return item.path

    try:
        last_update = time.monotonic()
        with ThreadPoolExecutor(max_workers=transfers, thread_name_prefix="vendor-file") as executor:
            futures = {executor.submit(upload_one, item): item for item in session.entries}
            for future in as_completed(futures):
                name = future.result()
                progress["uploaded_files"] += 1
                progress["uploaded_bytes"] += futures[future].size
                if time.monotonic() - last_update >= 3:
                    publish_progress("UPLOADING")
                    last_update = time.monotonic()
                print(f"UPLOADED — {session.source_name}/{name}", flush=True)
        publish_progress("VERIFYING")
        progress.update(vendor_duration_summary(session))
    except BaseException:
        try:
            publish_progress("INTERRUPTED")
        except (IngestError, OSError):
            pass
        raise

    verify_vendor_source(session)
    remote_objects = {item["key"]: int(item["size"]) for item in client.list(prefix + "data/")}
    if remote_objects != expected_data:
        missing = sorted(set(expected_data) - set(remote_objects))
        unexpected = sorted(set(remote_objects) - set(expected_data))
        raise IngestError(
            f"Remote verification failed for {session.source_name}; "
            f"missing={missing[:3]}, unexpected={unexpected[:3]}."
        )
    completed_at = iso_now()
    progress.update(finished_at=completed_at)
    marker = {
        "schema_version": 1,
        "status": "VERIFIED",
        "vendor": "nepal",
        "source_folder": session.source_name,
        "content_id": session.content_id,
        "classification": dict(classification),
        "identity_method": ("sha256-v1-nonvideo-full-content" if session.identity_files else "sha256-v2-video-samples"),
        "file_count": len(session.entries),
        "total_bytes": session.total_bytes,
        "verified_at": completed_at,
        "started_at": started_at,
        **{key: progress[key] for key in ("video_seconds", "video_count", "measured_video_count",
                                        "unknown_video_count", "duration_method")},
    }
    marker_bytes = json.dumps(marker, sort_keys=True, separators=(",", ":")).encode("utf-8")
    client.put_bytes(marker_key, marker_bytes)
    publish_progress("VERIFIED")
    print("Upload receipt: https://fpvnepal.satpal-341.workers.dev/")
    print(f"VERIFIED {session.source_name}: {session.content_id}")


def upload_vendor(args: argparse.Namespace, config: dict[str, Any], store: StateStore) -> int:
    source = Path(args.source).expanduser().resolve()
    with store.source_lock(source):
        sessions = discover_vendor_sessions(source)
        remote = args.remote or "r2:fpv-stereo-nepal"
        bucket, _key = remote_bucket_and_key(remote_join(remote, "raw"))
        if bucket != "fpv-stereo-nepal":
            raise IngestError("Vendor uploads must target the fpv-stereo-nepal bucket.")
        total_bytes = sum(session.total_bytes for session in sessions)
        print(f"Vendor sessions: {len(sessions)}; source data: {human_bytes(total_bytes)}")
        print("Identity: non-video content hashes; video-only folders use small video samples.")
        profile_name = args.profile if args.profile != "auto" else "fast"
        profile = PROFILES[profile_name]
        classification: dict[str, str] = {}
        for field in ("l1", "l2", "l3"):
            value = str(getattr(args, field, "") or "").strip()
            if len(value) > 200 or any(ord(character) < 32 for character in value):
                raise IngestError(f"{field.upper()} must be plain text of at most 200 characters.")
            classification[field] = value
        print(
            f"Upload profile: {profile_name} — {profile['transfers']} parallel files × "
            f"{profile['upload_concurrency']} multipart requests"
        )
        print(
            "Classification (free text): "
            f"L1={classification['l1'] or '(blank)'}; "
            f"L2={classification['l2'] or '(blank)'}; "
            f"L3={classification['l3'] or '(blank)'}"
        )
        for session in sessions:
            print(f"  {session.source_name}: {session.content_id} -> {vendor_destination(session, remote)}")
        if args.dry_run:
            return 0
        if not args.yes:
            answer = input("Upload these vendor sessions? [y/N] ").strip().lower()
            if answer not in {"y", "yes"}:
                print("Upload cancelled. Nothing was uploaded.")
                return 0
        with prevent_sleep_during_direct_upload():
            for index, session in enumerate(sessions, 1):
                upload_vendor_session(
                    session, remote, profile, classification, index, len(sessions)
                )
    return 0


def list_sessions(store: StateStore) -> int:
    rows = store.list_sessions()
    if not rows:
        print("No sessions recorded.")
        return 0
    print(f"{'STATUS':20} {'ENVIRONMENT':22} {'SITE':8} {'HOURS':8} {'CREATED':25} {'SESSION'}")
    for row in rows:
        hours = float(row["duration_seconds"] or 0) / 3600
        print(
            f"{row['status'][:20]:20} {(row['environment_id'] or '-')[:22]:22} "
            f"{(row['site_id'] or '-')[:8]:8} {hours:8.3f} "
            f"{row['created_at'][:25]:25} {row['id']}"
        )
    return 0


def list_environments(args: argparse.Namespace, config: dict[str, Any]) -> int:
    catalog, metadata = fetch_environment_catalog(
        args.catalog_url or config.get("catalog_url")
    )
    print(f"Shared catalog revision {metadata['revision']} ({metadata['updated_at']})")
    print(f"{'ID':24} {'ENVIRONMENT':24} {'HOURS':8} {'TARGET SITES':12}")
    for environment in catalog.values():
        print(
            f"{environment['id'][:24]:24} {environment['name'][:24]:24} "
            f"{environment['allowed_hours']:8.1f} {environment['target_site_count']:12d}"
        )
    print("\nGlobal sites (choose any one independently during upload):")
    for site in metadata["sites"].values():
        print(f"  {site['id']}: {site['name']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    launcher = "fpv-upload.cmd" if platform.system() == "Windows" else "./fpv-upload"
    parser = argparse.ArgumentParser(
        prog=APP_NAME,
        description="Reliably upload FPV recording folders.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""Common commands:
  {launcher} doctor
  {launcher} upload FOLDER --assignment ASSIGNMENT_ID
  {launcher} upload-batch --parent FOLDER --assignment ASSIGNMENT_ID
  {launcher} upload-vendor FOLDER
  {launcher} list

Run any command with --help for its detailed options.""",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Advanced: alternate JSON configuration file (default: config.json beside uploader)",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=DEFAULT_STATE_DIR,
        help="Advanced: durable local state directory (an OS-specific internal folder by default)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    upload_parser = subparsers.add_parser("upload", help="Upload or resume one recording session")
    upload_parser.add_argument("source", help="Recording session directory")
    upload_parser.add_argument("--remote", help="rclone destination, e.g. r2:recordings")
    upload_parser.add_argument(
        "--catalog-url", help="Dashboard /api/catalog URL (required unless set in config)"
    )
    upload_parser.add_argument("--prefix", help="Object prefix below the bucket (default: raw)")
    upload_parser.add_argument("--label", help="Human label used in the session ID")
    upload_parser.add_argument(
        "--assignment", required=True,
        help="Assignment ID generated by Assignment setup in the dashboard",
    )
    upload_parser.add_argument(
        "--duration-policy",
        choices=["grouped-views", "sum-all", "longest-file"],
        help="How automatic recording hours are counted",
    )
    upload_parser.add_argument(
        "--duration-hours",
        type=float,
        help="Authoritative manual recording duration; bypasses ffprobe accounting",
    )
    upload_parser.add_argument(
        "--profile", choices=["auto", *PROFILES], help="Performance profile"
    )
    upload_parser.add_argument(
        "--max-memory-mib", type=int, help="Maximum estimated multipart buffer memory"
    )
    upload_parser.add_argument(
        "--verify",
        choices=["standard", "enhanced", "download"],
        default="standard",
        help="Post-upload verification level",
    )
    upload_parser.add_argument(
        "--force-reupload",
        action="store_true",
        help="Upload every file again and allow R2 objects to be overwritten",
    )
    upload_parser.add_argument(
        "--new-session",
        action="store_true",
        help="Create a new destination even if this source inventory was seen before",
    )
    upload_parser.add_argument(
        "--yes",
        action="store_true",
        help="Accept the printed assignment details for unattended use",
    )
    upload_parser.add_argument("--dry-run", action="store_true", help=argparse.SUPPRESS)

    raw_parser = subparsers.add_parser("upload-raw", help="Upload complete Trinet takes before assigning business metadata")
    raw_parser.add_argument("source", help="SD card or Trinet recording directory")
    raw_parser.add_argument("--remote", help="R2 destination (default: r2:fpv-sv-stereo)")
    raw_parser.add_argument("--profile", choices=["auto", *PROFILES], default="auto")
    raw_parser.add_argument("--max-memory-mib", type=int)
    raw_parser.add_argument("--yes", action="store_true", help="Confirm the displayed takes without a prompt")
    raw_parser.add_argument("--dry-run", action="store_true", help="Show stable destinations without uploading")

    vendor_parser = subparsers.add_parser(
        "upload-vendor",
        help="Upload Nepal vendor session folders without dashboard metadata",
    )
    vendor_parser.add_argument(
        "source", help="One data folder or a parent containing session folders"
    )
    vendor_parser.add_argument(
        "--remote", help="R2 destination (fixed to r2:fpv-stereo-nepal)"
    )
    vendor_parser.add_argument("--profile", choices=["auto", *PROFILES], default="auto")
    vendor_parser.add_argument("--l1", default="", help="Optional free-text L1 label")
    vendor_parser.add_argument("--l2", default="", help="Optional free-text L2 label")
    vendor_parser.add_argument("--l3", default="", help="Optional free-text L3 label")
    vendor_parser.add_argument("--yes", action="store_true", help="Upload without a prompt")
    vendor_parser.add_argument("--dry-run", action="store_true", help="Calculate IDs and show destinations")

    batch_parser = subparsers.add_parser(
        "upload-batch",
        help="Confirm and upload multiple recording-session folders with one classification",
    )
    batch_parser.add_argument(
        "sources",
        nargs="*",
        help="Individual recording-session directories (omit when using --parent)",
    )
    batch_parser.add_argument(
        "--parent",
        help="Directory whose immediate video-containing child folders are sessions",
    )
    batch_parser.add_argument("--remote", help="rclone destination, e.g. r2:recordings")
    batch_parser.add_argument(
        "--catalog-url", help="Dashboard /api/catalog URL (required unless set in config)"
    )
    batch_parser.add_argument("--prefix", help="Object prefix below the bucket (default: raw)")
    batch_parser.add_argument(
        "--assignment", required=True,
        help="Assignment ID generated by Assignment setup in the dashboard",
    )
    batch_parser.add_argument(
        "--duration-policy",
        choices=["grouped-views", "sum-all", "longest-file"],
        help="How automatic recording hours are counted for every folder",
    )
    batch_parser.add_argument(
        "--duration-hours",
        type=float,
        help="Manual recording duration per folder; normally omit for automatic accounting",
    )
    batch_parser.add_argument(
        "--profile", choices=["auto", *PROFILES], help="Performance profile"
    )
    batch_parser.add_argument(
        "--max-memory-mib", type=int, help="Maximum estimated multipart buffer memory"
    )
    batch_parser.add_argument(
        "--verify",
        choices=["standard", "enhanced", "download"],
        default="standard",
        help="Post-upload verification level",
    )
    batch_parser.add_argument(
        "--force-reupload",
        action="store_true",
        help="Upload every file again and allow R2 objects to be overwritten",
    )
    batch_parser.add_argument("--dry-run", action="store_true", help=argparse.SUPPRESS)
    batch_parser.add_argument(
        "--yes",
        action="store_true",
        help="Accept the printed batch assignment without an interactive prompt",
    )
    batch_parser.set_defaults(label=None, new_session=False)

    inspect_parser = subparsers.add_parser("inspect", help="Scan a source and recommend a profile")
    inspect_parser.add_argument("source")
    inspect_parser.add_argument("--profile", choices=["auto", *PROFILES])
    inspect_parser.add_argument("--max-memory-mib", type=int)
    inspect_parser.add_argument(
        "--duration-policy", choices=["grouped-views", "sum-all", "longest-file"]
    )
    inspect_parser.add_argument("--duration-hours", type=float)

    doctor_parser = subparsers.add_parser("doctor", help="Check local tools and R2 connectivity")
    doctor_parser.add_argument("--remote", help="rclone destination to test")
    doctor_parser.add_argument("--catalog-url", help="Dashboard /api/catalog URL to test")
    subparsers.add_parser("list", help="List known upload sessions")
    environments_parser = subparsers.add_parser(
        "environments", help="Fetch and list shared environments, caps, and sites"
    )
    environments_parser.add_argument("--catalog-url", help="Dashboard /api/catalog URL")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    supplied_args = list(sys.argv[1:] if argv is None else argv)
    system_label = f"{platform.system()} {platform.release()} ({platform.machine()})"
    if not supplied_args:
        print(f"FPV Upload — detected system: {system_label}")
        print(f"Sleep prevention: {sleep_prevention_method() or 'UNAVAILABLE — run doctor'}\n")
        parser.print_help()
        return 0
    args = parser.parse_args(supplied_args)
    print(f"Detected system: {system_label}")
    store: StateStore | None = None
    try:
        credentials_loaded = load_upload_environment()
        if credentials_loaded:
            print(f"Upload credentials: loaded from {UPLOAD_ENV_PATH}")
        config = load_config(args.config)
        store = StateStore(args.state_dir)
        if args.command == "upload":
            return upload(args, config, store)
        if args.command == "upload-raw":
            return upload_raw(args, config, store)
        if args.command == "upload-vendor":
            return upload_vendor(args, config, store)
        if args.command == "upload-batch":
            return upload_batch(args, config, store)
        if args.command == "inspect":
            return inspect_source(args, config)
        if args.command == "doctor":
            return doctor(args, config)
        if args.command == "list":
            return list_sessions(store)
        if args.command == "environments":
            return list_environments(args, config)
        parser.error("Unknown command")
    except IngestError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    finally:
        if store is not None:
            store.close()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
