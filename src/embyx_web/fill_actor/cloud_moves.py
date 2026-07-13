from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol

MAX_STRM_BYTES = 4096
VIDEO_SUFFIXES = frozenset({'.mp4', '.mkv', '.avi', '.wmv', '.mov', '.flv', '.m4v', '.ts', '.rmvb'})


class InvalidStrmTargetError(ValueError):
    """Raised when a mapping file cannot safely identify one CloudDrive file."""


@dataclass(frozen=True)
class CloudFileMetadata:
    path: str
    file_id: str
    name: str
    size: int
    write_time: int
    hashes: tuple[tuple[str, str], ...] = ()

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> CloudFileMetadata:
        path = value.get('path')
        file_id = value.get('id')
        name = value.get('name')
        size = value.get('size')
        write_time = value.get('write_time')
        hashes_value = value.get('hashes', {})
        if (
            not isinstance(path, str)
            or not isinstance(file_id, str)
            or not isinstance(name, str)
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
            or not isinstance(write_time, int)
            or isinstance(write_time, bool)
            or write_time < 0
            or not isinstance(hashes_value, Mapping)
        ):
            msg = 'invalid CloudDrive file metadata'
            raise ValueError(msg)
        normalized_path = _absolute_cloud_path(path, name='CloudDrive metadata path')
        if normalized_path.name != name or not file_id:
            msg = 'invalid CloudDrive file identity'
            raise ValueError(msg)
        hashes: list[tuple[str, str]] = []
        for key, item in hashes_value.items():
            if not isinstance(key, str) or not isinstance(item, str):
                msg = 'invalid CloudDrive file hashes'
                raise TypeError(msg)
            hashes.append((key, item))
        return cls(
            path=str(normalized_path),
            file_id=file_id,
            name=name,
            size=size,
            write_time=write_time,
            hashes=tuple(sorted(hashes)),
        )

    def matches_identity(self, other: CloudFileMetadata) -> bool:
        return (
            self.file_id == other.file_id
            and self.name == other.name
            and self.size == other.size
            and self.write_time == other.write_time
            and self.hashes == other.hashes
        )


@dataclass(frozen=True)
class CloudMoveResponse:
    success: bool
    error_message: str | None = None
    result_paths: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> CloudMoveResponse:
        success = value.get('success')
        error_message = value.get('error_message')
        result_paths = value.get('result_paths', ())
        if (
            not isinstance(success, bool)
            or (error_message is not None and not isinstance(error_message, str))
            or not isinstance(result_paths, (list, tuple))
            or any(not isinstance(path, str) for path in result_paths)
        ):
            msg = 'invalid CloudDrive move response'
            raise ValueError(msg)
        return cls(
            success=success,
            error_message=error_message or None,
            result_paths=tuple(result_paths),
        )


class CloudFileMover(Protocol):
    async def list_directory(self, api_directory: str) -> tuple[CloudFileMetadata, ...]: ...

    async def ensure_directory(self, parent_api_directory: str, folder_name: str) -> bool: ...

    async def move_file(self, source_api_path: str, destination_api_directory: str) -> CloudMoveResponse: ...


@dataclass(frozen=True)
class ParsedStrmTarget:
    mapping_sha256: str
    mounted_path: str
    api_path: str


@dataclass(frozen=True)
class CloudMovePaths:
    strm_mount_prefix: PurePosixPath
    source_api_roots: tuple[PurePosixPath, ...]
    move_in_api_root: PurePosixPath

    @classmethod
    def from_values(
        cls,
        *,
        strm_mount_prefix: str,
        source_api_roots: tuple[str, ...],
        move_in_api_root: str,
    ) -> CloudMovePaths:
        if not source_api_roots:
            msg = 'CloudDrive source roots must not be empty'
            raise ValueError(msg)
        return cls(
            strm_mount_prefix=_absolute_cloud_path(strm_mount_prefix, name='STRM mount prefix'),
            source_api_roots=tuple(
                _absolute_cloud_path(root, name='CloudDrive source root') for root in source_api_roots
            ),
            move_in_api_root=_absolute_cloud_path(move_in_api_root, name='CloudDrive move-in root'),
        )

    def parse_mapping(self, mapping_path: Path, *, source_index: int) -> ParsedStrmTarget:
        try:
            expected_root = self.source_api_roots[source_index]
        except IndexError as exc:
            msg = 'CloudDrive source root mapping is incomplete'
            raise InvalidStrmTargetError(msg) from exc
        try:
            payload = mapping_path.read_bytes()
        except OSError as exc:
            msg = 'STRM mapping is unavailable'
            raise InvalidStrmTargetError(msg) from exc
        if not payload or len(payload) > MAX_STRM_BYTES or b'\x00' in payload:
            msg = 'STRM mapping size is invalid'
            raise InvalidStrmTargetError(msg)
        try:
            value = payload.decode('utf-8')
        except UnicodeDecodeError as exc:
            msg = 'STRM mapping must be UTF-8'
            raise InvalidStrmTargetError(msg) from exc
        lines = value.splitlines()
        if len(lines) != 1 or not lines[0] or lines[0] != lines[0].strip():
            msg = 'STRM mapping must contain one absolute path'
            raise InvalidStrmTargetError(msg)
        mounted = _absolute_cloud_path(lines[0], name='STRM target')
        if mounted.suffix.casefold() not in VIDEO_SUFFIXES:
            msg = 'STRM target is not a supported video file'
            raise InvalidStrmTargetError(msg)
        try:
            relative = mounted.relative_to(self.strm_mount_prefix)
        except ValueError as exc:
            msg = 'STRM target is outside the configured mount prefix'
            raise InvalidStrmTargetError(msg) from exc
        api_path = PurePosixPath('/') / relative
        try:
            api_path.relative_to(expected_root)
        except ValueError as exc:
            msg = 'STRM target is outside the configured CloudDrive source root'
            raise InvalidStrmTargetError(msg) from exc
        return ParsedStrmTarget(
            mapping_sha256=hashlib.sha256(payload).hexdigest(),
            mounted_path=str(mounted),
            api_path=str(api_path),
        )

    def destination_directory(self, brand: str) -> str:
        if not brand or brand in {'.', '..'} or '/' in brand or '\\' in brand or '\x00' in brand:
            msg = 'CloudDrive destination brand is invalid'
            raise ValueError(msg)
        return str(self.move_in_api_root / brand)


def _absolute_cloud_path(value: str, *, name: str) -> PurePosixPath:
    if not value or '\x00' in value or '://' in value or '\\' in value:
        msg = f'{name} must be an absolute POSIX path'
        raise ValueError(msg)
    path = PurePosixPath(value)
    if not path.is_absolute() or any(part in {'.', '..'} for part in path.parts) or str(path) != value.rstrip('/'):
        msg = f'{name} must be a normalized absolute POSIX path'
        raise ValueError(msg)
    return path
