from pathlib import Path

import pytest

from embyx_web.fill_actor.cloud_moves import (
    CloudFileMetadata,
    CloudMovePaths,
    CloudMoveResponse,
    InvalidStrmTargetError,
)


@pytest.fixture
def cloud_paths() -> CloudMovePaths:
    return CloudMovePaths.from_values(
        strm_mount_prefix='/mounted-cloud',
        source_api_roots=(
            '/cloud/library/source-a',
            '/cloud/library/source-b',
            '/cloud/library/source-c',
            '/cloud/library/source-d',
        ),
        move_in_api_root='/cloud/library/destination',
    )


def test_parses_one_strm_path_and_strips_only_the_configured_mount_prefix(
    tmp_path: Path,
    cloud_paths: CloudMovePaths,
) -> None:
    mapping = tmp_path / 'ABC-001.strm'
    mapping.write_text('/mounted-cloud/cloud/library/source-b/ABC/ABC-001.mp4\n', encoding='utf-8')

    parsed = cloud_paths.parse_mapping(mapping, source_index=1)

    assert parsed.mounted_path == '/mounted-cloud/cloud/library/source-b/ABC/ABC-001.mp4'
    assert parsed.api_path == '/cloud/library/source-b/ABC/ABC-001.mp4'
    assert len(parsed.mapping_sha256) == 64
    assert cloud_paths.destination_directory('ABC') == '/cloud/library/destination/ABC'


@pytest.mark.parametrize(
    'value',
    [
        '',
        'relative/video.mp4',
        'https://example.invalid/video.mp4',
        '/mounted-cloud/cloud/library/source-b/../escape/ABC-001.mp4',
        '/mounted-cloud/cloud/library/source-a/ABC-001.mp4',
        '/mounted-cloud/cloud/library/source-b/ABC-001.txt',
        '/mounted-cloud/cloud/library/source-b/ABC-001.mp4\n/another.mp4',
        ' /mounted-cloud/cloud/library/source-b/ABC-001.mp4',
    ],
)
def test_rejects_unsafe_or_cross_root_strm_targets(
    tmp_path: Path,
    cloud_paths: CloudMovePaths,
    value: str,
) -> None:
    mapping = tmp_path / 'ABC-001.strm'
    mapping.write_text(value, encoding='utf-8')

    with pytest.raises((InvalidStrmTargetError, ValueError)):
        cloud_paths.parse_mapping(mapping, source_index=1)


def test_rejects_oversized_or_non_utf8_mapping(tmp_path: Path, cloud_paths: CloudMovePaths) -> None:
    oversized = tmp_path / 'oversized.strm'
    oversized.write_bytes(b'/' + b'a' * 4096)
    invalid = tmp_path / 'invalid.strm'
    invalid.write_bytes(b'\xff')

    with pytest.raises(InvalidStrmTargetError):
        cloud_paths.parse_mapping(oversized, source_index=1)
    with pytest.raises(InvalidStrmTargetError):
        cloud_paths.parse_mapping(invalid, source_index=1)


def test_normalizes_runtime_metadata_without_accepting_untrusted_shapes() -> None:
    metadata = CloudFileMetadata.from_mapping(
        {
            'path': '/cloud/library/source-b/ABC/ABC-001.mp4',
            'id': 'file-id',
            'name': 'ABC-001.mp4',
            'size': 12,
            'write_time': 34,
            'hashes': {'sha1': 'abcd'},
        }
    )

    assert metadata.matches_identity(metadata)
    assert metadata.hashes == (('sha1', 'abcd'),)
    with pytest.raises(ValueError, match='identity'):
        CloudFileMetadata.from_mapping(
            {
                'path': '/cloud/library/source-b/ABC/not-the-name.mp4',
                'id': 'file-id',
                'name': 'ABC-001.mp4',
                'size': 12,
                'write_time': 34,
            }
        )


def test_normalizes_move_response_without_exposing_runtime_types() -> None:
    response = CloudMoveResponse.from_mapping(
        {'success': True, 'error_message': '', 'result_paths': ['/cloud/library/destination/ABC/ABC-001.mp4']}
    )

    assert response.success is True
    assert response.error_message is None
    assert response.result_paths == ('/cloud/library/destination/ABC/ABC-001.mp4',)
