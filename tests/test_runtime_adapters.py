import sys
from pathlib import Path

import pytest

from embyx_web.runtime_adapters import load_runtime_adapters


@pytest.mark.asyncio
async def test_loads_narrow_runtime_compatibility_api(tmp_path: Path) -> None:
    package = tmp_path / 'test_runtime_package'
    package.mkdir()
    (package / '__init__.py').write_text('', encoding='utf-8')
    (package / 'fill_actor_api.py').write_text(
        """
closed = False

async def list_actor_video_ids(actor_id):
    return [f'{actor_id}-001']

def resolve_brand(video_id):
    return video_id.split('-', 1)[0]

async def find_sukebei_magnet(video_id):
    return f'magnet:?xt={video_id}'

async def aclose():
    global closed
    closed = True
""",
        encoding='utf-8',
    )

    adapters = load_runtime_adapters(
        runtime_root=tmp_path,
        module_name='test_runtime_package.fill_actor_api',
    )

    assert tuple(await adapters.actor_catalog.list_video_ids('ABC')) == ('ABC-001',)
    assert adapters.brand_resolver.resolve_brand('ABC-001') == 'ABC'
    assert await adapters.magnet_provider.find_magnet('ABC-001') == 'magnet:?xt=ABC-001'
    await adapters.aclose()
    module = sys.modules['test_runtime_package.fill_actor_api']
    assert module.closed is True


@pytest.mark.asyncio
async def test_runtime_actor_adapter_forwards_optional_page_progress(tmp_path: Path) -> None:
    package = tmp_path / 'progress_runtime'
    package.mkdir()
    (package / '__init__.py').write_text('', encoding='utf-8')
    (package / 'fill_actor_api.py').write_text(
        """
async def list_actor_video_ids(actor_id, *, progress_callback=None):
    if progress_callback is not None:
        await progress_callback(0, 2, None)
        await progress_callback(1, 2, 1)
        await progress_callback(2, 2, 2)
    return [f'{actor_id}-001']

def resolve_brand(video_id):
    return video_id.split('-', 1)[0]

async def find_sukebei_magnet(_video_id):
    return None

async def aclose():
    return None
""",
        encoding='utf-8',
    )
    adapters = load_runtime_adapters(runtime_root=tmp_path, module_name='progress_runtime.fill_actor_api')
    events: list[tuple[int, int | None, int | None]] = []

    async def report(completed: int, total: int | None, current: int | None) -> None:
        events.append((completed, total, current))

    assert tuple(await adapters.actor_catalog.list_video_ids('ABC', progress_callback=report)) == ('ABC-001',)
    assert events == [(0, 2, None), (1, 2, 1), (2, 2, 2)]


@pytest.mark.asyncio
async def test_runtime_cloud_adapter_normalizes_builtin_metadata_and_move_response(tmp_path: Path) -> None:
    package = tmp_path / 'cloud_runtime'
    package.mkdir()
    (package / '__init__.py').write_text('', encoding='utf-8')
    (package / 'fill_actor_api.py').write_text(
        """
async def list_actor_video_ids(_actor_id): return ()
def resolve_brand(_video_id): return 'ABC'
async def find_sukebei_magnet(_video_id): return None
async def list_cloud_directory(_path):
    return (
        {'id': 'dir', 'name': 'nested', 'full_path': '/115/nested', 'size': 0,
         'is_directory': True, 'write_time': {'seconds': 1, 'nanos': 2}, 'hashes': {}},
        {'id': 'file', 'name': 'ABC-001.mp4', 'full_path': '/115/ABC-001.mp4', 'size': 3,
         'is_directory': False, 'write_time': {'seconds': 4, 'nanos': 5}, 'hashes': {'1': 'hash'}},
    )
async def ensure_cloud_directory(_parent, _name):
    return {'success': True, 'created': True, 'path': '/cloud/dst/ABC'}
async def move_cloud_file(_source, _destination):
    return {'success': True, 'error_message': '', 'result_file_paths': ('/115/dst/ABC-001.mp4',)}
async def aclose(): return None
""",
        encoding='utf-8',
    )

    adapters = load_runtime_adapters(runtime_root=tmp_path, module_name='cloud_runtime.fill_actor_api')

    assert adapters.cloud_file_mover is not None
    listing = await adapters.cloud_file_mover.list_directory('/115')
    assert len(listing) == 1
    assert listing[0].path == '/115/ABC-001.mp4'
    assert listing[0].write_time == 4_000_000_005
    response = await adapters.cloud_file_mover.move_file('/115/ABC-001.mp4', '/115/dst')
    assert response.success is True
    assert response.result_paths == ('/115/dst/ABC-001.mp4',)


def test_rejects_runtime_module_loaded_outside_configured_root(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match='outside'):
        load_runtime_adapters(runtime_root=tmp_path, module_name='embyx_web.fill_actor.models')


def test_missing_root_module_never_executes_same_named_module_elsewhere(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured_root = tmp_path / 'configured'
    configured_root.mkdir()
    outside = tmp_path / 'outside'
    outside.mkdir()
    marker = tmp_path / 'executed'
    (outside / 'untrusted_runtime.py').write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).write_text('executed')\n",
        encoding='utf-8',
    )
    monkeypatch.syspath_prepend(str(outside))

    with pytest.raises(ModuleNotFoundError, match='configured root'):
        load_runtime_adapters(runtime_root=configured_root, module_name='untrusted_runtime')

    assert not marker.exists()
