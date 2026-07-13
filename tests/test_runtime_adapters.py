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
