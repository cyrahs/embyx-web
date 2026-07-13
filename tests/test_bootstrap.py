import sys
from pathlib import Path

from fastapi.testclient import TestClient

from embyx_web.bootstrap import build_app
from embyx_web.settings import Settings


def test_bootstrap_wires_runtime_repository_api_and_shutdown(tmp_path: Path) -> None:
    runtime_package = tmp_path / 'runtime' / 'bootstrap_runtime'
    runtime_package.mkdir(parents=True)
    (runtime_package / '__init__.py').write_text('', encoding='utf-8')
    (runtime_package / 'api.py').write_text(
        """
closed = False

async def list_actor_video_ids(_actor_id):
    return ('ABC-001',)

def resolve_brand(_video_id):
    return 'ABC'

async def find_sukebei_magnet(_video_id):
    return None

async def aclose():
    global closed
    closed = True
""",
        encoding='utf-8',
    )
    actor = tmp_path / 'actor'
    additional = tmp_path / 'additional'
    move_in = tmp_path / 'move-in'
    for path in (actor, additional, move_in):
        path.mkdir()
        (path / '.embyx-root').write_text('ready', encoding='utf-8')
    settings = Settings(
        database_path=tmp_path / 'state' / 'app.sqlite3',
        mutation_lock_path=tmp_path / 'state' / 'move.lock',
        actor_brand_path=actor,
        additional_brand_paths=(additional,),
        move_in_path=move_in,
        embyx_runtime_path=tmp_path / 'runtime',
        embyx_runtime_module='bootstrap_runtime.api',
        move_in_by_brand=True,
        rsshub_url=None,
    )

    with TestClient(build_app(settings)) as client:
        assert client.get('/api/health').json()['status'] == 'ok'
        response = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        assert response.status_code == 202

    assert sys.modules['bootstrap_runtime.api'].closed is True
