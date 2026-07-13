import asyncio
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from embyx_web.api import JobProgressView, create_app
from embyx_web.fill_actor.jobs import FillActorJobManager
from embyx_web.fill_actor.persistence import (
    JobProgress,
    JobProgressUnit,
    JobStage,
    JobState,
    MemoryFillActorRepository,
)
from embyx_web.fill_actor.service import FillActorPaths, FillActorService


class ActorCatalog:
    async def list_video_ids(self, _actor_id: str) -> list[str]:
        return ['ABC-001']


class BlockingActorCatalog:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()

    async def list_video_ids(self, _actor_id: str) -> list[str]:
        self.started.set()
        assert await asyncio.to_thread(self.release.wait, 2)
        return ['ABC-001']


class MagnetProvider:
    async def find_magnet(self, _video_id: str) -> None:
        return None


class BrandResolver:
    def resolve_brand(self, _video_id: str) -> str:
        return 'ABC'


def make_client(
    tmp_path: Path,
    *,
    api_token: str | None = None,
    max_request_bytes: int = 65_536,
    actor_catalog=None,
) -> tuple[TestClient, FillActorPaths, MemoryFillActorRepository]:
    paths = FillActorPaths.from_iterable(
        actor_brand_path=tmp_path / 'actor',
        additional_brand_paths=(tmp_path / 'additional',),
        move_in_path=tmp_path / 'move-in',
    )
    for path in (paths.actor_brand_path, *paths.additional_brand_paths, paths.move_in_path):
        path.mkdir()
    repository = MemoryFillActorRepository()
    service = FillActorService(
        paths=paths,
        actor_catalog=actor_catalog or ActorCatalog(),
        magnet_provider=MagnetProvider(),
        brand_resolver=BrandResolver(),
        repository=repository,
    )
    jobs = FillActorJobManager(service=service, repository=repository)
    app = create_app(
        service=service,
        repository=repository,
        jobs=jobs,
        api_token=api_token,
        max_request_bytes=max_request_bytes,
    )
    return TestClient(app), paths, repository


def wait_for_plan(client: TestClient, plan_id: str) -> dict:
    for _ in range(100):
        response = client.get(f'/api/fill-actor/plans/{plan_id}')
        payload = response.json()
        if payload['job']['state'] not in {'queued', 'running'}:
            return payload
        time.sleep(0.005)
    pytest.fail('plan job did not complete')


def test_plan_job_and_apply_end_to_end(tmp_path: Path) -> None:
    client, paths, _ = make_client(tmp_path)
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')

    with client:
        response = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        assert response.status_code == 202
        assert response.headers['cache-control'] == 'no-store'
        assert response.json()['job']['progress']['stage'] == 'queued'
        assert response.json()['job']['progress']['total'] == 1
        plan_id = response.json()['job']['plan_id']
        payload = wait_for_plan(client, plan_id)
        assert payload['job']['state'] == 'completed'
        assert payload['job']['progress']['stage'] == 'done'
        assert payload['job']['progress']['eta_seconds'] == 0
        candidate = payload['plan']['videos'][0]['move_candidates'][0]

        applied = client.post(
            f'/api/fill-actor/plans/{plan_id}/apply',
            json={
                'revision': payload['plan']['revision'],
                'candidate_ids': [candidate['candidate_id']],
            },
        )

    assert applied.status_code == 200
    assert applied.json()['state'] == 'succeeded'
    assert applied.json()['results'][0]['state'] == 'moved'
    assert not source.exists()
    assert (paths.move_in_path / source.name).read_bytes() == b'video'


def test_mutations_require_configured_bearer_token(tmp_path: Path) -> None:
    token = 'test-bearer-value'  # noqa: S105
    client, _, _ = make_client(tmp_path, api_token=token)
    with client:
        denied = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        allowed = client.post(
            '/api/fill-actor/plans',
            json={'actor_ids': ['actor']},
            headers={'Authorization': f'Bearer {token}'},
        )

    assert denied.status_code == 401
    assert denied.json() == {'error': {'code': 'unauthorized'}}
    assert 'WWW-Authenticate' in denied.headers
    assert allowed.status_code == 202


def test_api_maps_service_errors_without_raw_messages(tmp_path: Path) -> None:
    client, _, _ = make_client(tmp_path)
    with client:
        invalid = client.post('/api/fill-actor/plans', json={'actor_ids': ['bad actor']})
        malformed = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor'], 'unexpected': True})
        unknown = client.get('/api/fill-actor/plans/not-found')

    assert invalid.status_code == 422
    assert invalid.json() == {'error': {'code': 'invalid_actor_id'}}
    assert malformed.status_code == 422
    assert malformed.json() == {'error': {'code': 'invalid_request'}}
    assert unknown.status_code == 404
    assert unknown.json() == {'error': {'code': 'unknown_plan'}}


def test_request_size_limit_and_health(tmp_path: Path) -> None:
    client, _, _ = make_client(tmp_path, max_request_bytes=20)
    with client:
        oversized = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        health = client.get('/api/health')

    assert oversized.status_code == 413
    assert oversized.json() == {'error': {'code': 'request_too_large'}}
    assert health.status_code == 200
    assert health.json() == {'status': 'ok', 'database': True, 'roots': True}


def test_running_job_does_not_publish_or_apply_plan(tmp_path: Path) -> None:
    actor_catalog = BlockingActorCatalog()
    client, _, _ = make_client(tmp_path, actor_catalog=actor_catalog)
    try:
        with client:
            created = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
            plan_id = created.json()['job']['plan_id']
            assert actor_catalog.started.wait(timeout=1)

            current = client.get(f'/api/fill-actor/plans/{plan_id}')
            applied = client.post(
                f'/api/fill-actor/plans/{plan_id}/apply',
                json={'revision': 'not-published', 'candidate_ids': []},
            )

            assert current.status_code == 200
            assert current.json()['job']['state'] == 'running'
            assert current.json()['plan'] is None
            assert applied.status_code == 409
            assert applied.json() == {'error': {'code': 'plan_not_ready'}}
            actor_catalog.release.set()
            assert wait_for_plan(client, plan_id)['job']['state'] == 'completed'
    finally:
        actor_catalog.release.set()


def test_completed_job_without_plan_returns_not_found(tmp_path: Path) -> None:
    client, _, repository = make_client(tmp_path)
    with client:
        created = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        plan_id = created.json()['job']['plan_id']
        assert wait_for_plan(client, plan_id)['job']['state'] == 'completed'
        assert client.portal is not None
        assert client.portal.call(repository.delete_plan, plan_id)

        missing = client.get(f'/api/fill-actor/plans/{plan_id}')

    assert missing.status_code == 404
    assert missing.json() == {'error': {'code': 'unknown_plan'}}


def test_job_progress_view_derives_stage_eta_and_activity_ages() -> None:
    started = datetime(2026, 7, 13, 10, 0, tzinfo=UTC)
    view = JobProgressView.from_record(
        JobProgress(
            stage=JobStage.LIBRARY_SCAN,
            completed=4,
            total=10,
            unit=JobProgressUnit.VIDEOS,
            current='ABC-004',
            stage_started_at=started,
            updated_at=started + timedelta(seconds=30),
        ),
        state=JobState.RUNNING,
        now=started + timedelta(seconds=40),
    )

    assert view.percent == 40.0
    assert view.eta_seconds == 60
    assert view.elapsed_seconds == 40
    assert view.last_progress_seconds == 10
