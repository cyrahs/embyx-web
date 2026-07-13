import asyncio
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx2
import pytest
from fastapi.testclient import TestClient

from embyx_web.api import ActorFeedView, JobProgressView, create_app
from embyx_web.fill_actor.feeds import RSSHubFeedWarmer
from embyx_web.fill_actor.jobs import FillActorJobManager
from embyx_web.fill_actor.persistence import (
    JobFeedRecord,
    JobFeedState,
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
    feed_warmer_factory=None,
    freshrss_url: str | None = None,
    freshrss_rsshub_url: str | None = None,
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
    feed_warmer = feed_warmer_factory(repository) if feed_warmer_factory is not None else None
    jobs = FillActorJobManager(service=service, repository=repository, feed_warmer=feed_warmer)
    app = create_app(
        service=service,
        repository=repository,
        jobs=jobs,
        api_token=api_token,
        max_request_bytes=max_request_bytes,
        freshrss_url=freshrss_url,
        freshrss_rsshub_url=freshrss_rsshub_url,
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


def test_plan_envelope_exposes_persisted_feed_status_and_freshrss_action(tmp_path: Path) -> None:
    async def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200,
            headers={'content-type': 'application/xml', 'rsshub-cache-status': 'HIT'},
            content=b'<rss version="2.0"><channel /></rss>' if request.method == 'GET' else b'',
        )

    http_client = httpx2.AsyncClient(transport=httpx2.MockTransport(handler))

    def feed_warmer_factory(repository):
        return RSSHubFeedWarmer(
            repository=repository,
            rsshub_url='http://rsshub.internal.test',
            freshrss_url='https://freshrss.example.test',
            freshrss_rsshub_url='https://rsshub.example.test',
            client=http_client,
            poll_interval=0.001,
        )

    client, _, _ = make_client(
        tmp_path,
        feed_warmer_factory=feed_warmer_factory,
        freshrss_url='https://freshrss.example.test',
        freshrss_rsshub_url='https://rsshub.example.test',
    )
    with client:
        created = client.post('/api/fill-actor/plans', json={'actor_ids': ['actor']})
        assert created.status_code == 202
        assert created.json()['feeds'][0]['actor_id'] == 'actor'
        plan_id = created.json()['job']['plan_id']
        payload = wait_for_plan(client, plan_id)

    assert payload['job']['state'] == 'completed'
    assert payload['feeds'] == [
        {
            'actor_id': 'actor',
            'state': 'ready',
            'attempts': 2,
            'updated_at': payload['feeds'][0]['updated_at'],
            'error_code': None,
            'freshrss_add_url': (
                'https://freshrss.example.test/i/?c=feed&a=add&url_rss='
                'https%3A%2F%2Frsshub.example.test%2Fjavbus%2Fstar%2Factor'
            ),
            'freshrss_url': 'https://freshrss.example.test',
        }
    ]


def test_feed_view_rebuilds_legacy_add_url_from_current_configuration() -> None:
    updated_at = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    record = JobFeedRecord(
        job_id='legacy-job',
        actor_id='actor',
        state=JobFeedState.READY,
        attempts=2,
        updated_at=updated_at,
        freshrss_add_url='https://legacy.example.test/i/?c=feed&a=add',
    )

    payload = ActorFeedView.from_record(
        record,
        freshrss_url='https://current-freshrss.example.test',
        freshrss_rsshub_url='https://current-rsshub.example.test',
    ).model_dump()

    assert payload['freshrss_add_url'] == (
        'https://current-freshrss.example.test/i/?c=feed&a=add&url_rss='
        'https%3A%2F%2Fcurrent-rsshub.example.test%2Fjavbus%2Fstar%2Factor'
    )
    assert payload['freshrss_url'] == 'https://current-freshrss.example.test'


def test_feed_view_hides_legacy_freshrss_actions_when_current_configuration_is_disabled() -> None:
    record = JobFeedRecord(
        job_id='legacy-job',
        actor_id='actor',
        state=JobFeedState.READY,
        attempts=2,
        updated_at=datetime(2026, 7, 13, 12, 0, tzinfo=UTC),
        freshrss_add_url='https://legacy.example.test/i/?c=feed&a=add',
    )

    payload = ActorFeedView.from_record(record).model_dump()

    assert payload['freshrss_add_url'] is None
    assert payload['freshrss_url'] is None


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
