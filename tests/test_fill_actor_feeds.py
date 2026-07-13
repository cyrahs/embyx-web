import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx2
import pytest

from embyx_web.fill_actor.feeds import RSSHubFeedWarmer
from embyx_web.fill_actor.jobs import FillActorJobManager
from embyx_web.fill_actor.persistence import (
    JobFeedErrorCode,
    JobFeedState,
    JobOperation,
    JobRecord,
    JobState,
    MemoryFillActorRepository,
)
from embyx_web.fill_actor.service import FillActorPaths, FillActorService

RSS_BODY = b'<?xml version="1.0"?><rss version="2.0"><channel><title>actor</title></channel></rss>'


class ActorCatalog:
    def __init__(self, callback=None) -> None:
        self._callback = callback

    async def list_video_ids(self, _actor_id: str) -> list[str]:
        if self._callback is not None:
            await self._callback()
        return ['ABC-001']


class MagnetProvider:
    async def find_magnet(self, _video_id: str) -> None:
        return None


class BrandResolver:
    def resolve_brand(self, _video_id: str) -> str:
        return 'ABC'


def make_service(
    tmp_path: Path,
    actor_catalog: ActorCatalog,
    repository: MemoryFillActorRepository,
) -> FillActorService:
    paths = FillActorPaths.from_iterable(
        actor_brand_path=tmp_path / 'actor',
        additional_brand_paths=(tmp_path / 'additional',),
        move_in_path=tmp_path / 'move-in',
    )
    for path in (paths.actor_brand_path, *paths.additional_brand_paths, paths.move_in_path):
        path.mkdir()
    return FillActorService(
        paths=paths,
        actor_catalog=actor_catalog,
        magnet_provider=MagnetProvider(),
        brand_resolver=BrandResolver(),
        repository=repository,
    )


async def claim_job(
    repository: MemoryFillActorRepository,
    warmer: RSSHubFeedWarmer,
    actor_ids: tuple[str, ...] = ('rw6',),
) -> JobRecord:
    now = datetime.now(UTC)
    job = JobRecord(
        job_id='job-1',
        plan_id='job-1',
        operation=JobOperation.CREATE_PLAN,
        state=JobState.QUEUED,
        created_at=now,
        updated_at=now,
        actor_ids=actor_ids,
    )
    feeds = warmer.initial_records(job_id=job.job_id, actor_ids=actor_ids, now=now)
    assert await repository.enqueue_job(job, max_active=1, feeds=feeds)
    claimed = await repository.claim_next_job(
        owner_id='owner',
        now=now,
        lease_expires_at=now + timedelta(minutes=5),
    )
    assert claimed is not None
    return claimed


@pytest.mark.asyncio
async def test_cache_probe_and_freshrss_subscription_use_independent_rsshub_bases() -> None:
    requests: list[tuple[str, str]] = []

    async def handler(request: httpx2.Request) -> httpx2.Response:
        requests.append((request.method, str(request.url)))
        return httpx2.Response(
            200,
            headers={
                'content-type': 'application/xml; charset=utf-8',
                'rsshub-cache-status': 'HIT',
            },
            content=RSS_BODY if request.method == 'GET' else b'',
        )

    repository = MemoryFillActorRepository()
    client = httpx2.AsyncClient(transport=httpx2.MockTransport(handler))
    warmer = RSSHubFeedWarmer(
        repository=repository,
        rsshub_url='http://rsshub.internal.test',
        freshrss_url='https://freshrss.example.test',
        freshrss_rsshub_url='https://rsshub.example.test',
        client=client,
        poll_interval=0.001,
    )
    claimed = await claim_job(repository, warmer)

    task = await warmer.start_job(claimed, owner_id='owner')
    await task

    feeds = await repository.list_job_feeds(claimed.job_id)
    assert requests == [
        ('GET', 'http://rsshub.internal.test/javbus/star/rw6'),
        ('HEAD', 'http://rsshub.internal.test/javbus/star/rw6'),
    ]
    assert feeds[0].state is JobFeedState.READY
    assert feeds[0].attempts == 2
    add_url = urlsplit(feeds[0].freshrss_add_url or '')
    assert f'{add_url.scheme}://{add_url.netloc}{add_url.path}' == 'https://freshrss.example.test/i/'
    assert parse_qs(add_url.query) == {
        'c': ['feed'],
        'a': ['add'],
        'url_rss': ['https://rsshub.example.test/javbus/star/rw6'],
    }
    await client.aclose()


@pytest.mark.asyncio
async def test_missing_cache_hit_stays_warming_until_fixed_not_ready_failure() -> None:
    async def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200,
            headers={'content-type': 'application/rss+xml'},
            content=RSS_BODY if request.method == 'GET' else b'',
        )

    repository = MemoryFillActorRepository()
    client = httpx2.AsyncClient(transport=httpx2.MockTransport(handler))
    warmer = RSSHubFeedWarmer(
        repository=repository,
        rsshub_url='http://rsshub',
        client=client,
        poll_interval=0.001,
        max_attempts=3,
    )
    claimed = await claim_job(repository, warmer)

    task = await warmer.start_job(claimed, owner_id='owner')
    await task

    feed = (await repository.list_job_feeds(claimed.job_id))[0]
    assert feed.state is JobFeedState.FAILED
    assert feed.attempts == 3
    assert feed.error_code is JobFeedErrorCode.NOT_READY
    await client.aclose()


@pytest.mark.asyncio
async def test_unexpected_transport_exception_becomes_fixed_failure_without_escaping() -> None:
    async def handler(_request: httpx2.Request) -> httpx2.Response:
        message = 'unexpected transport failure'
        raise RuntimeError(message)

    repository = MemoryFillActorRepository()
    client = httpx2.AsyncClient(transport=httpx2.MockTransport(handler))
    warmer = RSSHubFeedWarmer(repository=repository, rsshub_url='http://rsshub', client=client)
    claimed = await claim_job(repository, warmer)

    task = await warmer.start_job(claimed, owner_id='owner')
    await task

    feed = (await repository.list_job_feeds(claimed.job_id))[0]
    assert feed.state is JobFeedState.FAILED
    assert feed.error_code is JobFeedErrorCode.NOT_READY
    job = await repository.get_job(claimed.job_id)
    assert job is not None
    assert job.state is JobState.RUNNING
    await client.aclose()


@pytest.mark.asyncio
async def test_invalid_xml_failure_is_isolated_to_its_actor() -> None:
    async def handler(request: httpx2.Request) -> httpx2.Response:
        if request.url.path.endswith('/bad'):
            return httpx2.Response(
                200,
                headers={'content-type': 'application/xml', 'rsshub-cache-status': 'HIT'},
                content=b'<!DOCTYPE rss [<!ENTITY unsafe "value">]><rss>&unsafe;</rss>',
            )
        return httpx2.Response(
            200,
            headers={'content-type': 'application/xml', 'rsshub-cache-status': 'HIT'},
            content=RSS_BODY if request.method == 'GET' else b'',
        )

    repository = MemoryFillActorRepository()
    client = httpx2.AsyncClient(transport=httpx2.MockTransport(handler))
    warmer = RSSHubFeedWarmer(
        repository=repository,
        rsshub_url='http://rsshub',
        client=client,
        poll_interval=0.001,
    )
    claimed = await claim_job(repository, warmer, ('bad', 'good'))

    task = await warmer.start_job(claimed, owner_id='owner')
    await task

    feeds = {feed.actor_id: feed for feed in await repository.list_job_feeds(claimed.job_id)}
    assert feeds['bad'].state is JobFeedState.FAILED
    assert feeds['bad'].error_code is JobFeedErrorCode.INVALID_FEED
    assert feeds['good'].state is JobFeedState.READY
    assert feeds['good'].error_code is None
    await client.aclose()


@pytest.mark.asyncio
async def test_get_only_buffers_a_bounded_prefix_to_validate_the_root() -> None:
    async def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200,
            headers={'content-type': 'application/xml', 'rsshub-cache-status': 'HIT'},
            content=(b'<rss>' + b'x' * 1_000_000) if request.method == 'GET' else b'',
        )

    repository = MemoryFillActorRepository()
    client = httpx2.AsyncClient(transport=httpx2.MockTransport(handler))
    warmer = RSSHubFeedWarmer(
        repository=repository,
        rsshub_url='http://rsshub',
        client=client,
        poll_interval=0.001,
        max_feed_bytes=16,
    )
    claimed = await claim_job(repository, warmer)

    task = await warmer.start_job(claimed, owner_id='owner')
    await task

    feed = (await repository.list_job_feeds(claimed.job_id))[0]
    assert feed.state is JobFeedState.READY
    assert feed.attempts == 2
    await client.aclose()


@pytest.mark.asyncio
async def test_warmup_respects_global_concurrency_bound() -> None:
    active = 0
    peak = 0
    two_started = asyncio.Event()

    async def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal active, peak
        if request.method == 'HEAD':
            active += 1
            peak = max(peak, active)
            if active == 2:
                two_started.set()
            await two_started.wait()
            active -= 1
        return httpx2.Response(
            200,
            headers={'content-type': 'application/xml', 'rsshub-cache-status': 'HIT'},
            content=RSS_BODY if request.method == 'GET' else b'',
        )

    repository = MemoryFillActorRepository()
    client = httpx2.AsyncClient(transport=httpx2.MockTransport(handler))
    warmer = RSSHubFeedWarmer(
        repository=repository,
        rsshub_url='http://rsshub',
        client=client,
        poll_interval=0.001,
        concurrency=2,
    )
    claimed = await claim_job(repository, warmer, ('a', 'b', 'c', 'd'))

    task = await warmer.start_job(claimed, owner_id='owner')
    await task

    assert peak == 2
    assert {feed.state for feed in await repository.list_job_feeds(claimed.job_id)} == {JobFeedState.READY}
    await client.aclose()


@pytest.mark.asyncio
async def test_all_actor_gets_enter_transport_before_catalog_starts(tmp_path: Path) -> None:
    actor_ids = ('actor-a', 'actor-b', 'actor-c')
    requested: set[str] = set()
    release_gets = asyncio.Event()

    async def handler(request: httpx2.Request) -> httpx2.Response:
        if request.method == 'GET':
            requested.add(request.url.path.rsplit('/', 1)[-1])
            await release_gets.wait()
        return httpx2.Response(
            200,
            headers={'content-type': 'application/xml', 'rsshub-cache-status': 'HIT'},
            content=RSS_BODY if request.method == 'GET' else b'',
        )

    async def actor_callback() -> None:
        assert requested == set(actor_ids)
        release_gets.set()

    repository = MemoryFillActorRepository()
    service = make_service(tmp_path, ActorCatalog(actor_callback), repository)
    client = httpx2.AsyncClient(transport=httpx2.MockTransport(handler))
    warmer = RSSHubFeedWarmer(
        repository=repository,
        rsshub_url='http://rsshub',
        client=client,
        poll_interval=0.001,
    )
    manager = FillActorJobManager(service=service, repository=repository, feed_warmer=warmer)

    job = await manager.start_plan(actor_ids)
    terminal = await wait_for_terminal(repository, job.job_id)

    assert terminal.state is JobState.COMPLETED
    assert requested == set(actor_ids)
    assert {feed.state for feed in await repository.list_job_feeds(job.job_id)} == {JobFeedState.READY}
    await manager.aclose()
    await client.aclose()


async def wait_for_terminal(repository: MemoryFillActorRepository, job_id: str) -> JobRecord:
    for _ in range(200):
        job = await repository.get_job(job_id)
        if job is not None and job.state not in {JobState.QUEUED, JobState.RUNNING}:
            return job
        await asyncio.sleep(0.005)
    pytest.fail('job did not reach a terminal state')


@pytest.mark.asyncio
async def test_job_starts_rsshub_get_before_actor_catalog_and_feed_failure_does_not_fail_scan(tmp_path: Path) -> None:
    get_started = asyncio.Event()
    catalog_started = asyncio.Event()

    async def handler(_request: httpx2.Request) -> httpx2.Response:
        get_started.set()
        return httpx2.Response(403, headers={'content-type': 'text/html'})

    async def actor_callback() -> None:
        assert get_started.is_set()
        catalog_started.set()

    repository = MemoryFillActorRepository()
    service = make_service(tmp_path, ActorCatalog(actor_callback), repository)
    client = httpx2.AsyncClient(transport=httpx2.MockTransport(handler))
    warmer = RSSHubFeedWarmer(repository=repository, rsshub_url='http://rsshub', client=client)
    manager = FillActorJobManager(
        service=service, repository=repository, feed_warmer=warmer, token_factory=lambda: 'job'
    )

    job = await manager.start_plan(['actor'])
    terminal = await wait_for_terminal(repository, job.job_id)

    assert catalog_started.is_set()
    assert terminal.state is JobState.COMPLETED
    feed = (await repository.list_job_feeds(job.job_id))[0]
    assert feed.state is JobFeedState.FAILED
    assert feed.error_code is JobFeedErrorCode.HTTP
    await manager.aclose()
    await client.aclose()


@pytest.mark.asyncio
async def test_shutdown_after_plan_save_cancels_blocked_feed_and_leaves_terminal_records(tmp_path: Path) -> None:
    get_started = asyncio.Event()
    never_release = asyncio.Event()

    async def handler(_request: httpx2.Request) -> httpx2.Response:
        get_started.set()
        await never_release.wait()
        return httpx2.Response(200, headers={'content-type': 'application/xml'}, content=RSS_BODY)

    repository = MemoryFillActorRepository()
    service = make_service(tmp_path, ActorCatalog(), repository)
    client = httpx2.AsyncClient(transport=httpx2.MockTransport(handler))
    warmer = RSSHubFeedWarmer(repository=repository, rsshub_url='http://rsshub', client=client)
    manager = FillActorJobManager(
        service=service, repository=repository, feed_warmer=warmer, token_factory=lambda: 'job'
    )

    job = await manager.start_plan(['actor'])
    await asyncio.wait_for(get_started.wait(), timeout=1)
    for _ in range(200):
        if await repository.get_plan(job.job_id) is not None:
            break
        await asyncio.sleep(0.005)
    else:
        pytest.fail('plan was not saved before shutdown')

    await asyncio.wait_for(manager.aclose(), timeout=1)

    terminal = await repository.get_job(job.job_id)
    feed = (await repository.list_job_feeds(job.job_id))[0]
    assert terminal is not None
    assert terminal.state is JobState.FAILED
    assert terminal.error_code == 'job_interrupted'
    assert feed.state is JobFeedState.FAILED
    assert feed.error_code is JobFeedErrorCode.CANCELLED
    await client.aclose()
