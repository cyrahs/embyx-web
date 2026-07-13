import asyncio
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

import httpx2
from defusedxml.common import DefusedXmlException
from defusedxml.ElementTree import DefusedXMLParser, ParseError

from embyx_web.fill_actor.persistence import (
    FillActorRepository,
    JobFeedErrorCode,
    JobFeedRecord,
    JobFeedState,
    JobRecord,
)

_XML_MEDIA_TYPES = frozenset({'application/atom+xml', 'application/rss+xml', 'application/xml', 'text/xml'})
_TRANSIENT_HTTP_STATUSES = frozenset({202, 408, 425, 429, 500, 502, 503, 504})
_HTTP_OK = 200
_MIN_ATTEMPTS = 2


@dataclass(frozen=True)
class _HttpResult:
    status_code: int
    content_type: str | None
    cache_status: str | None
    root_tag: str | None = None


class _RootElementFoundError(Exception):
    def __init__(self, tag: str) -> None:
        super().__init__(tag)
        self.tag = tag


class _RootElementTarget:
    def start(self, tag: str, _attributes: dict[str, str]) -> None:
        raise _RootElementFoundError(tag)

    def end(self, _tag: str) -> None:
        return None

    def data(self, _data: str) -> None:
        return None

    def close(self) -> None:
        return None


def build_freshrss_add_url(
    actor_id: str,
    *,
    freshrss_url: str | None,
    freshrss_rsshub_url: str | None,
) -> str | None:
    if freshrss_url is None or freshrss_rsshub_url is None:
        return None
    parsed = urlsplit(freshrss_url.rstrip('/'))
    path = f'{parsed.path.rstrip("/")}/i/'
    feed_url = f'{freshrss_rsshub_url.rstrip("/")}/javbus/star/{quote(actor_id, safe="")}'
    query = urlencode({'c': 'feed', 'a': 'add', 'url_rss': feed_url})
    return urlunsplit((parsed.scheme, parsed.netloc, path, query, ''))


class RSSHubFeedWarmer:
    def __init__(  # noqa: PLR0913
        self,
        *,
        repository: FillActorRepository,
        rsshub_url: str,
        freshrss_url: str | None = None,
        freshrss_rsshub_url: str | None = None,
        client: httpx2.AsyncClient | None = None,
        request_timeout: float = 70.0,
        poll_interval: float = 1.0,
        max_attempts: int = 6,
        concurrency: int = 2,
        batch_deadline: float = 90.0,
        max_feed_bytes: int = 64 * 1024,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if request_timeout <= 0 or poll_interval <= 0 or batch_deadline <= 0:
            msg = 'RSSHub feed warm-up timing must be positive'
            raise ValueError(msg)
        if max_attempts < _MIN_ATTEMPTS or concurrency < 1 or max_feed_bytes < 1:
            msg = 'RSSHub feed warm-up limits are invalid'
            raise ValueError(msg)
        self._repository = repository
        self._rsshub_url = rsshub_url.rstrip('/')
        self._freshrss_url = freshrss_url.rstrip('/') if freshrss_url else None
        self._freshrss_rsshub_url = freshrss_rsshub_url.rstrip('/') if freshrss_rsshub_url else None
        self._client = client or httpx2.AsyncClient(
            timeout=request_timeout,
            follow_redirects=False,
            trust_env=False,
            headers={
                'Accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml',
                'User-Agent': 'embyx-web-feed-warmer/1.0',
            },
        )
        self._owns_client = client is None
        self._poll_interval = poll_interval
        self._max_attempts = max_attempts
        self._semaphore = asyncio.Semaphore(concurrency)
        self._batch_deadline = batch_deadline
        self._max_feed_bytes = max_feed_bytes
        self._clock = clock or (lambda: datetime.now(UTC))

    def initial_records(
        self,
        *,
        job_id: str,
        actor_ids: Sequence[str],
        now: datetime,
    ) -> tuple[JobFeedRecord, ...]:
        return tuple(
            JobFeedRecord(
                job_id=job_id,
                actor_id=actor_id,
                state=JobFeedState.QUEUED,
                attempts=0,
                updated_at=now,
                freshrss_add_url=self._freshrss_add_url(actor_id),
            )
            for actor_id in actor_ids
        )

    async def start_job(self, job: JobRecord, *, owner_id: str) -> asyncio.Task[None]:
        feeds = await self._repository.list_job_feeds(job.job_id)
        started = {feed.actor_id: asyncio.Event() for feed in feeds}
        task = asyncio.create_task(
            self._run_job(job, feeds, owner_id=owner_id, started=started),
            name=f'rsshub-warmup-{job.job_id}',
        )
        try:
            await asyncio.gather(*(event.wait() for event in started.values()))
        except asyncio.CancelledError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise
        return task

    async def abort_job(
        self,
        task: asyncio.Task[None] | None,
        job: JobRecord,
        *,
        owner_id: str,
    ) -> None:
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await self._fail_pending(job, owner_id=owner_id, error_code=JobFeedErrorCode.CANCELLED)

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _run_job(
        self,
        job: JobRecord,
        feeds: Sequence[JobFeedRecord],
        *,
        owner_id: str,
        started: dict[str, asyncio.Event],
    ) -> None:
        try:
            if not feeds:
                return
            async with asyncio.timeout(self._batch_deadline):
                async with asyncio.TaskGroup() as group:
                    for feed in feeds:
                        group.create_task(self._warm_feed(job, feed, owner_id=owner_id, started=started[feed.actor_id]))
        except TimeoutError:
            await self._fail_pending(job, owner_id=owner_id, error_code=JobFeedErrorCode.TIMEOUT)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            with suppress(Exception):
                await self._fail_pending(job, owner_id=owner_id, error_code=JobFeedErrorCode.NOT_READY)
        finally:
            for event in started.values():
                event.set()

    async def _warm_feed(  # noqa: C901
        self,
        job: JobRecord,
        feed: JobFeedRecord,
        *,
        owner_id: str,
        started: asyncio.Event,
    ) -> None:
        attempts = feed.attempts
        method = 'GET'
        last_error = JobFeedErrorCode.NOT_READY
        while attempts < self._max_attempts:
            attempts += 1
            if not await self._save_state(
                job,
                feed.actor_id,
                owner_id=owner_id,
                state=JobFeedState.WARMING,
                attempts=attempts,
            ):
                started.set()
                return
            if method == 'GET':
                started.set()
            try:
                result = await self._request_with_probe_limit(method, feed.actor_id)
            except httpx2.TimeoutException:
                last_error = JobFeedErrorCode.TIMEOUT
            except httpx2.RequestError:
                last_error = JobFeedErrorCode.NETWORK
            except Exception:  # noqa: BLE001
                last_error = JobFeedErrorCode.NOT_READY
            else:
                outcome = self._evaluate_get(result) if method == 'GET' else self._evaluate_head(result)
                if outcome is JobFeedState.READY:
                    await self._save_state(
                        job,
                        feed.actor_id,
                        owner_id=owner_id,
                        state=JobFeedState.READY,
                        attempts=attempts,
                    )
                    return
                if isinstance(outcome, JobFeedErrorCode):
                    if outcome in {JobFeedErrorCode.HTTP, JobFeedErrorCode.INVALID_FEED}:
                        await self._save_state(
                            job,
                            feed.actor_id,
                            owner_id=owner_id,
                            state=JobFeedState.FAILED,
                            attempts=attempts,
                            error_code=outcome,
                        )
                        return
                    last_error = outcome
                else:
                    last_error = JobFeedErrorCode.NOT_READY
            method = 'HEAD'
            if attempts < self._max_attempts:
                await asyncio.sleep(self._poll_interval)

        await self._save_state(
            job,
            feed.actor_id,
            owner_id=owner_id,
            state=JobFeedState.FAILED,
            attempts=attempts,
            error_code=last_error,
        )

    async def _request_with_probe_limit(self, method: str, actor_id: str) -> _HttpResult:
        if method == 'GET':
            return await self._request(method, actor_id)
        async with self._semaphore:
            return await self._request(method, actor_id)

    async def _request(self, method: str, actor_id: str) -> _HttpResult:
        url = self._feed_url(actor_id)
        if method == 'HEAD':
            response = await self._client.request('HEAD', url)
            return _HttpResult(
                status_code=response.status_code,
                content_type=response.headers.get('content-type'),
                cache_status=response.headers.get('rsshub-cache-status'),
            )

        async with self._client.stream('GET', url) as response:
            root_tag = await self._probe_root_element(response) if response.status_code == _HTTP_OK else None
            return _HttpResult(
                status_code=response.status_code,
                content_type=response.headers.get('content-type'),
                cache_status=response.headers.get('rsshub-cache-status'),
                root_tag=root_tag,
            )

    def _evaluate_get(self, result: _HttpResult) -> JobFeedState | JobFeedErrorCode | None:
        if result.status_code == _HTTP_OK:
            root_tag = (result.root_tag or '').rsplit('}', 1)[-1].casefold()
            if not self._is_xml_content_type(result.content_type) or root_tag not in {'feed', 'rss'}:
                return JobFeedErrorCode.INVALID_FEED
            return None
        if result.status_code in _TRANSIENT_HTTP_STATUSES:
            return JobFeedErrorCode.NOT_READY
        return JobFeedErrorCode.HTTP

    @classmethod
    def _evaluate_head(cls, result: _HttpResult) -> JobFeedState | JobFeedErrorCode | None:
        if result.status_code == _HTTP_OK:
            if not cls._is_xml_content_type(result.content_type):
                return JobFeedErrorCode.INVALID_FEED
            if (result.cache_status or '').casefold() == 'hit':
                return JobFeedState.READY
            return None
        if result.status_code in _TRANSIENT_HTTP_STATUSES:
            return JobFeedErrorCode.NOT_READY
        return JobFeedErrorCode.HTTP

    @staticmethod
    def _is_xml_content_type(content_type: str | None) -> bool:
        media_type = (content_type or '').split(';', 1)[0].strip().casefold()
        return media_type in _XML_MEDIA_TYPES or media_type.endswith('+xml')

    async def _probe_root_element(self, response: httpx2.Response) -> str | None:
        parser = DefusedXMLParser(target=_RootElementTarget(), forbid_dtd=True)
        remaining = self._max_feed_bytes
        try:
            async for chunk in response.aiter_bytes():
                if remaining <= 0:
                    return None
                probe = chunk[:remaining]
                remaining -= len(probe)
                try:
                    parser.feed(probe)
                except _RootElementFoundError as found:
                    return found.tag
                if len(probe) != len(chunk):
                    return None
            parser.close()
        except (ParseError, DefusedXmlException):
            return None
        return None

    async def _fail_pending(
        self,
        job: JobRecord,
        *,
        owner_id: str,
        error_code: JobFeedErrorCode,
    ) -> None:
        feeds = await self._repository.list_job_feeds(job.job_id)
        await asyncio.gather(
            *(
                self._save_state(
                    job,
                    feed.actor_id,
                    owner_id=owner_id,
                    state=JobFeedState.FAILED,
                    attempts=feed.attempts,
                    error_code=error_code,
                )
                for feed in feeds
                if feed.state in {JobFeedState.QUEUED, JobFeedState.WARMING}
            )
        )

    async def _save_state(  # noqa: PLR0913
        self,
        job: JobRecord,
        actor_id: str,
        *,
        owner_id: str,
        state: JobFeedState,
        attempts: int,
        error_code: JobFeedErrorCode | None = None,
    ) -> bool:
        return await self._repository.update_owned_job_feed(
            job_id=job.job_id,
            actor_id=actor_id,
            owner_id=owner_id,
            state=state,
            attempts=attempts,
            error_code=error_code,
            now=self._now(),
        )

    def _feed_url(self, actor_id: str) -> str:
        return f'{self._rsshub_url}/javbus/star/{quote(actor_id, safe="")}'

    def _freshrss_add_url(self, actor_id: str) -> str | None:
        return build_freshrss_add_url(
            actor_id,
            freshrss_url=self._freshrss_url,
            freshrss_rsshub_url=self._freshrss_rsshub_url,
        )

    def _now(self) -> datetime:
        now = self._clock()
        return now if now.tzinfo is not None else now.replace(tzinfo=UTC)
