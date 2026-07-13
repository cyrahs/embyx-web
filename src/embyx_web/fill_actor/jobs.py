import asyncio
import logging
import secrets
from collections.abc import Callable, Sequence
from contextlib import suppress
from datetime import UTC, datetime, timedelta

from embyx_web.fill_actor.errors import FillActorError, JobQueueFullError
from embyx_web.fill_actor.feeds import RSSHubFeedWarmer
from embyx_web.fill_actor.models import FillActorPlan, VideoState
from embyx_web.fill_actor.persistence import (
    FillActorRepository,
    JobFeedRecord,
    JobOperation,
    JobProgress,
    JobProgressEvent,
    JobProgressUnit,
    JobRecord,
    JobStage,
    JobState,
)
from embyx_web.fill_actor.service import FillActorService

LOGGER = logging.getLogger(__name__)


class _OwnedProgressReporter:
    def __init__(
        self,
        *,
        repository: FillActorRepository,
        job: JobRecord,
        owner_id: str,
        clock: Callable[[], datetime],
        flush_interval: float,
    ) -> None:
        if job.progress is None:  # pragma: no cover - JobRecord normalizes this invariant
            msg = 'claimed job must have progress'
            raise ValueError(msg)
        self._repository = repository
        self._job_id = job.job_id
        self._owner_id = owner_id
        self._clock = clock
        self._flush_interval = flush_interval
        self._current = job.progress
        self._dirty = False
        self._owned = True
        self._closed = False
        self._last_flush = 0.0
        self._timer: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    async def __call__(self, event: JobProgressEvent) -> None:
        async with self._lock:
            if self._closed or not self._owned:
                return
            values = (event.stage, event.completed, event.total, event.unit, event.current)
            current_values = (
                self._current.stage,
                self._current.completed,
                self._current.total,
                self._current.unit,
                self._current.current,
            )
            if values == current_values:
                return
            now = self._clock()
            scope_changed = (
                event.stage is not self._current.stage
                or event.unit is not self._current.unit
                or event.completed < self._current.completed
            )
            self._current = JobProgress(
                stage=event.stage,
                completed=event.completed,
                total=event.total,
                unit=event.unit,
                current=event.current,
                stage_started_at=now if scope_changed else self._current.stage_started_at,
                updated_at=now,
            )
            self._dirty = True
            loop_time = asyncio.get_running_loop().time()
            if scope_changed or self._last_flush == 0.0 or loop_time - self._last_flush >= self._flush_interval:
                await self._flush_locked()
            else:
                self._schedule_locked()

    async def finish(self, state: JobState, *, error_code: str | None = None) -> bool:
        timer: asyncio.Task[None] | None
        async with self._lock:
            if self._closed:
                return False
            self._closed = True
            timer = self._timer
            self._timer = None
            now = self._clock()
            progress = JobProgress(
                stage=JobStage.DONE,
                completed=self._current.completed,
                total=self._current.total,
                unit=self._current.unit,
                current=self._current.current,
                stage_started_at=now,
                updated_at=now,
            )
        if timer is not None:
            timer.cancel()
            await asyncio.gather(timer, return_exceptions=True)
        if not self._owned:
            return False
        return await self._repository.finish_owned_job(
            job_id=self._job_id,
            owner_id=self._owner_id,
            state=state,
            error_code=error_code,
            now=now,
            progress=progress,
        )

    async def mark_unowned(self) -> None:
        timer: asyncio.Task[None] | None
        async with self._lock:
            self._owned = False
            self._dirty = False
            timer = self._timer
            self._timer = None
        if timer is not None:
            timer.cancel()
            await asyncio.gather(timer, return_exceptions=True)

    async def _flush_locked(self) -> None:
        if not self._dirty or not self._owned or self._closed:
            return
        attempted_at = asyncio.get_running_loop().time()
        try:
            owned = await self._repository.update_owned_job_progress(
                job_id=self._job_id,
                owner_id=self._owner_id,
                progress=self._current,
                now=self._clock(),
            )
        except Exception:
            LOGGER.exception('fill-actor job progress update failed; retrying')
            self._last_flush = attempted_at
            self._schedule_locked()
            return
        if not owned:
            self._owned = False
            self._dirty = False
            return
        self._dirty = False
        self._last_flush = attempted_at

    def _schedule_locked(self) -> None:
        if self._timer is not None or self._closed or not self._owned:
            return
        elapsed = asyncio.get_running_loop().time() - self._last_flush
        delay = max(self._flush_interval - elapsed, 0.0)
        self._timer = asyncio.create_task(self._flush_after(delay), name=f'fill-actor-progress-{self._job_id}')

    async def _flush_after(self, delay: float) -> None:
        await asyncio.sleep(delay)
        async with self._lock:
            self._timer = None
            await self._flush_locked()
            if self._dirty:
                self._schedule_locked()


class FillActorJobManager:
    def __init__(  # noqa: PLR0913
        self,
        *,
        service: FillActorService,
        repository: FillActorRepository,
        clock: Callable[[], datetime] | None = None,
        token_factory: Callable[[], str] | None = None,
        max_concurrent_jobs: int = 2,
        max_active_jobs: int = 32,
        lease_duration: timedelta = timedelta(seconds=30),
        poll_interval: float = 0.25,
        progress_flush_interval: float = 0.75,
        feed_warmer: RSSHubFeedWarmer | None = None,
    ) -> None:
        if max_concurrent_jobs < 1 or max_active_jobs < max_concurrent_jobs:
            msg = 'job capacity must be positive and at least the worker count'
            raise ValueError(msg)
        if lease_duration <= timedelta(0) or poll_interval <= 0 or progress_flush_interval <= 0:
            msg = 'job lease and poll interval must be positive'
            raise ValueError(msg)
        self._service = service
        self._repository = repository
        self._clock = clock or (lambda: datetime.now(UTC))
        self._token_factory = token_factory or (lambda: secrets.token_urlsafe(18))
        self._owner_id = self._token_factory()
        self._max_concurrent_jobs = max_concurrent_jobs
        self._max_active_jobs = max_active_jobs
        self._lease_duration = lease_duration
        self._poll_interval = poll_interval
        self._progress_flush_interval = progress_flush_interval
        self._feed_warmer = feed_warmer
        self._wake = asyncio.Event()
        self._lifecycle_lock = asyncio.Lock()
        self._workers: tuple[asyncio.Task[None], ...] = ()
        self._reaper: asyncio.Task[None] | None = None

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._workers:
                return
            await self.recover_interrupted_jobs()
            self._workers = tuple(
                asyncio.create_task(self._worker_loop(), name=f'fill-actor-worker-{index}')
                for index in range(self._max_concurrent_jobs)
            )
            self._reaper = asyncio.create_task(self._reaper_loop(), name='fill-actor-job-reaper')
            self._wake.set()

    async def start_plan(self, actor_ids: Sequence[str]) -> JobRecord:
        await self.start()
        normalized = self._service.validate_actor_ids(actor_ids)
        plan_id = self._token_factory()
        now = self._now()
        job = JobRecord(
            job_id=plan_id,
            plan_id=plan_id,
            operation=JobOperation.CREATE_PLAN,
            state=JobState.QUEUED,
            created_at=now,
            updated_at=now,
            actor_ids=normalized,
            progress=JobProgress(
                stage=JobStage.QUEUED,
                completed=0,
                total=len(normalized),
                unit=JobProgressUnit.ACTORS,
                current=None,
                stage_started_at=now,
                updated_at=now,
            ),
        )
        feeds: tuple[JobFeedRecord, ...] = ()
        if self._feed_warmer is not None:
            feeds = self._feed_warmer.initial_records(job_id=job.job_id, actor_ids=normalized, now=now)
        if not await self._repository.enqueue_job(job, max_active=self._max_active_jobs, feeds=feeds):
            raise JobQueueFullError(str(self._max_active_jobs))
        self._wake.set()
        return job

    async def get_job(self, plan_id: str) -> JobRecord | None:
        return await self._repository.get_job(plan_id)

    async def get_plan(self, plan_id: str) -> FillActorPlan | None:
        job = await self._repository.get_job(plan_id)
        if job is None or job.state not in {JobState.COMPLETED, JobState.PARTIAL_FAILED}:
            return None
        record = await self._repository.get_plan(plan_id)
        return await self._service.get_plan(plan_id) if record is not None else None

    async def get_feeds(self, plan_id: str) -> tuple[JobFeedRecord, ...]:
        return await self._repository.list_job_feeds(plan_id)

    async def recover_interrupted_jobs(self) -> int:
        return await self._repository.fail_expired_jobs(now=self._now(), error_code='job_interrupted')

    async def aclose(self) -> None:
        async with self._lifecycle_lock:
            tasks = (*self._workers, *((self._reaper,) if self._reaper is not None else ()))
            self._workers = ()
            self._reaper = None
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if self._feed_warmer is not None:
                await self._feed_warmer.aclose()

    async def _worker_loop(self) -> None:
        while True:
            try:
                if not await self._service.roots_ready():
                    await asyncio.sleep(self._poll_interval)
                    continue
                now = self._now()
                job = await self._repository.claim_next_job(
                    owner_id=self._owner_id,
                    now=now,
                    lease_expires_at=now + self._lease_duration,
                )
                if job is not None:
                    await self._run_plan(job)
                    continue
                self._wake.clear()
                with suppress(TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), timeout=self._poll_interval)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception('fill-actor worker iteration failed')
                await asyncio.sleep(self._poll_interval)

    async def _run_plan(self, job: JobRecord) -> None:
        reporter = _OwnedProgressReporter(
            repository=self._repository,
            job=job,
            owner_id=self._owner_id,
            clock=self._now,
            flush_interval=self._progress_flush_interval,
        )
        heartbeat = asyncio.create_task(self._heartbeat(job, reporter))
        feed_task: asyncio.Task[None] | None = None
        try:
            feed_task = await self._start_feed_warmup(job)
            plan = await self._service.create_plan(job.actor_ids, plan_id=job.plan_id, progress=reporter)
            await self._wait_feed_warmup(feed_task, job)
        except asyncio.CancelledError:
            await self._stop_heartbeat(heartbeat)
            await asyncio.shield(self._abort_feed_warmup(feed_task, job))
            await asyncio.shield(self._finish_terminal(reporter, JobState.FAILED, error_code='job_interrupted'))
            raise
        except FillActorError as exc:
            await self._stop_heartbeat(heartbeat)
            await self._abort_feed_warmup(feed_task, job)
            await self._finish_terminal(reporter, JobState.FAILED, error_code=exc.code)
            return
        except Exception:  # noqa: BLE001
            await self._stop_heartbeat(heartbeat)
            await self._abort_feed_warmup(feed_task, job)
            await self._finish_terminal(reporter, JobState.FAILED, error_code='plan_creation_failed')
            return

        await self._stop_heartbeat(heartbeat)
        partial = any(actor.error_code is not None for actor in plan.actors) or any(
            video.state is VideoState.SCAN_FAILED or bool(video.warnings) for video in plan.videos
        )
        saved = await self._finish_terminal(reporter, JobState.PARTIAL_FAILED if partial else JobState.COMPLETED)
        if not saved:
            LOGGER.warning(
                'plan %s completed after its job lease was lost; retaining it for audited TTL cleanup',
                plan.plan_id,
            )

    async def _heartbeat(self, job: JobRecord, reporter: _OwnedProgressReporter) -> None:
        interval = max(self._lease_duration.total_seconds() / 3, 0.05)
        retry_interval = min(self._poll_interval, interval)
        while True:
            await asyncio.sleep(interval)
            while True:
                now = self._now()
                try:
                    owned = await self._repository.renew_owned_job_lease(
                        job_id=job.job_id,
                        owner_id=self._owner_id,
                        now=now,
                        lease_expires_at=now + self._lease_duration,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.exception('fill-actor job heartbeat failed; retrying')
                    await asyncio.sleep(retry_interval)
                    continue
                if not owned:
                    await reporter.mark_unowned()
                    return
                break

    async def _start_feed_warmup(self, job: JobRecord) -> asyncio.Task[None] | None:
        if self._feed_warmer is None:
            return None
        try:
            return await self._feed_warmer.start_job(job, owner_id=self._owner_id)
        except Exception:
            LOGGER.exception('RSSHub feed warm-up could not start')
            await self._feed_warmer.abort_job(None, job, owner_id=self._owner_id)
            return None

    async def _wait_feed_warmup(self, task: asyncio.Task[None] | None, job: JobRecord) -> None:
        if task is None:
            return
        try:
            await task
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception('RSSHub feed warm-up failed unexpectedly')
            await self._abort_feed_warmup(None, job)

    async def _abort_feed_warmup(self, task: asyncio.Task[None] | None, job: JobRecord) -> None:
        if self._feed_warmer is not None:
            await self._feed_warmer.abort_job(task, job, owner_id=self._owner_id)

    async def _reaper_loop(self) -> None:
        interval = max(self._lease_duration.total_seconds() / 3, self._poll_interval)
        while True:
            await asyncio.sleep(interval)
            try:
                await self.recover_interrupted_jobs()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception('fill-actor job reaper iteration failed')

    @staticmethod
    async def _finish_terminal(
        reporter: _OwnedProgressReporter,
        state: JobState,
        *,
        error_code: str | None = None,
    ) -> bool:
        try:
            return await reporter.finish(state, error_code=error_code)
        except Exception:
            LOGGER.exception('fill-actor terminal job update failed')
            return False

    @staticmethod
    async def _stop_heartbeat(task: asyncio.Task[None]) -> None:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    def _now(self) -> datetime:
        now = self._clock()
        return now if now.tzinfo is not None else now.replace(tzinfo=UTC)
