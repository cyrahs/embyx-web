import asyncio
import logging
import secrets
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from embyx_web.fill_actor.errors import FillActorError, JobQueueFullError
from embyx_web.fill_actor.feeds import RSSHubFeedWarmer
from embyx_web.fill_actor.models import FillActorPlan, VideoState
from embyx_web.fill_actor.persistence import (
    CancelJobOutcome,
    CancelJobResult,
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


class _ExecutionStopReason(StrEnum):
    USER_CANCEL = 'user_cancel'
    OWNERSHIP_LOST = 'ownership_lost'
    SHUTDOWN = 'shutdown'


_STOP_REASON_PRIORITY = {
    _ExecutionStopReason.SHUTDOWN: 0,
    _ExecutionStopReason.OWNERSHIP_LOST: 1,
    _ExecutionStopReason.USER_CANCEL: 2,
}


@dataclass
class _JobExecution:
    job_id: str
    task: asyncio.Task[None] | None = None
    stop_reason: _ExecutionStopReason | None = None
    cancel_delivered: bool = False

    def attach_task(self, task: asyncio.Task[None]) -> None:
        self.task = task
        self._deliver_cancel()

    def request_stop(self, reason: _ExecutionStopReason) -> bool:
        if self.stop_reason is None or _STOP_REASON_PRIORITY[reason] > _STOP_REASON_PRIORITY[self.stop_reason]:
            self.stop_reason = reason
        return self._deliver_cancel()

    def _deliver_cancel(self) -> bool:
        if self.cancel_delivered or self.stop_reason is None or self.task is None or self.task.done():
            return False
        self.cancel_delivered = True
        if self.task.cancelling() == 0:
            self.task.cancel()
            return True
        return False


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
        self._executions: dict[str, _JobExecution] = {}
        self._cancel_operations: set[asyncio.Task[CancelJobResult]] = set()
        self._closing = False

    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._workers:
                return
            self._closing = False
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

    async def cancel_plan(self, plan_id: str) -> CancelJobResult:
        operation = asyncio.create_task(
            self._cancel_and_signal(plan_id),
            name=f'fill-actor-cancel-{plan_id}',
        )
        self._cancel_operations.add(operation)
        operation.add_done_callback(self._cancel_operation_done)
        return await asyncio.shield(operation)

    async def recover_interrupted_jobs(self) -> int:
        return await self._repository.fail_expired_jobs(now=self._now(), error_code='job_interrupted')

    async def aclose(self) -> None:
        async with self._lifecycle_lock:
            self._closing = True
            if self._cancel_operations:
                await asyncio.gather(*tuple(self._cancel_operations), return_exceptions=True)
            executions = tuple(self._executions.values())
            for execution in executions:
                execution.request_stop(_ExecutionStopReason.SHUTDOWN)
            execution_tasks = tuple(execution.task for execution in executions if execution.task is not None)
            if execution_tasks:
                await asyncio.gather(*execution_tasks, return_exceptions=True)
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
                if self._closing:
                    return
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
                    await self._run_claimed_job(job)
                    continue
                self._wake.clear()
                with suppress(TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), timeout=self._poll_interval)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception('fill-actor worker iteration failed')
                await asyncio.sleep(self._poll_interval)

    async def _run_claimed_job(self, job: JobRecord) -> None:
        execution = _JobExecution(job_id=job.job_id)
        self._executions[job.job_id] = execution
        task = asyncio.create_task(
            self._run_plan(job, execution),
            name=f'fill-actor-plan-{job.job_id}',
        )
        execution.attach_task(task)
        try:
            await task
        except asyncio.CancelledError:
            if (
                execution.stop_reason in {_ExecutionStopReason.USER_CANCEL, _ExecutionStopReason.OWNERSHIP_LOST}
                and asyncio.current_task() is not None
                and asyncio.current_task().cancelling() == 0
            ):
                return
            raise
        finally:
            if self._executions.get(job.job_id) is execution:
                self._executions.pop(job.job_id, None)

    async def _run_plan(self, job: JobRecord, execution: _JobExecution) -> None:
        reporter = _OwnedProgressReporter(
            repository=self._repository,
            job=job,
            owner_id=self._owner_id,
            clock=self._now,
            flush_interval=self._progress_flush_interval,
        )
        heartbeat: asyncio.Task[None] | None = None
        feed_task: asyncio.Task[None] | None = None
        try:
            if not await self._revalidate_claim(job):
                await reporter.mark_unowned()
                return
            heartbeat = asyncio.create_task(self._heartbeat(job, reporter, execution))
            feed_task = await self._start_feed_warmup(job)
            plan = await self._service.create_plan(job.actor_ids, plan_id=job.plan_id, progress=reporter)
            await self._wait_feed_warmup(feed_task, job)
            await self._stop_heartbeat(heartbeat)
            heartbeat = None
            partial = any(actor.error_code is not None for actor in plan.actors) or any(
                video.state is VideoState.SCAN_FAILED or bool(video.warnings) for video in plan.videos
            )
            saved = await self._finish_terminal(reporter, JobState.PARTIAL_FAILED if partial else JobState.COMPLETED)
            if not saved:
                LOGGER.warning(
                    'plan %s completed after its job lease was lost; retaining it for audited TTL cleanup',
                    plan.plan_id,
                )
        except asyncio.CancelledError:
            cleanup = asyncio.create_task(
                self._cleanup_stopped_plan(job, execution, reporter, heartbeat, feed_task),
                name=f'fill-actor-cleanup-{job.job_id}',
            )
            await self._wait_managed_task(cleanup)
            if execution.stop_reason in {_ExecutionStopReason.USER_CANCEL, _ExecutionStopReason.OWNERSHIP_LOST}:
                return
            raise
        except FillActorError as exc:
            cleanup = asyncio.create_task(
                self._cleanup_failed_plan(job, reporter, heartbeat, feed_task, error_code=exc.code),
                name=f'fill-actor-cleanup-{job.job_id}',
            )
            await self._wait_managed_task(cleanup)
            return
        except Exception:  # noqa: BLE001
            cleanup = asyncio.create_task(
                self._cleanup_failed_plan(
                    job,
                    reporter,
                    heartbeat,
                    feed_task,
                    error_code='plan_creation_failed',
                ),
                name=f'fill-actor-cleanup-{job.job_id}',
            )
            await self._wait_managed_task(cleanup)
            return

    async def _revalidate_claim(self, job: JobRecord) -> bool:
        while True:
            now = self._now()
            if job.lease_expires_at is None or job.lease_expires_at <= now:
                return False
            try:
                return await self._repository.renew_owned_job_lease(
                    job_id=job.job_id,
                    owner_id=self._owner_id,
                    now=now,
                    lease_expires_at=now + self._lease_duration,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception('fill-actor claimed job revalidation failed; retrying')
                await asyncio.sleep(self._poll_interval)

    async def _heartbeat(
        self,
        job: JobRecord,
        reporter: _OwnedProgressReporter,
        execution: _JobExecution,
    ) -> None:
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
                    execution.request_stop(_ExecutionStopReason.OWNERSHIP_LOST)
                    await reporter.mark_unowned()
                    return
                break

    async def _cancel_and_signal(self, plan_id: str) -> CancelJobResult:
        started_at = asyncio.get_running_loop().time()
        execution: _JobExecution | None = None
        try:
            result = await self._repository.cancel_job(job_id=plan_id, now=self._now())
            execution = self._executions.get(plan_id)
            if (
                result.outcome in {CancelJobOutcome.CANCELLED, CancelJobOutcome.ALREADY_CANCELLED}
                and execution is not None
            ):
                execution.request_stop(_ExecutionStopReason.USER_CANCEL)
            if (
                result.outcome in {CancelJobOutcome.CANCELLED, CancelJobOutcome.ALREADY_CANCELLED}
                and execution is not None
                and execution.task is not None
            ):
                await asyncio.gather(execution.task, return_exceptions=True)
            elapsed_ms = round((asyncio.get_running_loop().time() - started_at) * 1000, 1)
            LOGGER.info(
                'fill-actor cancellation completed outcome=%s previous_state=%s local_execution=%s '
                'cleanup_complete=%s elapsed_ms=%s',
                result.outcome.value,
                result.previous_state.value if result.previous_state is not None else None,
                execution is not None,
                execution is None or execution.task is None or execution.task.done(),
                elapsed_ms,
                extra={
                    'embyx_event': 'fill_actor_cancel_complete',
                    'embyx_cancel_outcome': result.outcome.value,
                    'embyx_previous_state': (
                        result.previous_state.value if result.previous_state is not None else None
                    ),
                    'embyx_local_execution': execution is not None,
                    'embyx_cleanup_complete': execution is None or execution.task is None or execution.task.done(),
                    'embyx_elapsed_ms': elapsed_ms,
                },
            )
            return result  # noqa: TRY300
        except Exception:
            elapsed_ms = round((asyncio.get_running_loop().time() - started_at) * 1000, 1)
            LOGGER.exception(
                'fill-actor cancellation failed local_execution=%s cleanup_complete=%s elapsed_ms=%s',
                execution is not None,
                execution is None or execution.task is None or execution.task.done(),
                elapsed_ms,
                extra={
                    'embyx_event': 'fill_actor_cancel_failed',
                    'embyx_local_execution': execution is not None,
                    'embyx_cleanup_complete': execution is None or execution.task is None or execution.task.done(),
                    'embyx_elapsed_ms': elapsed_ms,
                },
            )
            raise

    def _cancel_operation_done(self, task: asyncio.Task[CancelJobResult]) -> None:
        self._cancel_operations.discard(task)
        if task.cancelled():
            return
        exception = task.exception()
        if exception is not None:
            LOGGER.error(
                'fill-actor cancellation operation failed',
                exc_info=(type(exception), exception, exception.__traceback__),
            )

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

    async def _abort_feed_warmup_safely(self, task: asyncio.Task[None] | None, job: JobRecord) -> None:
        try:
            await self._abort_feed_warmup(task, job)
        except Exception:
            LOGGER.exception('RSSHub feed warm-up cleanup failed')

    async def _cleanup_stopped_plan(
        self,
        job: JobRecord,
        execution: _JobExecution,
        reporter: _OwnedProgressReporter,
        heartbeat: asyncio.Task[None] | None,
        feed_task: asyncio.Task[None] | None,
    ) -> None:
        if heartbeat is not None:
            await self._stop_heartbeat(heartbeat)
        try:
            await self._abort_feed_warmup_safely(feed_task, job)
        finally:
            if execution.stop_reason in {_ExecutionStopReason.USER_CANCEL, _ExecutionStopReason.OWNERSHIP_LOST}:
                await reporter.mark_unowned()
            else:
                await self._finish_terminal(reporter, JobState.FAILED, error_code='job_interrupted')

    async def _cleanup_failed_plan(
        self,
        job: JobRecord,
        reporter: _OwnedProgressReporter,
        heartbeat: asyncio.Task[None] | None,
        feed_task: asyncio.Task[None] | None,
        *,
        error_code: str,
    ) -> None:
        if heartbeat is not None:
            await self._stop_heartbeat(heartbeat)
        try:
            await self._abort_feed_warmup_safely(feed_task, job)
        finally:
            await self._finish_terminal(reporter, JobState.FAILED, error_code=error_code)

    @staticmethod
    async def _wait_managed_task(task: asyncio.Task[None]) -> None:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
        task.result()

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
