import asyncio
import logging
import secrets
from collections.abc import Callable, Sequence
from contextlib import suppress
from datetime import UTC, datetime, timedelta

from embyx_web.fill_actor.errors import FillActorError, JobQueueFullError
from embyx_web.fill_actor.models import FillActorPlan, VideoState
from embyx_web.fill_actor.persistence import (
    FillActorRepository,
    JobOperation,
    JobRecord,
    JobState,
)
from embyx_web.fill_actor.service import FillActorService

LOGGER = logging.getLogger(__name__)


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
    ) -> None:
        if max_concurrent_jobs < 1 or max_active_jobs < max_concurrent_jobs:
            msg = 'job capacity must be positive and at least the worker count'
            raise ValueError(msg)
        if lease_duration <= timedelta(0) or poll_interval <= 0:
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
        )
        if not await self._repository.enqueue_job(job, max_active=self._max_active_jobs):
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
        heartbeat = asyncio.create_task(self._heartbeat(job))
        try:
            plan = await self._service.create_plan(job.actor_ids, plan_id=job.plan_id)
        except asyncio.CancelledError:
            await self._stop_heartbeat(heartbeat)
            await asyncio.shield(self._save_terminal(job, JobState.FAILED, error_code='job_interrupted'))
            raise
        except FillActorError as exc:
            await self._stop_heartbeat(heartbeat)
            await self._save_terminal(job, JobState.FAILED, error_code=exc.code)
            return
        except Exception:  # noqa: BLE001
            await self._stop_heartbeat(heartbeat)
            await self._save_terminal(job, JobState.FAILED, error_code='plan_creation_failed')
            return

        await self._stop_heartbeat(heartbeat)
        partial = any(actor.error_code is not None for actor in plan.actors) or any(
            video.state is VideoState.SCAN_FAILED or bool(video.warnings) for video in plan.videos
        )
        saved = await self._save_terminal(job, JobState.PARTIAL_FAILED if partial else JobState.COMPLETED)
        if not saved:
            LOGGER.warning(
                'plan %s completed after its job lease was lost; retaining it for audited TTL cleanup',
                plan.plan_id,
            )

    async def _heartbeat(self, job: JobRecord) -> None:
        interval = max(self._lease_duration.total_seconds() / 3, 0.05)
        retry_interval = min(self._poll_interval, interval)
        while True:
            await asyncio.sleep(interval)
            while True:
                now = self._now()
                refreshed = JobRecord(
                    job_id=job.job_id,
                    plan_id=job.plan_id,
                    operation=job.operation,
                    state=JobState.RUNNING,
                    created_at=job.created_at,
                    updated_at=now,
                    owner_id=self._owner_id,
                    lease_expires_at=now + self._lease_duration,
                    actor_ids=job.actor_ids,
                )
                try:
                    owned = await self._repository.update_owned_job(
                        refreshed,
                        owner_id=self._owner_id,
                        expected_states=(JobState.RUNNING,),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.exception('fill-actor job heartbeat failed; retrying')
                    await asyncio.sleep(retry_interval)
                    continue
                if not owned:
                    return
                break

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

    async def _save_terminal(
        self,
        job: JobRecord,
        state: JobState,
        *,
        error_code: str | None = None,
    ) -> bool:
        return await self._repository.update_owned_job(
            JobRecord(
                job_id=job.job_id,
                plan_id=job.plan_id,
                operation=job.operation,
                state=state,
                created_at=job.created_at,
                updated_at=self._now(),
                error_code=error_code,
                actor_ids=job.actor_ids,
            ),
            owner_id=self._owner_id,
            expected_states=(JobState.RUNNING,),
        )

    @staticmethod
    async def _stop_heartbeat(task: asyncio.Task[None]) -> None:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    def _now(self) -> datetime:
        now = self._clock()
        return now if now.tzinfo is not None else now.replace(tzinfo=UTC)
