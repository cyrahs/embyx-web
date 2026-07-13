import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from embyx_web.fill_actor.errors import JobQueueFullError
from embyx_web.fill_actor.jobs import FillActorJobManager
from embyx_web.fill_actor.models import FillActorPlan
from embyx_web.fill_actor.persistence import (
    JobOperation,
    JobProgress,
    JobProgressEvent,
    JobProgressUnit,
    JobRecord,
    JobStage,
    JobState,
    MemoryFillActorRepository,
    PlanRecord,
)
from embyx_web.fill_actor.sqlite_repository import SQLiteFillActorRepository


class ControlledService:
    def __init__(self, *, block: bool = False, repository=None, rapid_updates: int = 0) -> None:
        self.block = block
        self.repository = repository
        self.rapid_updates = rapid_updates
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    def validate_actor_ids(self, actor_ids):
        return tuple(actor_ids)

    async def roots_ready(self) -> bool:
        return True

    async def create_plan(self, _actor_ids, *, plan_id=None, revision=None, progress=None) -> FillActorPlan:
        self.started.set()
        if progress is not None:
            await progress(
                JobProgressEvent(
                    stage=JobStage.ACTOR_CATALOG,
                    completed=0,
                    total=1,
                    unit=JobProgressUnit.ACTORS,
                    current='actor',
                )
            )
        if self.block:
            await self.release.wait()
        if progress is not None and self.rapid_updates:
            for completed in range(self.rapid_updates + 1):
                await progress(
                    JobProgressEvent(
                        stage=JobStage.LIBRARY_SCAN,
                        completed=completed,
                        total=self.rapid_updates,
                        unit=JobProgressUnit.VIDEOS,
                        current=f'video-{completed}',
                    )
                )
        if progress is not None:
            await progress(
                JobProgressEvent(
                    stage=JobStage.PERSISTING,
                    completed=1,
                    total=1,
                    unit=JobProgressUnit.STEPS,
                    current='saved',
                )
            )
        now = datetime.now(UTC)
        plan = FillActorPlan(
            plan_id=plan_id or 'plan',
            revision=revision or 'revision',
            created_at=now,
            expires_at=now + timedelta(hours=1),
            actors=(),
            videos=(),
        )
        if self.repository is not None:
            await self.repository.save_plan(PlanRecord(public=plan, candidates=()))
        return plan

    async def get_plan(self, plan_id: str) -> FillActorPlan:
        record = await self.repository.get_plan(plan_id)
        assert record is not None
        return record.public


class FlakyHeartbeatRepository(MemoryFillActorRepository):
    def __init__(self) -> None:
        super().__init__()
        self.remaining_heartbeat_failures = 1

    async def renew_owned_job_lease(self, **kwargs):
        if self.remaining_heartbeat_failures:
            self.remaining_heartbeat_failures -= 1
            msg = 'temporary database error'
            raise OSError(msg)
        return await super().renew_owned_job_lease(**kwargs)


class CountingProgressRepository(MemoryFillActorRepository):
    def __init__(self) -> None:
        super().__init__()
        self.progress_writes = 0
        self.progress_records: list[JobProgress] = []

    async def update_owned_job_progress(self, **kwargs):
        self.progress_writes += 1
        saved = await super().update_owned_job_progress(**kwargs)
        if saved:
            self.progress_records.append(kwargs['progress'])
        return saved


class FailingFinishRepository(MemoryFillActorRepository):
    async def finish_owned_job(self, **_kwargs):
        msg = 'terminal persistence unavailable'
        raise OSError(msg)


class MutableProgressClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 7, 13, 10, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


class ScopeProgressService(ControlledService):
    def __init__(self, clock: MutableProgressClock) -> None:
        super().__init__()
        self.clock = clock

    async def create_plan(self, _actor_ids, *, plan_id=None, revision=None, progress=None) -> FillActorPlan:
        assert progress is not None
        await progress(
            JobProgressEvent(
                stage=JobStage.ACTOR_CATALOG,
                completed=0,
                total=1,
                unit=JobProgressUnit.ACTORS,
            )
        )
        self.clock.advance(5)
        await progress(
            JobProgressEvent(
                stage=JobStage.ACTOR_CATALOG,
                completed=1,
                total=26,
                unit=JobProgressUnit.PAGES,
                current='页面 1/26',
            )
        )
        self.clock.advance(5)
        await progress(
            JobProgressEvent(
                stage=JobStage.ACTOR_CATALOG,
                completed=2,
                total=26,
                unit=JobProgressUnit.PAGES,
                current='页面 2/26',
            )
        )
        await asyncio.sleep(0.02)
        self.clock.advance(5)
        await progress(
            JobProgressEvent(
                stage=JobStage.ACTOR_CATALOG,
                completed=0,
                total=26,
                unit=JobProgressUnit.PAGES,
                current='下一页范围',
            )
        )
        self.clock.advance(5)
        await progress(
            JobProgressEvent(
                stage=JobStage.ACTOR_CATALOG,
                completed=1,
                total=1,
                unit=JobProgressUnit.ACTORS,
                current='actor',
            )
        )
        now = self.clock()
        return FillActorPlan(
            plan_id=plan_id or 'plan',
            revision=revision or 'revision',
            created_at=now,
            expires_at=now + timedelta(hours=1),
            actors=(),
            videos=(),
        )


def make_repository(kind: str, tmp_path: Path):
    if kind == 'memory':
        return MemoryFillActorRepository()
    return SQLiteFillActorRepository(tmp_path / 'jobs.sqlite3')


async def wait_for_state(repository, job_id: str, states: set[JobState]) -> JobRecord:
    for _ in range(200):
        job = await repository.get_job(job_id)
        if job is not None and job.state in states:
            return job
        await asyncio.sleep(0.005)
    pytest.fail('job did not reach expected state')


@pytest.mark.asyncio
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_repository_atomically_enqueues_claims_and_expires_leases(
    tmp_path: Path,
    repository_kind: str,
) -> None:
    repository = make_repository(repository_kind, tmp_path)
    now = datetime(2026, 7, 13, tzinfo=UTC)
    queued = JobRecord(
        job_id='job-1',
        plan_id='job-1',
        operation=JobOperation.CREATE_PLAN,
        state=JobState.QUEUED,
        created_at=now,
        updated_at=now,
        actor_ids=('actor',),
    )
    assert await repository.enqueue_job(queued, max_active=1)
    assert not await repository.enqueue_job(
        JobRecord(
            job_id='job-2',
            operation=JobOperation.CREATE_PLAN,
            state=JobState.QUEUED,
            created_at=now,
            updated_at=now,
        ),
        max_active=1,
    )

    first, second = await asyncio.gather(
        repository.claim_next_job(owner_id='owner-a', now=now, lease_expires_at=now + timedelta(seconds=10)),
        repository.claim_next_job(owner_id='owner-b', now=now, lease_expires_at=now + timedelta(seconds=10)),
    )
    claimed = first or second
    assert claimed is not None
    assert (first is None) != (second is None)
    assert claimed.actor_ids == ('actor',)

    assert not await repository.renew_owned_job_lease(
        job_id=claimed.job_id,
        owner_id='old-owner',
        now=now + timedelta(seconds=5),
        lease_expires_at=now + timedelta(seconds=20),
    )
    assert await repository.renew_owned_job_lease(
        job_id=claimed.job_id,
        owner_id=claimed.owner_id,
        now=now + timedelta(seconds=5),
        lease_expires_at=now + timedelta(seconds=20),
    )
    progress = JobProgress(
        stage=JobStage.ACTOR_CATALOG,
        completed=1,
        total=1,
        unit=JobProgressUnit.ACTORS,
        current='actor',
        stage_started_at=now,
        updated_at=now + timedelta(seconds=6),
    )
    assert not await repository.update_owned_job_progress(
        job_id=claimed.job_id,
        owner_id='old-owner',
        progress=progress,
        now=now + timedelta(seconds=6),
    )
    assert await repository.update_owned_job_progress(
        job_id=claimed.job_id,
        owner_id=claimed.owner_id,
        progress=progress,
        now=now + timedelta(seconds=6),
    )
    assert await repository.fail_expired_jobs(now=now + timedelta(seconds=15), error_code='job_interrupted') == 0
    assert await repository.fail_expired_jobs(now=now + timedelta(seconds=21), error_code='job_interrupted') == 1
    failed = await repository.get_job(claimed.job_id)
    assert failed is not None
    assert failed.state is JobState.FAILED


@pytest.mark.asyncio
async def test_manager_capacity_and_shutdown_are_persisted() -> None:
    repository = MemoryFillActorRepository()
    service = ControlledService(block=True)
    manager = FillActorJobManager(
        service=service,
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=1,
        poll_interval=0.01,
    )
    first = await manager.start_plan(['actor-a'])
    await asyncio.wait_for(service.started.wait(), timeout=1)

    with pytest.raises(JobQueueFullError):
        await manager.start_plan(['actor-b'])

    await manager.aclose()
    interrupted = await repository.get_job(first.job_id)
    assert interrupted is not None
    assert interrupted.state is JobState.FAILED
    assert interrupted.error_code == 'job_interrupted'


@pytest.mark.asyncio
async def test_shutdown_remains_bounded_when_terminal_persistence_fails() -> None:
    repository = FailingFinishRepository()
    service = ControlledService(block=True)
    manager = FillActorJobManager(
        service=service,
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=1,
        poll_interval=0.01,
    )
    await manager.start_plan(['actor'])
    await asyncio.wait_for(service.started.wait(), timeout=1)

    await asyncio.wait_for(manager.aclose(), timeout=1)


@pytest.mark.asyncio
async def test_new_manager_executes_durable_queued_job(tmp_path: Path) -> None:
    repository = SQLiteFillActorRepository(tmp_path / 'jobs.sqlite3')
    now = datetime.now(UTC)
    queued = JobRecord(
        job_id='queued-plan',
        plan_id='queued-plan',
        operation=JobOperation.CREATE_PLAN,
        state=JobState.QUEUED,
        created_at=now,
        updated_at=now,
        actor_ids=('actor',),
    )
    assert await repository.enqueue_job(queued, max_active=4)
    manager = FillActorJobManager(
        service=ControlledService(),
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=4,
        poll_interval=0.01,
    )

    await manager.start()
    completed = await wait_for_state(repository, queued.job_id, {JobState.COMPLETED})
    await manager.aclose()

    assert completed.state is JobState.COMPLETED
    assert completed.progress is not None
    assert completed.progress.stage is JobStage.DONE
    assert completed.progress.completed == completed.progress.total == 1
    assert completed.progress.current == 'saved'


@pytest.mark.asyncio
async def test_manager_coalesces_same_stage_progress_and_flushes_terminal_snapshot() -> None:
    repository = CountingProgressRepository()
    manager = FillActorJobManager(
        service=ControlledService(rapid_updates=50),
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=2,
        poll_interval=0.01,
        progress_flush_interval=10,
    )

    job = await manager.start_plan(['actor'])
    completed = await wait_for_state(repository, job.job_id, {JobState.COMPLETED})
    await manager.aclose()

    assert repository.progress_writes == 3
    assert completed.progress is not None
    assert completed.progress.stage is JobStage.DONE
    assert completed.progress.current == 'saved'


@pytest.mark.asyncio
async def test_reporter_resets_stage_timer_when_progress_scope_changes() -> None:
    repository = CountingProgressRepository()
    clock = MutableProgressClock()
    manager = FillActorJobManager(
        service=ScopeProgressService(clock),
        repository=repository,
        clock=clock,
        token_factory=iter(('owner', 'job')).__next__,
        max_concurrent_jobs=1,
        max_active_jobs=2,
        poll_interval=0.01,
        progress_flush_interval=0.01,
    )

    job = await manager.start_plan(['actor'])
    await wait_for_state(repository, job.job_id, {JobState.COMPLETED})
    await manager.aclose()

    page_records = [record for record in repository.progress_records if record.unit is JobProgressUnit.PAGES]
    assert [(record.completed, record.total) for record in page_records] == [(1, 26), (2, 26), (0, 26)]
    assert page_records[0].stage_started_at == datetime(2026, 7, 13, 10, 0, 5, tzinfo=UTC)
    assert page_records[1].stage_started_at == page_records[0].stage_started_at
    assert page_records[2].stage_started_at == datetime(2026, 7, 13, 10, 0, 15, tzinfo=UTC)
    actor_records = [record for record in repository.progress_records if record.unit is JobProgressUnit.ACTORS]
    assert actor_records[-1].stage_started_at == datetime(2026, 7, 13, 10, 0, 20, tzinfo=UTC)


@pytest.mark.asyncio
async def test_lost_job_lease_keeps_completed_plan_hidden_for_ttl_cleanup() -> None:
    repository = MemoryFillActorRepository()
    service = ControlledService(block=True, repository=repository)
    manager = FillActorJobManager(
        service=service,
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=2,
        poll_interval=0.01,
    )
    job = await manager.start_plan(['actor'])
    await asyncio.wait_for(service.started.wait(), timeout=1)
    running = await wait_for_state(repository, job.job_id, {JobState.RUNNING})
    assert running.lease_expires_at is not None
    assert (
        await repository.fail_expired_jobs(
            now=running.lease_expires_at + timedelta(seconds=1),
            error_code='job_interrupted',
        )
        == 1
    )

    service.release.set()
    for _ in range(200):
        if await repository.get_plan(job.plan_id) is not None:
            break
        await asyncio.sleep(0.005)
    else:
        pytest.fail('completed orphan plan was not persisted')

    await manager.aclose()
    failed = await repository.get_job(job.job_id)
    assert failed is not None
    assert failed.state is JobState.FAILED
    assert await manager.get_plan(job.plan_id) is None
    assert await repository.get_plan(job.plan_id) is not None


@pytest.mark.asyncio
async def test_heartbeat_retries_transient_repository_failure() -> None:
    repository = FlakyHeartbeatRepository()
    service = ControlledService(block=True, repository=repository)
    manager = FillActorJobManager(
        service=service,
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=2,
        lease_duration=timedelta(milliseconds=150),
        poll_interval=0.01,
    )
    job = await manager.start_plan(['actor'])
    await asyncio.wait_for(service.started.wait(), timeout=1)
    await asyncio.sleep(0.25)
    service.release.set()

    completed = await wait_for_state(repository, job.job_id, {JobState.COMPLETED, JobState.FAILED})
    await manager.aclose()

    assert repository.remaining_heartbeat_failures == 0
    assert completed.state is JobState.COMPLETED
    assert await manager.get_plan(job.plan_id) is not None
