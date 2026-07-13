import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from embyx_web.fill_actor.errors import JobQueueFullError
from embyx_web.fill_actor.jobs import FillActorJobManager
from embyx_web.fill_actor.models import FillActorPlan
from embyx_web.fill_actor.persistence import (
    JobOperation,
    JobRecord,
    JobState,
    MemoryFillActorRepository,
    PlanRecord,
)
from embyx_web.fill_actor.sqlite_repository import SQLiteFillActorRepository


class ControlledService:
    def __init__(self, *, block: bool = False, repository=None) -> None:
        self.block = block
        self.repository = repository
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    def validate_actor_ids(self, actor_ids):
        return tuple(actor_ids)

    async def roots_ready(self) -> bool:
        return True

    async def create_plan(self, _actor_ids, *, plan_id=None, revision=None) -> FillActorPlan:
        self.started.set()
        if self.block:
            await self.release.wait()
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

    async def update_owned_job(self, record, *, owner_id, expected_states):
        if record.state is JobState.RUNNING and self.remaining_heartbeat_failures:
            self.remaining_heartbeat_failures -= 1
            msg = 'temporary database error'
            raise OSError(msg)
        return await super().update_owned_job(record, owner_id=owner_id, expected_states=expected_states)


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

    refreshed = JobRecord(
        job_id=claimed.job_id,
        plan_id=claimed.plan_id,
        operation=claimed.operation,
        state=JobState.RUNNING,
        created_at=claimed.created_at,
        updated_at=now + timedelta(seconds=5),
        owner_id=claimed.owner_id,
        lease_expires_at=now + timedelta(seconds=20),
        actor_ids=claimed.actor_ids,
    )
    assert not await repository.update_owned_job(
        refreshed,
        owner_id='old-owner',
        expected_states=(JobState.RUNNING,),
    )
    assert await repository.update_owned_job(
        refreshed,
        owner_id=claimed.owner_id,
        expected_states=(JobState.RUNNING,),
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
