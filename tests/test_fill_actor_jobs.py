import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from embyx_web.fill_actor.errors import ApplyJobNotCancellableError, FillActorError, JobQueueFullError
from embyx_web.fill_actor.jobs import FillActorJobManager
from embyx_web.fill_actor.models import ApplyResult, ApplyState, FillActorPlan, MoveResult, MoveState
from embyx_web.fill_actor.persistence import (
    JOB_CANCELLED_ERROR_CODE,
    ApplyJobRecord,
    CancelJobOutcome,
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


class PausedClaimRepository(MemoryFillActorRepository):
    def __init__(self) -> None:
        super().__init__()
        self.claimed = asyncio.Event()
        self.release_claim = asyncio.Event()

    async def claim_next_job(self, **kwargs):
        job = await super().claim_next_job(**kwargs)
        if job is not None:
            self.claimed.set()
            await self.release_claim.wait()
        return job


class PausedCancelRepository(MemoryFillActorRepository):
    def __init__(self) -> None:
        super().__init__()
        self.cancel_persisted = asyncio.Event()
        self.release_cancel = asyncio.Event()

    async def cancel_job(self, **kwargs):
        result = await super().cancel_job(**kwargs)
        self.cancel_persisted.set()
        await self.release_cancel.wait()
        return result


class OwnershipLossRepository(MemoryFillActorRepository):
    def __init__(self) -> None:
        super().__init__()
        self.rejected_renewal = asyncio.Event()

    async def renew_owned_job_lease(self, **kwargs):
        owned = await super().renew_owned_job_lease(**kwargs)
        if not owned:
            self.rejected_renewal.set()
        return owned


class CancellationAwareService(ControlledService):
    def __init__(self) -> None:
        super().__init__(block=True)
        self.cancelled = asyncio.Event()

    async def create_plan(self, *args, **kwargs) -> FillActorPlan:
        try:
            return await super().create_plan(*args, **kwargs)
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class DirtyProgressService(ControlledService):
    async def create_plan(self, *args, progress=None, **kwargs) -> FillActorPlan:
        assert progress is not None
        await progress(
            JobProgressEvent(
                stage=JobStage.ACTOR_CATALOG,
                completed=0,
                total=2,
                unit=JobProgressUnit.ACTORS,
                current='actor-0',
            )
        )
        await progress(
            JobProgressEvent(
                stage=JobStage.ACTOR_CATALOG,
                completed=1,
                total=2,
                unit=JobProgressUnit.ACTORS,
                current='actor-1',
            )
        )
        self.started.set()
        await self.release.wait()
        return await super().create_plan(*args, progress=progress, **kwargs)


class FailingDirtyProgressService(ControlledService):
    def __init__(self, failure_type: type[Exception]) -> None:
        super().__init__()
        self.failure_type = failure_type

    async def create_plan(self, *args, progress=None, **kwargs) -> FillActorPlan:
        del args, kwargs
        assert progress is not None
        await progress(
            JobProgressEvent(
                stage=JobStage.ACTOR_CATALOG,
                completed=0,
                total=2,
                unit=JobProgressUnit.ACTORS,
                current='actor-0',
            )
        )
        await progress(
            JobProgressEvent(
                stage=JobStage.ACTOR_CATALOG,
                completed=1,
                total=2,
                unit=JobProgressUnit.ACTORS,
                current='actor-1',
            )
        )
        self.started.set()
        message = 'simulated plan failure'
        raise self.failure_type(message)


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


class ControlledApplyService(ControlledService):
    def __init__(self, *, repository, block_apply: bool = False, result_state: ApplyState = ApplyState.SUCCEEDED):
        super().__init__(repository=repository)
        self.apply_started = asyncio.Event()
        self.apply_release = asyncio.Event()
        self.block_apply = block_apply
        self.result_state = result_state
        self.apply_calls: list[tuple[str, str, tuple[str, ...]]] = []
        self.processed_candidates: list[str] = []

    async def apply_ready(self) -> bool:
        return True

    async def apply(
        self,
        *,
        plan_id,
        revision,
        candidate_ids,
        progress=None,
        stop_requested=None,
    ) -> ApplyResult:
        selected = tuple(candidate_ids)
        self.apply_calls.append((plan_id, revision, selected))
        self.apply_started.set()
        if progress is not None:
            await progress(
                JobProgressEvent(
                    stage=JobStage.UNKNOWN,
                    completed=0,
                    total=len(selected),
                    unit=JobProgressUnit.ITEMS,
                    current='ABC-001.mp4' if selected else None,
                )
            )
        if self.block_apply:
            await self.apply_release.wait()
        results: list[MoveResult] = []
        for index, candidate_id in enumerate(selected):
            if index > 0 and stop_requested is not None and stop_requested():
                break
            self.processed_candidates.append(candidate_id)
            results.append(
                MoveResult(
                    candidate_id=candidate_id,
                    video_id='ABC-001',
                    file_name='ABC-001.mp4',
                    state=MoveState.MOVED if self.result_state is ApplyState.SUCCEEDED else MoveState.FAILED,
                    error_code=None if self.result_state is ApplyState.SUCCEEDED else 'simulated_failure',
                )
            )
            if progress is not None:
                await progress(
                    JobProgressEvent(
                        stage=JobStage.UNKNOWN,
                        completed=index + 1,
                        total=len(selected),
                        unit=JobProgressUnit.ITEMS,
                        current=None,
                    )
                )
        return ApplyResult(
            plan_id=plan_id,
            revision=revision,
            state=self.result_state,
            results=tuple(results),
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


def make_apply_job(*, now: datetime, job_id: str = 'apply-request-0001') -> ApplyJobRecord:
    return ApplyJobRecord(
        job=JobRecord(
            job_id=job_id,
            plan_id='plan-1',
            operation=JobOperation.APPLY,
            state=JobState.QUEUED,
            created_at=now,
            updated_at=now,
            progress=JobProgress(
                stage=JobStage.QUEUED,
                completed=0,
                total=1,
                unit=JobProgressUnit.ITEMS,
                current=None,
                stage_started_at=now,
                updated_at=now,
            ),
        ),
        revision='revision-1',
        candidate_ids=('candidate-1',),
    )


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
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_manager_recovers_queued_apply_and_persists_business_result(
    tmp_path: Path,
    repository_kind: str,
) -> None:
    repository = make_repository(repository_kind, tmp_path)
    now = datetime.now(UTC)
    queued = make_apply_job(now=now)
    assert (await repository.enqueue_apply_job(queued, max_active=1)).record == queued
    service = ControlledApplyService(repository=repository, result_state=ApplyState.FAILED)
    manager = FillActorJobManager(
        service=service,
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=1,
        poll_interval=0.01,
    )

    await manager.start()
    completed = await wait_for_state(repository, queued.job.job_id, {JobState.COMPLETED})
    stored = await repository.get_apply_job(queued.job.job_id)
    await manager.aclose()

    assert service.apply_calls == [('plan-1', 'revision-1', ('candidate-1',))]
    assert not service.started.is_set()
    assert completed.operation is JobOperation.APPLY
    assert completed.progress is not None
    assert completed.progress.completed == 1
    assert completed.progress.total == 1
    assert completed.progress.current is None
    assert stored is not None
    assert stored.result is not None
    assert stored.result.state is ApplyState.FAILED


@pytest.mark.asyncio
async def test_apply_job_rejects_cancel_and_shutdown_drains_current_move() -> None:
    repository = MemoryFillActorRepository()
    service = ControlledApplyService(repository=repository, block_apply=True)
    manager = FillActorJobManager(
        service=service,
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=1,
        poll_interval=0.01,
    )
    record = await manager.start_apply(
        plan_id='plan-1',
        revision='revision-1',
        candidate_ids=['candidate-1'],
        request_id='apply-request-0001',
    )
    await asyncio.wait_for(service.apply_started.wait(), timeout=1)

    with pytest.raises(ApplyJobNotCancellableError):
        await manager.cancel_plan(record.job.job_id)
    closing = asyncio.create_task(manager.aclose())
    await asyncio.sleep(0.02)
    running = await repository.get_job(record.job.job_id)
    assert not closing.done()
    assert running is not None
    assert running.state is JobState.RUNNING
    assert running.error_code is None

    service.apply_release.set()
    await asyncio.wait_for(closing, timeout=1)
    completed = await repository.get_apply_job(record.job.job_id)

    assert completed is not None
    assert completed.job.state is JobState.COMPLETED
    assert completed.job.error_code is None
    assert completed.result is not None
    assert completed.result.results[0].state is MoveState.MOVED


@pytest.mark.asyncio
async def test_apply_job_ownership_loss_drains_only_current_candidate() -> None:
    repository = OwnershipLossRepository()
    service = ControlledApplyService(repository=repository, block_apply=True)
    manager = FillActorJobManager(
        service=service,
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=1,
        lease_duration=timedelta(milliseconds=150),
        poll_interval=0.01,
    )
    record = await manager.start_apply(
        plan_id='plan-1',
        revision='revision-1',
        candidate_ids=['candidate-1', 'candidate-2'],
        request_id='apply-ownership-loss',
    )
    await asyncio.wait_for(service.apply_started.wait(), timeout=1)
    running = await wait_for_state(repository, record.job.job_id, {JobState.RUNNING})
    assert running.lease_expires_at is not None

    assert (
        await repository.fail_expired_jobs(
            now=running.lease_expires_at + timedelta(seconds=1),
            error_code='job_interrupted',
        )
        == 1
    )
    await asyncio.wait_for(repository.rejected_renewal.wait(), timeout=1)
    await asyncio.sleep(0)
    service.apply_release.set()
    for _ in range(100):
        if service.processed_candidates:
            break
        await asyncio.sleep(0.005)
    await manager.aclose()

    stored = await repository.get_apply_job(record.job.job_id)
    assert service.processed_candidates == ['candidate-1']
    assert stored is not None
    assert stored.job.state is JobState.FAILED
    assert stored.job.error_code == 'job_interrupted'
    assert stored.result is None


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
async def test_running_user_cancel_waits_for_execution_cleanup_and_worker_stays_available() -> None:
    repository = MemoryFillActorRepository()
    service = CancellationAwareService()
    manager = FillActorJobManager(
        service=service,
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=2,
        poll_interval=0.01,
    )
    first = await manager.start_plan(['actor-a'])
    await asyncio.wait_for(service.started.wait(), timeout=1)

    result = await asyncio.wait_for(manager.cancel_plan(first.job_id), timeout=1)

    assert result.outcome is CancelJobOutcome.CANCELLED
    assert service.cancelled.is_set()
    cancelled = await repository.get_job(first.job_id)
    assert cancelled is not None
    assert cancelled.state is JobState.FAILED
    assert cancelled.error_code == JOB_CANCELLED_ERROR_CODE

    service.block = False
    second = await manager.start_plan(['actor-b'])
    completed = await wait_for_state(repository, second.job_id, {JobState.COMPLETED})
    await manager.aclose()

    assert completed.state is JobState.COMPLETED


@pytest.mark.asyncio
async def test_repeated_cancel_waits_for_one_cleanup_and_clears_progress_timer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = MemoryFillActorRepository()
    service = DirtyProgressService()
    manager = FillActorJobManager(
        service=service,
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=1,
        poll_interval=0.01,
        progress_flush_interval=60,
    )
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()
    cleanup_cancelled = asyncio.Event()
    cleanup_calls = 0

    async def blocked_abort(_task, _job) -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1
        cleanup_started.set()
        try:
            await cleanup_release.wait()
        except asyncio.CancelledError:
            cleanup_cancelled.set()
            raise
        msg = 'simulated cleanup repository failure'
        raise OSError(msg)

    monkeypatch.setattr(manager, '_abort_feed_warmup', blocked_abort)
    job = await manager.start_plan(['actor'])
    await asyncio.wait_for(service.started.wait(), timeout=1)
    first = asyncio.create_task(manager.cancel_plan(job.job_id))
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)
    second = asyncio.create_task(manager.cancel_plan(job.job_id))
    await asyncio.sleep(0.05)

    assert not first.done()
    assert not second.done()
    assert not cleanup_cancelled.is_set()
    assert cleanup_calls == 1
    assert any(task.get_name() == f'fill-actor-progress-{job.job_id}' for task in asyncio.all_tasks())

    cleanup_release.set()
    first_result, second_result = await asyncio.gather(first, second)
    await asyncio.sleep(0)
    assert {first_result.outcome, second_result.outcome} == {
        CancelJobOutcome.CANCELLED,
        CancelJobOutcome.ALREADY_CANCELLED,
    }
    assert not cleanup_cancelled.is_set()
    assert cleanup_calls == 1
    assert not any(task.get_name() == f'fill-actor-progress-{job.job_id}' for task in asyncio.all_tasks())
    assert not any(task.get_name() == f'fill-actor-cleanup-{job.job_id}' for task in asyncio.all_tasks())
    await manager.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize('failure_type', [FillActorError, RuntimeError], ids=['domain-error', 'unexpected-error'])
async def test_cancel_during_failed_plan_cleanup_waits_and_clears_progress_timer(
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[Exception],
) -> None:
    repository = MemoryFillActorRepository()
    service = FailingDirtyProgressService(failure_type)
    manager = FillActorJobManager(
        service=service,
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=1,
        poll_interval=0.01,
        progress_flush_interval=60,
    )
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()
    cleanup_cancelled = asyncio.Event()

    async def blocked_abort(_task, _job) -> None:
        cleanup_started.set()
        try:
            await cleanup_release.wait()
        except asyncio.CancelledError:
            cleanup_cancelled.set()
            raise

    monkeypatch.setattr(manager, '_abort_feed_warmup', blocked_abort)
    job = await manager.start_plan(['actor'])
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)
    assert service.started.is_set()
    assert any(task.get_name() == f'fill-actor-progress-{job.job_id}' for task in asyncio.all_tasks())

    request = asyncio.create_task(manager.cancel_plan(job.job_id))
    await asyncio.sleep(0.05)
    completed_before_release = request.done()
    cleanup_release.set()
    result = await asyncio.wait_for(request, timeout=1)
    await asyncio.sleep(0)
    progress_tasks = [task for task in asyncio.all_tasks() if task.get_name() == f'fill-actor-progress-{job.job_id}']
    await manager.aclose()
    for task in progress_tasks:
        task.cancel()
    if progress_tasks:
        await asyncio.gather(*progress_tasks, return_exceptions=True)

    cancelled = await repository.get_job(job.job_id)
    assert result.outcome is CancelJobOutcome.CANCELLED
    assert not completed_before_release
    assert not cleanup_cancelled.is_set()
    assert not progress_tasks
    assert cancelled is not None
    assert cancelled.error_code == JOB_CANCELLED_ERROR_CODE


@pytest.mark.asyncio
async def test_already_cancelled_result_still_signals_local_execution() -> None:
    repository = MemoryFillActorRepository()
    service = CancellationAwareService()
    manager = FillActorJobManager(
        service=service,
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=1,
        poll_interval=0.01,
    )
    job = await manager.start_plan(['actor'])
    await asyncio.wait_for(service.started.wait(), timeout=1)
    persisted = await repository.cancel_job(job_id=job.job_id, now=datetime.now(UTC))
    assert persisted.outcome is CancelJobOutcome.CANCELLED

    request = asyncio.create_task(manager.cancel_plan(job.job_id))
    try:
        result = await asyncio.wait_for(asyncio.shield(request), timeout=0.2)
        signalled_before_release = service.cancelled.is_set()
    finally:
        service.release.set()
        await asyncio.gather(request, return_exceptions=True)
        await manager.aclose()

    assert result.outcome is CancelJobOutcome.ALREADY_CANCELLED
    assert signalled_before_release


@pytest.mark.asyncio
async def test_cancel_in_claim_register_gap_prevents_service_execution() -> None:
    repository = PausedClaimRepository()
    service = ControlledService()
    manager = FillActorJobManager(
        service=service,
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=1,
        poll_interval=0.01,
    )
    job = await manager.start_plan(['actor'])
    try:
        await asyncio.wait_for(repository.claimed.wait(), timeout=1)

        result = await manager.cancel_plan(job.job_id)
        repository.release_claim.set()
        await asyncio.sleep(0.05)

        assert result.outcome is CancelJobOutcome.CANCELLED
        assert not service.started.is_set()
        cancelled = await repository.get_job(job.job_id)
        assert cancelled is not None
        assert cancelled.error_code == JOB_CANCELLED_ERROR_CODE
    finally:
        repository.release_claim.set()
        await manager.aclose()


@pytest.mark.asyncio
async def test_cancel_operation_survives_request_task_cancellation_and_stops_execution() -> None:
    repository = PausedCancelRepository()
    service = CancellationAwareService()
    manager = FillActorJobManager(
        service=service,
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=1,
        poll_interval=0.01,
    )
    job = await manager.start_plan(['actor'])
    await asyncio.wait_for(service.started.wait(), timeout=1)
    request = asyncio.create_task(manager.cancel_plan(job.job_id))
    await asyncio.wait_for(repository.cancel_persisted.wait(), timeout=1)

    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    repository.release_cancel.set()
    await asyncio.wait_for(service.cancelled.wait(), timeout=1)
    for _ in range(100):
        if not manager._cancel_operations:  # noqa: SLF001
            break
        await asyncio.sleep(0.005)
    await manager.aclose()

    cancelled = await repository.get_job(job.job_id)
    assert cancelled is not None
    assert cancelled.error_code == JOB_CANCELLED_ERROR_CODE
    assert not manager._cancel_operations  # noqa: SLF001


@pytest.mark.asyncio
async def test_heartbeat_stops_execution_after_external_cancellation() -> None:
    repository = MemoryFillActorRepository()
    service = CancellationAwareService()
    manager = FillActorJobManager(
        service=service,
        repository=repository,
        max_concurrent_jobs=1,
        max_active_jobs=1,
        lease_duration=timedelta(milliseconds=150),
        poll_interval=0.01,
    )
    job = await manager.start_plan(['actor'])
    await asyncio.wait_for(service.started.wait(), timeout=1)

    result = await repository.cancel_job(job_id=job.job_id, now=datetime.now(UTC))
    assert result.outcome is CancelJobOutcome.CANCELLED
    await asyncio.wait_for(service.cancelled.wait(), timeout=1)
    await manager.aclose()

    cancelled = await repository.get_job(job.job_id)
    assert cancelled is not None
    assert cancelled.error_code == JOB_CANCELLED_ERROR_CODE


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
