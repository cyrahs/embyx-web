import asyncio
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from embyx_web.fill_actor.models import (
    ActorPlan,
    FillActorPlan,
    MoveCandidate,
    MoveResult,
    MoveState,
    VideoPlan,
    VideoState,
)
from embyx_web.fill_actor.persistence import (
    CandidateRecord,
    FileFingerprint,
    InvalidMoveJournalTransitionError,
    JobOperation,
    JobRecord,
    JobState,
    MemoryFillActorRepository,
    MoveJournalRecord,
    MoveJournalState,
    PlanRecord,
)
from embyx_web.fill_actor.sqlite_repository import (
    CURRENT_SCHEMA_VERSION,
    SQLiteFillActorRepository,
    UnsupportedSchemaVersionError,
)


def make_plan_record(tmp_path: Path, *, plan_id: str = 'plan-1') -> PlanRecord:
    created_at = datetime(2026, 7, 13, 10, 0, tzinfo=UTC)
    source_root = tmp_path / 'additional'
    destination_root = tmp_path / 'move-in'
    source_root.mkdir(exist_ok=True)
    destination_root.mkdir(exist_ok=True)
    source = source_root / 'ABC-001.mp4'
    source.write_bytes(b'video')
    destination = destination_root / source.name
    stat = source.stat()
    candidate = MoveCandidate(
        candidate_id='candidate-1',
        video_id='ABC-001',
        file_name=source.name,
        source_label='additional-1',
        destination_conflict=False,
    )
    public = FillActorPlan(
        plan_id=plan_id,
        revision='revision-1',
        created_at=created_at,
        expires_at=created_at + timedelta(hours=1),
        actors=(ActorPlan(actor_id='actor-1', scraped_count=1, video_ids=('ABC-001',)),),
        videos=(
            VideoPlan(
                video_id='ABC-001',
                actor_ids=('actor-1',),
                state=VideoState.ADDITIONAL_FOUND,
                move_candidates=(candidate,),
            ),
        ),
    )
    return PlanRecord(
        public=public,
        candidates=(
            CandidateRecord(
                candidate_id=candidate.candidate_id,
                video_id=candidate.video_id,
                source=source,
                source_root=source_root,
                destination=destination,
                fingerprint=FileFingerprint(
                    device=stat.st_dev,
                    inode=stat.st_ino,
                    size=stat.st_size,
                    mtime_ns=stat.st_mtime_ns,
                    ctime_ns=stat.st_ctime_ns,
                ),
            ),
        ),
    )


def make_repository(kind: str, tmp_path: Path):
    if kind == 'memory':
        return MemoryFillActorRepository()
    return SQLiteFillActorRepository(tmp_path / 'fill-actor.sqlite3')


def test_plan_record_rejects_private_candidate_mismatch(tmp_path: Path) -> None:
    plan = make_plan_record(tmp_path)
    with pytest.raises(ValueError, match='private candidate records must match public plan candidates'):
        PlanRecord(public=plan.public, candidates=())


@pytest.mark.asyncio
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_repository_contract_round_trips_persistent_records(
    tmp_path: Path,
    repository_kind: str,
) -> None:
    repository = make_repository(repository_kind, tmp_path)
    plan = make_plan_record(tmp_path)
    now = plan.public.created_at

    queued_job = JobRecord(
        job_id='job-1',
        operation=JobOperation.CREATE_PLAN,
        state=JobState.QUEUED,
        created_at=now,
        updated_at=now,
        plan_id=plan.public.plan_id,
    )
    await repository.save_job(queued_job)
    assert await repository.get_job(queued_job.job_id) == queued_job
    assert await repository.list_jobs((JobState.QUEUED,)) == (queued_job,)

    await repository.save_plan(plan)

    loaded_plan = await repository.get_plan(plan.public.plan_id)
    assert loaded_plan == plan
    assert await repository.get_candidate(plan.public.plan_id, 'candidate-1') == plan.candidates[0]
    assert str(plan.candidates[0].source) not in loaded_plan.public.model_dump_json()
    assert await repository.get_plan('unknown') is None

    result = MoveResult(
        candidate_id='candidate-1',
        video_id='ABC-001',
        file_name='ABC-001.mp4',
        state=MoveState.MOVED,
    )
    await repository.save_move_result(plan.public.plan_id, result)
    assert await repository.get_move_result(plan.public.plan_id, result.candidate_id) == result
    assert await repository.list_move_results(plan.public.plan_id) == (result,)

    job = JobRecord(
        job_id='job-1',
        operation=JobOperation.APPLY,
        state=JobState.RUNNING,
        created_at=now,
        updated_at=now,
        plan_id=plan.public.plan_id,
    )
    await repository.save_job(job)
    assert await repository.get_job(job.job_id) == job
    assert await repository.list_jobs((JobState.QUEUED,)) == ()
    assert await repository.list_jobs((JobState.RUNNING,)) == (job,)
    assert await repository.list_jobs() == (job,)
    assert await repository.health_check()

    journal = MoveJournalRecord(
        plan_id=plan.public.plan_id,
        candidate_id='candidate-1',
        state=MoveJournalState.PREPARED,
        updated_at=now,
    )
    await repository.save_move_journal(journal)
    assert await repository.get_move_journal(plan.public.plan_id, 'candidate-1') == journal
    assert await repository.list_unreconciled_moves() == (journal,)


@pytest.mark.asyncio
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_repository_contract_enforces_move_journal_transitions(
    tmp_path: Path,
    repository_kind: str,
) -> None:
    repository = make_repository(repository_kind, tmp_path)
    plan = make_plan_record(tmp_path)
    await repository.save_plan(plan)
    now = plan.public.created_at

    for offset, state in enumerate(
        (
            MoveJournalState.PREPARED,
            MoveJournalState.LINKED,
            MoveJournalState.SOURCE_REMOVED,
            MoveJournalState.RECONCILED,
        )
    ):
        record = MoveJournalRecord(
            plan_id=plan.public.plan_id,
            candidate_id='candidate-1',
            state=state,
            updated_at=now + timedelta(seconds=offset),
        )
        await repository.save_move_journal(record)
        assert await repository.get_move_journal(plan.public.plan_id, 'candidate-1') == record

    assert await repository.list_unreconciled_moves() == ()
    with pytest.raises(InvalidMoveJournalTransitionError):
        await repository.save_move_journal(
            MoveJournalRecord(
                plan_id=plan.public.plan_id,
                candidate_id='candidate-1',
                state=MoveJournalState.LINKED,
                updated_at=now + timedelta(seconds=5),
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_repository_contract_purges_expired_plan_dependents(
    tmp_path: Path,
    repository_kind: str,
) -> None:
    repository = make_repository(repository_kind, tmp_path)
    plan = make_plan_record(tmp_path)
    await repository.save_plan(plan)
    await repository.save_move_result(
        plan.public.plan_id,
        MoveResult(
            candidate_id='candidate-1',
            video_id='ABC-001',
            file_name='ABC-001.mp4',
            state=MoveState.MOVED,
        ),
    )
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.public.plan_id,
            candidate_id='candidate-1',
            state=MoveJournalState.PREPARED,
            updated_at=plan.public.created_at,
        )
    )
    job = JobRecord(
        job_id='job-1',
        operation=JobOperation.APPLY,
        state=JobState.COMPLETED,
        created_at=plan.public.created_at,
        updated_at=plan.public.created_at,
        plan_id=plan.public.plan_id,
    )
    await repository.save_job(job)

    assert await repository.purge_expired_plans(plan.public.expires_at) == 0
    assert await repository.get_plan(plan.public.plan_id) is not None
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.public.plan_id,
            candidate_id='candidate-1',
            state=MoveJournalState.RECONCILED,
            updated_at=plan.public.expires_at,
        )
    )
    assert await repository.purge_expired_plans(plan.public.expires_at) == 1
    assert await repository.get_plan(plan.public.plan_id) is None
    assert await repository.get_move_result(plan.public.plan_id, 'candidate-1') is None
    assert await repository.get_move_journal(plan.public.plan_id, 'candidate-1') is None
    assert await repository.get_job(job.job_id) == JobRecord(
        job_id=job.job_id,
        operation=job.operation,
        state=job.state,
        created_at=job.created_at,
        updated_at=job.updated_at,
        error_code=job.error_code,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_repository_refuses_to_delete_plan_with_unreconciled_move(
    tmp_path: Path,
    repository_kind: str,
) -> None:
    repository = make_repository(repository_kind, tmp_path)
    plan = make_plan_record(tmp_path)
    await repository.save_plan(plan)
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.public.plan_id,
            candidate_id='candidate-1',
            state=MoveJournalState.PREPARED,
            updated_at=plan.public.created_at,
        )
    )

    assert not await repository.delete_plan(plan.public.plan_id)
    assert await repository.get_plan(plan.public.plan_id) == plan
    assert len(await repository.list_unreconciled_moves()) == 1


@pytest.mark.asyncio
async def test_sqlite_repository_survives_reopen(tmp_path: Path) -> None:
    database_path = tmp_path / 'fill-actor.sqlite3'
    plan = make_plan_record(tmp_path)
    now = plan.public.created_at
    repository = SQLiteFillActorRepository(database_path)
    result = MoveResult(
        candidate_id='candidate-1',
        video_id='ABC-001',
        file_name='ABC-001.mp4',
        state=MoveState.MOVED,
    )
    job = JobRecord(
        job_id='job-1',
        operation=JobOperation.APPLY,
        state=JobState.PARTIAL_FAILED,
        created_at=now,
        updated_at=now + timedelta(seconds=1),
        plan_id=plan.public.plan_id,
        error_code='one_or_more_moves_failed',
    )
    journal = MoveJournalRecord(
        plan_id=plan.public.plan_id,
        candidate_id='candidate-1',
        state=MoveJournalState.PREPARED,
        updated_at=now,
    )
    await repository.save_plan(plan)
    await repository.save_move_result(plan.public.plan_id, result)
    await repository.save_job(job)
    await repository.save_move_journal(journal)

    reopened = SQLiteFillActorRepository(database_path)

    assert await reopened.get_plan(plan.public.plan_id) == plan
    assert await reopened.get_move_result(plan.public.plan_id, 'candidate-1') == result
    assert await reopened.get_job(job.job_id) == job
    assert await reopened.list_unreconciled_moves() == (journal,)


def test_sqlite_repository_applies_explicit_schema_migration(tmp_path: Path) -> None:
    database_path = tmp_path / 'fill-actor.sqlite3'

    SQLiteFillActorRepository(database_path)

    with sqlite3.connect(database_path) as connection:
        version = connection.execute('SELECT MAX(version) FROM schema_migrations').fetchone()[0]
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }
    assert version == CURRENT_SCHEMA_VERSION
    assert tables == {
        'schema_migrations',
        'plans',
        'candidates',
        'move_results',
        'jobs',
        'move_journal',
        'health_probe',
    }


def test_sqlite_repository_rejects_unknown_future_schema(tmp_path: Path) -> None:
    database_path = tmp_path / 'fill-actor.sqlite3'
    with sqlite3.connect(database_path) as connection:
        connection.execute('CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)')
        connection.execute(
            'INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)',
            (CURRENT_SCHEMA_VERSION + 1, datetime.now(tz=UTC).isoformat()),
        )

    with pytest.raises(UnsupportedSchemaVersionError):
        SQLiteFillActorRepository(database_path)


def test_job_states_are_stable_public_values() -> None:
    assert tuple(JobState) == (
        JobState.QUEUED,
        JobState.RUNNING,
        JobState.COMPLETED,
        JobState.PARTIAL_FAILED,
        JobState.FAILED,
    )


@pytest.mark.asyncio
async def test_sqlite_write_finishes_before_cancellation_propagates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = SQLiteFillActorRepository(tmp_path / 'fill-actor.sqlite3')
    now = datetime.now(UTC)
    job = JobRecord(
        job_id='job-1',
        operation=JobOperation.CREATE_PLAN,
        state=JobState.QUEUED,
        created_at=now,
        updated_at=now,
    )
    started = threading.Event()
    release = threading.Event()
    original_save = repository._save_job  # noqa: SLF001

    def delayed_save(record: JobRecord) -> None:
        started.set()
        assert release.wait(timeout=2)
        original_save(record)

    monkeypatch.setattr(repository, '_save_job', delayed_save)
    task = asyncio.create_task(repository.save_job(job))
    assert await asyncio.to_thread(started.wait, 2)
    task.cancel()
    await asyncio.sleep(0.02)
    assert not task.done()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert await repository.get_job(job.job_id) == job
